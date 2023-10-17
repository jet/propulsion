namespace Propulsion.CosmosStore

open Propulsion.Internal
open System

module Log =

    type [<Struct>] MetricContext = { database: string; container: string; group: string }
    type ReadMetric =       { context: MetricContext; rangeId: int; token: int64; latency: TimeSpan; rc: float; age: TimeSpan; docs: int }
    type IngestMetric =     { context: MetricContext; rangeId: int; ingestLatency: TimeSpan; ingestQueued: int }
    type LagMetric =        { context: MetricContext; rangeLags: struct (int * int64)[] }
    [<RequireQualifiedAccess; NoEquality; NoComparison>]
    type Metric =
        | Read of ReadMetric
        | Wait of IngestMetric
        | Lag of LagMetric

    let [<Literal>] PropertyTag = "propulsionCosmosEvent"
    /// Attach a property to the captured event record to hold the metric information
    let internal withMetric (value: Metric) = Log.withScalarProperty PropertyTag value
    let [<return: Struct>] (|MetricEvent|_|) (logEvent: Serilog.Events.LogEvent): Metric voption =
        let mutable p = Unchecked.defaultof<_>
        logEvent.Properties.TryGetValue(PropertyTag, &p) |> ignore
        match p with Log.ScalarValue (:? Metric as e) -> ValueSome e | _ -> ValueNone

type internal Stats(log: Serilog.ILogger, databaseId, containerId, processorName: string, interval: TimeSpan) =
    let context: Log.MetricContext = { database = databaseId; container = containerId; group = processorName }
    let metricsLog = log.ForContext("isMetric", true)
    member _.DumpStats _ct =
        Task.CompletedTask
    member x.PumpStats(ct) = Task.periodically x.DumpStats interval ct
    member _.RecordRead(rangeId: int, lastWait: TimeSpan, epoch, requestCharge, batchTimestamp, latency, itemCount, batchesInFlight, maxReadAhead) =
        let age = DateTime.UtcNow - batchTimestamp
        let m = Log.Metric.Read { context = context; rangeId = rangeId; token = epoch; latency = latency; rc = requestCharge; age = age; docs = itemCount }
        (log |> Log.withMetric m).Information("Reader {partition} {token,9} age {age:dddd\.hh\:mm\:ss} {count,4} docs {requestCharge,6:f1}RU {l,5:f1}s Wait {pausedS:f3}s Ahead {cur}/{max}",
                                              rangeId, epoch, age, itemCount, requestCharge, latency, batchesInFlight, maxReadAhead, lastWait.TotalSeconds, batchesInFlight, maxReadAhead)
    member _.RecordWait(rangeId: int, waitElapsed, batchesInFlight, maxReadAhead) =
        if metricsLog.IsEnabled LogEventLevel.Information then
            let m = Log.Metric.Wait { context = context; rangeId = rangeId; ingestLatency = waitElapsed; ingestQueued = batchesInFlight }
            // NOTE: Write to metrics log (App wiring has logic to also emit it to Console when in verboseStore mode, but main purpose is to feed to Prometheus ASAP)
            (metricsLog |> Log.withMetric m).Information("Reader {partition} Wait {pausedS:f3}s Ahead {cur}/{max}", rangeId, waitElapsed.TotalSeconds, batchesInFlight, maxReadAhead)

    (* Generic/ingester messages etc *)

    member val Log = log

    (* Change Feed Processor Source-level messages *)

    member _.LogStart(leaseAcquireInterval: TimeSpan, leaseTtl: TimeSpan, leaseRenewInterval: TimeSpan, feedPollDelay: TimeSpan, startFromTail: bool, ?maxItems) =
        log.Information("ChangeFeed {processorName} Lease acquire {leaseAcquireIntervalS:n0}s ttl {ttlS:n0}s renew {renewS:n0}s feedPollDelay {feedPollDelayS:n0}s Items limit {maxItems} fromTail {fromTail}",
                        processorName, leaseAcquireInterval.TotalSeconds, leaseTtl.TotalSeconds, leaseRenewInterval.TotalSeconds, feedPollDelay.TotalSeconds, Option.toNullable maxItems, startFromTail)
    member _.LogStateChange(rangeId: int, state: string) =
        log.Information("ChangeFeed {processorName}/{partition} {state}", processorName, rangeId, state)
    member _.LogExn(rangeId: int, ex: exn) =
        log.Error(ex, "ChangeFeed {processorName}/{partition} error", processorName, rangeId)
    member _.LogHandlerExn(rangeId: int, ex: exn) =
        log.Error(ex, "ChangeFeed {processorName}/{partition} Handler Threw", processorName, rangeId)

    (* Change Feed Estimator messages *)

    member _.ReportEstimationInterval(interval: TimeSpan) =
        log.Information("ChangeFeed {processorName} Lag stats interval {lagReportIntervalS:n0}s", processorName, interval.TotalSeconds)
    member _.ReportEstimation(remainingWork: struct (int * int64)[]) =
        let mutable synced, lagged, count, total = ResizeArray(), ResizeArray(), 0, 0L
        for partitionId, gap as partitionAndGap in remainingWork do
            total <- total + gap
            count <- count + 1
            if gap = 0L then synced.Add partitionId else lagged.Add partitionAndGap
        let m = Log.Metric.Lag { context = context; rangeLags = remainingWork }
        (log |> Log.withMetric m).Information("ChangeFeed {processorName} Lag Partitions {partitions} Gap {gapDocs:n0} docs {@laggingPartitions} Synced {@syncedPartitions}",
            processorName, count, total, lagged, synced)

[<NoComparison>]
type ChangeFeedContext = { group: string; epoch: int64; timestamp: DateTime; rangeId: int; requestCharge: float }

#if COSMOSV3
type ChangeFeedItem = Newtonsoft.Json.Linq.JObject
module ChangeFeedItem = let timestamp = EquinoxNewtonsoftParser.timestamp
#else
type ChangeFeedItem = System.Text.Json.JsonDocument
module ChangeFeedItem = let timestamp = EquinoxSystemTextJsonParser.timestamp
#endif
type ChangeFeedItems = System.Collections.Generic.IReadOnlyCollection<ChangeFeedItem>

type internal Observer<'Items>(stats: Stats, trancheIngester: Propulsion.Ingestion.Ingester<'Items>, mapContent: ChangeFeedItems -> 'Items) =

    let sw = Stopwatch.start () // we'll end up reporting the warmup/connect time on the first batch, but that's ok
    let lastWait = System.Diagnostics.Stopwatch()

    member _.Ingest(ctx: ChangeFeedContext, docs: ChangeFeedItems, checkpoint: Func<Task>, _ct) = task {
        sw.Stop() // Stop the clock after ChangeFeedProcessor hands off to us
        let checkpoint _ct = task { do! checkpoint.Invoke () }
        let batch: Propulsion.Ingestion.Batch<_> = { epoch = ctx.epoch; checkpoint = checkpoint; items = mapContent docs; onCompletion = ignore; isTail = false }
        let struct (cur, max) = trancheIngester.Ingest batch
        stats.RecordRead(int ctx.rangeId, lastWait.Elapsed, ctx.epoch, ctx.requestCharge, ctx.timestamp, sw.Elapsed, docs.Count, cur, max)
        lastWait.Restart()
        let! struct (cur, max) = trancheIngester.AwaitCapacity()
        lastWait.Stop()
        stats.RecordWait(int ctx.rangeId, lastWait.Elapsed, cur, max)
        sw.Restart() } // restart the clock as we handoff back to the ChangeFeedProcessor to fetch and pass that back to us

    interface IDisposable with
        member _.Dispose() =
            trancheIngester.Stop()

type internal Observers<'Items>(stats, startIngester: Serilog.ILogger * int -> Propulsion.Ingestion.Ingester<'Items>, mapContent: ChangeFeedItems -> 'Items) =

    // Its important we don't risk >1 instance https://andrewlock.net/making-getoradd-on-concurrentdictionary-thread-safe-using-lazy/
    // while it would be safe, there would be a risk of incurring the cost of multiple initialization loops
    let forTranche = System.Collections.Concurrent.ConcurrentDictionary<int, Lazy<Observer<_>>>()
    let build trancheId = lazy (new Observer<'Items>(stats, startIngester (stats.Log, trancheId), mapContent))
    let getOrAddForTranche trancheId = forTranche.GetOrAdd(trancheId, build).Value

    member _.Ingest(context, docs, checkpoint, ct) =
        let trancheObserver = getOrAddForTranche context.rangeId
        trancheObserver.Ingest(context, docs, checkpoint, ct)

    interface IDisposable with
        member _.Dispose() =
            for x in forTranche.Values do
                (x.Value : IDisposable).Dispose()
