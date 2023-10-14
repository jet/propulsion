namespace Propulsion.CosmosStore

open Microsoft.Azure.Cosmos
open Propulsion.Internal
open Serilog
open System
open System.Collections.Generic

module Log =

    type ReadMetric =
        {   database: string; container: string; group: string; rangeId: int
            token: int64; latency: TimeSpan; rc: float; age: TimeSpan; docs: int
            ingestLatency: TimeSpan; ingestQueued: int }
    type LagMetric =
        {   database: string; container: string; group: string
            rangeLags: struct (int * int64)[] }
    [<RequireQualifiedAccess; NoEquality; NoComparison>]
    type Metric =
        | Read of ReadMetric
        | Lag of LagMetric

    let [<Literal>] PropertyTag = "propulsionCosmosEvent"
    /// Attach a property to the captured event record to hold the metric information
    let internal withMetric (value: Metric) = Log.withScalarProperty PropertyTag value
    let [<return: Struct>] (|MetricEvent|_|) (logEvent: Serilog.Events.LogEvent): Metric voption =
        let mutable p = Unchecked.defaultof<_>
        logEvent.Properties.TryGetValue(PropertyTag, &p) |> ignore
        match p with Log.ScalarValue (:? Metric as e) -> ValueSome e | _ -> ValueNone

module internal CosmosStoreSource =

    let createTrancheObserver<'Items>
        (   log: ILogger,
            trancheIngester: Propulsion.Ingestion.Ingester<'Items>,
            mapContent: IReadOnlyCollection<_> -> 'Items): IChangeFeedObserver =

        let sw = Stopwatch.start () // we'll end up reporting the warmup/connect time on the first batch, but that's ok
        let ingest (ctx: ChangeFeedObserverContext) (checkpoint, docs: IReadOnlyCollection<_>, _ct) = task {
            sw.Stop() // Stop the clock after ChangeFeedProcessor hands off to us
            let readElapsed, age = sw.Elapsed, DateTime.UtcNow - ctx.timestamp
            let pt = Stopwatch.start ()
            let! struct (cur, max) = trancheIngester.Ingest { epoch = ctx.epoch; checkpoint = checkpoint; items = mapContent docs; onCompletion = ignore; isTail = false }
            let m = Log.Metric.Read {
                database = ctx.source.Database.Id; container = ctx.source.Id; group = ctx.group; rangeId = int ctx.rangeId
                token = ctx.epoch; latency = readElapsed; rc = ctx.requestCharge; age = age; docs = docs.Count
                ingestLatency = pt.Elapsed; ingestQueued = cur }
            (log |> Log.withMetric m).Information("Reader {partition} {token,9} age {age:dddd\.hh\:mm\:ss} {count,4} docs {requestCharge,6:f1}RU {l,5:f1}s Wait {pausedS:f3}s Ahead {cur}/{max}",
                                                  ctx.rangeId, ctx.epoch, age, docs.Count, ctx.requestCharge, readElapsed.TotalSeconds, pt.ElapsedSeconds, cur, max)
            sw.Restart() } // restart the clock as we handoff back to the ChangeFeedProcessor

        { new IChangeFeedObserver with
#if COSMOSV3
            member _.Ingest(context, checkpoint, docs, ct) = ingest context (checkpoint, docs, ct)
#else
            member _.Ingest(context, checkpoint, docs, ct) = ingest context (checkpoint, docs, ct)
#endif
          interface IDisposable with
            member _.Dispose() = trancheIngester.Stop() }

type internal CosmosStoreObserver<'Items>
    (   log: ILogger,
        startIngester: ILogger * int -> Propulsion.Ingestion.Ingester<'Items>,
        mapContent: IReadOnlyCollection<_> -> 'Items) =

    // Its important we don't risk >1 instance https://andrewlock.net/making-getoradd-on-concurrentdictionary-thread-safe-using-lazy/
    // while it would be safe, there would be a risk of incurring the cost of multiple initialization loops
    let forTranche = System.Collections.Concurrent.ConcurrentDictionary<int, Lazy<IChangeFeedObserver>>()
    let dispose () = for x in forTranche.Values do x.Value.Dispose()
    let build trancheId = lazy CosmosStoreSource.createTrancheObserver (log, startIngester (log, trancheId), mapContent)
    let forTranche trancheId = forTranche.GetOrAdd(trancheId, build).Value
    let ingest context (checkpoint, docs, ct) =
        let trancheObserver = forTranche context.rangeId
        trancheObserver.Ingest(context, checkpoint, docs, ct)

    interface IChangeFeedObserver with
        member _.Ingest(context, checkpoint, docs, ct) = ingest context (checkpoint, docs, ct)
    interface IDisposable with
        member _.Dispose() = dispose ()

type CosmosStoreSource
    (   log: ILogger,
        monitored: Container, leases: Container, processorName, parseFeedDoc, sink: Propulsion.Sinks.Sink,
        [<O; D null>] ?maxItems,
        [<O; D null>] ?tailSleepInterval,
        [<O; D null>] ?startFromTail,
        [<O; D null>] ?lagReportFreq: TimeSpan,
        [<O; D null>] ?notifyError,
        [<O; D null>] ?customize) =
    let observer = new CosmosStoreObserver<_>(Log.Logger, sink.StartIngester, Seq.collect parseFeedDoc)
    member _.Flush() = (observer: IDisposable).Dispose()
    member _.Start() =
        let databaseId, containerId = monitored.Database.Id, monitored.Id
        let logLag (interval: TimeSpan) (remainingWork: struct (int * int64)[]) = task {
            let mutable synced, lagged, count, total = ResizeArray(), ResizeArray(), 0, 0L
            for partitionId, gap as partitionAndGap in remainingWork do
                total <- total + gap
                count <- count + 1
                if gap = 0L then synced.Add partitionId else lagged.Add partitionAndGap
            let m = Log.Metric.Lag { database = databaseId; container = containerId; group = processorName; rangeLags = remainingWork }
            (log |> Log.withMetric m).Information("ChangeFeed {processorName} Lag Partitions {partitions} Gap {gapDocs:n0} docs {@laggingPartitions} Synced {@syncedPartitions}",
                                                  processorName, count, total, lagged, synced)
            return! Async.Sleep(TimeSpan.toMs interval) }
        let maybeLogLag = lagReportFreq |> Option.map logLag
        let pipeline =
            ChangeFeedProcessor.Start
              ( log, monitored, leases, processorName, observer, ?notifyError = notifyError, ?customize = customize,
                ?maxItems = maxItems, ?feedPollDelay = tailSleepInterval, ?reportLagAndAwaitNextEstimation = maybeLogLag,
                startFromTail = defaultArg startFromTail false,
                leaseAcquireInterval = TimeSpan.FromSeconds 5., leaseRenewInterval = TimeSpan.FromSeconds 5., leaseTtl = TimeSpan.FromSeconds 10.)
        lagReportFreq |> Option.iter (fun s -> log.Information("ChangeFeed {processorName} Lag stats interval {lagReportIntervalS:n0}s", processorName, s.TotalSeconds))
        pipeline
