namespace Propulsion.CosmosStore

open Propulsion.Feed
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

type TrancheStats() =

    let mutable batches, readPos, completedPos = 0, None, None
    let mutable closed, finishedReading = false, false

    let mutable accReadLatency, recentBatches, recentRu, recentBatchTimestamp, recentCommittedPos, accWaits = TimeSpan.Zero, 0, 0., None, None, TimeSpan.Zero

    let shutdownTimer = System.Diagnostics.Stopwatch()
    let mutable isActive, lastGap = false, None

    member _.Dump(log: Serilog.ILogger, processorName, partition, struct (currentBatches, maxReadAhead)) =
        if closed then () else

        let r = Option.map string >> Option.defaultValue null
        let tail = Option.isSome readPos && readPos = completedPos
        let state = if not tail then "Busy" elif finishedReading then "End" else "Tail"
        let recentAge = recentBatchTimestamp |> Option.map (fun lbt -> DateTime.UtcNow - lbt) |> Option.toNullable
        let log = if Option.isNone readPos then log else log.ForContext("tail", tail)
        log.Information("ChangeFeed {processorName}/{partition} {state} @ {completedPosition}/{readPosition} Active {active} read {batches} ahead {cur}/{max} Gap {gap} " +
                        "| Read {l:f1}s batches {recentBatches} age {age: d\.hh\:mm\:ss} {ru}RU Pause {pausedS:f1}s Committed {comittedPos} Wait {waitS:f1}s",
                        processorName, partition, state, r completedPos, r readPos, isActive, batches, currentBatches, maxReadAhead, Option.toNullable lastGap,
                        accReadLatency.TotalSeconds, recentBatches, recentAge, recentRu, accWaits.TotalSeconds, r recentCommittedPos, shutdownTimer.ElapsedSeconds)
        accReadLatency <- TimeSpan.Zero; accWaits <- TimeSpan.Zero
        recentBatches <- 0; recentRu <- 0; recentCommittedPos <- None; recentBatchTimestamp <- None; lastGap <- None
        closed <- finishedReading

    member _.RecordActive(state) =
        isActive <- state
    member _.RecordBatch(readTime, batchTimestamp, requestCharge, batch: Propulsion.Ingestion.Batch<_>) =
        recentBatchTimestamp <- Some batchTimestamp
        accReadLatency <- accReadLatency + readTime
        readPos <- batch.epoch |> Position.parse |> Some
        batches <- batches + 1
        recentBatches <- recentBatches + 1
        recentRu <- recentRu + requestCharge
        closed <- false // Any straggler reads (and/or bugs!) trigger logging
    member _.RecordWaitForCapacity(latency) =
        accWaits <- accWaits + latency
    member _.RecordCompleted(epoch) =
        completedPos <- epoch |> Position.parse |> Some

    member _.RecordEstimatedGap(gap) =
        lastGap <- Some gap

type internal Stats(log: Serilog.ILogger, databaseId, containerId, processorName: string, interval: TimeSpan) =

    let positions = Propulsion.Feed.Core.TranchePositions()
    let trancheStats = System.Collections.Concurrent.ConcurrentDictionary<int, TrancheStats>()
    let statsFor trancheId = trancheStats.GetOrAdd(trancheId, fun _trancheId -> TrancheStats())
    let context: Log.MetricContext = { database = databaseId; container = containerId; group = processorName }
    let metricsLog = log.ForContext("isMetric", true)

    member val Log = log
    member _.Intercept(rangeId: int) = positions.Intercept(TrancheId.parse (string rangeId))

    member _.DumpStats(trancheCapacity, runEstimation, ct) = task {
        try do! runEstimation ct
        with _ -> () // Dump will cope with absence of update (unless standalone estimation has already updated anyway)
        let stats = trancheStats.ToArray()
        stats |> Array.sortInPlaceBy (fun x -> x.Key)
        for kv in stats do
            kv.Value.Dump(log, processorName, kv.Key, trancheCapacity kv.Key) }
    member x.PumpStats(trancheCapacity, runEstimation, ct) =
        Task.periodically (fun ct -> x.DumpStats(trancheCapacity, runEstimation, ct)) interval ct

    member _.RecordRead(rangeId: int, lastWait: TimeSpan, epoch, requestCharge, batchTimestamp, latency, itemCount, batch: Propulsion.Ingestion.Batch<'items>, batchesInFlight, maxReadAhead) =
        let age = DateTime.UtcNow - batchTimestamp
        let m = Log.Metric.Read { context = context; rangeId = rangeId; token = epoch; latency = latency; rc = requestCharge; age = age; docs = itemCount }
        (log |> Log.withMetric m).Information(
            "ChangeFeed {partition} {token,9} age {age:dddd\.hh\:mm\:ss} {count,4} docs {requestCharge,6:f1}RU {l,5:f1}s Wait {pausedS:f3}s Ahead {cur}/{max}",
            rangeId, epoch, age, itemCount, requestCharge, latency, batchesInFlight, maxReadAhead, lastWait.TotalSeconds, batchesInFlight, maxReadAhead)
        (statsFor rangeId).RecordBatch(latency, batchTimestamp, requestCharge, batch)
    member _.RecordWait(rangeId: int, waitElapsed, batchesInFlight, maxReadAhead) =
        if metricsLog.IsEnabled LogEventLevel.Information then
            let m = Log.Metric.Wait { context = context; rangeId = rangeId; ingestLatency = waitElapsed; ingestQueued = batchesInFlight }
            // NOTE: Write to metrics log (App wiring has logic to also emit it to Console when in verboseStore mode, but main purpose is to feed to Prometheus ASAP)
            (metricsLog |> Log.withMetric m).Information(
                "Reader {partition} Wait {pausedS:f3}s Ahead {cur}/{max}",
                rangeId, waitElapsed.TotalSeconds, batchesInFlight, maxReadAhead)
        (statsFor rangeId).RecordWaitForCapacity(waitElapsed)
    member _.RecordCompleted(rangeId: int, epoch) =
        (statsFor rangeId).RecordCompleted(epoch)
(*
    member _.UpdateCommittedPosition(pos) =
        lastCommittedPosition <- pos
        closed <- false // Any updates trigger logging

    member _.EnteringShutdown() =
        finishedReading <- true
        shutdownTimer.Start()
    member _.ShutdownCompleted(cur, max) =
        shutdownTimer.Stop()
*)

    (* Change Feed Processor Source level config dump *)

    member _.LogStart(leaseAcquireInterval: TimeSpan, leaseTtl: TimeSpan, leaseRenewInterval: TimeSpan, feedPollDelay: TimeSpan, startFromTail: bool, ?maxItems) =
        log.Information("ChangeFeed {processorName} Lease acquire {leaseAcquireIntervalS:n0}s ttl {ttlS:n0}s renew {renewS:n0}s feedPollDelay {feedPollDelayS:n0}s Items limit {maxItems} fromTail {fromTail}",
                        processorName, leaseAcquireInterval.TotalSeconds, leaseTtl.TotalSeconds, leaseRenewInterval.TotalSeconds, feedPollDelay.TotalSeconds, Option.toNullable maxItems, startFromTail)

    (* Change Feed Processor notification callbacks *)

    member _.LogStateChange(rangeId: int, acquired) =
        log.Information("ChangeFeed {processorName} Lease {state} {partition}",
                        processorName, (if acquired then "Acquired" else "Released"), rangeId)
        (statsFor rangeId).RecordActive(acquired)
    member _.LogExn(rangeId: int, ex: exn) =
        log.Error(ex, "ChangeFeed {processorName}/{partition} error", processorName, rangeId)
    member _.LogHandlerExn(rangeId: int, ex: exn) =
        log.Error(ex, "ChangeFeed {processorName}/{partition} Handler Threw", processorName, rangeId)

    (* Change Feed Estimator messages *)

    member _.ReportEstimationInterval(interval: TimeSpan) =
        log.Information("ChangeFeed {processorName} Lag stats interval {lagReportIntervalS:n0}s", processorName, interval.TotalSeconds)
    member _.ReportEstimation(remainingWork: struct (int * int64)[]) =
        let mutable synced, lagged, count, total = ResizeArray(), ResizeArray(), 0, 0L
        for rangeId, gap as partitionAndGap in remainingWork do
            total <- total + gap
            count <- count + 1
            if gap = 0L then synced.Add rangeId else lagged.Add partitionAndGap
            (statsFor rangeId).RecordEstimatedGap(gap)
        let m = Log.Metric.Lag { context = context; rangeLags = remainingWork }
        (metricsLog |> Log.withMetric m).Information(
            "ChangeFeed {processorName} Lag Partitions {partitions} Gap {gapDocs:n0} docs {@laggingPartitions} Synced {@syncedPartitions}",
            processorName, count, total, lagged, synced)

[<NoComparison>]
type ChangeFeedContext = { group: string; rangeId: int; epoch: int64; timestamp: DateTime; requestCharge: float }

#if COSMOSV3
type ChangeFeedItem = Newtonsoft.Json.Linq.JObject
module ChangeFeedItem = let timestamp = EquinoxNewtonsoftParser.timestamp
#else
type ChangeFeedItem = System.Text.Json.JsonDocument
module ChangeFeedItem = let timestamp = EquinoxSystemTextJsonParser.timestamp
#endif
type ChangeFeedItems = System.Collections.Generic.IReadOnlyCollection<ChangeFeedItem>

type internal Observer<'Items>(
    stats: Stats, trancheIngester: Propulsion.Ingestion.Ingester<'Items>, parseFeedBatch: ChangeFeedItems -> 'Items, decorate) =

    let readSw = Stopwatch.start () // we'll end up reporting the warmup/connect time on the first batch, but that's ok
    let awaitCapacitySw = System.Diagnostics.Stopwatch()

    member _.Ingest(ctx: ChangeFeedContext, docs: ChangeFeedItems, checkpoint: Func<Task>, _ct) = task {
        readSw.Stop() // Stop the clock after ChangeFeedProcessor hands off to us
        let batch: Propulsion.Ingestion.Batch<'Items> = {
            epoch = ctx.epoch; items = parseFeedBatch docs; isTail = false
            checkpoint = fun _ct -> task { do! checkpoint.Invoke () }
            onCompletion = fun () -> stats.RecordCompleted(ctx.rangeId, ctx.epoch) }
        let struct (cur, max) = decorate batch |> trancheIngester.Ingest
        stats.RecordRead(ctx.rangeId, awaitCapacitySw.Elapsed, ctx.epoch, ctx.requestCharge, ctx.timestamp, readSw.Elapsed, docs.Count, batch, cur, max)
        awaitCapacitySw.Restart()
        do! trancheIngester.AwaitCapacity()
        let struct (cur, max) = trancheIngester.CurrentCapacity()
        awaitCapacitySw.Stop()
        stats.RecordWait(int ctx.rangeId, awaitCapacitySw.Elapsed, cur, max)
        readSw.Restart() } // restart the clock as we handoff back to the ChangeFeedProcessor to fetch and pass that back to us
    member _.CurrentCapacity() = trancheIngester.CurrentCapacity()

    interface IDisposable with
        member _.Dispose() =
            trancheIngester.Stop()

type internal Observers<'Items>(stats: Stats, startIngester: Serilog.ILogger * int -> Propulsion.Ingestion.Ingester<'Items>, parseFeedBatch: ChangeFeedItems -> 'Items) =

    // Its important we don't risk >1 instance https://andrewlock.net/making-getoradd-on-concurrentdictionary-thread-safe-using-lazy/
    // while it would be safe, there would be a risk of incurring the cost of multiple initialization loops
    let forTranche = System.Collections.Concurrent.ConcurrentDictionary<int, Lazy<Observer<'Items>>>()
    let build trancheId = lazy (
         let ingester = startIngester (stats.Log, trancheId)
         let interceptBatch = stats.Intercept(trancheId)
         new Observer<'Items>(stats, ingester, parseFeedBatch, interceptBatch))
    let getOrAddForTranche trancheId = forTranche.GetOrAdd(trancheId, build).Value

    member _.CurrentTrancheCapacity(trancheId) =
        match forTranche.TryGetValue trancheId with
        | true, obs -> obs.Value.CurrentCapacity()
        | false, _ -> 0, 0
    member _.Ingest(context, docs, checkpoint, ct) =
        let trancheObserver = getOrAddForTranche context.rangeId
        trancheObserver.Ingest(context, docs, checkpoint, ct)

    interface IDisposable with
        member _.Dispose() =
            for x in forTranche.Values do
                (x.Value : IDisposable).Dispose()
