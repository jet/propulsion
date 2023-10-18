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

    let renderPos = string
    let mutable batchLastPosition = Position.parse -1L
    let mutable closed, finishedReading = false, false

    let mutable batches = 0
    // let pagesEmpty, events = 0, 0L
    let mutable readLatency, recentBatches, recentRu = TimeSpan.Zero, 0, 0.
    // let mutable recentEvents, recentPagesEmpty = 0, 0

    let mutable lastBatchTimestamp = DateTime.MinValue
    let mutable waitElapsed, shutdownTimer, currentBatches, maxReadAhead = TimeSpan.Zero, System.Diagnostics.Stopwatch(), 0, 0
    let updateIngesterState cur max =
        currentBatches <- cur
        maxReadAhead <- max

    let mutable isActive, lastGap, lastCommittedPosition = false, -1L, Position.parse -1L

    member _.Dump(processorName, partition, log: Serilog.ILogger) =
        if closed then () else

        // let p pos = match pos with p when p = Position.parse -1L -> Nullable() | x -> Nullable x
        let readS, postS = readLatency.TotalSeconds, waitElapsed.TotalSeconds
        let inline r pos = match pos with p when p = Position.parse -1L -> null | x -> renderPos x
        let state = if lastGap <> 0 then "Busy"
                    elif lastCommittedPosition = batchLastPosition then "COMPLETE"
                    else if finishedReading then "End" else "Tail"
        let lastAge = if lastBatchTimestamp = DateTime.MinValue then Nullable() else Nullable (DateTime.UtcNow - lastBatchTimestamp)
        let gap = match lastGap with -1L -> Nullable() | g -> Nullable g
        log.ForContext("tail", (lastGap = 0L)).Information(
            "Reader {processorName}/{partition} {state} @ {lastCommittedPosition}/{readPosition} Active {active} Batches {batchesRead} ahead {cur}/{max} Items gap {gap} | Recent {l:f1}s batches {recentPagesRead} age {age:dddd\.hh\:mm\:ss} {ru}RU Pause {pausedS:f1}s Wait {waitS:f1}s",
            processorName, partition, state, r lastCommittedPosition, r batchLastPosition, isActive, batches, currentBatches, maxReadAhead, gap, readS, recentBatches, lastAge, recentRu, (*pagesEmpty, events,*) postS, shutdownTimer.ElapsedSeconds)
        readLatency <- TimeSpan.Zero; waitElapsed <- TimeSpan.Zero
        recentBatches <- 0; recentRu <- 0 //; recentEvents <- 0; recentPagesEmpty <- 0
        closed <- finishedReading

    member _.RecordWaitForCapacity(latency, cur, max) =
        updateIngesterState cur max
        waitElapsed <- waitElapsed + latency

    member _.RecordBatch(readTime, batchTimestamp, requestCharge, batch: Propulsion.Ingestion.Batch<_>, cur, max) =
        updateIngesterState cur max
        lastBatchTimestamp <- batchTimestamp
        readLatency <- readLatency + readTime
        batchLastPosition <- Position.parse batch.epoch
        // lastWasTail <- batch.isTail
        // match Array.length batch.items with
        // | 0 ->  pagesEmpty <- pagesEmpty + 1
        //         recentPagesEmpty <- recentPagesEmpty + 1
        batches <- batches + 1
        // recentEvents <- recentEvents + c
        recentBatches <- recentBatches + 1
        recentRu <- recentRu + requestCharge
        closed <- false // Any straggler reads (and/or bugs!) trigger logging

    member _.RecordEstimatedGap(gap) =
        lastGap <- gap
    member _.RecordActive(state) =
        isActive <- state

type internal Stats(log: Serilog.ILogger, databaseId, containerId, processorName: string, interval: TimeSpan) =

    let positions = Propulsion.Feed.Core.TranchePositions()
    let trancheStats = System.Collections.Concurrent.ConcurrentDictionary<int, TrancheStats>()
    let statsFor trancheId = trancheStats.GetOrAdd( trancheId, fun trancheId -> TrancheStats())
    let context: Log.MetricContext = { database = databaseId; container = containerId; group = processorName }
    let metricsLog = log.ForContext("isMetric", true)

    member val Log = log
    member _.Intercept(rangeId: int) = positions.Intercept(TrancheId.parse (string rangeId))
    // member _.TranchePositions = positions.Current()

    member _.DumpStats _ct =
        let stats = trancheStats.ToArray()
        stats |> Array.sortInPlaceBy (fun x -> x.Key)
        for kv in stats do
            kv.Value.Dump(processorName, kv.Key, log)
        Task.CompletedTask
    member x.PumpStats(ct) = Task.periodically x.DumpStats interval ct
    member _.RecordRead(rangeId: int, lastWait: TimeSpan, epoch, requestCharge, batchTimestamp, latency, itemCount, batch: Propulsion.Ingestion.Batch<_>, batchesInFlight, maxReadAhead) =
        let age = DateTime.UtcNow - batchTimestamp
        let m = Log.Metric.Read { context = context; rangeId = rangeId; token = epoch; latency = latency; rc = requestCharge; age = age; docs = itemCount }
        (log |> Log.withMetric m).Information(
            "ChangeFeed {partition} {token,9} age {age:dddd\.hh\:mm\:ss} {count,4} docs {requestCharge,6:f1}RU {l,5:f1}s Wait {pausedS:f3}s Ahead {cur}/{max}",
            rangeId, epoch, age, itemCount, requestCharge, latency, batchesInFlight, maxReadAhead, lastWait.TotalSeconds, batchesInFlight, maxReadAhead)
        (statsFor rangeId).RecordBatch(latency, batchTimestamp, requestCharge, batch, batchesInFlight, maxReadAhead)
    member _.RecordWait(rangeId: int, waitElapsed, batchesInFlight, maxReadAhead) =
        if metricsLog.IsEnabled LogEventLevel.Information then
            let m = Log.Metric.Wait { context = context; rangeId = rangeId; ingestLatency = waitElapsed; ingestQueued = batchesInFlight }
            // NOTE: Write to metrics log (App wiring has logic to also emit it to Console when in verboseStore mode, but main purpose is to feed to Prometheus ASAP)
            (metricsLog |> Log.withMetric m).Information(
                "Reader {partition} Wait {pausedS:f3}s Ahead {cur}/{max}",
                rangeId, waitElapsed.TotalSeconds, batchesInFlight, maxReadAhead)
        (statsFor rangeId).RecordWaitForCapacity(waitElapsed, batchesInFlight, maxReadAhead)

(*
    member _.UpdateCommittedPosition(pos) =
        lastCommittedPosition <- pos
        closed <- false // Any updates trigger logging

    member _.EnteringShutdown() =
        finishedReading <- true
        shutdownTimer.Start()
    member _.ShutdownCompleted(cur, max) =
        updateIngesterState cur max
        shutdownTimer.Stop()
*)

    (* Change Feed Processor Source-level messages *)

    member _.LogStart(leaseAcquireInterval: TimeSpan, leaseTtl: TimeSpan, leaseRenewInterval: TimeSpan, feedPollDelay: TimeSpan, startFromTail: bool, ?maxItems) =
        log.Information("ChangeFeed {processorName} Lease acquire {leaseAcquireIntervalS:n0}s ttl {ttlS:n0}s renew {renewS:n0}s feedPollDelay {feedPollDelayS:n0}s Items limit {maxItems} fromTail {fromTail}",
                        processorName, leaseAcquireInterval.TotalSeconds, leaseTtl.TotalSeconds, leaseRenewInterval.TotalSeconds, feedPollDelay.TotalSeconds, Option.toNullable maxItems, startFromTail)
    member _.LogStateChange(rangeId: int, acquired) =
        log.Information("ChangeFeed {processorName}/{partition} {state}", processorName, rangeId, if acquired then "Acquired" else "Released")
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
    stats: Stats, trancheIngester: Propulsion.Ingestion.Ingester<'Items>, mapContent: ChangeFeedItems -> 'Items, ingest) =

    let sw = Stopwatch.start () // we'll end up reporting the warmup/connect time on the first batch, but that's ok
    let lastWait = System.Diagnostics.Stopwatch()

    member _.Ingest(ctx: ChangeFeedContext, docs: ChangeFeedItems, checkpoint: Func<Task>, _ct) = task {
        sw.Stop() // Stop the clock after ChangeFeedProcessor hands off to us
        let checkpoint _ct = task { do! checkpoint.Invoke () }
        let batch: Propulsion.Ingestion.Batch<'Items> = { epoch = ctx.epoch; items = mapContent docs; isTail = false; checkpoint = checkpoint; onCompletion = ignore }
        let struct (cur, max) = ingest batch // NOTE not raw trancheIngester.Ingest - needs to be intercepted to capture positions
        stats.RecordRead(ctx.rangeId, lastWait.Elapsed, ctx.epoch, ctx.requestCharge, ctx.timestamp, sw.Elapsed, docs.Count, batch, cur, max)
        lastWait.Restart()
        let! struct (cur, max) = trancheIngester.AwaitCapacity()
        lastWait.Stop()
        stats.RecordWait(int ctx.rangeId, lastWait.Elapsed, cur, max)
        sw.Restart() } // restart the clock as we handoff back to the ChangeFeedProcessor to fetch and pass that back to us

    interface IDisposable with
        member _.Dispose() =
            trancheIngester.Stop()

type internal Observers<'Items>(stats: Stats, startIngester: Serilog.ILogger * int -> Propulsion.Ingestion.Ingester<'Items>, mapContent: ChangeFeedItems -> 'Items) =

    // Its important we don't risk >1 instance https://andrewlock.net/making-getoradd-on-concurrentdictionary-thread-safe-using-lazy/
    // while it would be safe, there would be a risk of incurring the cost of multiple initialization loops
    let forTranche = System.Collections.Concurrent.ConcurrentDictionary<int, Lazy<Observer<'Items>>>()
    let build trancheId = lazy (
         let ingester = startIngester (stats.Log, trancheId)
         let ingest = stats.Intercept(trancheId) >> ingester.Ingest
         new Observer<'Items>(stats, ingester, mapContent, ingest))
    let getOrAddForTranche trancheId = forTranche.GetOrAdd(trancheId, build).Value

    member _.Ingest(context, docs, checkpoint, ct) =
        let trancheObserver = getOrAddForTranche context.rangeId
        trancheObserver.Ingest(context, docs, checkpoint, ct)

    interface IDisposable with
        member _.Dispose() =
            for x in forTranche.Values do
                (x.Value : IDisposable).Dispose()
