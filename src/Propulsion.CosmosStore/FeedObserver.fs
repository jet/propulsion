namespace Propulsion.CosmosStore

open Propulsion.Feed
open Propulsion.Internal
open System

module Log =

    type [<Struct>] MetricContext = { database: string; container: string; group: string }
    type ReadMetric =       { context: MetricContext; rangeId: int; token: int64; latency: TimeSpan; rc: float; age: TimeSpan; docs: int }
    type WaitMetric =       { context: MetricContext; rangeId: int; waits: TimeSpan; activeBatches: int }
    type LagMetric =        { context: MetricContext; rangeLags: struct (int * int64)[] }
    [<RequireQualifiedAccess; NoEquality; NoComparison>]
    type Metric =
        | Read of ReadMetric
        | Wait of WaitMetric
        | Lag of LagMetric

    let [<Literal>] PropertyTag = "propulsionCosmosEvent"
    /// Attach a property to the captured event record to hold the metric information
    let internal withMetric (value: Metric) = Log.withScalarProperty PropertyTag value
    let [<return: Struct>] (|MetricEvent|_|) (logEvent: Serilog.Events.LogEvent): Metric voption =
        match logEvent.Properties.TryGetValue PropertyTag with true, Log.ScalarValue (:? Metric as e) -> ValueSome e | _ -> ValueNone

[<NoComparison; NoEquality>]
type ChangeFeedContext = { group: string; rangeId: int; epoch: int64; timestamp: DateTime; requestCharge: float }

#if COSMOSV3
type ChangeFeedItem = Newtonsoft.Json.Linq.JObject
module ChangeFeedItem = let timestamp = EquinoxNewtonsoftParser.timestamp
#else
type ChangeFeedItem = System.Text.Json.JsonDocument
module ChangeFeedItem = let timestamp = EquinoxSystemTextJsonParser.timestamp
#endif
type ChangeFeedItems = System.Collections.Generic.IReadOnlyCollection<ChangeFeedItem>

type internal Stats(log: Serilog.ILogger, context: Log.MetricContext) =
    let metricsLog = log.ForContext("isMetric", true)
    new(log, databaseId, containerId, processorName: string) =
        let context: Log.MetricContext = { database = databaseId; container = containerId; group = processorName }
        Stats(log, context)

    member _.ReportRead(rangeId: int, lastWait: TimeSpan, epoch, requestCharge, batchTimestamp, latency, itemCount, batchesInFlight, maxReadAhead) =
        let age = DateTime.UtcNow - batchTimestamp
        let m = Log.Metric.Read { context = context; rangeId = rangeId; token = epoch; latency = latency; rc = requestCharge; age = age; docs = itemCount }
        (log |> Log.withMetric m).Information(
            "ChangeFeed {partition} {token,9} age {age:dddd\.hh\:mm\:ss} {count,4} docs {requestCharge,6:f1}RU {l,5:f1}s Wait {pausedS:f3}s Ahead {cur}/{max}",
            rangeId, epoch, age, itemCount, requestCharge, latency, batchesInFlight, maxReadAhead, lastWait.TotalSeconds, batchesInFlight, maxReadAhead)
    member _.ReportWait(rangeId: int, waitElapsed, batchesInFlight, maxReadAhead) =
        if metricsLog.IsEnabled LogEventLevel.Information then
            let m = Log.Metric.Wait { context = context; rangeId = rangeId; waits = waitElapsed; activeBatches = batchesInFlight }
            // NOTE: Write to metrics log (App wiring has logic to also emit it to Console when in verboseStore mode, but main purpose is to feed to Prometheus ASAP)
            (metricsLog |> Log.withMetric m).Information(
                "Reader {partition} Wait {pausedS:f3}s Ahead {cur}/{max}",
                rangeId, waitElapsed.TotalSeconds, batchesInFlight, maxReadAhead)
    member _.ReportEstimationInterval(interval: TimeSpan) =
        log.Information("ChangeFeed {processorName} Lag stats interval {lagReportIntervalS:n0}s",
                        context.group, interval.TotalSeconds)
    member _.ReportEstimation(remainingWork) =
        let mutable synced, lagged, count, total = ResizeArray(), ResizeArray(), 0, 0L
        for struct (rangeId, gap) as partitionAndGap in remainingWork do
            total <- total + gap
            count <- count + 1
            if gap = 0L then synced.Add rangeId else lagged.Add partitionAndGap
        let m = Log.Metric.Lag { context = context; rangeLags = remainingWork }
        (metricsLog |> Log.withMetric m).Information(
            "ChangeFeed {processorName} Lag Partitions {partitions} Gap {gapDocs:n0} docs {@laggingPartitions} Synced {@syncedPartitions}",
            context.group, count, total, lagged, synced)

type internal TrancheStats() as this =
    inherit Core.TranchePosition()
    let mutable batches = 0
    let mutable closed, finishedReading = false, false

    let mutable accReadLat, recentBatches, recentRu, recentBatchTimestamp, recentCommittedPos, accWaits = TimeSpan.Zero, 0, 0., None, ValueNone, TimeSpan.Zero

    let shutdownTimer = System.Diagnostics.Stopwatch()
    let mutable isActive, lastGap = false, None

    member x.Dump(log: Serilog.ILogger, processorName: string, partition: int, struct (currentBatches: int, maxReadAhead: int)) =
        if closed then () else

        let r = ValueOption.map string >> ValueOption.defaultValue null
        let p : Core.ITranchePosition = x
        let state = if not p.IsTail then "Busy" elif finishedReading then "End" else "Tail"
        let recentAge = recentBatchTimestamp |> Option.map (fun lbt -> DateTime.UtcNow - lbt) |> Option.toNullable
        log.ForContext("tail", p.IsTail).Information(
            "ChangeFeed {processorName}/{partition} {state} @ {completedPosition}/{readPosition} Active {active} read {batches} ahead {cur}/{max} Gap {gap} " +
            "| Read {l:f1}s batches {recentBatches} age {age:d\.hh\:mm\:ss} {ru:n0}RU Pause {pausedS:f1}s Committed {comittedPos} Wait {waitS:f1}s",
            processorName, partition, state, r p.CompletedPos, r p.ReadPos, isActive, batches, currentBatches, maxReadAhead, Option.toNullable lastGap,
            accReadLat.TotalSeconds, recentBatches, recentAge, recentRu, accWaits.TotalSeconds, r recentCommittedPos, shutdownTimer.ElapsedSeconds)
        accReadLat <- TimeSpan.Zero; accWaits <- TimeSpan.Zero
        recentBatches <- 0; recentRu <- 0; recentCommittedPos <- ValueNone; recentBatchTimestamp <- None; lastGap <- None
        closed <- finishedReading

    member _.RecordActive(state) =
        isActive <- state
        finishedReading <- not state
    member _.RecordBatch(batchLat, batchTimestamp, batchRu, batchReadPos) =
        recentBatchTimestamp <- Some batchTimestamp
        accReadLat <- accReadLat + batchLat
        this.read <- ValueSome batchReadPos
        batches <- batches + 1
        recentBatches <- recentBatches + 1
        recentRu <- recentRu + batchRu
        closed <- false // Any straggler reads (and/or bugs!) trigger logging
    member _.RecordWaitForCapacity(latency) =
        accWaits <- accWaits + latency
    member _.RecordCommitted(epoch) =
        recentCommittedPos <- epoch |> Position.parse |> ValueSome
        closed <- false
    member _.RecordCompleted(epoch) =
        this.completed <- epoch |> Position.parse |> ValueSome
        closed <- false

    member x.RecordEstimatedGap(gap) =
        lastGap <- Some gap
        x.isTail <- gap = 0L
        closed <- false

type internal Observer<'Items>(stats: Stats, startIngester: unit -> Propulsion.Ingestion.Ingester<'Items>, parseFeedBatch: ChangeFeedItems -> 'Items) as this =
    inherit TrancheStats()
    let readSw = System.Diagnostics.Stopwatch()
    let awaitCapacitySw = System.Diagnostics.Stopwatch()
    let ingester = lazy startIngester ()
    member _.RecordStateChange(assigned) =
        if assigned then readSw.Start()  // we'll end up reporting the warmup/connect time on the first batch, but that's ok
        else readSw.Stop()
    member x.Ingest(ctx: ChangeFeedContext, docs: ChangeFeedItems, checkpoint: Func<Task>, _ct) = task {
        readSw.Stop() // Stop the clock after ChangeFeedProcessor hands off to us
        let ingester = ingester.Value
        let batch: Propulsion.Ingestion.Batch<'Items> = {
            epoch = ctx.epoch; items = parseFeedBatch docs; isTail = false
            checkpoint = fun _ct -> task { do! checkpoint.Invoke()
                                           this.RecordCommitted(ctx.epoch) }
            onCompletion = fun () -> this.RecordCompleted(ctx.epoch) }
        let struct (cur, max) = batch |> (x : Core.ITranchePosition).Decorate |> ingester.Ingest
        stats.ReportRead(ctx.rangeId, awaitCapacitySw.Elapsed, ctx.epoch, ctx.requestCharge, ctx.timestamp, readSw.Elapsed, docs.Count, cur, max)
        let batchReadPos = batch.epoch |> Position.parse
        this.RecordBatch(readSw.Elapsed, ctx.timestamp, ctx.requestCharge, batchReadPos)

        awaitCapacitySw.Restart()
        do! ingester.AwaitCapacity()
        let struct (cur, max) = ingester.CurrentCapacity()
        awaitCapacitySw.Stop()
        stats.ReportWait(int ctx.rangeId, awaitCapacitySw.Elapsed, cur, max)
        this.RecordWaitForCapacity(awaitCapacitySw.Elapsed)
        readSw.Restart() } // restart the clock as we handoff back to the ChangeFeedProcessor to fetch and pass that back to us
    member _.CurrentCapacity() =
        if ingester.IsValueCreated then ingester.Value.CurrentCapacity()
        else 0, 0

    interface IDisposable with member _.Dispose() = if ingester.IsValueCreated then ingester.Value.Stop()

type internal Observers<'Items>(log: Serilog.ILogger, processorName, buildObserver) as this =
    inherit Propulsion.Feed.Core.SourcePositions<Observer<'Items>>(buildObserver)
    let (|Observer|) (rangeId: int) = (this : Core.ISourcePositions<_>).For(TrancheId.parse (string rangeId))
    member _.LogStart(leaseAcquireInterval: TimeSpan, leaseTtl: TimeSpan, leaseRenewInterval: TimeSpan, feedPollInterval: TimeSpan, startFromTail: bool, ?maxItems) =
        log.Information("ChangeFeed {processorName} Lease acquire {leaseAcquireIntervalS:n0}s ttl {ttlS:n0}s renew {renewS:n0}s feedPollInterval {feedPollIntervalS:n0}s Items limit {maxItems} fromTail {fromTail}",
                        processorName, leaseAcquireInterval.TotalSeconds, leaseTtl.TotalSeconds, leaseRenewInterval.TotalSeconds, feedPollInterval.TotalSeconds, Option.toNullable maxItems, startFromTail)
    member _.LogReaderExn(rangeId: int, ex: exn, isNoise: bool) =
        log.Write((if isNoise then LogEventLevel.Debug else LogEventLevel.Error), ex, "ChangeFeed {processorName}/{partition} error", processorName, rangeId)
    member _.LogHandlerExn(rangeId: int, ex: exn) =
        log.Error(ex, "ChangeFeed {processorName}/{partition} Handler Threw", processorName, rangeId)
    member _.Ingest(context, docs, checkpoint, ct) =
        let (Observer obs) = context.rangeId
        obs.Ingest(context, docs, checkpoint, ct)
    member _.RecordStateChange(Observer o as rangeId, acquired) =
        log.Information("ChangeFeed {processorName} Lease {state} {partition}",
            processorName, (if acquired then "Acquired" else "Released"), rangeId)
        o.RecordActive(acquired)
    member _.RecordEstimation(remainingWork: struct (int * int64)[]) =
        for Observer o, gap in remainingWork do
            o.RecordEstimatedGap(gap)

    interface IDisposable with member _.Dispose() = base.Iter (fun x -> (x : IDisposable).Dispose())
