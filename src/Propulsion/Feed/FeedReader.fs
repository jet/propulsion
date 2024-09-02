namespace Propulsion.Feed.Core

open FSharp.Control
open Propulsion.Feed
open Propulsion.Internal
open Serilog
open System

module internal TimelineEvent =

    let toCheckpointPosition (x: FsCodec.ITimelineEvent<'t>) = x.Index + 1L |> Position.parse

module Log =

    [<RequireQualifiedAccess; NoEquality; NoComparison>]
    type Metric =
        | Read of ReadMetric
     and [<NoEquality; NoComparison>] ReadMetric =
        {   source: SourceId; tranche: TrancheId
            token: Nullable<Position>; latency: TimeSpan; pages: int; items: int
            wait: TimeSpan; activeBatches: int }

    let [<Literal>] PropertyTag = "propulsionFeedEvent"
    /// Attach a property to the captured event record to hold the metric information
    let internal withMetric (value: Metric) = Log.withScalarProperty PropertyTag value
    let [<return: Struct>] (|MetricEvent|_|) (logEvent: Serilog.Events.LogEvent): Metric voption =
        match logEvent.Properties.TryGetValue PropertyTag with true, Log.ScalarValue (:? Metric as e) -> ValueSome e | _ -> ValueNone

type internal Stats(partition: int, source: SourceId, tranche: TrancheId) =

    let mutable readPos, pages, empty, events, commitPos = None, 0, 0, 0L, None
    let mutable accReadLat, recentPages, recentEvents, recentEmpty = TimeSpan.Zero, 0, 0, 0
    let mutable accWaits, shutdownTimer, currentBatches, maxReadAhead = TimeSpan.Zero, System.Diagnostics.Stopwatch(), 0, 0
    let mutable lastWasTail, finishedReading, closed = false, false, false
    let updateIngesterState cur max =
        currentBatches <- cur
        maxReadAhead <- max

    member _.Dump(log: ILogger) =
        if closed then () else

        closed <- finishedReading
        let m = Log.Metric.Read {
            source = source; tranche = tranche
            token = Option.toNullable readPos; latency = accReadLat; pages = recentPages; items = recentEvents
            wait = accWaits; activeBatches = currentBatches }
        let r = Option.map string >> Option.defaultValue null
        let state = if not lastWasTail then "Busy"
                    elif commitPos = readPos then "COMPLETE"
                    else if finishedReading then "End" else "Tail"
        (Log.withMetric m log).ForContext("tail", lastWasTail).Information(
            "Reader {partition} {state} @ {commitPos}/{readPosition} Pages {pagesRead} empty {pagesEmpty} events {events} "+
            "| Recent {l:f1}s Pages {recentPages} empty {recentEmpty} events {recentEvents} Pause {pausedS:f1}s Ahead {cur}/{max} Wait {waitS:f1}s",
            partition, state, r commitPos, r readPos, pages, empty, events,
            accReadLat.TotalSeconds, recentPages, recentEmpty, recentEvents, accWaits.TotalSeconds, currentBatches, maxReadAhead, shutdownTimer.ElapsedSeconds)
        accReadLat <- TimeSpan.Zero; accWaits <- TimeSpan.Zero; recentPages <- 0; recentEvents <- 0; recentEmpty <- 0

    member _.RecordBatch(readTime, batch: Batch<_>, cur, max) =
        updateIngesterState cur max
        accReadLat <- accReadLat + readTime
        readPos <- Some batch.checkpoint
        lastWasTail <- batch.isTail
        match Array.length batch.items with
        | 0 ->  empty <- empty + 1
                recentEmpty <- recentEmpty + 1
        | c ->  pages <- pages + 1
                events <- events + int64 c
                recentEvents <- recentEvents + c
                recentPages <- recentPages + 1
        closed <- false // Any straggler reads (and/or bugs!) trigger logging
    member _.RecordWaitForCapacity(latency) =
        accWaits <- accWaits + latency

    member _.UpdateCommittedPosition(pos) =
        commitPos <- Some pos
        closed <- false // Any updates trigger logging

    member _.EnteringShutdown() =
        finishedReading <- true
        shutdownTimer.Start()
    member _.ShutdownCompleted(cur, max) =
        updateIngesterState cur max
        shutdownTimer.Stop()

type FeedReader
    (   log: ILogger, partition, source, tranche,
        // Walk all content in the source. Responsible for managing exceptions, retries and backoff.
        // Implementation is expected to inject an appropriate sleep based on the supplied `Position`
        // Processing loop will abort if an exception is yielded
        crawl :
            bool // lastWasTail: may be used to induce a suitable backoff when repeatedly reading from tail
            * Position // checkpointPosition
            -> CancellationToken
            -> IAsyncEnumerable<struct (TimeSpan * Batch<Propulsion.Sinks.EventBody>)>,
        // Feed a batch into the ingester. Internal checkpointing decides which Commit callback will be called
        // Throwing will tear down the processing loop, which is intended; we fail fast on poison messages
        // In the case where the number of batches reading has gotten ahead of processing exceeds the limit,
        //   <c>submitBatch</c> triggers the backoff of the reading ahead loop by sleeping prior to returning
        // Yields (current batches pending,max readAhead) for logging purposes
        ingester: Propulsion.Ingestion.Ingester<Propulsion.Sinks.StreamEvent seq>,
        decorate: Propulsion.Ingestion.Batch<Propulsion.Sinks.StreamEvent seq> -> Propulsion.Ingestion.Batch<Propulsion.Sinks.StreamEvent seq>,
        // Periodically triggered, asynchronously, by the scheduler as processing of submitted batches progresses
        // Should make one attempt to persist a checkpoint
        // Throwing exceptions is acceptable; retrying and handling of exceptions is managed by the internal loop
        commitCheckpoint :
            SourceId
            * TrancheId // identifiers of source and tranche within that; a checkpoint is maintained per such pairing
            * Position // index representing next read position in stream
            * CancellationToken
            // permitted to throw if it fails; failures are counted and/or retried with throttling
            -> Task,
        renderPos: Position -> string, stopAtTail, ?logCommitFailure) =

    let stats = Stats(partition, source, tranche)

    let logCommitFailure_ (e: exn) = log.ForContext<FeedReader>().Debug(e, "Exception while committing checkpoint")
    let logCommitFailure =  defaultArg logCommitFailure logCommitFailure_
    let commit (position: Position) ct = task {
        try do! commitCheckpoint (source, tranche, position, ct)
            log.Debug("Committed checkpoint {position}", position)
            stats.UpdateCommittedPosition(position)
        with Exception.Log logCommitFailure () -> () }

    let submitPage (readLatency, batch: Batch<_>) = task {
        let sinkBatch = decorate { isTail = batch.isTail; epoch = Position.toInt64 batch.checkpoint
                                   checkpoint = commit batch.checkpoint; items = batch.items; onCompletion = ignore }
        let struct (cur, max) = ingester.Ingest(sinkBatch)
        stats.RecordBatch(readLatency, batch, cur, max)

        match Array.length batch.items with
        | 0 -> log.Verbose("Page {latency:f0}ms Checkpoint {checkpoint} Empty", readLatency.TotalMilliseconds, batch.checkpoint)
        | c -> if log.IsEnabled LogEventLevel.Debug then
                   let streamsCount = batch.items |> Seq.distinctBy ValueTuple.fst |> Seq.length
                   log.Debug("Page {latency:f0}ms Checkpoint {checkpoint} {eventCount}e {streamCount}s",
                             readLatency.TotalMilliseconds, batch.checkpoint, c, streamsCount)
        let waitTimer = Stopwatch.timestamp ()
        do! ingester.AwaitCapacity()
        stats.RecordWaitForCapacity(Stopwatch.elapsed waitTimer) }

    member _.LogPartitionStarting(pos: Position) =
        log.Information("Reading {partition} {source:l}/{tranche:l} From {pos}",
                        partition, source, tranche, renderPos pos)
    member _.LogPartitionExn(ex: exn) =
        log.Warning(ex, "Finishing {partition}", partition)

    member _.DumpStats() = stats.Dump(log)

    member _.Pump(initialPosition: Position, ct: CancellationToken) = task {
        log.Debug("Starting {partition} from {initialPosition}", partition, renderPos initialPosition)
        stats.UpdateCommittedPosition(initialPosition)
        let mutable currentPos, lastWasTail = initialPosition, false
        while not ct.IsCancellationRequested && not (stopAtTail && lastWasTail) do
            for readLatency, batch in crawl (lastWasTail, currentPos) ct do
                do! submitPage (readLatency, batch)
                currentPos <- batch.checkpoint
                lastWasTail <- batch.isTail
        if not ct.IsCancellationRequested && stopAtTail then
            stats.EnteringShutdown()
            let! struct (cur, max) = ingester.AwaitCompleted()
            stats.ShutdownCompleted(cur, max) }
