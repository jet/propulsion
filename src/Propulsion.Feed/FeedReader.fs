namespace Propulsion.Feed.Internal

open FSharp.Control
open Propulsion // Async.Sleep, Raise
open Propulsion.Feed
open Serilog
open System

[<NoComparison; NoEquality>]
type Batch<'e> = { items : Propulsion.Streams.StreamEvent<'e>[]; checkpoint : Position; isTail : bool }

module internal TimelineEvent =

    let toCheckpointPosition (x : FsCodec.ITimelineEvent<'t>) = x.Index + 1L |> Position.parse

module Log =

    [<RequireQualifiedAccess; NoEquality; NoComparison>]
    type Metric =
        | Read of ReadMetric
     and [<NoEquality; NoComparison>] ReadMetric =
        {   source : SourceId; tranche : TrancheId
            token : Nullable<Position>; latency : TimeSpan; pages : int; items : int
            ingestLatency : TimeSpan; ingestQueued : int }

    /// Attach a property to the captured event record to hold the metric information
    // Sidestep Log.ForContext converting to a string; see https://github.com/serilog/serilog/issues/1124
    let [<Literal>] PropertyTag = "propulsionFeedEvent"
    let internal metric (value : Metric) (log : ILogger) =
        let enrich (e : Serilog.Events.LogEvent) =
            e.AddPropertyIfAbsent(Serilog.Events.LogEventProperty(PropertyTag, Serilog.Events.ScalarValue(value)))
        log.ForContext({ new Serilog.Core.ILogEventEnricher with member _.Enrich(evt,_) = enrich evt })
    let internal (|SerilogScalar|_|) : Serilog.Events.LogEventPropertyValue -> obj option = function
        | :? Serilog.Events.ScalarValue as x -> Some x.Value
        | _ -> None
    let (|MetricEvent|_|) (logEvent : Serilog.Events.LogEvent) : Metric option =
        match logEvent.Properties.TryGetValue PropertyTag with
        | true, SerilogScalar (:? Metric as e) -> Some e
        | _ -> None

[<AutoOpen>]
module private Impl =

    type Stats(log : ILogger, statsInterval : TimeSpan, source : SourceId, tranche : TrancheId, renderPos : Position -> string) =

        let mutable batchLastPosition = Position.parse -1L
        let mutable batchCaughtUp = false

        let mutable pagesRead, pagesEmpty, events = 0, 0, 0L
        let mutable readLatency, recentPagesRead, recentEvents, recentPagesEmpty = TimeSpan.Zero, 0, 0, 0

        let mutable ingestLatency, currentBatches, maxBatches = TimeSpan.Zero, 0, 0

        let mutable lastCommittedPosition = Position.parse -1L

        let report () =
            let p pos = match pos with p when p = Position.parse -1L -> Nullable() | x -> Nullable x
            let m = Log.Metric.Read {
                source = source; tranche = tranche
                token = p batchLastPosition; latency = readLatency; pages = recentPagesRead; items = recentEvents
                ingestLatency = ingestLatency; ingestQueued = currentBatches }
            let readS, postS = readLatency.TotalSeconds, ingestLatency.TotalSeconds
            let inline r pos = match pos with p when p = Position.parse -1L -> null | x -> renderPos x
            (log |> Log.metric m).Information(
                "Reader {source:l}/{tranche:l} Tail {caughtUp} Position {readPosition} Committed {lastCommittedPosition} Pages {pagesRead} Empty {pagesEmpty} Events {events} | Recent {l:f1}s Pages {recentPagesRead} Empty {recentPagesEmpty} Events {recentEvents} | Wait {pausedS:f1}s Ahead {cur}/{max}",
                source, tranche, batchCaughtUp, r batchLastPosition, r lastCommittedPosition, pagesRead, pagesEmpty, events, readS, recentPagesRead, recentPagesEmpty, recentEvents, postS, currentBatches, maxBatches)
            readLatency <- TimeSpan.Zero; ingestLatency <- TimeSpan.Zero;
            recentPagesRead <- 0; recentEvents <- 0; recentPagesEmpty <- 0

        member _.RecordBatch(readTime, batch: Batch<_>) =
            readLatency <- readLatency + readTime
            batchLastPosition <- batch.checkpoint
            batchCaughtUp <- batch.isTail
            match Array.length batch.items with
            | 0 ->  pagesEmpty <- pagesEmpty + 1
                    recentPagesEmpty <- recentPagesEmpty + 1
            | c ->  pagesRead <- pagesRead + 1
                    events <- events + int64 c
                    recentEvents <- recentEvents + c
                    recentPagesRead <- recentPagesRead + 1

        member _.UpdateCommittedPosition(pos) =
            lastCommittedPosition <- pos

        member _.UpdateCurMax(latency, cur, max) =
            ingestLatency <- ingestLatency + latency
            currentBatches <- cur
            maxBatches <- max

        member _.Pump = async {
            let! ct = Async.CancellationToken
            while not ct.IsCancellationRequested do
                report ()
                do! Async.Sleep statsInterval
            log.Information("... Stats Stopped") }

type FeedReader
    (   log : ILogger, sourceId, trancheId, statsInterval : TimeSpan,
        // Walk all content in the source. Responsible for managing exceptions, retries and backoff.
        // Implementation is expected to inject an appropriate sleep based on the supplied `Position`
        // Processing loop will abort if an exception is yielded
        crawl :
            bool // lastWasTail : may be used to induce a suitable backoff when repeatedly reading from tail
            * Position // checkpointPosition
            -> AsyncSeq<TimeSpan * Batch<byte[]>>,
        // <summary>Feed a batch into the ingester. Internal checkpointing decides which Commit callback will be called
        // Throwing will tear down the processing loop, which is intended; we fail fast on poison messages
        // In the case where the number of batches reading has gotten ahead of processing exceeds the limit,
        //   <c>submitBatch</c> triggers the backoff of the reading ahead loop by sleeping prior to returning</summary>
        submitBatch :
            int64 // unique tag used to identify batch in internal logging
            * Async<unit> // commit callback. Internal checkpointing dictates when it will be called.
            * seq<Propulsion.Streams.StreamEvent<byte[]>>
            // Yields (current batches pending,max readAhead) for logging purposes
            -> Async<int*int>,
        // Periodically triggered, asynchronously, by the scheduler as processing of submitted batches progresses
        // Should make one attempt to persist a checkpoint
        // Throwing exceptions is acceptable; retrying and handling of exceptions is managed by the internal loop
        commitCheckpoint :
            SourceId
            * TrancheId// identifiers of source and tranche within that; a checkpoint is maintained per such pairing
            * Position // index representing next read position in stream
            // permitted to throw if it fails; failures are counted and/or retried with throttling
            -> Async<unit>,
        renderPos,
        ?logCommitFailure) =

    let log = log.ForContext("source", sourceId).ForContext("tranche", trancheId)
    let stats = Stats(log, statsInterval, sourceId, trancheId, renderPos)

    let commit position = async {
        try do! commitCheckpoint (sourceId, trancheId, position)
            stats.UpdateCommittedPosition(position)
            log.Debug("Committed checkpoint {position}", position)
        with e ->
            match logCommitFailure with None -> log.ForContext<FeedReader>().Debug(e, "Exception while committing checkpoint {position}", position) | Some l -> l e
            return! Async.Raise e }

    let submitPage (readLatency, batch : Batch<byte[]>) = async {
        stats.RecordBatch(readLatency, batch)
        match Array.length batch.items with
        | 0 -> log.Verbose("Page {latency:f0}ms Checkpoint {checkpoint} Empty", readLatency.TotalMilliseconds, batch.checkpoint)
        | c -> if log.IsEnabled(Serilog.Events.LogEventLevel.Debug) then
                   let streamsCount = batch.items |> Seq.distinctBy (fun x -> x.stream) |> Seq.length
                   log.Debug("Page {latency:f0}ms Checkpoint {checkpoint} {eventCount}e {streamCount}s",
                             readLatency.TotalMilliseconds, batch.checkpoint, c, streamsCount)
        let epoch, streamEvents : int64 * Propulsion.Streams.StreamEvent<_> seq = int64 batch.checkpoint, Seq.ofArray batch.items
        let ingestTimer = System.Diagnostics.Stopwatch.StartNew()
        let! cur, max = submitBatch (epoch, commit batch.checkpoint, streamEvents)
        stats.UpdateCurMax(ingestTimer.Elapsed, cur, max) }

    member _.Pump(initialPosition : Position) = async {
        log.Debug("Starting reading stream from position {initialPosition}", renderPos initialPosition)
        stats.UpdateCommittedPosition(initialPosition)
        // Commence reporting stats until such time as we quit pumping
        let! ct = Async.CancellationToken
        Async.Start(stats.Pump, ct)
        let mutable currentPos, lastWasTail = initialPosition, false
        while not ct.IsCancellationRequested do
            for readLatency, batch in crawl (lastWasTail, currentPos) do
                do! submitPage (readLatency, batch)
                currentPos <- batch.checkpoint
                lastWasTail <- batch.isTail }
