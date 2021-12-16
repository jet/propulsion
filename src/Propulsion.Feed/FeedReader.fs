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

    type Batch<'e> with
        member batch.IsEmpty = Array.isEmpty batch.items
        member batch.Size = batch.items.Length
        member batch.FirstPosition = batch.items.[0].event.Index |> Position.parse
        member batch.LastPosition = (Array.last batch.items).event |> TimelineEvent.toCheckpointPosition

    type Stats(log : ILogger, statsInterval : TimeSpan, source : SourceId, tranche : TrancheId) =

        let mutable batchLastPosition = Position.parse -1L
        let mutable batchCaughtUp = false

        let mutable pagesRead, pagesEmpty = 0, 0
        let mutable readLatency, recentPagesRead, itemsRead, recentPagesEmpty = TimeSpan.Zero, 0, 0, 0

        let mutable ingestLatency, currentBatches, maxBatches = TimeSpan.Zero, 0, 0

        let mutable lastCommittedPosition = Position.parse -1L

        let report () =
            let p pos = match pos with p when p = Position.parse -1L -> Nullable() | x -> Nullable x
            let m = Log.Metric.Read {
                source = source; tranche = tranche
                token = p batchLastPosition; latency = readLatency; pages = recentPagesRead; items = itemsRead
                ingestLatency = ingestLatency; ingestQueued = currentBatches }
            let readS, postS = readLatency.TotalSeconds, ingestLatency.TotalSeconds
            (log |> Log.metric m).Information(
                "Reader {source}/{tranche} Pages {pagesRead} Empty {pagesEmpty} | Recent {l:f1}s Pages {recentPagesRead} Empty {recentPagesEmpty} Items {itemsRead} | Position Read {batchLastPosition} Committed {lastCommittedPosition} | Caught up {caughtUp} | Wait {pausedS:f1}s Ahead {cur}/{max}",
                source, tranche, pagesRead, pagesEmpty, readS, recentPagesRead, recentPagesEmpty, itemsRead, p batchLastPosition, p lastCommittedPosition, batchCaughtUp, postS, currentBatches, maxBatches)
            readLatency <- TimeSpan.Zero; ingestLatency <- TimeSpan.Zero;
            recentPagesRead <- 0; itemsRead <- 0; recentPagesEmpty <- 0

        member _.RecordReadLatency(latency) =
            readLatency <- readLatency + latency

        member _.RecordBatch(batch: Batch<_>) =
            batchLastPosition <- batch.LastPosition
            batchCaughtUp <- batch.isTail

            pagesRead <- pagesRead + 1
            itemsRead <- itemsRead + batch.items.Length
            recentPagesRead <- recentPagesRead + 1

        member _.RecordEmptyPage(isTail) =
            batchCaughtUp <- isTail

            pagesEmpty <- pagesEmpty + 1
            recentPagesEmpty <- recentPagesEmpty + 1

        member _.UpdateCommittedPosition(pos) =
            lastCommittedPosition <- pos

        member _.UpdateCurMax(latency, cur, max) =
            ingestLatency <- ingestLatency + latency
            currentBatches <- cur
            maxBatches <- max

        member this.Pump = async {
            let! ct = Async.CancellationToken
            while not ct.IsCancellationRequested do
                report ()
                do! Async.Sleep statsInterval }

type FeedReader
    (   log : ILogger, sourceId, trancheId, statsInterval : TimeSpan,
        /// Walk all content in the source. Responsible for managing exceptions, retries and backoff.
        /// Implementation is expected to inject an appropriate sleep based on the supplied `Position`
        /// Processing loop will abort if an exception is yielded
        crawl :
            bool // lastWasTail : may be used to induce a suitable backoff when repeatedly reading from tail
            * Position // checkpointPosition
            -> AsyncSeq<Batch<byte[]>>,
        /// <summary>Feed a batch into the ingester. Internal checkpointing decides which Commit callback will be called
        /// Throwing will tear down the processing loop, which is intended; we fail fast on poison messages
        /// In the case where the number of batches reading has gotten ahead of processing exceeds the limit,
        ///   <c>submitBatch</c> triggers the backoff of the reading ahead loop by sleeping prior to returning</summary>
        submitBatch :
            int64 // unique tag used to identify batch in internal logging
            * Async<unit> // commit callback. Internal checkpointing dictates when it will be called.
            * seq<Propulsion.Streams.StreamEvent<byte[]>>
            // Yields (current batches pending,max readAhead) for logging purposes
            -> Async<int*int>,
        /// Periodically triggered, asynchronously, by the scheduler as processing of submitted batches progresses
        /// Should make one attempt to persist a checkpoint
        /// Throwing exceptions is acceptable; retrying and handling of exceptions is managed by the internal loop
        commitCheckpoint :
            SourceId
            * TrancheId// identifiers of source and tranche within that; a checkpoint is maintained per such pairing
            * Position // index representing next read position in stream
            // permitted to throw if it fails; failures are counted and/or retried with throttling
            -> Async<unit>) =

    let log = log.ForContext("source", sourceId).ForContext("tranche", trancheId)
    let stats = Stats(log, statsInterval, sourceId, trancheId)

    let commit position = async {
        try do! commitCheckpoint (sourceId, trancheId, position)
            stats.UpdateCommittedPosition(position)
            log.Debug("Committed position {position}", position)
        with exc ->
            log.Warning(exc, "Exception while committing position {position}", position)
            return! Async.Raise exc }

    let submitPage (batch: Batch<byte[]>) = async {
        let epoch, streamEvents : int64 * Propulsion.Streams.StreamEvent<_> seq =
            if batch.IsEmpty then
                log.Debug("Empty page retrieved, nothing to submit")
                stats.RecordEmptyPage(batch.isTail)
                int64 batch.checkpoint, Seq.empty
            else
                log.Debug("Submitting a batch of {batchSize} events, position {firstPosition} through {lastPosition}",
                    batch.Size, batch.FirstPosition, batch.LastPosition)
                stats.RecordBatch(batch)
                int64 batch.FirstPosition, Seq.ofArray batch.items

        let ingestTimer = System.Diagnostics.Stopwatch.StartNew()
        let! cur, max = submitBatch (epoch, commit batch.checkpoint, streamEvents)
        stats.UpdateCurMax(ingestTimer.Elapsed, cur, max) }

    member _.Pump(initialPosition : Position) = async {
        stats.UpdateCommittedPosition(initialPosition)
        // Commence reporting stats until such time as we quit pumping
        let! _ = Async.StartChild stats.Pump

        log.Debug("Starting reading stream from position {initialPosition}", initialPosition)
        let mutable currentPos, lastWasTail = initialPosition, false
        let! ct = Async.CancellationToken
        while not ct.IsCancellationRequested do
            let readTimer = System.Diagnostics.Stopwatch.StartNew()
            for batch in crawl (lastWasTail, currentPos) do
                stats.RecordReadLatency(readTimer.Elapsed)
                do! submitPage batch
                readTimer.Restart()
                currentPos <- batch.checkpoint
                lastWasTail <- batch.isTail }
