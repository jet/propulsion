namespace Propulsion.Feed

open System
open Serilog

[<NoComparison; NoEquality>]
type Page<'e> = { items : FsCodec.ITimelineEvent<'e>[]; checkpoint : Position; isTail : bool }

[<AutoOpen>]
module private Impl =

    type Page<'e> with
        member page.IsEmpty = Array.isEmpty page.items
        member page.Size = page.items.Length
        member page.FirstPosition = page.items.[0].Index |> Position.parse
        member page.LastPosition = (Array.last page.items).Index + 1L |> Position.parse

    type Stats(log : ILogger, statsInterval : TimeSpan) =

        let mutable batchLastPosition = Position.parse -1L
        let mutable batchCaughtUp = false

        let mutable pagesRead = 0
        let mutable pagesEmpty = 0
        let mutable recentPagesRead = 0
        let mutable recentPagesEmpty = 0

        let mutable currentBatches = 0
        let mutable maxBatches = 0

        let mutable lastCommittedPosition = Position.parse -1L

        let report () =
            let p pos = match pos with p when p = Position.parse -1L -> Nullable() | x -> Nullable x
            log.Information(
                "Pages Read {pagesRead} Empty {pagesEmpty} | Recent Read {recentPagesRead} Empty {recentPagesEmpty} | Position Read {batchLastPosition} Committed {lastCommittedPosition} | Caught up {caughtUp} | cur {cur} / max {max}",
                pagesRead, pagesEmpty, recentPagesRead, recentPagesEmpty, p batchLastPosition, p lastCommittedPosition, batchCaughtUp, currentBatches, maxBatches)
            recentPagesRead <- 0
            recentPagesEmpty <- 0

        member _.RecordBatch(batch: Page<_>) =
            batchLastPosition <- batch.LastPosition
            batchCaughtUp <- batch.isTail

            pagesRead <- pagesRead + 1
            recentPagesRead <- recentPagesRead + 1

        member _.RecordEmptyPage(isTail) =
            batchCaughtUp <- isTail

            pagesEmpty <- pagesEmpty + 1
            recentPagesEmpty <- recentPagesEmpty + 1

        member _.UpdateCommittedPosition(pos) =
            lastCommittedPosition <- pos

        member _.UpdateCurMax(cur, max) =
            currentBatches <- cur
            maxBatches <- max

        member this.Pump = async {
            let! ct = Async.CancellationToken
            while not ct.IsCancellationRequested do
                report ()
                do! Async.Sleep statsInterval }

type FeedReader
    (   log : ILogger, sourceId, trancheId, statsInterval : TimeSpan,
        /// Read a page from source. Responsible for managing exceptions, retries and backoff.
        /// Processing loop will abort if an exception is yielded
        readPage :
            bool // lastWasTail : may be used to induce a suitable backoff when repeatedly reading from tail
            * Position // checkpointPosition
            -> Async<Page<byte[]>>,
        /// Feed a batch into the ingester. Internal checkpointing decides which Commit callback will be called
        /// Throwing will tear down the processing loop, which is intended; we fail fast on poison messages
        /// In the case where the number of batches reading has gotten ahead of processing exceeds the limit,
        ///   <c>submitBatch</c> triggers the backoff of the reading ahead loop by sleeping prior to returning
        submitBatch :
            int64 // tag used to identify batch in logging
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
    let streamName = FsCodec.StreamName.compose "Messages" [SourceId.toString sourceId; TrancheId.toString trancheId]
    let stats = Stats(log, statsInterval)

    let commit position = async {
        try do! commitCheckpoint (sourceId, trancheId, position)
            stats.UpdateCommittedPosition(position)
            log.Debug("Committed position {position}", position)
        with exc ->
            log.Warning(exc, "Exception while committing position {position}", position)
            return! Async.Raise exc }

    let submitPage (batch: Page<byte[]>) = async {
        let streamEvents : Propulsion.Streams.StreamEvent<_> seq =
            if Array.isEmpty batch.items then
                log.Debug("Empty page retrieved, nothing to submit")
                stats.RecordEmptyPage(batch.isTail)
                Seq.empty
            else
                log.Debug("Submitting a batch of {batchSize} events, position {firstPosition} through {lastPosition}",
                    batch.Size, batch.FirstPosition, batch.LastPosition)
                stats.RecordBatch(batch)
                seq { for x in batch.items -> { stream = streamName; event = x } }
        let! cur, max = submitBatch (int64 batch.checkpoint, commit batch.checkpoint, streamEvents)
        stats.UpdateCurMax(cur, max) }

    member _.Pump(initialPosition : Position) = async {
        // Commence reporting stats until such time as we quit pumping
        let! _ = Async.StartChild stats.Pump

        log.Debug("Starting reading stream from position {initialPosition}", initialPosition)
        let mutable currentPos, lastWasTail = initialPosition, false
        let! ct = Async.CancellationToken
        while not ct.IsCancellationRequested do
            let! page = readPage (lastWasTail, currentPos)
            do! submitPage page
            currentPos <- page.checkpoint
            lastWasTail <- page.isTail }
