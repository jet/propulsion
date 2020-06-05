namespace Propulsion.SqlStreamStore

open System
open System.Text

open SqlStreamStore
open SqlStreamStore.Streams

open Serilog
open Propulsion.Streams

[<AutoOpen>]
module private Internal =

    [<NoComparison;NoEquality>]
    type InternalBatch =
        {
            firstPosition  : int64
            lastPosition   : int64
            messages       : StreamEvent<byte []> array
            isEnd          : bool
        }
        member this.Length = this.messages.Length

    [<RequireQualifiedAccess>]
    module StreamMessage =

        let intoStreamEvent (msg: StreamMessage) : Propulsion.Streams.StreamEvent<_> =
            let inline len0ToNull (x : _[]) =
                match x with
                | null -> null
                | x when x.Length = 0 -> null
                | x -> x

            let data =
                let json = msg.GetJsonData().Result
                Encoding.UTF8.GetBytes(json)

            let metadata =
                Encoding.UTF8.GetBytes(msg.JsonMetadata)

            let event =
                FsCodec.Core.TimelineEvent.Create (
                    int64 msg.StreamVersion,
                    msg.Type,
                    len0ToNull data,
                    len0ToNull metadata,
                    msg.MessageId,
                    timestamp = DateTimeOffset(msg.CreatedUtc)
                )

            { stream = StreamName.internalParseSafe msg.StreamId; event = event }

    type Stats(logger: ILogger, statsInterval: TimeSpan) =

        let mutable batchFirstPosition = 0L
        let mutable batchLastPosition = 0L
        let mutable batchCaughtUp = false

        let mutable pagesRead = 0
        let mutable pagesEmpty = 0
        let mutable recentPagesRead = 0
        let mutable recentPagesEmpty = 0

        let mutable currentBatches = 0
        let mutable maxBatches = 0

        let mutable lastCommittedPosition = 0L

        let report () =
            logger.Information(
                "Pages Read {pagesRead} Empty {pagesEmpty} | Recent Read {recentPagesRead} Empty {recentPagesEmpty} | Position Read {batchLastPosition} Committed {lastCommittedPosition} | Caught up {caughtUp} | cur {cur} / max {max}",
                pagesRead, pagesEmpty, recentPagesRead, recentPagesEmpty, batchLastPosition, lastCommittedPosition, batchCaughtUp, currentBatches, maxBatches)

        member this.UpdateBatch (batch: InternalBatch) =
            batchFirstPosition <- batch.firstPosition
            batchLastPosition <- batch.lastPosition
            batchCaughtUp <- batch.isEnd

            pagesRead <- pagesRead + 1
            recentPagesRead <- recentPagesRead + 1

        member this.UpdateEmptyPage () =
            pagesEmpty <- pagesEmpty + 1
            recentPagesEmpty <- recentPagesEmpty + 1

        member this.UpdateCommitedPosition(pos) =
            lastCommittedPosition <- pos

        member this.UpdateCurMax(cur, max) =
            currentBatches <- cur
            maxBatches <- max

        member this.Start =
            async {
                let! ct = Async.CancellationToken
                while not ct.IsCancellationRequested do
                    report ()
                    recentPagesRead <- 0
                    recentPagesEmpty <- 0
                    do! Async.Sleep statsInterval
            }

    [<RequireQualifiedAccess>]
    [<NoComparison>]
    type Work =
        | TakeInitial
        | Page of ReadAllPage
        | TakeNext of ReadAllPage

    type SubmitBatchHandler =
        // ingester submit method: epoch * checkpoint * items -> write result
        int64 * Async<unit> * seq<Propulsion.Streams.StreamEvent<byte[]>> -> Async<int*int>

type StreamReader
    (
        logger: ILogger,
        store: IStreamStore,
        checkpointer: ICheckpointer,
        submitBatch: SubmitBatchHandler,
        streamId,
        consumerGroup,
        maxBatchSize: int,
        tailSleepInterval: TimeSpan,
        statsInterval: TimeSpan
    ) =

    let stats = Stats(logger, statsInterval)

    let commit batch =
        async {
            try
                do! checkpointer.CommitPosition(streamId, consumerGroup, batch.lastPosition)
                stats.UpdateCommitedPosition(batch.lastPosition)
                logger.Debug("Committed position {position}", batch.lastPosition)
            with
            | exc ->
                logger.Error(exc, "Exception while commiting position {position}", batch.lastPosition)
                return! Async.Raise exc
        }

    let processPage (page: ReadAllPage) =
        async {
            if page.Messages.Length > 0 then

                let events =
                    page.Messages
                    |> Seq.map StreamMessage.intoStreamEvent
                    |> Array.ofSeq

                let batch =
                    {
                        firstPosition = page.Messages.[0].Position
                        lastPosition = page.Messages.[page.Messages.Length - 1].Position
                        messages = events
                        isEnd = page.IsEnd
                    }

                logger.Debug("Submitting a batch of {batchSize} events, position {firstPosition} through {lastPosition}",
                    batch.Length, batch.firstPosition, batch.lastPosition)

                stats.UpdateBatch(batch)

                let! cur, max = submitBatch (batch.lastPosition, commit batch, batch.messages)

                stats.UpdateCurMax(cur, max)
            else
                logger.Debug("Empty page retrieved, nothing to submit")
                stats.UpdateEmptyPage()
        }

    member this.Start (commitedPosition: Nullable<int64>) =
        async {
            // Start reporting stats
            do! Async.StartChild stats.Start |> Async.Ignore

            let mutable workItem = Work.TakeInitial

            let! ct = Async.CancellationToken
            while not ct.IsCancellationRequested do
                let! page =
                    async {
                        let! ct = Async.CancellationToken
                        match workItem with
                        | Work.Page page -> return page
                        | Work.TakeInitial ->
                            let initialPosition =
                                if commitedPosition.HasValue then commitedPosition.Value + 1L else 0L

                            logger.Information("Starting reading stream from position {initialPosition}, maxBatchSize {maxBatchSize}", initialPosition, maxBatchSize)

                            return! store.ReadAllForwards(initialPosition, maxBatchSize, true, ct) |> Async.AwaitTaskCorrect
                        | Work.TakeNext page ->
                            return! page.ReadNext(ct) |> Async.AwaitTaskCorrect
                    }

                workItem <- Work.Page page

                // Process the page and submit the batch of messages to ingester.
                do! processPage page

                // If processPage was successful, ask for new page on the next iteration
                workItem <- Work.TakeNext page

                if page.IsEnd then
                    do! Async.Sleep tailSleepInterval
        }
