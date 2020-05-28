namespace Propulsion.SqlStreamStore

module StreamReader =

    open System
    open System.Text

    open SqlStreamStore
    open SqlStreamStore.Streams

    open Serilog

    open Propulsion.Streams

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
                    msg.Position,
                    msg.Type,
                    len0ToNull data,
                    len0ToNull metadata,
                    msg.MessageId,
                    timestamp = DateTimeOffset(msg.CreatedUtc)
                )

            { stream = StreamName.internalParseSafe msg.StreamId; event = event }

    type Stats(logger: ILogger, ?statsInterval: TimeSpan) =

        let statsInterval = defaultArg statsInterval (TimeSpan.FromSeconds(30.))

        let mutable batchFirstPosition = 0L
        let mutable batchLastPosition = 0L
        let mutable batchCaughtUp = false

        let mutable pagesRead = 0
        let mutable pagesEmpty = 0

        let mutable recentPagesRead = 0
        let mutable recentPagesEmpty = 0

        let report () =
            logger.Information(
                "Pages Read {pagesRead} Empty {pagesEmpty} | Recent Read {recentPagesRead} Empty {recentPagesEmpty} | Batch {firstPosition}-{lastPosition} Caught up {caughtUp}",
                pagesRead, pagesEmpty, recentPagesRead, recentPagesEmpty, batchFirstPosition, batchLastPosition, batchCaughtUp)

        member this.UpdateBatch (batch: InternalBatch) =
            batchFirstPosition <- batch.firstPosition
            batchLastPosition <- batch.lastPosition
            batchCaughtUp <- batch.isEnd

            pagesRead <- pagesRead + 1
            recentPagesRead <- recentPagesRead + 1

        member this.UpdateEmptyPage () =
            pagesEmpty <- pagesEmpty + 1
            recentPagesEmpty <- recentPagesEmpty + 1

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
    type private Work =
        | TakeInitial
        | Page of ReadAllPage
        | TakeNext of ReadAllPage

    type StreamReader
        (
            logger : ILogger,
            store: IStreamStore,
            submitBatch: InternalBatch -> Async<unit>,
            ?maxBatchSize: int,
            ?sleepInterval: TimeSpan,
            ?statsInterval: TimeSpan
        ) =

        let logger = logger.ForContext("component", "StreamReader")

        let maxBatchSize = defaultArg maxBatchSize 100
        let sleepInterval = defaultArg sleepInterval (TimeSpan.FromSeconds(5.))

        let stats = Stats(logger, ?statsInterval = statsInterval)

        let processPage (page: ReadAllPage) =
            async {
                if page.Messages.Length > 0 then

                    let events =
                        page.Messages
                        |> Seq.map StreamMessage.intoStreamEvent
                        |> Seq.sortBy (fun x -> x.event.Index)
                        |> Array.ofSeq

                    let batch =
                        {
                            firstPosition = events.[0].event.Index
                            lastPosition = events.[events.Length - 1].event.Index
                            messages = events
                            isEnd = page.IsEnd
                        }

                    logger.Debug("Submitting a batch of {batchSize} events, position {firstPosition} through {lastPosition}",
                        batch.Length, batch.firstPosition, batch.lastPosition)

                    stats.UpdateBatch(batch)

                    do! submitBatch batch
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
                    try
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
                            do! Async.Sleep sleepInterval
                    with
                    | exc ->
                        logger.Error(exc, "Exception while running StreamReader loop")
                        return! Async.Raise exc
            }
