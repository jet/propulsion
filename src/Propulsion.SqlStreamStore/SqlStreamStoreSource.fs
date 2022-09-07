namespace Propulsion.SqlStreamStore

module private Impl =

    open Propulsion.Infrastructure // AwaitTaskCorrect

    let toStreamEvent (dataJson : string) (msg: SqlStreamStore.Streams.StreamMessage) : Propulsion.Streams.Default.StreamEvent =
        let e = FsCodec.Core.TimelineEvent.Create
                  ( int64 msg.StreamVersion,
                    msg.Type,
                    (match dataJson with null -> System.ReadOnlyMemory.Empty | x -> x |> System.Text.Encoding.UTF8.GetBytes |> System.ReadOnlyMemory),
                    msg.JsonMetadata |> System.Text.Encoding.UTF8.GetBytes |> System.ReadOnlyMemory,
                    msg.MessageId,
                    timestamp = System.DateTimeOffset(msg.CreatedUtc))
        Propulsion.Streams.StreamName.internalParseSafe msg.StreamId, e
    let readWithDataAsStreamEvent (msg : SqlStreamStore.Streams.StreamMessage) = async {
        let! json = msg.GetJsonData() |> Async.AwaitTaskCorrect
        return toStreamEvent json msg }
    let readBatch hydrateBodies batchSize categoryFilter (store : SqlStreamStore.IStreamStore) pos : Async<Propulsion.Feed.Core.Batch<_>> = async {
        let! ct = Async.CancellationToken
        let! page = store.ReadAllForwards(Propulsion.Feed.Position.toInt64 pos, batchSize, hydrateBodies, ct) |> Async.AwaitTaskCorrect
        let! items =
            if hydrateBodies then page.Messages |> Seq.map readWithDataAsStreamEvent |> Async.Sequential
            else async { return page.Messages |> Array.map (toStreamEvent null) }
        return { checkpoint = Propulsion.Feed.Position.parse page.NextPosition; items = items; isTail = page.IsEnd } }

    let private fetchMax (log : Serilog.ILogger) (store : SqlStreamStore.IStreamStore) ct : Async<Propulsion.Feed.Position> =
        let rec aux () = async { // Note can't be a Task as tail recursion will blow stack
            try let! lastEventPos = store.ReadHeadPosition(cancellationToken = ct) |> Async.AwaitTaskCorrect
                log.Information("SqlStreamStore Tail Position: @ {pos}", lastEventPos)
                return Propulsion.Feed.Position.parse(lastEventPos + 1L)
            with e ->
                log.Warning(e, "Could not establish max position; Waiting...")
                do! Async.Sleep 1000
                return! aux () }
        aux ()
    let readTailPositionForTranche log store _trancheId : Propulsion.Feed.Position Async = async {
        let! ct = Async.CancellationToken
        return! fetchMax log store ct }

type SqlStreamStoreSource
    (   log : Serilog.ILogger, statsInterval,
        store : SqlStreamStore.IStreamStore, batchSize, tailSleepInterval,
        checkpoints : Propulsion.Feed.IFeedCheckpointStore, sink : Propulsion.Streams.Default.Sink,
        categoryFilter : string -> bool,
        // If the Handler does not require the Data/Meta of the events, the query to load the events can be much more efficient. Default: false
        ?hydrateBodies,
        ?fromTail,
        ?sourceId) =
    inherit Propulsion.Feed.Core.AllFeedSource
        (   log, statsInterval, defaultArg sourceId FeedSourceId.wellKnownId, tailSleepInterval,
            Impl.readBatch (hydrateBodies = Some true) batchSize categoryFilter store, checkpoints, sink,
            ?establishOrigin = if fromTail <> Some true then None else Some (Impl.readTailPositionForTranche log store))
