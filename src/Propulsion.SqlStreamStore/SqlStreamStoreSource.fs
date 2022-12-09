namespace Propulsion.SqlStreamStore

module private Impl =

    open Propulsion.Infrastructure // AwaitTaskCorrect

    let private toStreamEvent (dataJson : string) struct (sn, msg: SqlStreamStore.Streams.StreamMessage) : Propulsion.Streams.Default.StreamEvent =
        let c = msg.Type
        let d = match dataJson with null -> System.ReadOnlyMemory.Empty | x -> x |> System.Text.Encoding.UTF8.GetBytes |> System.ReadOnlyMemory
        let m = msg.JsonMetadata |> System.Text.Encoding.UTF8.GetBytes |> System.ReadOnlyMemory
        let sz = c.Length + d.Length + m.Length
        sn, FsCodec.Core.TimelineEvent.Create(msg.StreamVersion, c, d, m, msg.MessageId, timestamp = System.DateTimeOffset(msg.CreatedUtc), size = sz)
    let private readWithDataAsStreamEvent ct (struct (_sn, msg : SqlStreamStore.Streams.StreamMessage) as m) = async {
        let! json = msg.GetJsonData(ct) |> Async.AwaitTaskCorrect
        return toStreamEvent json m }
    let readBatch hydrateBodies batchSize categoryFilter (store : SqlStreamStore.IStreamStore) (pos, ct) = task {
        let! page = store.ReadAllForwards(Propulsion.Feed.Position.toInt64 pos, batchSize, hydrateBodies, ct)
        let filtered = page.Messages
                       |> Seq.choose (fun (msg : SqlStreamStore.Streams.StreamMessage) ->
                           let sn = Propulsion.Streams.StreamName.internalParseSafe msg.StreamId
                           if categoryFilter (FsCodec.StreamName.category sn) then Some struct (sn, msg) else None)
        let! items = if not hydrateBodies then async { return filtered |> Seq.map (toStreamEvent null) |> Array.ofSeq }
                     else filtered |> Seq.map (readWithDataAsStreamEvent ct) |> Async.Sequential
        return ({ checkpoint = Propulsion.Feed.Position.parse page.NextPosition; items = items; isTail = page.IsEnd } : Propulsion.Feed.Core.Batch<_>)  }

    let readTailPositionForTranche (store : SqlStreamStore.IStreamStore) _trancheId : Async<Propulsion.Feed.Position> = async {
        let! ct = Async.CancellationToken
        let! lastEventPos = store.ReadHeadPosition(cancellationToken = ct) |> Async.AwaitTaskCorrect
        return Propulsion.Feed.Position.parse(lastEventPos + 1L) }

type SqlStreamStoreSource
    (   log : Serilog.ILogger, statsInterval,
        store : SqlStreamStore.IStreamStore, batchSize, tailSleepInterval,
        checkpoints : Propulsion.Feed.IFeedCheckpointStore, sink : Propulsion.Streams.Default.Sink,
        categoryFilter : string -> bool,
        // If the Handler does not require the Data/Meta of the events, the query to load the events can be much more efficient. Default: false
        ?hydrateBodies,
        ?startFromTail,
        ?sourceId) =
    inherit Propulsion.Feed.Core.AllFeedSource
        (   log, statsInterval, defaultArg sourceId FeedSourceId.wellKnownId, tailSleepInterval,
            Impl.readBatch (hydrateBodies = Some true) batchSize categoryFilter store, checkpoints, sink,
            ?establishOrigin = if startFromTail <> Some true then None else Some (Impl.readTailPositionForTranche store))
