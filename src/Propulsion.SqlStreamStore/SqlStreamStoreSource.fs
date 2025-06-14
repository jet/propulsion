namespace Propulsion.SqlStreamStore

module private Impl =

    let private toStreamEvent (dataJson: string) struct (sn, msg: SqlStreamStore.Streams.StreamMessage): Propulsion.Sinks.StreamEvent =
        let c = msg.Type
        let d = dataJson |> NotNull.map System.Text.Encoding.UTF8.GetBytes |> FsCodec.Encoding.OfBlob
        let m = msg.JsonMetadata |> NotNull.map System.Text.Encoding.UTF8.GetBytes |> FsCodec.Encoding.OfBlob
        let sz = c.Length + FsCodec.Encoding.ByteCount d + FsCodec.Encoding.ByteCount m
        sn, FsCodec.Core.TimelineEvent.Create(msg.StreamVersion, c, d, m, msg.MessageId, timestamp = System.DateTimeOffset(msg.CreatedUtc), size = sz)
    let private readWithDataAsStreamEvent (struct (_sn, msg: SqlStreamStore.Streams.StreamMessage) as m) ct = task {
        let! json = msg.GetJsonData(ct)
        return toStreamEvent json m }
    let readBatch withData batchSize streamFilter (store: SqlStreamStore.IStreamStore) pos ct = task {
        let! page = store.ReadAllForwards(Propulsion.Feed.Position.toInt64 pos, batchSize, withData, ct)
        let filtered = page.Messages
                       |> Seq.choose (fun (msg: SqlStreamStore.Streams.StreamMessage) ->
                           let sn = Propulsion.Streams.StreamName.internalParseSafe msg.StreamId
                           if streamFilter sn then Some struct (sn, msg) else None)
        let! items = if not withData then task { return filtered |> Seq.map (toStreamEvent null) |> Array.ofSeq }
                     else filtered |> Seq.map readWithDataAsStreamEvent |> Propulsion.Internal.Task.sequential ct
        return ({ checkpoint = Propulsion.Feed.Position.parse page.NextPosition; items = items; isTail = page.IsEnd }: Propulsion.Feed.Batch<_>)  }

    let readTailPositionForTranche (store: SqlStreamStore.IStreamStore) _trancheId ct = task {
        let! lastEventPos = store.ReadHeadPosition(ct)
        return Propulsion.Feed.Position.parse (lastEventPos + 1L) }

type SqlStreamStoreSource
    (   log: Serilog.ILogger, statsInterval,
        store: SqlStreamStore.IStreamStore, batchSize, tailSleepInterval,
        checkpoints: Propulsion.Feed.IFeedCheckpointStore, sink: Propulsion.Sinks.SinkPipeline,
        // The whitelist of Categories to use
        ?categories,
        // Predicate to filter <c>StreamName</c>'s to use
        ?streamFilter: System.Func<FsCodec.StreamName, bool>,
        // If the Handler does not require the Data/Meta of the events, the query to load the events can be much more efficient. Default: false
        ?withData,
        // Override default start position to be at the tail of the index. Default: Replay all events.
        ?startFromTail, ?sourceId) =
    inherit Propulsion.Feed.Core.AllFeedSource
        (   log, statsInterval, defaultArg sourceId FeedSourceId.wellKnownId, tailSleepInterval,
            Impl.readBatch (withData = Some true) batchSize (Propulsion.Feed.Core.Categories.mapFilters categories streamFilter) store, checkpoints, sink,
            ?establishOrigin = match startFromTail with Some true -> Some (Impl.readTailPositionForTranche store) | _ -> None)
