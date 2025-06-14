namespace Propulsion.EventStoreDb

module private Impl =

    open EventStore.Client
    open FSharp.Control

    let private toItems streamFilter (events: EventRecord[]): Propulsion.Sinks.StreamEvent[] = [|
        for e in events do
            let sn = Propulsion.Streams.StreamName.internalParseSafe e.EventStreamId
            if streamFilter sn then
                yield sn, Equinox.EventStoreDb.ClientCodec.timelineEvent e |> FsCodec.Core.TimelineEvent.mapBodies FsCodec.Encoding.OfBlob |]
    let private checkpointPos (xs: EventRecord[]) =
        match Array.tryLast xs with Some e -> int64 e.Position.CommitPosition | None -> -1L
        |> Propulsion.Feed.Position.parse
    let readBatch withData batchSize streamFilter (store: EventStoreClient) pos ct = task {
        let pos = let p = pos |> Propulsion.Feed.Position.toInt64 |> uint64 in Position(p, p)
        let res = store.ReadAllAsync(Direction.Forwards, pos, batchSize, withData, cancellationToken = ct)
        let! batch = res |> TaskSeq.map _.Event |> TaskSeq.toArrayAsync
        return ({ checkpoint = checkpointPos batch; items = toItems streamFilter batch; isTail = batch.LongLength <> batchSize }: Propulsion.Feed.Batch<_>) }

    // @scarvel8: event_global_position = 256 x 1024 x 1024 x chunk_number + chunk_header_size (128) + event_position_offset_in_chunk
    let private chunk (pos: Position) = uint64 pos.CommitPosition >>> 28

    let readTailPositionForTranche (log: Serilog.ILogger) (client: EventStoreClient) _trancheId ct = task {
        let lastItemBatch = client.ReadAllAsync(Direction.Backwards, Position.End, maxCount = 1, cancellationToken = ct)
        let! lastItem = TaskSeq.exactlyOne lastItemBatch
        let pos = lastItem.Event.Position
        let max = int64 pos.CommitPosition
        log.Information("EventStore Tail Position: @ {pos} ({chunks} chunks, ~{gb:n1}GiB)", pos, chunk pos, Propulsion.Internal.Log.miB (float max / 1024.))
        return Propulsion.Feed.Position.parse max }

type EventStoreSource
    (   log: Serilog.ILogger, statsInterval,
        client: EventStore.Client.EventStoreClient, batchSize, tailSleepInterval,
        checkpoints: Propulsion.Feed.IFeedCheckpointStore, sink: Propulsion.Sinks.SinkPipeline,
        // The whitelist of Categories to use
        ?categories,
        // Predicate to filter <c>StreamName</c>'s to use
        ?streamFilter: System.Func<FsCodec.StreamName, bool>,
        // If the Handler does not utilize the Data/Meta of the events, we can avoid shipping them from the Store in the first instance. Default false.
        ?withData,
        // Override default start position to be at the tail of the index. Default: Replay all events.
        ?startFromTail,
        ?sourceId) =
    inherit Propulsion.Feed.Core.AllFeedSource
        (   log, statsInterval, defaultArg sourceId FeedSourceId.wellKnownId, tailSleepInterval,
            Impl.readBatch (withData = Some true) batchSize (Propulsion.Feed.Core.Categories.mapFilters categories streamFilter) client, checkpoints, sink,
            ?establishOrigin = match startFromTail with Some true -> Some (Impl.readTailPositionForTranche log client) | _ -> None)
