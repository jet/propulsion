namespace Propulsion.EventStoreDb

module private Impl =

    open EventStore.Client
    open FSharp.Control

    let private toTimelineEvent (e : EventRecord) =
        // TOCONSIDER wire e.Metadata["$correlationId"] and ["$causationId"] into correlationId and causationId
        // https://eventstore.org/docs/server/metadata-and-reserved-names/index.html#event-metadata
        let n, d, m, eu, ts = e.EventNumber, e.Data, e.Metadata, e.EventId, System.DateTimeOffset e.Created
        FsCodec.Core.TimelineEvent.Create(n.ToInt64(), e.EventType, d, m, eu.ToGuid(), correlationId = null, causationId = null, timestamp = ts)
    let private toItems categoryFilter (events : EventRecord array) : Propulsion.Streams.Default.StreamEvent array = [|
        for e in events do
            let sn = Propulsion.Streams.StreamName.internalParseSafe e.EventStreamId
            if categoryFilter (FsCodec.StreamName.category sn) then
                yield sn, Equinox.EventStoreDb.ClientCodec.timelineEvent e |]
    let private checkpointPos (xs : EventRecord array) =
        match Array.tryLast xs with Some e -> int64 e.Position.CommitPosition | None -> -1L
        |> Propulsion.Feed.Position.parse
    let readBatch hydrateBodies batchSize categoryFilter (store : EventStoreClient) pos : Async<Propulsion.Feed.Core.Batch<_>> = async {
        let! ct = Async.CancellationToken
        let pos = let p = pos |> Propulsion.Feed.Position.toInt64 |> uint64 in Position(p, p)
        let res = store.ReadAllAsync(Direction.Forwards, pos, batchSize, hydrateBodies, cancellationToken = ct)
        let! batch = AsyncSeq.ofAsyncEnum res |> AsyncSeq.map (fun e -> e.Event) |> AsyncSeq.toArrayAsync
        return { checkpoint = checkpointPos batch; items = toItems categoryFilter batch; isTail = batch.LongLength <> batchSize } }

    // @scarvel8: event_global_position = 256 x 1024 x 1024 x chunk_number + chunk_header_size (128) + event_position_offset_in_chunk
    let private chunk (pos : Position) = uint64 pos.CommitPosition >>> 28

    let readTailPositionForTranche (log : Serilog.ILogger) (client : EventStoreClient) _trancheId : Async<Propulsion.Feed.Position> = async {
        let! ct = Async.CancellationToken
        let lastItemBatch = client.ReadAllAsync(Direction.Backwards, Position.End, maxCount = 1, cancellationToken = ct)
        let! items = AsyncSeq.ofAsyncEnum lastItemBatch |> AsyncSeq.toArrayAsync
        let pos = (Array.last items).Event.Position
        let max = int64 pos.CommitPosition
        log.Information("EventStore Tail Position: @ {pos} ({chunks} chunks, ~{gb:n1}GiB)", pos, chunk pos, Propulsion.Internal.Log.miB (float max / 1024.))
        return Propulsion.Feed.Position.parse max }

type EventStoreSource
    (   log : Serilog.ILogger, statsInterval,
        client : EventStore.Client.EventStoreClient, batchSize, tailSleepInterval,
        checkpoints : Propulsion.Feed.IFeedCheckpointStore, sink : Propulsion.Streams.Default.Sink,
        categoryFilter : string -> bool,
        // If the Handler does not utilize the Data/Meta of the events, we can avoid shipping them from the Store in the first instance. Default false.
        ?hydrateBodies,
        // Override default start position to be at the tail of the index. Default: Replay all events.
        ?startFromTail,
        ?sourceId) =
    inherit Propulsion.Feed.Core.AllFeedSource
        (   log, statsInterval, defaultArg sourceId FeedSourceId.wellKnownId, tailSleepInterval,
            Impl.readBatch (hydrateBodies = Some true) batchSize categoryFilter client, checkpoints, sink,
            ?establishOrigin = if startFromTail <> Some true then None else Some (Impl.readTailPositionForTranche log client))
