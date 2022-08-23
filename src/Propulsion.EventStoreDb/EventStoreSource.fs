namespace Propulsion.EventStoreDb

module private Impl =

    open FSharp.Control

    let toStreamEvent (x : EventStore.Client.ResolvedEvent) : Propulsion.Streams.Default.StreamEvent =
        let e = x.Event
        // TOCONSIDER wire e.Metadata["$correlationId"] and ["$causationId"] into correlationId and causationId
        // https://eventstore.org/docs/server/metadata-and-reserved-names/index.html#event-metadata
        let n, d, m, eu, ts = e.EventNumber, e.Data, e.Metadata, e.EventId, System.DateTimeOffset e.Created
        let e = FsCodec.Core.TimelineEvent.Create(n.ToInt64(), e.EventType, d, m, eu.ToGuid(), correlationId = null, causationId = null, timestamp = ts)
        Propulsion.Streams.StreamName.internalParseSafe x.Event.EventStreamId, e
    let readBatch hydrateBodies batchSize (store : EventStore.Client.EventStoreClient) pos : Async<Propulsion.Feed.Core.Batch<_>> = async {
        let! ct = Async.CancellationToken
        let pos = let p = pos |> Propulsion.Feed.Position.toInt64 |> uint64 in EventStore.Client.Position(p, p)
        let res = store.ReadAllAsync(EventStore.Client.Direction.Forwards, pos, batchSize, hydrateBodies, cancellationToken = ct)
        let! events =
            AsyncSeq.ofAsyncEnum res
            |> AsyncSeq.map (fun x -> struct (x, toStreamEvent x))
            |> AsyncSeq.toArrayAsync
        let p = match Array.tryLast events with Some (r, _) -> int64 r.Event.Position.CommitPosition | None -> -1L
        return { checkpoint = Propulsion.Feed.Position.parse p; items = Array.map (fun struct (_, e) -> e) events; isTail = events.LongLength = batchSize } }

type EventStoreSource
    (   log : Serilog.ILogger, statsInterval,
        store : EventStore.Client.EventStoreClient, batchSize, tailSleepInterval,
        checkpoints : Propulsion.Feed.IFeedCheckpointStore, sink : Propulsion.Streams.Default.Sink,
        // If the Handler does not utilize the bodies of the events, we can avoid shipping them from the Store in the first instance. Default false.
        ?hydrateBodies,
        // TODO borrow impl of determining tail from Propulsion.EventStore, pass that to base as ?establishOrigin
        // ?fromTail,
        ?sourceId) =
    inherit Propulsion.Feed.Core.AllFeedSource
        (   log, statsInterval, defaultArg sourceId FeedSourceId.wellKnownId, tailSleepInterval,
            Impl.readBatch (hydrateBodies = Some true) batchSize store, checkpoints, sink)
