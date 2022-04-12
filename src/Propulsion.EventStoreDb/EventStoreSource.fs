namespace Propulsion.EventStoreDb

open FSharp.Control
open System

type StreamEvent = Propulsion.Streams.StreamEvent<byte[]>

module private Impl =

    let toStreamEvent (x : EventStore.Client.ResolvedEvent) : StreamEvent =
        let e = x.Event
        // TOCONSIDER wire e.Metadata["$correlationId"] and ["$causationId"] into correlationId and causationId
        // https://eventstore.org/docs/server/metadata-and-reserved-names/index.html#event-metadata
        let n, d, m, eu, ts = e.EventNumber, e.Data, e.Metadata, e.EventId, DateTimeOffset e.Created
        let inline (|Len0ToNull|) (x : _[]) = match x with null -> null | x when x.Length = 0 -> null | x -> x
        let Len0ToNull d, Len0ToNull m = d.ToArray(), m.ToArray()
        {   stream = Propulsion.Streams.StreamName.internalParseSafe x.Event.EventStreamId
            event = FsCodec.Core.TimelineEvent.Create(n.ToInt64(), e.EventType, d, m, eu.ToGuid(), correlationId = null, causationId = null, timestamp = ts) }
    let readBatch excludeBodies maxBatchSize (store : EventStore.Client.EventStoreClient) pos : Async<Propulsion.Feed.Internal.Batch<_>> = async {
        let! ct = Async.CancellationToken
        let pos = let p = pos |> Propulsion.Feed.Position.toInt64 |> uint64 in EventStore.Client.Position(p, p)
        let res = store.ReadAllAsync(EventStore.Client.Direction.Forwards, pos, maxBatchSize, not excludeBodies, cancellationToken = ct)
        let! events =
            AsyncSeq.ofAsyncEnum res
            |> AsyncSeq.map (fun x -> struct (x, toStreamEvent x))
            |> AsyncSeq.toArrayAsync
        let p = match Array.tryLast events with Some (r, _) -> int64 r.Event.Position.CommitPosition | None -> -1L
        return { checkpoint = Propulsion.Feed.Position.parse p; items = Array.map (fun struct (_, e) -> e) events; isTail = events.LongLength = maxBatchSize } }

type EventStoreSource
    (   log : Serilog.ILogger, statsInterval : TimeSpan,
        store : EventStore.Client.EventStoreClient, sourceId, maxBatchSize, tailSleepInterval : TimeSpan,
        checkpoints : Propulsion.Feed.IFeedCheckpointStore,
        sink : Propulsion.ProjectorPipeline<Propulsion.Ingestion.Ingester<seq<StreamEvent>, Propulsion.Submission.SubmissionBatch<int, StreamEvent>>>,
        // If the Handler does not utilize the bodies of the events, we can avoid shipping them from the Store in the first instance. Default false.
        ?excludeBodies) =
    inherit Propulsion.Feed.Internal.AllFeedSource(log, statsInterval, sourceId, tailSleepInterval,
                                                   Impl.readBatch (excludeBodies = Some true) maxBatchSize store, checkpoints, sink)
