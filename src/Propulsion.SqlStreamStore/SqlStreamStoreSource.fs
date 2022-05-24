namespace Propulsion.SqlStreamStore

type StreamEvent = Propulsion.Streams.StreamEvent<byte[]>

module private Impl =

    open Propulsion.Infrastructure // AwaitTaskCorrect

    let toStreamEvent (dataJson : string) (msg: SqlStreamStore.Streams.StreamMessage) : StreamEvent =
        let inline len0ToNull (x : _[]) = match x with null -> null | x when x.Length = 0 -> null | x -> x
        let e = FsCodec.Core.TimelineEvent.Create
                  ( int64 msg.StreamVersion,
                    msg.Type,
                    (match dataJson with null -> null | x -> x |> System.Text.Encoding.UTF8.GetBytes |> len0ToNull),
                    msg.JsonMetadata |> System.Text.Encoding.UTF8.GetBytes |> len0ToNull,
                    msg.MessageId,
                    timestamp = System.DateTimeOffset(msg.CreatedUtc))
        { stream = Propulsion.Streams.StreamName.internalParseSafe msg.StreamId; event = e }
    let readWithDataAsStreamEvent (msg : SqlStreamStore.Streams.StreamMessage) = async {
        let! json = msg.GetJsonData() |> Async.AwaitTaskCorrect
        return toStreamEvent json msg }
    let readPage hydrateBodies batchSize (store : SqlStreamStore.IStreamStore) pos : Async<Propulsion.Feed.Internal.Batch<_>> = async {
        let! ct = Async.CancellationToken
        let! page = store.ReadAllForwards(Propulsion.Feed.Position.toInt64 pos, batchSize, hydrateBodies, ct) |> Async.AwaitTaskCorrect
        let! items =
            if hydrateBodies then page.Messages |> Seq.map readWithDataAsStreamEvent |> Async.Sequential
            else async { return page.Messages |> Array.map (toStreamEvent null) }
        return { checkpoint = Propulsion.Feed.Position.parse page.NextPosition; items = items; isTail = page.IsEnd } }

type SqlStreamStoreSource
    (   log : Serilog.ILogger, statsInterval,
        store : SqlStreamStore.IStreamStore, batchSize, tailSleepInterval,
        checkpoints : Propulsion.Feed.IFeedCheckpointStore,
        sink : Propulsion.ProjectorPipeline<Propulsion.Ingestion.Ingester<seq<StreamEvent>, Propulsion.Submission.SubmissionBatch<int, StreamEvent>>>,
        // If the Handler does not require the bodies of the events, we can save significant Read Capacity by not having to load them. Default: false
        ?hydrateBodies,
        ?sourceId) =
    inherit Propulsion.Feed.Internal.AllFeedSource(log, statsInterval, defaultArg sourceId FeedSourceId.wellKnownId, tailSleepInterval,
                                                   Impl.readPage (hydrateBodies = Some true) batchSize store, checkpoints, sink)
