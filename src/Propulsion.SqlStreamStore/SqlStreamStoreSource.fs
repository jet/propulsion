namespace Propulsion.SqlStreamStore

open System

module private Impl =

    open Propulsion.AsyncHelpers // AwaitTaskCorrect
    open Propulsion.Feed

    let toTimelineEvent (dataJson : string) (msg: SqlStreamStore.Streams.StreamMessage) =
        let inline len0ToNull (x : _[]) = match x with null -> null | x when x.Length = 0 -> null | x -> x
        FsCodec.Core.TimelineEvent.Create
            (   int64 msg.StreamVersion,
                msg.Type,
                (match dataJson with null -> null | x -> x |> System.Text.Encoding.UTF8.GetBytes |> len0ToNull),
                msg.JsonMetadata |> System.Text.Encoding.UTF8.GetBytes |> len0ToNull,
                msg.MessageId,
                timestamp = DateTimeOffset(msg.CreatedUtc))
    let readWithDataAsTimelineEvent (msg : SqlStreamStore.Streams.StreamMessage) = async {
        let! json = msg.GetJsonData() |> Async.AwaitTaskCorrect
        return toTimelineEvent json msg }
    let readPage excludeBodies maxBatchSize (store : SqlStreamStore.IStreamStore) (_tranche, pos) : Async<Page<_>> = async {
        let! ct = Async.CancellationToken
        let! page = store.ReadAllForwards(Position.toInt64 pos, maxBatchSize, not excludeBodies, ct) |> Async.AwaitTaskCorrect
        let! items =
            if excludeBodies then async { return page.Messages |> Array.map (toTimelineEvent null) }
            else page.Messages |> Seq.map readWithDataAsTimelineEvent |> Async.Sequential
        return { checkpoint = Position.parse page.NextPosition; items = items; isTail = page.IsEnd } }

type StreamEvent = Propulsion.Streams.StreamEvent<byte[]>

type SqlStreamStoreSource
    (   log : Serilog.ILogger, statsInterval : TimeSpan,
        sourceId, maxBatchSize, tailSleepInterval : TimeSpan,
        checkpoints : Propulsion.Feed.IFeedCheckpointStore,
        store : SqlStreamStore.IStreamStore,
        sink : Propulsion.ProjectorPipeline<Propulsion.Ingestion.Ingester<seq<StreamEvent>, Propulsion.Submission.SubmissionBatch<int, StreamEvent>>>,
        ?defaultCheckpointEventInterval : TimeSpan,
        ?excludeBodies) =
    inherit Propulsion.Feed.FeedSource(log, statsInterval, sourceId, tailSleepInterval,
                                       checkpoints, (defaultArg defaultCheckpointEventInterval (TimeSpan.FromSeconds 5.)),
                                       Impl.readPage (excludeBodies = Some true) maxBatchSize store, sink)

    member _.Pump(consumerGroupName) =
        let readTranches () = async { return [| Propulsion.Feed.TrancheId.parse consumerGroupName |] }
        base.Pump(readTranches)
