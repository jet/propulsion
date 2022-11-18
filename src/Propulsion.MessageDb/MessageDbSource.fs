namespace Propulsion.MessageDb

open FSharp.Control
open FsCodec
open FsCodec.Core
open NpgsqlTypes
open Propulsion.Feed
open Propulsion.Feed.Core
open Propulsion.Internal
open System
open System.Data.Common
open System.Diagnostics


type MessageDbCategoryClient(connectionString) =
    let connect = Npgsql.connect connectionString
    let parseRow (reader: DbDataReader) =
        let readNullableString idx = if reader.IsDBNull(idx) then None else Some (reader.GetString idx)
        let streamName = reader.GetString(8)
        let event = TimelineEvent.Create(
            index = reader.GetInt64(0),
            eventType = reader.GetString(1),
            data = ReadOnlyMemory(Text.Encoding.UTF8.GetBytes(reader.GetString 2)),
            meta = ReadOnlyMemory(Text.Encoding.UTF8.GetBytes(reader.GetString 3)),
            eventId = reader.GetGuid(4),
            ?correlationId = readNullableString 5,
            ?causationId = readNullableString 6,
            context = reader.GetInt64(9),
            timestamp = DateTimeOffset(DateTime.SpecifyKind(reader.GetDateTime(7), DateTimeKind.Utc)))

        struct(StreamName.parse streamName, event)
    member _.ReadCategoryMessages(category: TrancheId, fromPositionInclusive: int64, batchSize: int, ct) = task {
        use! conn = connect ct
        let command = conn.CreateCommand(
            CommandText = "select position, type, data, metadata, id::uuid,
                                  (metadata::jsonb->>'$correlationId')::text,
                                  (metadata::jsonb->>'$causationId')::text,
                                  time, stream_name, global_position
                           from get_category_messages(@Category, @Position, @BatchSize);")
        command.Parameters.AddWithValue("Category", NpgsqlDbType.Text, TrancheId.toString category) |> ignore
        command.Parameters.AddWithValue("Position", NpgsqlDbType.Bigint, fromPositionInclusive) |> ignore
        command.Parameters.AddWithValue("BatchSize", NpgsqlDbType.Bigint, int64 batchSize) |> ignore

        let mutable checkpoint = fromPositionInclusive

        use! reader = command.ExecuteReaderAsync(ct)
        let events = [| while reader.Read() do yield parseRow reader |]

        checkpoint <- match Array.tryLast events with Some (_, ev) -> unbox<int64> ev.Context | None -> checkpoint

        return { checkpoint = Position.parse checkpoint; items = events; isTail = events.Length = 0 } }
    member _.ReadCategoryLastVersion(category: TrancheId, ct) = task {
        use! conn = connect ct
        let command = conn.CreateCommand()
        command.CommandText <- "select max(global_position) from messages where category(stream_name) = @Category;"
        command.Parameters.AddWithValue("Category", NpgsqlDbType.Text, TrancheId.toString category) |> ignore

        use! reader = command.ExecuteReaderAsync(ct)
        return if reader.Read() then reader.GetInt64(0) else 0L }

module private Impl =
    open Propulsion.Infrastructure // AwaitTaskCorrect

    let readBatch batchSize (store : MessageDbCategoryClient) (category, pos) : Async<Propulsion.Feed.Core.Batch<_>> = async {
        let! ct = Async.CancellationToken
        let positionInclusive = Position.toInt64 pos
        let! x = store.ReadCategoryMessages(category, positionInclusive, batchSize, ct) |> Async.AwaitTaskCorrect
        return x }

    let readTailPositionForTranche (store : MessageDbCategoryClient) trancheId : Async<Propulsion.Feed.Position> = async {
        let! ct = Async.CancellationToken
        let! lastEventPos = store.ReadCategoryLastVersion(trancheId, ct) |> Async.AwaitTaskCorrect
        return Position.parse lastEventPos }

type MessageDbSource
    (   log : Serilog.ILogger, statsInterval,
        client: MessageDbCategoryClient, batchSize, tailSleepInterval,
        checkpoints : Propulsion.Feed.IFeedCheckpointStore, sink : Propulsion.Streams.Default.Sink,
        categories,
        // Override default start position to be at the tail of the index. Default: Replay all events.
        ?startFromTail,
        ?sourceId) =
    inherit Propulsion.Feed.Core.TailingFeedSource
        (   log, statsInterval, defaultArg sourceId FeedSourceId.wellKnownId, tailSleepInterval, checkpoints,
            (   if startFromTail <> Some true then None
                else Some (Impl.readTailPositionForTranche client)),
            sink,
            (fun req -> asyncSeq {
                let sw = Stopwatch.StartNew()
                let! b = Impl.readBatch batchSize client req
                yield sw.Elapsed, b }),
            string)
    new (log, statsInterval, connectionString, batchSize, tailSleepInterval, checkpoints, sink, trancheIds, ?startFromTail, ?sourceId) =
        MessageDbSource(log, statsInterval, MessageDbCategoryClient(connectionString),
                        batchSize, tailSleepInterval, checkpoints, sink, trancheIds, ?startFromTail=startFromTail, ?sourceId=sourceId)

    abstract member ListTranches : unit -> Async<Propulsion.Feed.TrancheId array>
    default _.ListTranches() = async { return categories |> Array.map TrancheId.parse }

    abstract member Pump : unit -> Async<unit>
    default x.Pump() = base.Pump(x.ListTranches)

    abstract member Start : unit -> Propulsion.SourcePipeline<Propulsion.Feed.Core.FeedMonitor>
    default x.Start() = base.Start(x.Pump())


    /// Pumps to the Sink until either the specified timeout has been reached, or all items in the Source have been fully consumed
    member x.RunUntilCaughtUp(timeout : TimeSpan, statsInterval : IntervalTimer) = task {
        let sw = Stopwatch.start ()
        use pipeline = x.Start()

        try System.Threading.Tasks.Task.Delay(timeout).ContinueWith(fun _ -> pipeline.Stop()) |> ignore

            let initialReaderTimeout = TimeSpan.FromMinutes 1.
            do! pipeline.Monitor.AwaitCompletion(initialReaderTimeout, awaitFullyCaughtUp = true, logInterval = TimeSpan.FromSeconds 30)
            pipeline.Stop()

            if sw.ElapsedSeconds > 2 then statsInterval.Trigger()
            // force a final attempt to flush anything not already checkpointed (normally checkpointing is at 5s intervals)
            return! x.Checkpoint()
        finally statsInterval.SleepUntilTriggerCleared() }
