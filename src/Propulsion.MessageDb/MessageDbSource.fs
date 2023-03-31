namespace Propulsion.MessageDb

open FSharp.Control
open Propulsion.Internal
open System
open System.Threading
open System.Threading.Tasks

module internal Npgsql =

    let connect connectionString ct = task {
        let conn = new Npgsql.NpgsqlConnection(connectionString)
        do! conn.OpenAsync(ct)
        return conn }

module Internal =

    open NpgsqlTypes
    open Propulsion.Feed
    open System.Data.Common
    open System.Text.Json

    module private Json =
        let private jsonNull = ReadOnlyMemory(JsonSerializer.SerializeToUtf8Bytes(null))

        let fromReader idx (reader: DbDataReader) =
            if reader.IsDBNull(idx) then jsonNull
            else reader.GetString(idx) |> Text.Encoding.UTF8.GetBytes |> ReadOnlyMemory

    type MessageDbCategoryClient(connectionString) =
        let connect = Npgsql.connect connectionString
        let parseRow (reader: DbDataReader) =
            let readNullableString idx = if reader.IsDBNull(idx) then None else Some (reader.GetString idx)
            let readUtf8String idx = Json.fromReader idx reader
            let et, data, meta = reader.GetString(1), readUtf8String 2, readUtf8String 3
            let sz = data.Length + meta.Length + et.Length
            let event = FsCodec.Core.TimelineEvent.Create(
                index = reader.GetInt64(0), // index within the stream, 0 based
                eventType = et, data = data, meta = meta, eventId = reader.GetGuid(4),
                ?correlationId = readNullableString 5, ?causationId = readNullableString 6,
                context = reader.GetInt64(9) + 1L, // global_position is passed through the Context for checkpointing purposes
                timestamp = DateTimeOffset(DateTime.SpecifyKind(reader.GetDateTime(7), DateTimeKind.Utc)),
                size = sz) // precomputed Size is required for stats purposes when fed to a StreamsSink
            let sn = reader.GetString(8) |> FsCodec.StreamName.parse
            struct (sn, event)

        member _.ReadCategoryMessages(category: TrancheId, fromPositionInclusive: int64, batchSize: int, ct) : Task<Core.Batch<_>> = task {
            use! conn = connect ct
            let command = conn.CreateCommand(CommandText = "select position, type, data, metadata, id::uuid,
                                                                   (metadata::jsonb->>'$correlationId')::text,
                                                                   (metadata::jsonb->>'$causationId')::text,
                                                                   time, stream_name, global_position
                                                            from get_category_messages(@Category, @Position, @BatchSize);")
            command.Parameters.AddWithValue("Category", NpgsqlDbType.Text, TrancheId.toString category) |> ignore
            command.Parameters.AddWithValue("Position", NpgsqlDbType.Bigint, fromPositionInclusive) |> ignore
            command.Parameters.AddWithValue("BatchSize", NpgsqlDbType.Bigint, int64 batchSize) |> ignore

            use! reader = command.ExecuteReaderAsync(ct)
            let events = [| while reader.Read() do parseRow reader |]

            let checkpoint = match Array.tryLast events with Some (_, ev) -> unbox<int64> ev.Context | None -> fromPositionInclusive
            return ({ checkpoint = Position.parse checkpoint; items = events; isTail = events.Length = 0 } : Core.Batch<_>) }

        member _.ReadCategoryLastVersion(category: TrancheId, ct) : Task<int64> = task {
            use! conn = connect ct
            let command = conn.CreateCommand(CommandText = "select max(global_position) from messages where category(stream_name) = @Category;")
            command.Parameters.AddWithValue("Category", NpgsqlDbType.Text, TrancheId.toString category) |> ignore

            use! reader = command.ExecuteReaderAsync(ct)
            return if reader.Read() then reader.GetInt64(0) else 0L }

    let internal readBatch batchSize (store : MessageDbCategoryClient) (category, pos, ct) : Task<Core.Batch<_>> =
        let positionInclusive = Position.toInt64 pos
        store.ReadCategoryMessages(category, positionInclusive, batchSize, ct)

    let internal readTailPositionForTranche (store : MessageDbCategoryClient) trancheId ct : Task<Position> = task {
        let! lastEventPos = store.ReadCategoryLastVersion(trancheId, ct)
        return Position.parse lastEventPos }

type MessageDbSource internal
    (   log : Serilog.ILogger, statsInterval,
        client : Internal.MessageDbCategoryClient, batchSize, tailSleepInterval,
        checkpoints : Propulsion.Feed.IFeedCheckpointStore, sink : Propulsion.Streams.Default.Sink,
        tranches, ?startFromTail, ?sourceId) =
    inherit Propulsion.Feed.Core.TailingFeedSource
        (   log, statsInterval, defaultArg sourceId FeedSourceId.wellKnownId, tailSleepInterval, checkpoints,
            (   if startFromTail <> Some true then None
                else Some (Internal.readTailPositionForTranche client)),
            sink,
            (fun (cat, pos, ct) -> taskSeq {
                let sw = Stopwatch.start ()
                let! b = Internal.readBatch batchSize client (cat, pos, ct)
                yield struct (sw.Elapsed, b) }),
            string)
    new(    log, statsInterval,
            connectionString, batchSize, tailSleepInterval,
            checkpoints, sink,
            categories,
            // Override default start position to be at the tail of the index. Default: Replay all events.
            ?startFromTail, ?sourceId) =
        MessageDbSource(log, statsInterval, Internal.MessageDbCategoryClient(connectionString),
                        batchSize, tailSleepInterval, checkpoints, sink,
                        categories |> Array.map Propulsion.Feed.TrancheId.parse,
                        ?startFromTail = startFromTail, ?sourceId = sourceId)

    abstract member ListTranches : ct : CancellationToken -> Task<Propulsion.Feed.TrancheId array>
    default _.ListTranches(_ct) = task { return tranches }

    abstract member Pump : CancellationToken -> Task<unit>
    default x.Pump(ct) = base.Pump(x.ListTranches, ct)

    abstract member Start : unit -> Propulsion.SourcePipeline<Propulsion.Feed.Core.FeedMonitor>
    default x.Start() = base.Start(x.Pump)

    /// Pumps to the Sink until either the specified timeout has been reached, or all items in the Source have been fully consumed
    member x.RunUntilCaughtUp(timeout : TimeSpan, statsInterval : IntervalTimer) = task {
        let sw = Stopwatch.start ()
        use pipeline = x.Start()

        try Task.Delay(timeout).ContinueWith(fun _ -> pipeline.Stop()) |> ignore

            let initialReaderTimeout = TimeSpan.FromMinutes 1.
            do! pipeline.Monitor.AwaitCompletion(initialReaderTimeout, awaitFullyCaughtUp = true, logInterval = TimeSpan.FromSeconds 30)
            pipeline.Stop()

            if sw.ElapsedSeconds > 2 then statsInterval.Trigger()
            // force a final attempt to flush anything not already checkpointed (normally checkpointing is at 5s intervals)
            return! x.Checkpoint()
        finally statsInterval.SleepUntilTriggerCleared() }
