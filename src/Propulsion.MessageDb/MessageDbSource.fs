namespace Propulsion.MessageDb

open FSharp.Control
open Npgsql
open NpgsqlTypes
open Propulsion.Feed
open Propulsion.Internal
open System
open System.Threading
open System.Threading.Tasks

module private GetCategoryMessages =
    [<Literal>]
    let getCategoryMessages =
        "select position, type, data, metadata, id::uuid, time, stream_name, global_position
         from get_category_messages($1, $2, $3);"
    let prepareCommand connection (category: TrancheId) (fromPositionInclusive: int64) (batchSize: int) =
        let cmd = new NpgsqlCommand(getCategoryMessages, connection)
        cmd.Parameters.AddWithValue(NpgsqlDbType.Text, TrancheId.toString category) |> ignore
        cmd.Parameters.AddWithValue(NpgsqlDbType.Bigint, fromPositionInclusive) |> ignore
        cmd.Parameters.AddWithValue(NpgsqlDbType.Bigint, int64 batchSize) |> ignore
        cmd

module private GetLastPosition =
    [<Literal>]
    let getLastPosition = "select max(global_position) from messages where category(stream_name) = $1;"
    let prepareCommand connection category =
        let cmd = new NpgsqlCommand(getLastPosition, connection)
        cmd.Parameters.AddWithValue(NpgsqlDbType.Text, TrancheId.toString category) |> ignore
        cmd

module Internal =
    let createConnectionAndOpen connectionString ct = task {
        let conn = new NpgsqlConnection(connectionString)
        do! conn.OpenAsync(ct)
        return conn }

    let private jsonNull = ReadOnlyMemory(System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(null))

    type System.Data.Common.DbDataReader with
        member reader.GetJson idx =
            if reader.IsDBNull(idx) then jsonNull
            else reader.GetString(idx) |> Text.Encoding.UTF8.GetBytes |> ReadOnlyMemory

    type MessageDbCategoryClient(connectionString) =
        let connect = createConnectionAndOpen connectionString
        let parseRow (reader: System.Data.Common.DbDataReader) =
            let et, data, meta = reader.GetString(1), reader.GetJson 2, reader.GetJson 3
            let sz = data.Length + meta.Length + et.Length
            let event = FsCodec.Core.TimelineEvent.Create(
                index = reader.GetInt64(0), // index within the stream, 0 based
                eventType = et, data = data, meta = meta, eventId = reader.GetGuid(4),
                context = reader.GetInt64(7) + 1L, // global_position is passed through the Context for checkpointing purposes
                timestamp = DateTimeOffset(DateTime.SpecifyKind(reader.GetDateTime(5), DateTimeKind.Utc)),
                size = sz) // precomputed Size is required for stats purposes when fed to a StreamsSink
            let sn = reader.GetString(6) |> FsCodec.StreamName.parse
            struct (sn, event)

        member _.ReadCategoryMessages(category: TrancheId, fromPositionInclusive: int64, batchSize: int, ct) : Task<Core.Batch<_>> = task {
            use! conn = connect ct
            use command = GetCategoryMessages.prepareCommand conn category fromPositionInclusive batchSize

            use! reader = command.ExecuteReaderAsync(ct)
            let events = [| while reader.Read() do parseRow reader |]

            let checkpoint = match Array.tryLast events with Some (_, ev) -> unbox<int64> ev.Context | None -> fromPositionInclusive
            return ({ checkpoint = Position.parse checkpoint; items = events; isTail = events.Length = 0 } : Core.Batch<_>) }

        member _.TryReadCategoryLastVersion(category: TrancheId, ct) : Task<int64 voption> = task {
            use! conn = connect ct
            use command = GetLastPosition.prepareCommand conn category

            use! reader = command.ExecuteReaderAsync(ct)
            return if reader.Read() then ValueSome (reader.GetInt64 0) else ValueNone }

    let internal readBatch batchSize (store : MessageDbCategoryClient) struct (category, pos, ct) : Task<Core.Batch<_>> =
        let positionInclusive = Position.toInt64 pos
        store.ReadCategoryMessages(category, positionInclusive, batchSize, ct)

    let internal readTailPositionForTranche (store : MessageDbCategoryClient) trancheId ct : Task<Position> = task {
        match! store.TryReadCategoryLastVersion(trancheId, ct) with
        | ValueSome lastEventPos -> return Position.parse (lastEventPos + 1L)
        | ValueNone -> return Position.initial }

type MessageDbSource =
    inherit Propulsion.Feed.Core.TailingFeedSource
    val tranches: TrancheId[]
    new(log, statsInterval,
        connectionString, batchSize, tailSleepInterval,
        checkpoints, sink, categories,
        // Override default start position to be at the tail of the index. Default: Replay all events.
        ?startFromTail, ?sourceId) =
        let client = Internal.MessageDbCategoryClient(connectionString)
        // let readStartPosition = match startFromTail with Some true -> Some (Internal.readTailPositionForTranche client) | _ -> None
        let tail = Propulsion.Feed.Core.TailingFeedSource.readOne (Internal.readBatch batchSize client)
        { inherit Propulsion.Feed.Core.TailingFeedSource(
            log, statsInterval, defaultArg sourceId FeedSourceId.wellKnownId, tailSleepInterval, checkpoints,
            (match startFromTail with Some true -> Some (Internal.readTailPositionForTranche client) | _ -> None),
            sink, string, tail);
            tranches = categories |> Array.map TrancheId.parse }

    abstract member ListTranches : ct : CancellationToken -> Task<Propulsion.Feed.TrancheId[]>
    default x.ListTranches(_ct) = task { return x.tranches }

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
            return! x.Checkpoint(CancellationToken.None)
        finally statsInterval.SleepUntilTriggerCleared() }
