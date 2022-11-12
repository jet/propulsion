namespace Propulsion.MessageDb

open System
open System.Data.Common
open System.Diagnostics
open FSharp.Control
open FsCodec
open FsCodec.Core
open Npgsql
open NpgsqlTypes
open Propulsion.Feed

type MessageDbCategoryReader(connectionString) =
    let readonly (bytes: byte array) = ReadOnlyMemory.op_Implicit(bytes)
    let readRow (reader: DbDataReader) =
            let readNullableString idx = if reader.IsDBNull(idx) then None else Some (reader.GetString idx)
            let timestamp = DateTimeOffset(DateTime.SpecifyKind(reader.GetDateTime(7), DateTimeKind.Utc))
            let streamName = reader.GetString(9)
            let event =TimelineEvent.Create(
               index = reader.GetInt64(0),
               eventType = reader.GetString(1),
               data = (reader.GetFieldValue<byte array>(2) |> readonly),
               meta = (reader.GetFieldValue<byte array>(3) |> readonly),
               eventId = reader.GetGuid(4),
               ?correlationId = readNullableString 5,
               ?causationId = readNullableString 6,
               timestamp = timestamp)

            struct(StreamName.parse streamName, event)
    member _.ReadCategoryMessages(category: TrancheId, fromPositionInclusive: int64, batchSize: int, ct) = task {
        use conn = new NpgsqlConnection(connectionString)
        do! conn.OpenAsync(ct)
        let command = conn.CreateCommand()
        command.CommandText <- "select
                                   global_position, type, data, metadata, id::uuid,
                                   (metadata::jsonb->>'$correlationId')::text,
                                   (metadata::jsonb->>'$causationId')::text,
                                   time, stream_name
                                 from get_category_messages(@Category, @Position, @BatchSize);"
        command.Parameters.AddWithValue("Category", NpgsqlDbType.Text, TrancheId.toString category) |> ignore
        command.Parameters.AddWithValue("Position", NpgsqlDbType.Bigint, fromPositionInclusive) |> ignore
        command.Parameters.AddWithValue("BatchSize", NpgsqlDbType.Bigint, int64 batchSize) |> ignore

        let! reader = command.ExecuteReaderAsync(ct)
        let! hasRow = reader.ReadAsync(ct)
        let mutable hasRow = hasRow

        let events = ResizeArray()
        while hasRow do
            events.Add(readRow reader)
            let! nextHasRow = reader.ReadAsync(ct)
            hasRow <- nextHasRow
        return events.ToArray() }
    member _.ReadCategoryLastVersion(category: TrancheId, ct) = task {
        use conn = new NpgsqlConnection(connectionString)
        do! conn.OpenAsync(ct)
        let command = conn.CreateCommand()
        command.CommandText <- "select max(global_position) from messages where category(stream_name) = @Category;"
        command.Parameters.AddWithValue("Category", NpgsqlDbType.Text, TrancheId.toString category) |> ignore

        let! reader = command.ExecuteReaderAsync(ct)
        let! hasRow = reader.ReadAsync(ct)
        return if hasRow then reader.GetInt64(0) else 0L
    }

module private Impl =
    open Propulsion.Infrastructure // AwaitTaskCorrect

    let readBatch batchSize (store : MessageDbCategoryReader) (category, pos) : Async<Propulsion.Feed.Core.Batch<_>> = async {
        let! ct = Async.CancellationToken
        let positionInclusive = Position.toInt64 pos
        let! page = store.ReadCategoryMessages(category, positionInclusive, batchSize, ct) |> Async.AwaitTaskCorrect
        let checkpoint = match Array.tryLast page with Some struct(_, evt) -> evt.Index + 1L | None -> positionInclusive
        return { checkpoint = Position.parse checkpoint; items = page; isTail = false } }

    let readTailPositionForTranche (store : MessageDbCategoryReader) trancheId : Async<Propulsion.Feed.Position> = async {
        let! ct = Async.CancellationToken
        let! lastEventPos = store.ReadCategoryLastVersion(trancheId, ct) |> Async.AwaitTaskCorrect
        return Position.parse(lastEventPos + 1L) }

type MessageDbSource
    (   log : Serilog.ILogger, statsInterval,
        reader: MessageDbCategoryReader, batchSize, tailSleepInterval,
        checkpoints : Propulsion.Feed.IFeedCheckpointStore, sink : Propulsion.Streams.Default.Sink,
        // Override default start position to be at the tail of the index. Default: Replay all events.
        ?startFromTail,
        ?sourceId) =
    inherit Propulsion.Feed.Core.TailingFeedSource
        (   log, statsInterval, defaultArg sourceId FeedSourceId.wellKnownId, tailSleepInterval, checkpoints,
            (   if startFromTail <> Some true then None
                else Some (Impl.readTailPositionForTranche reader)),
            sink,
            (fun req -> asyncSeq {
                let sw = Stopwatch.StartNew()
                let! b = Impl.readBatch batchSize reader req
                yield sw.Elapsed, b }),
            string
            )
        // (   log, statsInterval, defaultArg sourceId FeedSourceId.wellKnownId, tailSleepInterval,
        //     checkpoints, sink,
        //     Impl.readPage (hydrateBodies = Some true) batchSize store)
