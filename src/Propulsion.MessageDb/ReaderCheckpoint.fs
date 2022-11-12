module Propulsion.MessageDb.ReaderCheckpoint

open Npgsql
open NpgsqlTypes
open Propulsion.Feed
open Propulsion.Infrastructure
open System

let createIfNotExists (conn : NpgsqlConnection, schema: string) =
    let cmd = conn.CreateCommand()
    cmd.CommandText <- $"
      create table if not exists {schema}.propulsion_checkpoints (
        stream_name text not null,
        consumer_group text not null,
        global_position bigint not null,
        primary key (stream_name, consumer_group)
      );
    "
    cmd.ExecuteNonQueryAsync() |> Async.AwaitTaskCorrect |> Async.Ignore<int>

let commitPosition (conn : NpgsqlConnection, schema: string) (stream : string) (consumerGroup : string) (position : int64) = async {
     let cmd = conn.CreateCommand()
     cmd.CommandText <-
         $"insert into {schema}.propulsion_checkpoints(stream_name, consumer_group, global_position)
           values (@StreamName, @ConsumerGroup, @GlobalPosition)
           on conflict (stream_name, consumer_group)
           do update set global_position = @GlobalPosition;"
     cmd.Parameters.AddWithValue("StreamName", NpgsqlDbType.Text, stream) |> ignore
     cmd.Parameters.AddWithValue("ConsumerGroup", NpgsqlDbType.Text, consumerGroup) |> ignore
     cmd.Parameters.AddWithValue("GlobalPosition", NpgsqlDbType.Bigint, position) |> ignore

     let! ct = Async.CancellationToken
     do! cmd.ExecuteNonQueryAsync(ct) |> Async.AwaitTaskCorrect |> Async.Ignore<int> }

let tryGetPosition (conn : NpgsqlConnection, schema : string) (stream : string) (consumerGroup : string) = async {
    let cmd = conn.CreateCommand()
    cmd.CommandText <-
        $"select global_position from {schema}.propulsion_checkpoints where stream_name = @StreamName and consumer_group = @ConsumerGroup"

    cmd.Parameters.AddWithValue("StreamName", NpgsqlDbType.Text, stream) |> ignore
    cmd.Parameters.AddWithValue("ConsumerGroup", NpgsqlDbType.Text, consumerGroup) |> ignore

    use reader = cmd.ExecuteReader()
    let! ct = Async.CancellationToken
    let! hasRow = reader.ReadAsync(ct) |> Async.AwaitTaskCorrect
    return if hasRow then Some (reader.GetInt64 0) else None }

type Service(connString : string, schema: string, consumerGroupName, defaultCheckpointFrequency) =

    let streamName source tranche =
        match SourceId.toString source, TrancheId.toString tranche with
        | s, null -> s
        | s, tid -> String.Join("_", s, tid)

    member _.CreateSchemaIfNotExists() = async {
        use conn = new NpgsqlConnection(connString)
        let! ct = Async.CancellationToken
        do! conn.OpenAsync(ct) |> Async.AwaitTaskCorrect
        return! createIfNotExists (conn, schema) }

    interface IFeedCheckpointStore with

        member _.Start(source, tranche, ?establishOrigin) = async {
            use conn = new NpgsqlConnection(connString)
            let! ct = Async.CancellationToken
            do! conn.OpenAsync(ct) |> Async.AwaitTaskCorrect
            let! maybePos = tryGetPosition (conn, schema) (streamName source tranche) consumerGroupName
            let! pos =
                match maybePos, establishOrigin with
                | Some pos, _ -> async { return Position.parse pos }
                | None, Some f -> f
                | None, None -> async { return Position.initial }
            return defaultCheckpointFrequency, pos }

        member _.Commit(source, tranche, pos) = async {
            use conn = new NpgsqlConnection(connString)
            let! ct = Async.CancellationToken
            do! conn.OpenAsync(ct) |> Async.AwaitTaskCorrect
            return! commitPosition (conn, schema) (streamName source tranche) consumerGroupName (Position.toInt64 pos) }


