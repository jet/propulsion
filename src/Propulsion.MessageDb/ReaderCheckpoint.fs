module Propulsion.MessageDb.ReaderCheckpoint

open Npgsql
open NpgsqlTypes
open Propulsion.Feed
open Propulsion.Infrastructure

let createIfNotExists (conn : NpgsqlConnection, schema: string) =
    let cmd = conn.CreateCommand()
    cmd.CommandText <- $"
      create table if not exists {schema}.propulsion_checkpoint (
        source text not null,
        tranche text not null,
        consumer_group text not null,
        position bigint not null,
        primary key (source, tranche, consumer_group)
      )
    "
    cmd.ExecuteNonQueryAsync() |> Async.AwaitTaskCorrect |> Async.Ignore<int>

let commitPosition (conn : NpgsqlConnection, schema: string) source tranche (consumerGroup : string) (position : int64)
    = async {
    let cmd = conn.CreateCommand()
    cmd.CommandText <-
        $"insert into {schema}.propulsion_checkpoint(source, tranche, consumer_group, position)
          values (@Source, @Tranche, @ConsumerGroup, @GlobalPosition)
          on conflict (source, tranche, consumer_group)
          do update set position = @Position;"
    cmd.Parameters.AddWithValue("Source", NpgsqlDbType.Text, SourceId.toString source) |> ignore
    cmd.Parameters.AddWithValue("Tranche", NpgsqlDbType.Text, TrancheId.toString tranche) |> ignore
    cmd.Parameters.AddWithValue("ConsumerGroup", NpgsqlDbType.Text, consumerGroup) |> ignore
    cmd.Parameters.AddWithValue("Position", NpgsqlDbType.Bigint, position) |> ignore

    let! ct = Async.CancellationToken
    do! cmd.ExecuteNonQueryAsync(ct) |> Async.AwaitTaskCorrect |> Async.Ignore<int> }

let tryGetPosition (conn : NpgsqlConnection, schema : string) source tranche (consumerGroup : string) = async {
    let cmd = conn.CreateCommand()
    cmd.CommandText <-
        $"select position from {schema}.propulsion_checkpoint
          where source = @Source
            and tranche = @Tranche
            and consumer_group = @ConsumerGroup"

    cmd.Parameters.AddWithValue("Source", NpgsqlDbType.Text, SourceId.toString source) |> ignore
    cmd.Parameters.AddWithValue("Tranche", NpgsqlDbType.Text, TrancheId.toString tranche) |> ignore
    cmd.Parameters.AddWithValue("ConsumerGroup", NpgsqlDbType.Text, consumerGroup) |> ignore

    let! ct = Async.CancellationToken
    use! reader = cmd.ExecuteReaderAsync(ct) |> Async.AwaitTaskCorrect
    return if reader.Read() then ValueSome (reader.GetInt64 0) else ValueNone }

type CheckpointStore(connString : string, schema: string, consumerGroupName, defaultCheckpointFrequency) =
    let connect = Npgsql.connect connString

    member _.CreateSchemaIfNotExists() = async {
        let! ct = Async.CancellationToken
        use! conn = connect ct |> Async.AwaitTaskCorrect
        return! createIfNotExists (conn, schema) }

    interface IFeedCheckpointStore with

        member _.Start(source, tranche, ?establishOrigin) = async {
            let! ct = Async.CancellationToken
            use! conn = connect ct |> Async.AwaitTaskCorrect
            let! maybePos = tryGetPosition (conn, schema) source tranche consumerGroupName
            let! pos =
                match maybePos, establishOrigin with
                | ValueSome pos, _ -> async { return Position.parse pos }
                | ValueNone, Some f -> f
                | ValueNone, None -> async { return Position.initial }
            return defaultCheckpointFrequency, pos }

        member _.Commit(source, tranche, pos) = async {
            let! ct = Async.CancellationToken
            use! conn = connect ct |> Async.AwaitTaskCorrect
            return! commitPosition (conn, schema) source tranche consumerGroupName (Position.toInt64 pos) }


