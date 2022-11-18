module Propulsion.MessageDb.ReaderCheckpoint

open Npgsql
open NpgsqlTypes
open Propulsion.Feed
open Propulsion.Infrastructure

let [<Literal>] TableName = "propulsion_checkpoint"

module internal Impl =

    open System.Threading
    open System.Threading.Tasks

    let createIfNotExists (conn : NpgsqlConnection, schema: string) ct = task {
        let cmd = conn.CreateCommand(CommandText = $"create table if not exists {schema}.{TableName} (
                                                       source text not null,
                                                       tranche text not null,
                                                       consumer_group text not null,
                                                       position bigint not null,
                                                       primary key (source, tranche, consumer_group));")
        do! cmd.ExecuteNonQueryAsync(ct) : Task }

    let commitPosition (conn : NpgsqlConnection, schema: string) source tranche (consumerGroup : string) (position : int64) ct = task {
        let cmd = conn.CreateCommand(CommandText = $"insert into {schema}.{TableName}(source, tranche, consumer_group, position)
                                                     values (@Source, @Tranche, @ConsumerGroup, @Position)
                                                     on conflict (source, tranche, consumer_group)
                                                     do update set position = @Position;")
        cmd.Parameters.AddWithValue("Source", NpgsqlDbType.Text, SourceId.toString source) |> ignore
        cmd.Parameters.AddWithValue("Tranche", NpgsqlDbType.Text, TrancheId.toString tranche) |> ignore
        cmd.Parameters.AddWithValue("ConsumerGroup", NpgsqlDbType.Text, consumerGroup) |> ignore
        cmd.Parameters.AddWithValue("Position", NpgsqlDbType.Bigint, position) |> ignore
        do! cmd.ExecuteNonQueryAsync(ct) :> Task }

    let tryGetPosition (conn : NpgsqlConnection, schema : string) source tranche (consumerGroup : string) (ct : CancellationToken) = task {
        let cmd = conn.CreateCommand(CommandText = $"select position from {schema}.{TableName}
                                                      where source = @Source
                                                        and tranche = @Tranche
                                                        and consumer_group = @ConsumerGroup")
        cmd.Parameters.AddWithValue("Source", NpgsqlDbType.Text, SourceId.toString source) |> ignore
        cmd.Parameters.AddWithValue("Tranche", NpgsqlDbType.Text, TrancheId.toString tranche) |> ignore
        cmd.Parameters.AddWithValue("ConsumerGroup", NpgsqlDbType.Text, consumerGroup) |> ignore
        use! reader = cmd.ExecuteReaderAsync(ct)
        return if reader.Read() then ValueSome (reader.GetInt64 0) else ValueNone }

    let exec connString f= async {
        let! ct = Async.CancellationToken
        use! conn = connect connString ct |> Async.AwaitTaskCorrect
        return! f conn ct |> Async.AwaitTaskCorrect }

type CheckpointStore(connString : string, schema : string, consumerGroupName, defaultCheckpointFrequency : System.TimeSpan) =

    let exec f = Impl.exec connString f
    let setPos source tranche pos =
        let commit conn = Impl.commitPosition (conn, schema) source tranche consumerGroupName (Position.toInt64 pos)
        exec commit

    member _.CreateSchemaIfNotExists() : Async<unit> =
        let creat conn = Impl.createIfNotExists (conn, schema)
        exec creat

    member _.Override(source, tranche, pos : Position) : Async<unit> =
        setPos source tranche pos

    interface IFeedCheckpointStore with

        member _.Start(source, tranche, ?establishOrigin) =
            let start conn ct = task {
                let! maybePos = Impl.tryGetPosition (conn, schema) source tranche consumerGroupName ct |> Async.AwaitTaskCorrect
                let! pos =
                    match maybePos, establishOrigin with
                    | ValueSome pos, _ -> async { return Position.parse pos }
                    | ValueNone, Some f -> f
                    | ValueNone, None -> async { return Position.initial }
                return struct (defaultCheckpointFrequency, pos) }
            exec start

        member _.Commit(source, tranche, pos) : Async<unit> =
            setPos source tranche pos
