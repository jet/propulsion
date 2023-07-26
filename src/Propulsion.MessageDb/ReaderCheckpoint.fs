module Propulsion.MessageDb.ReaderCheckpoint

open FSharp.Control
open Npgsql
open NpgsqlTypes
open Propulsion.Feed
open Propulsion.Internal
open System.Threading

let [<Literal>] TableName = "propulsion_checkpoint"

module internal Impl =

    let createIfNotExists (conn : NpgsqlConnection, schema: string) ct =
        let cmd = conn.CreateCommand(CommandText = $"create table if not exists {schema}.{TableName} (
                                                       source text not null,
                                                       tranche text not null,
                                                       consumer_group text not null,
                                                       position bigint not null,
                                                       primary key (source, tranche, consumer_group));")
        cmd.ExecuteNonQueryAsync(ct)

    let commitPosition (conn : NpgsqlConnection, schema: string) source tranche (consumerGroup : string) (position : int64) ct =
        let cmd = conn.CreateCommand(CommandText = $"insert into {schema}.{TableName}(source, tranche, consumer_group, position)
                                                     values (@Source, @Tranche, @ConsumerGroup, @Position)
                                                     on conflict (source, tranche, consumer_group)
                                                     do update set position = @Position;")
        cmd.Parameters.AddWithValue("Source", NpgsqlDbType.Text, SourceId.toString source) |> ignore
        cmd.Parameters.AddWithValue("Tranche", NpgsqlDbType.Text, TrancheId.toString tranche) |> ignore
        cmd.Parameters.AddWithValue("ConsumerGroup", NpgsqlDbType.Text, consumerGroup) |> ignore
        cmd.Parameters.AddWithValue("Position", NpgsqlDbType.Bigint, position) |> ignore
        cmd.ExecuteNonQueryAsync(ct)

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

    let exec connString f ct = task {
        use! conn = Internal.createConnectionAndOpen connString ct
        return! f conn ct }

type CheckpointStore(connString : string, schema : string, consumerGroupName) =

    let exec f = Impl.exec connString f
    let setPos source tranche pos ct =
        let commit conn = Impl.commitPosition (conn, schema) source tranche consumerGroupName (Position.toInt64 pos)
        exec commit ct

    member _.CreateSchemaIfNotExists([<O; D null>]?ct) =
        let creat conn = Impl.createIfNotExists (conn, schema)
        exec creat (defaultArg ct CancellationToken.None) |> Task.ignore

    member _.Override(source, tranche, pos : Position, ct) =
        setPos source tranche pos ct |> Task.ignore

    interface IFeedCheckpointStore with

        member _.Start(source, tranche, establishOrigin, ct) =
            let start conn ct = task {
                let! maybePos = Impl.tryGetPosition (conn, schema) source tranche consumerGroupName ct
                let! pos =
                    match maybePos, establishOrigin with
                    | ValueSome pos, _ -> task { return Position.parse pos }
                    | ValueNone, Some f -> f.Invoke ct
                    | ValueNone, None -> task { return Position.initial }
                return pos }
            exec start ct

        member _.Commit(source, tranche, pos, ct) =
            setPos source tranche pos ct
