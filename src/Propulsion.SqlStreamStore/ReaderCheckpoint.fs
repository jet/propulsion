module Propulsion.SqlStreamStore.ReaderCheckpoint

open Dapper
open FSharp.Control
open Microsoft.Data.SqlClient
open Propulsion.Feed
open System
open System.Data

[<Struct; NoComparison; CLIMutable>]
type CheckpointEntry = { Stream: string; ConsumerGroup: string; Position: Nullable<int64> }

let createConnection connString =
    new SqlConnection(connString)

let createIfNotExists (conn: IDbConnection) =
    conn.ExecuteAsync(
        """IF NOT (EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'Checkpoints'))
           BEGIN
               CREATE TABLE [dbo].[Checkpoints] (
                   Stream nvarchar(200) not null,
                   ConsumerGroup nvarchar(50) not null,
                   Position bigint null,
                   CONSTRAINT [PK_Checkpoints] PRIMARY KEY CLUSTERED
                   (
                       [Stream] ASC,
                       [ConsumerGroup] ASC
                   ) WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF)
               );
           END""")
    |> Task.ignore<int>

let commitPosition (conn: IDbConnection) (stream: string) (consumerGroup: string) (position: int64) =
     conn.ExecuteAsync(
         """UPDATE Checkpoints
            SET Position = @Position
            WHERE Stream = @Stream AND ConsumerGroup=@ConsumerGroup

            IF @@ROWCOUNT = 0
                INSERT INTO Checkpoints (Stream, ConsumerGroup, Position)
                VALUES (@Stream, @ConsumerGroup, @Position)
            """, { Stream = stream; ConsumerGroup = consumerGroup; Position = Nullable(position) })
     |> Task.ignore<int>

let tryGetPosition (conn: IDbConnection) (stream: string) (consumerGroup: string) = task {
    let! res = conn.QueryAsync<CheckpointEntry>(
         """SELECT * FROM Checkpoints WHERE Stream = @Stream AND ConsumerGroup = @ConsumerGroup""",
         { Stream = stream; ConsumerGroup = consumerGroup; Position = Nullable() })
    return Seq.tryHead res |> Option.bind (fun r -> Option.ofNullable r.Position) }

type Service(connString: string, consumerGroupName) =

    let streamName source tranche =
        match SourceId.toString source, TrancheId.toString tranche with
        | s, null -> s
        | s, tid -> String.Join("_", s, tid)

    member _.CreateSchemaIfNotExists() = task {
        use conn = createConnection connString
        return! createIfNotExists conn }

    interface IFeedCheckpointStore with

        member _.Start(source, tranche, establishOrigin, ct) = task {
            use conn = createConnection connString
            let! maybePos = tryGetPosition conn (streamName source tranche) consumerGroupName
            let! pos =
                match maybePos, establishOrigin with
                | Some pos, _ -> task { return Position.parse pos }
                | None, Some f -> f.Invoke ct
                | None, None -> task { return Position.initial }
            return pos }

        member _.Commit(source, tranche, pos, _ct) = task {
            use conn = createConnection connString
            return! commitPosition conn (streamName source tranche) consumerGroupName (Position.toInt64 pos) }
