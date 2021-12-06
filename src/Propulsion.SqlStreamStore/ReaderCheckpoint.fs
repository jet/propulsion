module Propulsion.SqlStreamStore.ReaderCheckpoint

open Dapper
open Microsoft.Data.SqlClient
open Propulsion.AsyncHelpers // Infrastructure
open Propulsion.Feed
open System
open System.Data

[<Struct; NoComparison; CLIMutable>]
type CheckpointEntry = { Stream : string; ConsumerGroup : string; Position : Nullable<int64> }

let createConnection connString =
    new SqlConnection(connString)

let createIfNotExists (conn : IDbConnection) =
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
    |> Async.AwaitTaskCorrect
    |> Async.Ignore<int>

let commitPosition (conn : IDbConnection) (stream : string) (consumerGroup : string) (position : int64) =
     conn.ExecuteAsync(
         """UPDATE Checkpoints
            SET Position = @Position
            WHERE Stream = @Stream AND ConsumerGroup=@ConsumerGroup

            IF @@ROWCOUNT = 0
                INSERT INTO Checkpoints (Stream, ConsumerGroup, Position)
                VALUES (@Stream, @ConsumerGroup, @Position)
            """, { Stream = stream; ConsumerGroup = consumerGroup; Position = Nullable(position) })
     |> Async.AwaitTaskCorrect
     |> Async.Ignore<int>

let getPosition (conn : IDbConnection) (stream : string) (consumerGroup : string) = async {
    let! res =
        conn.QueryAsync<CheckpointEntry>(
            """SELECT * FROM Checkpoints WHERE Stream = @Stream AND ConsumerGroup = @ConsumerGroup""",
            { Stream = stream; ConsumerGroup = consumerGroup; Position = Nullable() })
        |> Async.AwaitTaskCorrect

    match Seq.tryHead res with
    | Some res -> return res.Position
    | None -> return Nullable() }

type Service(connString : string) =

    member _.CreateSchemaIfNotExists() = async {
        use conn = createConnection connString
        return! createIfNotExists conn }

    interface IFeedCheckpointStore with

        member _.Start(source, tranche, defaultCheckpointFrequency) = async {
            use conn = createConnection connString
            let! pos = getPosition conn (SourceId.toString source) (TrancheId.toString tranche)
            return defaultCheckpointFrequency, if pos.HasValue then Position.parse pos.Value else Position.initial }

        member _.Commit(source, tranche, pos) = async {
            use conn = createConnection connString
            return! commitPosition conn (SourceId.toString source) (TrancheId.toString tranche) (Position.toInt64 pos) }
