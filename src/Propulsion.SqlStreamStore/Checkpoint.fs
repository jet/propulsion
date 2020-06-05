namespace Propulsion.SqlStreamStore

open System
open System.Data
open Microsoft.Data.SqlClient
open Dapper

[<Struct;NoComparison>]
[<CLIMutable>]
type CheckpointEntry =
    {
        Stream           : string
        ConsumerGroup    : string
        Position         : Nullable<int64>
    }

type ICheckpointer =
    abstract member GetPosition: stream: string * consumerGroup: string -> Async<Nullable<int64>>
    abstract member CommitPosition: stream: string * consumerGroup: string * position: int64 -> Async<unit>

[<RequireQualifiedAccess>]
module SqlCheckpointer =

    let connection (connString) =
        new SqlConnection(connString)

    let createIfNotExists (conn: IDbConnection) =
        async {
            do! conn.ExecuteAsync(
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
                |> Async.Ignore
        }

    let commitPosition (conn: IDbConnection) (stream: string) (consumerGroup: string) (position: int64) =
        async {
             do! conn.ExecuteAsync(
                     """UPDATE Checkpoints
                        SET Position = @Position
                        WHERE Stream = @Stream AND ConsumerGroup=@ConsumerGroup

                        IF @@ROWCOUNT = 0
                            INSERT INTO Checkpoints (Stream, ConsumerGroup, Position)
                            VALUES (@Stream, @ConsumerGroup, @Position)
                        """, { Stream = stream; ConsumerGroup = consumerGroup; Position = Nullable(position) })
                 |> Async.AwaitTaskCorrect
                 |> Async.Ignore
        }

    let getPosition (conn: IDbConnection) (stream: string) (consumerGroup: string) =
        async {
            let! res =
                conn.QueryAsync<CheckpointEntry>(
                    """SELECT * FROM Checkpoints WHERE Stream = @Stream AND ConsumerGroup = @ConsumerGroup""",
                    { Stream = stream; ConsumerGroup = consumerGroup; Position = Nullable() })
                |> Async.AwaitTaskCorrect

            match Seq.tryHead res with
            | Some res ->
                return res.Position
            | None ->
                return Nullable()
        }

type SqlCheckpointer(connString: string) =

    member this.CreateSchemaIfNotExists() =
        async {
            use conn = SqlCheckpointer.connection connString
            do! SqlCheckpointer.createIfNotExists conn
        }

    interface ICheckpointer with
        member this.GetPosition (stream: string, consumerGroup: string) =
            async {
                use conn = SqlCheckpointer.connection connString
                return! SqlCheckpointer.getPosition conn stream consumerGroup
            }
        member this.CommitPosition (stream: string, consumerGroup: string, position: int64) =
            async {
                use conn = SqlCheckpointer.connection connString
                do! SqlCheckpointer.commitPosition conn stream consumerGroup position
            }
