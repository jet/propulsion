namespace Propulsion.SqlStreamStore

open System
open System.Data
open System.Data.SqlClient
open Dapper

[<Struct;>]
[<CLIMutable>]
type CheckpointRequest =
    {
        Stream           : string
        ConsumerGroup    : string
    }

[<Struct;NoComparison>]
[<CLIMutable>]
type CheckpointEntry =
    {
        Stream           : string
        ConsumerGroup    : string
        Position         : Nullable<int64>
    }
    static member FromRequest(request: CheckpointRequest, ?position: int64) =
        {
            Stream        = request.Stream
            ConsumerGroup = request.ConsumerGroup
            Position      = Option.toNullable position
        }

type ICheckpointer =
    abstract member GetPosition: CheckpointRequest-> Async<Nullable<int64>>
    abstract member CommitPosition: CheckpointEntry -> Async<unit>

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

    let commitPosition (conn: IDbConnection) (entry: CheckpointEntry) =
        async {
             do! conn.ExecuteAsync(
                     """UPDATE Checkpoints
                        SET Position = @Position
                        WHERE Stream = @Stream AND ConsumerGroup=@ConsumerGroup

                        IF @@ROWCOUNT = 0
                            INSERT INTO Checkpoints (Stream, ConsumerGroup, Position)
                            VALUES (@Stream, @ConsumerGroup, @Position)
                        """, entry)
                 |> Async.AwaitTaskCorrect
                 |> Async.Ignore
        }

    let getPosition (conn: IDbConnection) (request: CheckpointRequest) =
        async {
            let! res =
                conn.QueryAsync<CheckpointEntry>(
                    """SELECT * FROM Checkpoints WHERE Stream = @Stream AND ConsumerGroup = @ConsumerGroup""", request)
                |> Async.AwaitTaskCorrect

            match Seq.tryHead res with
            | Some res ->
                return res.Position
            | None ->
                return Nullable()
        }

type SqlCheckpointer(connString : string) =

    member this.CreateIfNotExists() =
        async {
            use conn = SqlCheckpointer.connection connString
            do! SqlCheckpointer.createIfNotExists conn
        }

    interface ICheckpointer with
        member this.GetPosition (request: CheckpointRequest) =
            async {
                use conn = SqlCheckpointer.connection connString
                return! SqlCheckpointer.getPosition conn request
            }
        member this.CommitPosition (entry: CheckpointEntry) =
            async {
                use conn = SqlCheckpointer.connection connString
                do! SqlCheckpointer.commitPosition conn entry
            }