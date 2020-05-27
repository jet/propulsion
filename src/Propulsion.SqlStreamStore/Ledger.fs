namespace Propulsion.SqlStreamStore

open System
open System.Collections.Concurrent
open System.Data
open System.Data.SqlClient
open Dapper
open Medallion.Threading.Sql
open Polly

[<Struct;>]
[<CLIMutable>]
type LedgerRequest =
    {
        Stream           : string
        ConsumerGroup    : string
    }

[<Struct;NoComparison>]
[<CLIMutable>]
type LedgerEntry =
    {
        Stream           : string
        ConsumerGroup    : string
        Position         : Nullable<int64>
    }
    static member FromRequest(request: LedgerRequest, ?position: int64) =
        {
            Stream        = request.Stream
            ConsumerGroup = request.ConsumerGroup
            Position      = Option.toNullable position
        }

type ILedger =
    abstract member GetPosition: LedgerRequest-> Async<Nullable<int64>>
    abstract member CommitPosition: LedgerEntry -> Async<unit>
    abstract member Lock: consumerGroup: string -> Async<IDisposable>

[<RequireQualifiedAccess>]
module SqlLedger =

    let connection (connString) =
        new SqlConnection(connString)

    let createIfNotExists (conn: IDbConnection) =
        async {
            do! conn.ExecuteAsync(
                    """IF NOT (EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'Ledger'))
                       BEGIN
                           CREATE TABLE [dbo].[Ledger] (
                               Stream nvarchar(200) not null,
                               ConsumerGroup nvarchar(50) not null,
                               Position bigint null,
                               CONSTRAINT [PK_Ledger] PRIMARY KEY CLUSTERED
                               (
                                   [Stream] ASC,
                                   [ConsumerGroup] ASC
                               ) WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF)
                           );
                       END""")
                |> Async.AwaitTaskCorrect
                |> Async.Ignore
        }

    let commitPosition (conn: IDbConnection) (retryPolicy: AsyncPolicy) (entry: LedgerEntry) =
        async {
             do! retryPolicy.ExecuteAsync(fun () ->
                     conn.ExecuteAsync(
                         """UPDATE Ledger
                            SET Position = @Position
                            WHERE Stream = @Stream AND ConsumerGroup=@ConsumerGroup

                            IF @@ROWCOUNT = 0
                                INSERT INTO Ledger (Stream, ConsumerGroup, Position)
                                VALUES (@Stream, @ConsumerGroup, @Position)
                            """, entry))
                 |> Async.AwaitTaskCorrect
                 |> Async.Ignore
        }

    let getPosition (conn: IDbConnection) (retryPolicy: AsyncPolicy) (request: LedgerRequest) =
        async {
            let! res =
                retryPolicy.ExecuteAsync(fun () ->
                    conn.QueryAsync<LedgerEntry>(
                        """SELECT * FROM Ledger WHERE Stream = @Stream AND ConsumerGroup = @ConsumerGroup""", request))
                |> Async.AwaitTaskCorrect

            match Seq.tryHead res with
            | Some res ->
                return res.Position
            | None ->
                return Nullable()
        }

    module Locked =

        /// Creates a distributed lock that manages its own db connection.
        let distributedLock (connString: string) (consumerGroup: string) =
            let lockName = SqlDistributedLock.GetSafeLockName(sprintf "lock-%s" consumerGroup)
            SqlDistributedLock(lockName, connString, SqlDistributedLockConnectionStrategy.Azure)

        /// Attempt acquiring a lock with a given timeout.
        let acquireDistributedLock (timeout: TimeSpan) (lock: SqlDistributedLock) =
            async {
                let! ct = Async.CancellationToken
                let! handle =
                    lock.TryAcquireAsync(timeout = timeout, cancellationToken = ct)
                    |> Async.AwaitTaskCorrect

                return handle
            }

type SqlLedger(connString : string, ?retryPolicy: AsyncPolicy, ?lockAcquisitionTimeout: TimeSpan) =

    let lockAcquisitionTimeout = defaultArg lockAcquisitionTimeout TimeSpan.Zero
    let retryPolicy = defaultArg retryPolicy (upcast Policy.NoOpAsync())

    let locks = ConcurrentDictionary<string, SqlDistributedLock>()

    member this.CreateIfNotExists() =
        async {
            use conn = SqlLedger.connection connString
            do! SqlLedger.createIfNotExists conn
        }

    interface ILedger with
        member this.GetPosition (request: LedgerRequest) =
            async {
                use conn = SqlLedger.connection connString
                return! SqlLedger.getPosition conn retryPolicy request
            }
        member this.CommitPosition (entry: LedgerEntry) =
            async {
                use conn = SqlLedger.connection connString
                do! SqlLedger.commitPosition conn retryPolicy entry
            }
        member this.Lock (consumerGroup : string) =
            async {
                let lock = locks.GetOrAdd(consumerGroup, fun cg -> SqlLedger.Locked.distributedLock connString cg)
                let! handle = SqlLedger.Locked.acquireDistributedLock lockAcquisitionTimeout lock

                return handle
            }

open System.Collections.Generic
open System.Threading

type InMemoryLedger() =

    let mutable lockWaitTimeoutMs = 10000

    let locks = ConcurrentDictionary<string, Semaphore>()
    let store = Dictionary<LedgerRequest, Nullable<int64>>()

    let disposable (func: unit -> unit) =
        { new System.IDisposable with member this.Dispose() = func () }

    member this.Store = store
    member this.LockWaitTimeoutMs with get () = lockWaitTimeoutMs and set value = lockWaitTimeoutMs <- value

    interface ILedger with
        member this.GetPosition (request: LedgerRequest) =
            async {
                let key = request
                match store.TryGetValue(key) with
                | true, value -> return value
                | false, _ -> return Nullable()
            }
        member this.CommitPosition (entry: LedgerEntry) =
            async {
                let key = { Stream = entry.Stream; ConsumerGroup = entry.ConsumerGroup }
                match store.TryGetValue(key) with
                | true, _ -> store.[key] <- entry.Position
                | false, _ -> store.[key] <- Nullable(int64 entry.Position)

            }
        member this.Lock (consumerGroup: string) =
            async {
                let sem : Semaphore = locks.GetOrAdd(consumerGroup, fun _ ->  new Semaphore(1, 1))

                if sem.WaitOne(lockWaitTimeoutMs) then
                    return disposable (fun () -> sem.Release() |> ignore )
                else
                    return null
            }
