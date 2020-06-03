namespace Propulsion.SqlStreamStore

open System
open SqlStreamStore
open Medallion.Threading.Sql

module Locked =

    /// Creates a distributed lock that manages its own db connection.
    let distributedLock (connString: string) (consumerGroup: string) =
        let lockName = SqlDistributedLock.GetSafeLockName(sprintf "lock-%s" consumerGroup)
        SqlDistributedLock(lockName, connString, SqlDistributedLockConnectionStrategy.Azure)

    /// Attempt acquiring a lock, waiting indefinitely.
    let acquireDistributedLock (lock: SqlDistributedLock) =
        async {
            let! ct = Async.CancellationToken
            let! handle =
                lock.AcquireAsync(cancellationToken = ct)
                |> Async.AwaitTaskCorrect

            return handle
        }

    type SqlStreamStoreSource with

        /// Run SqlStreamStore within a distributed sql lock,
        /// to prevent consumers competing.
        static member RunLocked
                (logger : Serilog.ILogger,
                 store: IStreamStore,
                 checkpointer: ICheckpointer,
                 sink : Propulsion.ProjectorPipeline<_>,
                 consumerGroup: string,
                 lockConnString: string,
                 ?reacquireLockInterval: TimeSpan,
                 ?statsInterval: TimeSpan,
                 ?readerMaxBatchSize: int,
                 ?readerSleepInterval: TimeSpan) : Async<unit> =
            async {
                // Distributed lock provides no API for monitoring whether the lock is still being held,
                // which makes obtaining and holding the lock impractical. As a workaround, the source is going to be periodically stopped,
                // the lock reacquired, and the work restarted in a controlled way at set intervals to minimize overlap.
                let reacquireLockInterval = defaultArg reacquireLockInterval (TimeSpan.FromMinutes(15.))

                let lock = distributedLock lockConnString consumerGroup

                let! ct = Async.CancellationToken
                while not ct.IsCancellationRequested do

                    logger.Information ("Attempting to acquire lock for consumer group {consumerGroup}", consumerGroup)

                    let! handle = acquireDistributedLock lock

                    logger.Information ("Acquired lock for consumer group {consumerGroup}", consumerGroup)

                    try
                        do! SqlStreamStoreSource.RunInternal(
                                logger,
                                store,
                                checkpointer,
                                sink,
                                consumerGroup,
                                ?statsInterval = statsInterval,
                                ?readerMaxBatchSize = readerMaxBatchSize,
                                ?readerSleepInterval = readerSleepInterval,
                                readerStopAfterInterval = reacquireLockInterval)
                    finally
                        handle.Dispose()
                        logger.Information ("Released lock for consumer group {consumerGroup}", consumerGroup)
            }