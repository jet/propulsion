namespace Propulsion.SqlStreamStore

// Adapted from Ben Hyrman's blog post SQL Server Retries with Dapper and Polly
// licensed under CC BY-SA 4.0
// https://hyr.mn/dapper-and-polly/
module RetryPolicy =

    open System
    open System.ComponentModel
    open System.Data.SqlClient

    open Serilog
    open Polly
    open Polly.Retry

    let private retryTimes =
        [
            TimeSpan.FromSeconds(1.)
            TimeSpan.FromSeconds(2.)
            TimeSpan.FromSeconds(3.)
        ]

    // This comes from:
    // https://raw.githubusercontent.com/aspnet/EntityFrameworkCore/master/src/EFCore.SqlServer/Storage/Internal/SqlServerTransientExceptionDetector.cs
    // and
    // https://github.com/Azure/elastic-db-tools/blob/master/Src/ElasticScale.Client/ElasticScale.Common/TransientFaultHandling/Implementation/SqlDatabaseTransientErrorDetectionStrategy.cs
    // With the addition of
    // SQL Error 11001 (connection failed)
    module SqlServerTransientExceptionDetector =

        let shouldRetrySqlException (exc: SqlException) =
            let mutable shouldRetry = false

            for err in exc.Errors do
                match err.Number with
                // SQL Error Code: 49920
                // Cannot process request. Too many operations in progress for subscription "%ld".
                // The service is busy processing multiple requests for this subscription.
                // Requests are currently blocked for resource optimization. Query sys.dm_operation_status for operation status.
                // Wait until pending requests are complete or delete one of your pending requests and retry your request later.
                | 49920
                // SQL Error Code: 49919
                // Cannot process create or update request. Too many create or update operations in progress for subscription "%ld".
                // The service is busy processing multiple create or update requests for your subscription or server.
                // Requests are currently blocked for resource optimization. Query sys.dm_operation_status for pending operations.
                // Wait till pending create or update requests are complete or delete one of your pending requests and
                // retry your request later.
                | 49919
                // SQL Error Code: 49918
                // Cannot process request. Not enough resources to process request.
                // The service is currently busy.Please retry the request later.
                | 49918
                // SQL Error Code: 41839
                // Transaction exceeded the maximum number of commit dependencies.
                | 41839
                // SQL Error Code: 41325
                // The current transaction failed to commit due to a serializable validation failure.
                | 41325
                // SQL Error Code: 41305
                // The current transaction failed to commit due to a repeatable read validation failure.
                | 41305
                // SQL Error Code: 41302
                // The current transaction attempted to update a record that has been updated since the transaction started.
                | 41302
                // SQL Error Code: 41301
                // Dependency failure: a dependency was taken on another transaction that later failed to commit.
                | 41301
                // SQL Error Code: 40613
                // Database XXXX on server YYYY is not currently available. Please retry the connection later.
                // If the problem persists, contact customer support, and provide them the session tracing ID of ZZZZZ.
                | 40613
                // SQL Error Code: 40501
                // The service is currently busy. Retry the request after 10 seconds. Code: (reason code to be decoded).
                | 40501
                // SQL Error Code: 40197
                // The service has encountered an error processing your request. Please try again.
                | 40197
                // SQL Error Code: 11001
                // A connection attempt failed
                | 11001
                // SQL Error Code: 10929
                // Resource ID: %d. The %s minimum guarantee is %d, maximum limit is %d and the current usage for the database is %d.
                // However, the server is currently too busy to support requests greater than %d for this database.
                // For more information, see http://go.microsoft.com/fwlink/?LinkId=267637. Otherwise, please try again.
                | 10929
                // SQL Error Code: 10928
                // Resource ID: %d. The %s limit for the database is %d and has been reached. For more information,
                // see http://go.microsoft.com/fwlink/?LinkId=267637.
                | 10928
                // SQL Error Code: 10060
                // A network-related or instance-specific error occurred while establishing a connection to SQL Server.
                // The server was not found or was not accessible. Verify that the instance name is correct and that SQL Server
                // is configured to allow remote connections. (provider: TCP Provider, error: 0 - A connection attempt failed
                // because the connected party did not properly respond after a period of time, or established connection failed
                // because connected host has failed to respond.)"}
                | 10060
                // SQL Error Code: 10054
                // A transport-level error has occurred when sending the request to the server.
                // (provider: TCP Provider, error: 0 - An existing connection was forcibly closed by the remote host.)
                | 10054
                // SQL Error Code: 10053
                // A transport-level error has occurred when receiving results from the server.
                // An established connection was aborted by the software in your host machine.
                | 10053
                // SQL Error Code: 1205
                // Deadlock
                | 1205
                // SQL Error Code: 233
                // The client was unable to establish a connection because of an error during connection initialization process before login.
                // Possible causes include the following: the client tried to connect to an unsupported version of SQL Server;
                // the server was too busy to accept new connections; or there was a resource limitation (insufficient memory or maximum
                // allowed connections) on the server. (provider: TCP Provider, error: 0 - An existing connection was forcibly closed by
                // the remote host.)
                | 233
                // SQL Error Code: 121
                // The semaphore timeout period has expired
                | 121
                // SQL Error Code: 64
                // A connection was successfully established with the server, but then an error occurred during the login process.
                // (provider: TCP Provider, error: 0 - The specified network name is no longer available.)
                | 64
                // DBNETLIB Error Code: 20
                // The instance of SQL Server you attempted to connect to does not support encryption.
                | 20 ->
                    shouldRetry <- true
                | _ ->
                    ()

            shouldRetry

        let shouldRetryWin32Exception (exc: Win32Exception) =
            match exc.NativeErrorCode with
            // Timeout expired
            | 0x102
            // Semaphore timeout expired
            | 0x121 -> true
            | _ -> false

    let defaultSqlLedgerRetryPolicy (logger: ILogger) : AsyncRetryPolicy =
        Policy
            .Handle<SqlException>(fun exc -> SqlServerTransientExceptionDetector.shouldRetrySqlException exc)
            .Or<TimeoutException>()
            .OrInner<Win32Exception>(fun exc -> SqlServerTransientExceptionDetector.shouldRetryWin32Exception exc)
            .WaitAndRetryAsync(retryTimes, (fun exc retryTimeSpan retryCount context ->
                logger.Warning(
                    exc,
                    "Error talking to SqlLedger database, will retry after {retryTimeSpan}. Retry attempt {retryCount}",
                    retryTimeSpan,
                    retryCount)))
