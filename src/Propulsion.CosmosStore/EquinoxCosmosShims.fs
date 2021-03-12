/// Placeholder until such time as the V3 SDK has proper CFP support
/// At that point, the standard Equinox.CosmosStore.CosmosClientFactory can be used instead
namespace Equinox.Cosmos

open Microsoft.Azure.Documents
open Serilog
open System

type OAttribute = System.Runtime.InteropServices.OptionalAttribute
type DAttribute = System.Runtime.InteropServices.DefaultParameterValueAttribute

[<RequireQualifiedAccess>]
module private Regex =
    open System.Text.RegularExpressions

    let DefaultTimeout = TimeSpan.FromMilliseconds 250.
    let private mkRegex p = Regex(p, RegexOptions.None, DefaultTimeout)

    /// Active pattern for branching on successful regex matches
    let (|Match|_|) (pattern : string) (input : string) =
        let m = (mkRegex pattern).Match input
        if m.Success then Some m else None

[<RequireQualifiedAccess; NoComparison>]
type Discovery =
    | UriAndKey of databaseUri:Uri * key:string
    /// Implements connection string parsing logic curiously missing from the CosmosDB SDK
    static member FromConnectionString (connectionString: string) =
        match connectionString with
        | _ when String.IsNullOrWhiteSpace connectionString -> nullArg "connectionString"
        | Regex.Match "^\s*AccountEndpoint\s*=\s*([^;\s]+)\s*;\s*AccountKey\s*=\s*([^;\s]+)\s*;?\s*$" m ->
            let uri = m.Groups.[1].Value
            let key = m.Groups.[2].Value
            UriAndKey (Uri uri, key)
        | _ -> invalidArg "connectionString" "unrecognized connection string format; must be `AccountEndpoint=https://...;AccountKey=...=;`"

[<RequireQualifiedAccess>]
type ConnectionMode =
    /// Default mode, uses Https - inefficient as uses a double hop
    | Gateway
    /// Most efficient, but requires direct connectivity
    | Direct
    // More efficient than Gateway, but suboptimal
    | DirectHttps

type Connector
    (   /// Timeout to apply to individual reads/write round-trips going to CosmosDb
        requestTimeout: TimeSpan,
        /// Maximum number of times attempt when failure reason is a 429 from CosmosDb, signifying RU limits have been breached
        maxRetryAttemptsOnRateLimitedRequests: int,
        /// Maximum number of seconds to wait (especially if a higher wait delay is suggested by CosmosDb in the 429 response)
        // naming matches SDK ver >=3
        maxRetryWaitTimeOnRateLimitedRequests: TimeSpan,
        /// Log to emit connection messages to
        log : ILogger,
        /// Connection limit for Gateway Mode (default 1000)
        [<O; D(null)>]?gatewayModeMaxConnectionLimit,
        /// Connection mode (default: ConnectionMode.Gateway (lowest perf, least trouble))
        [<O; D(null)>]?mode : ConnectionMode,
        /// consistency mode (default: ConsistencyLevel.Session)
        [<O; D(null)>]?defaultConsistencyLevel : ConsistencyLevel,

        /// Retries for read requests, over and above those defined by the mandatory policies
        [<O; D(null)>]?readRetryPolicy,
        /// Retries for write requests, over and above those defined by the mandatory policies
        [<O; D(null)>]?writeRetryPolicy,
        /// Additional strings identifying the context of this connection; should provide enough context to disambiguate all potential connections to a cluster
        /// NB as this will enter server and client logs, it should not contain sensitive information
        [<O; D(null)>]?tags : (string*string) seq,
        /// Inhibits certificate verification when set to <c>true</c>, i.e. for working with the CosmosDB Emulator (default <c>false</c>)
        [<O; D(null)>]?bypassCertificateValidation : bool) =
    do if log = null then nullArg "log"

    let logName (uri : Uri) name =
        let name = String.concat ";" <| seq {
            yield name
            match tags with None -> () | Some tags -> for key, value in tags do yield sprintf "%s=%s" key value }
        let sanitizedName = name.Replace('\'','_').Replace(':','_') // sic; Align with logging for ES Adapter
        log.ForContext("uri", uri).Information("CosmosDb Connecting {connectionName}", sanitizedName)

    /// ClientOptions (ConnectionPolicy with v2 SDK) for this Connector as configured
    member val ClientOptions =
        let co = Client.ConnectionPolicy.Default
        match mode with
        | None | Some ConnectionMode.Gateway -> co.ConnectionMode <- Client.ConnectionMode.Gateway // default; only supports Https
        | Some ConnectionMode.DirectHttps -> co.ConnectionMode <- Client.ConnectionMode.Direct; co.ConnectionProtocol <- Client.Protocol.Https // Https is default when using Direct
        | Some ConnectionMode.Direct -> co.ConnectionMode <- Client.ConnectionMode.Direct; co.ConnectionProtocol <- Client.Protocol.Tcp
        co.RetryOptions <-
            Client.RetryOptions(
                MaxRetryAttemptsOnThrottledRequests = maxRetryAttemptsOnRateLimitedRequests,
                MaxRetryWaitTimeInSeconds = (Math.Ceiling(maxRetryWaitTimeOnRateLimitedRequests.TotalSeconds) |> int))
        co.RequestTimeout <- requestTimeout
        co.MaxConnectionLimit <- defaultArg gatewayModeMaxConnectionLimit 1000
        co

    /// Yields a DocumentClient configured per the specified strategy
    member __.CreateClient
        (   /// Name should be sufficient to uniquely identify this connection within a single app instance's logs
            name, discovery : Discovery,
            /// <c>true</c> to inhibit logging of client name
            [<O; D null>]?skipLog) : Client.DocumentClient =
        let (Discovery.UriAndKey (databaseUri=uri; key=key)) = discovery
        if skipLog <> Some true then logName uri name
        let consistencyLevel = Nullable(defaultArg defaultConsistencyLevel ConsistencyLevel.Session)
        if defaultArg bypassCertificateValidation false then
            let inhibitCertCheck = new System.Net.Http.HttpClientHandler(ServerCertificateCustomValidationCallback = fun _ _ _ _ -> true)
            new Client.DocumentClient(uri, key, inhibitCertCheck, __.ClientOptions, consistencyLevel) // overload introduced in 2.2.0 SDK
        else new Client.DocumentClient(uri, key, __.ClientOptions, consistencyLevel)
