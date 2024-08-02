[<AutoOpen>]
module Propulsion.Tool.Infrastructure

open Serilog

// NOTE it's critical that the program establishes Serilog.Logger before anyone calls `Metrics.log`
//      Yes, that's a footgun whose symptom is logging showing '0.0R/0.0W CU @ 0 rps`, but explicitly passing a `storeLog` gets ugly fast
module Metrics =

    let [<Literal>] PropertyTag = "isMetric"
    let mutable log = Log.ForContext(PropertyTag, true)
    // In a real app, you have a separate module Store with Metrics inside, and the static initializer is not triggered too early
    // TODO get rid of this; I have up this time around :(
    let init () = log <- Log.ForContext(PropertyTag, true)

module EnvVar =

    let tryGet = System.Environment.GetEnvironmentVariable >> Option.ofObj
    let getOr raise key = tryGet key |> Option.defaultWith (fun () -> raise $"Missing Argument/Environment Variable %s{key}")

module Sinks =

    let equinoxMetricsOnly (l: LoggerConfiguration) =
        l.WriteTo.Sink(Equinox.CosmosStore.Core.Log.InternalMetrics.Stats.LogSink(categorize = true))
         .WriteTo.Sink(Equinox.DynamoStore.Core.Log.InternalMetrics.Stats.LogSink())
    let console verbose (configuration: LoggerConfiguration) =
        let outputTemplate =
            let t = "{Timestamp:HH:mm:ss} {Level:u1} {Message:lj} {SourceContext} {Properties}{NewLine}{Exception}"
            if verbose then t else t.Replace("{Properties}", "")
        configuration.WriteTo.Console(theme = Sinks.SystemConsole.Themes.AnsiConsoleTheme.Code, outputTemplate = outputTemplate)

[<System.Runtime.CompilerServices.Extension>]
type Logging() =

    [<System.Runtime.CompilerServices.Extension>]
    static member Configure(configuration: LoggerConfiguration, ?verbose) =
        configuration
            .Enrich.FromLogContext()
        |> fun c -> if verbose = Some true then c.MinimumLevel.Debug() else c

    [<System.Runtime.CompilerServices.Extension>]
    static member private Sinks(configuration: LoggerConfiguration, configureMetricsSinks, configureConsoleSink, ?isMetric) =
        let removeMetricsProps (c: LoggerConfiguration): LoggerConfiguration =
            let trim (e: Serilog.Events.LogEvent) =
                e.RemovePropertyIfPresent(Propulsion.Streams.Log.PropertyTag)
                e.RemovePropertyIfPresent(Propulsion.Feed.Core.Log.PropertyTag)
                e.RemovePropertyIfPresent(Propulsion.CosmosStore.Log.PropertyTag)
                e.RemovePropertyIfPresent(Equinox.CosmosStore.Core.Log.PropertyTag)
                e.RemovePropertyIfPresent(Equinox.DynamoStore.Core.Log.PropertyTag)
            c.Enrich.With({ new Serilog.Core.ILogEventEnricher with member _.Enrich(evt,_) = trim evt })
        let configure (a: Configuration.LoggerSinkConfiguration): unit =
            a.Logger(configureMetricsSinks >> ignore) |> ignore // unconditionally feed all log events to the metrics sinks
            a.Logger(fun l -> // but filter what gets emitted to the console sink
                let l = match isMetric with None -> l | Some predicate -> l.Filter.ByExcluding(System.Func<Serilog.Events.LogEvent, bool> predicate)
                l |> removeMetricsProps |> configureConsoleSink |> ignore)
            |> ignore
        configuration.WriteTo.Async(bufferSize = 65536, blockWhenFull = true, configure = System.Action<_> configure)

    [<System.Runtime.CompilerServices.Extension>]
    static member Sinks(configuration: LoggerConfiguration, configureMetricsSinks, verboseStore, verboseConsole) =
        let logEventIsMetric x = Serilog.Filters.Matching.WithProperty(Metrics.PropertyTag).Invoke x
        configuration.Sinks(configureMetricsSinks, Sinks.console verboseConsole, ?isMetric = if verboseStore then None else Some logEventIsMetric)

    module CosmosStoreConnector =

        let private get (role: string) (client: Microsoft.Azure.Cosmos.CosmosClient) databaseId containerId =
            Log.Information("CosmosDB {role} {database}/{container}", role, databaseId, containerId)
            client.GetDatabase(databaseId).GetContainer(containerId)
        let getSource = get "Source"
        let getLeases = get "Leases"
        let createMonitoredAndLeases client databaseId containerId auxContainerId =
            getSource client databaseId containerId, getLeases client databaseId auxContainerId

    type Equinox.CosmosStore.CosmosStoreContext with

        member x.LogConfiguration(role, databaseId: string, containerId: string) =
            Log.Information("CosmosStore {role:l} {database}/{container} Tip maxEvents {maxEvents} maxSize {maxJsonLen} Query maxItems {queryMaxItems}",
                            role, databaseId, containerId, x.TipOptions.MaxEvents, x.TipOptions.MaxJsonLength, x.QueryOptions.MaxItems)

    type Equinox.CosmosStore.CosmosStoreClient with

        member x.CreateContext(role: string, databaseId, containerId, tipMaxEvents, ?queryMaxItems, ?tipMaxJsonLength, ?skipLog) =
            let c = Equinox.CosmosStore.CosmosStoreContext(x, databaseId, containerId, tipMaxEvents, ?queryMaxItems = queryMaxItems, ?tipMaxJsonLength = tipMaxJsonLength)
            if skipLog = Some true then () else c.LogConfiguration(role, databaseId, containerId)
            c

    type Equinox.CosmosStore.CosmosStoreConnector with

        member private x.LogConfiguration(role, databaseId: string, containers: string[]) =
            let o = x.Options
            let timeout, retries429, timeout429 = o.RequestTimeout, o.MaxRetryAttemptsOnRateLimitedRequests, o.MaxRetryWaitTimeOnRateLimitedRequests
            Log.Information("CosmosDB {role} {mode} {endpointUri} {database}/{containers} timeout {timeout}s Retries {retries}<{maxRetryWaitTime}s",
                            role, o.ConnectionMode, x.Endpoint, databaseId, containers, timeout.TotalSeconds, retries429, let t = timeout429.Value in t.TotalSeconds)
        member private x.CreateAndInitialize(role, databaseId, containers) =
            x.LogConfiguration(role, databaseId, containers)
            x.CreateAndInitialize(databaseId, containers)
        member private x.Connect(role, databaseId, containers) =
            x.LogConfiguration(role, databaseId, containers)
            x.Connect(databaseId, containers)

        // NOTE uses CreateUninitialized as the Database/Container may not actually exist yet
        member x.CreateLeasesContainer(databaseId, auxContainerId) =
            x.LogConfiguration("Feed", databaseId, [| auxContainerId |])
            let client = x.CreateUninitialized()
            CosmosStoreConnector.getLeases client databaseId auxContainerId

        member x.ConnectFeed(databaseId, containerId, auxContainerId) = async {
            let! cosmosClient = x.CreateAndInitialize("Feed", databaseId, [| containerId; auxContainerId |])
            return CosmosStoreConnector.createMonitoredAndLeases cosmosClient databaseId containerId auxContainerId }

        /// CosmosSync: When using a ReadOnly connection string, the leases need to be maintained alongside the target
        member x.ConnectFeedReadOnly(databaseId, containerId, auxClient, auxDatabaseId, auxContainerId) = async {
            let! client = x.CreateAndInitialize("Main", databaseId, [| containerId |])
            let source = CosmosStoreConnector.getSource client databaseId containerId
            let leases = CosmosStoreConnector.getLeases auxClient auxDatabaseId auxContainerId
            return source, leases }

        member x.ConnectContext(role, databaseId, containerId: string, maxEvents) = async {
            let! client = x.Connect(role, databaseId, [| containerId |])
            return client.CreateContext(role, databaseId, containerId, tipMaxEvents = maxEvents) }
