module Propulsion.Tool.Program

open Argu
open Serilog
open Serilog.Events
open System

exception MissingArg of string

let private getEnvVarForArgumentOrThrow varName argName =
    match Environment.GetEnvironmentVariable varName with
    | null -> raise (MissingArg(sprintf "Please provide a %s, either as an argument or via the %s environment variable" argName varName))
    | x -> x
let private defaultWithEnvVar varName argName = function None -> getEnvVarForArgumentOrThrow varName argName | Some x -> x

module Cosmos =

    type [<NoEquality; NoComparison>] Arguments =
        | [<AltCommandLine("-V")>]          VerboseStore
        | [<AltCommandLine("-m")>]          ConnectionMode of Microsoft.Azure.Cosmos.ConnectionMode
        | [<AltCommandLine("-o")>]          Timeout of float
        | [<AltCommandLine("-r")>]          Retries of int
        | [<AltCommandLine("-rt")>]         RetriesWaitTime of float
        | [<AltCommandLine("-s")>]          Connection of string
        | [<AltCommandLine("-d")>]          Database of string
        | [<AltCommandLine("-c")>]          Container of string
        | [<AltCommandLine("-a"); Unique>]  LeaseContainer of string
        | [<AltCommandLine("-as"); Unique>]  Suffix of string
        interface IArgParserTemplate with
            member a.Usage =
                match a with
                | VerboseStore ->           "Include low level Store logging."
                | Timeout _ ->              "specify operation timeout in seconds (default: 5)."
                | Retries _ ->              "specify operation retries (default: 1)."
                | RetriesWaitTime _ ->      "specify max wait-time for retry when being throttled by Cosmos in seconds (default: 5)"
                | Connection _ ->           "specify a connection string for a Cosmos account (defaults: envvar:EQUINOX_COSMOS_CONNECTION, Cosmos Emulator)."
                | ConnectionMode _ ->       "override the connection mode (default: DirectTcp)."
                | Database _ ->             "specify a database name for Cosmos store (defaults: envvar:EQUINOX_COSMOS_DATABASE, test)."
                | Container _ ->            "specify a container name for Cosmos store (defaults: envvar:EQUINOX_COSMOS_CONTAINER, test)."
                | Suffix _ ->               "Specify Container Name suffix (default: `-aux`)."
                | LeaseContainer _ ->       "Specify full Lease Container Name (default: Container + Suffix)."
    type Equinox.CosmosStore.CosmosStoreConnector with
        member private x.LogConfiguration(log : Serilog.ILogger, connectionName, databaseId, containerId) =
            let o = x.Options
            let timeout, retries429, timeout429 = o.RequestTimeout, o.MaxRetryAttemptsOnRateLimitedRequests, o.MaxRetryWaitTimeOnRateLimitedRequests
            log.Information("CosmosDb {name} {mode} {endpointUri} timeout {timeout}s; Throttling retries {retries}, max wait {maxRetryWaitTime}s",
                            connectionName, o.ConnectionMode, x.Endpoint, timeout.TotalSeconds, retries429, let t = timeout429.Value in t.TotalSeconds)
            log.Information("CosmosDb {name} Database {database} Container {container}",
                            connectionName, databaseId, containerId)

        /// Use sparingly; in general one wants to use CreateAndInitialize to avoid slow first requests
        member private x.CreateUninitialized(databaseId, containerId) =
            x.CreateUninitialized().GetDatabase(databaseId).GetContainer(containerId)

        /// Creates a CosmosClient suitable for running a CFP via CosmosStoreSource
        member x.CreateClient(log, databaseId, containerId, ?connectionName) =
            x.LogConfiguration(log, defaultArg connectionName "Source", databaseId, containerId)
            x.CreateUninitialized(databaseId, containerId)
    type Info(a : ParseResults<Arguments>) =
        let discovery =                     a.TryGetResult Connection |> defaultWithEnvVar "EQUINOX_COSMOS_CONNECTION" "Connection" |> Equinox.CosmosStore.Discovery.ConnectionString
        let mode =                          a.TryGetResult ConnectionMode
        let timeout =                       a.GetResult(Timeout, 5.) |> TimeSpan.FromSeconds
        let retries =                       a.GetResult(Retries, 1)
        let maxRetryWaitTime =              a.GetResult(RetriesWaitTime, 5.) |> TimeSpan.FromSeconds
        let connector =                     Equinox.CosmosStore.CosmosStoreConnector(discovery, timeout, retries, maxRetryWaitTime, ?mode = mode)
        let database =                      a.TryGetResult Database  |> defaultWithEnvVar "EQUINOX_COSMOS_DATABASE"  "Database"
        member val ContainerId =            a.TryGetResult Container |> defaultWithEnvVar "EQUINOX_COSMOS_CONTAINER" "Container"
        member x.MonitoredContainer(log) =  connector.CreateClient(log, database, x.ContainerId)

        member val LeaseContainerId =       a.TryGetResult LeaseContainer
        member private _.ConnectLeases(log, containerId) = connector.CreateClient(log, database, containerId, "Leases")
        member x.ConnectLeases(log) =       match x.LeaseContainerId with
                                            | None ->    x.ConnectLeases(log, x.ContainerId + "-aux")
                                            | Some sc -> x.ConnectLeases(log, sc)

[<NoEquality; NoComparison>]
type Arguments =
    | [<AltCommandLine("-V")>]              Verbose
    | [<AltCommandLine("-C")>]              VerboseConsole
    | [<AltCommandLine("-S")>]              LocalSeq
    | [<CliPrefix(CliPrefix.None); Last; Unique>] Init of ParseResults<InitAuxArguments>
    | [<CliPrefix(CliPrefix.None); Last; Unique>] Project of ParseResults<ProjectArguments>
    interface IArgParserTemplate with
        member a.Usage = a |> function
            | Verbose ->                    "Include low level logging regarding specific test runs."
            | VerboseConsole ->             "Include low level test and store actions logging in on-screen output to console."
            | LocalSeq ->                   "Configures writing to a local Seq endpoint at http://localhost:5341, see https://getseq.net"
            | Init _ ->                     "Initialize auxilliary store (presently only relevant for `cosmos`, when you intend to run the Projector)."
            | Project _ ->                  "Project from store specified as the last argument, storing state in the specified `aux` Store (see init)."
and [<NoComparison; NoEquality>]InitDbArguments =
    | [<AltCommandLine("-ru"); Mandatory>]  Rus of int
    | [<AltCommandLine("-P")>]              SkipStoredProc
    | [<CliPrefix(CliPrefix.None)>]         Cosmos of ParseResults<Cosmos.Arguments>
    interface IArgParserTemplate with
        member a.Usage = a |> function
            | Rus _ ->                      "Specify RU/s level to provision for the Database."
            | SkipStoredProc ->             "Inhibit creation of stored procedure in cited Container."
            | Cosmos _ ->                   "Cosmos Connection parameters."
and [<NoComparison; NoEquality>]InitAuxArguments =
    | [<AltCommandLine("-ru"); Mandatory>]  Rus of int
    | [<AltCommandLine("-s")>]              Suffix of string
    | [<CliPrefix(CliPrefix.None)>]         Cosmos of ParseResults<Cosmos.Arguments>
    interface IArgParserTemplate with
        member a.Usage = a |> function
            | Rus _ ->                      "Specify RU/s level to provision for the Aux Container."
            | Suffix _ ->                    "Specify Container Name suffix (default: `-aux`)."
            | Cosmos _ ->                   "Cosmos Connection parameters."
and [<NoComparison; NoEquality; RequireSubcommand>] ProjectArguments =
    | [<AltCommandLine "-g"; Mandatory>]    ConsumerGroupName of string
    | [<AltCommandLine("-Z"); Unique>]      FromTail
    | [<AltCommandLine("-m"); Unique>]      MaxItems of int
    | [<AltCommandLine("-l"); Unique>]      LagFreqM of float
    | [<CliPrefix(CliPrefix.None); Last>]   Stats of ParseResults<StatsTarget>
    | [<CliPrefix(CliPrefix.None); Last>]   Kafka of ParseResults<KafkaTarget>
    interface IArgParserTemplate with
        member a.Usage = a |> function
            | ConsumerGroupName _ ->        "Projector instance context name."
            | FromTail _ ->                 "(iff fresh projection) - force starting from present Position. Default: Ensure each and every event is projected from the start."
            | MaxItems _ ->                 "Maximum item count to supply to ChangeFeed Api when querying. Default: Unlimited"
            | LagFreqM _ ->                 "Specify frequency to dump lag stats. Default: off"

            | Stats _ ->                    "Do not emit events, only stats."
            | Kafka _ ->                    "Project to Kafka."
and [<NoComparison; NoEquality>] KafkaTarget =
    | [<AltCommandLine("-t"); Unique; MainCommand>] Topic of string
    | [<AltCommandLine("-b"); Unique>]      Broker of string
    | [<CliPrefix(CliPrefix.None); Last>]   Cosmos of ParseResults<Cosmos.Arguments>
    interface IArgParserTemplate with
        member a.Usage = a |> function
            | Topic _ ->                    "Specify target topic. Default: Use $env:PROPULSION_KAFKA_TOPIC"
            | Broker _ ->                   "Specify target broker. Default: Use $env:PROPULSION_KAFKA_BROKER"
            | Cosmos _ ->                   "Cosmos Connection parameters."
and [<NoComparison; NoEquality>] StatsTarget =
    | [<CliPrefix(CliPrefix.None); Last; Unique>] Cosmos of ParseResults<Cosmos.Arguments>
    interface IArgParserTemplate with
        member a.Usage = a |> function
            | Cosmos _ ->                   "Cosmos Connection parameters."

let createLog verbose verboseConsole maybeSeqEndpoint =
    let c = LoggerConfiguration().Destructure.FSharpTypes().Enrich.FromLogContext()
    let c = if verbose then c.MinimumLevel.Debug() else c
    let c = c.WriteTo.Sink(Equinox.CosmosStore.Core.Log.InternalMetrics.Stats.LogSink())
    let c = c.WriteTo.Console((if verboseConsole then LogEventLevel.Debug else LogEventLevel.Information), theme=Sinks.SystemConsole.Themes.AnsiConsoleTheme.Code)
    let c = match maybeSeqEndpoint with None -> c | Some endpoint -> c.WriteTo.Seq(endpoint)
    c.CreateLogger()

let [<Literal>] appName = "propulsion-tool"

module CosmosInit =

    let aux (log : ILogger) (iargs : ParseResults<InitAuxArguments>) = async {
        match iargs.TryGetSubCommand() with
        | Some (InitAuxArguments.Cosmos sargs) ->
            let args = Cosmos.Info sargs
            let client = args.ConnectLeases(log)
            let rus = iargs.GetResult(InitAuxArguments.Rus)
            log.Information("Provisioning Leases Container for {rus:n0} RU/s", rus)
            return! Equinox.CosmosStore.Core.Initialization.initAux client.Database.Client (client.Database.Id, client.Id) rus
        | _ -> return failwith "please specify a `cosmos` endpoint" }

type Stats(log, statsInterval, statesInterval) =
    inherit Propulsion.Streams.Stats<unit>(log, statsInterval=statsInterval, statesInterval=statesInterval)
    member val StatsInterval = statsInterval
    override _.HandleOk(_log) = ()
    override _.HandleExn(_log, _stream, _exn) = ()

[<EntryPoint>]
let main argv =
    let programName = System.Reflection.Assembly.GetEntryAssembly().GetName().Name
    let parser = ArgumentParser.Create<Arguments>(programName=programName)
    try
        let args = parser.ParseCommandLine argv
        let verboseConsole = args.Contains VerboseConsole
        let maybeSeq = if args.Contains LocalSeq then Some "http://localhost:5341" else None
        let verbose = args.Contains Verbose
        let log = createLog verbose verboseConsole maybeSeq
        match args.GetSubCommand() with
        | Init iargs -> CosmosInit.aux log iargs |> Async.Ignore<Microsoft.Azure.Cosmos.Container> |> Async.RunSynchronously
        | Project pargs ->
            let group, startFromTail, maxItems = pargs.GetResult ConsumerGroupName, pargs.Contains FromTail, pargs.TryGetResult MaxItems
            maxItems |> Option.iter (fun bs -> log.Information("ChangeFeed Max items Count {changeFeedMaxItems}", bs))
            if startFromTail then Log.Warning("ChangeFeed (If new projector group) Skipping projection of all existing events.")
            let maybeLogLagInterval = pargs.TryGetResult LagFreqM |> Option.map TimeSpan.FromMinutes
            let broker, topic, storeArgs =
                match pargs.GetSubCommand() with
                | Kafka kargs ->
                    let broker = kargs.TryGetResult Broker |> defaultWithEnvVar "PROPULSION_KAFKA_BROKER" "Broker"
                    let topic =  kargs.TryGetResult Topic  |> defaultWithEnvVar "PROPULSION_KAFKA_TOPIC"  "Topic"
                    Some broker, Some topic, kargs.GetResult KafkaTarget.Cosmos
                | Stats sargs -> None, None, sargs.GetResult StatsTarget.Cosmos
                | x -> failwithf "Invalid subcommand %A" x
            let args = Cosmos.Info storeArgs
            let monitored = args.MonitoredContainer(log)
            let leases = args.ConnectLeases(log)

            let producer =
                match broker, topic with
                | Some b,Some t ->
                    let linger = FsKafka.Batching.BestEffortSerial (TimeSpan.FromMilliseconds 100.)
                    let cfg = FsKafka.KafkaProducerConfig.Create(appName, b, Confluent.Kafka.Acks.Leader, linger, Confluent.Kafka.CompressionType.Lz4)
                    let p = FsKafka.KafkaProducer.Create(log, cfg, t)
                    Some p
                | _ -> None
            let sink =
                let stats = Stats(log, TimeSpan.FromMinutes 1., TimeSpan.FromMinutes 5.)
                let maxReadAhead, maxConcurrentStreams=2, 16
                let handle (stream : FsCodec.StreamName, span : Propulsion.Streams.StreamSpan<_>) = async {
                    match producer with
                    | None -> ()
                    | Some producer ->
                        let json = Propulsion.Codec.NewtonsoftJson.RenderedSpan.ofStreamSpan stream span |> Newtonsoft.Json.JsonConvert.SerializeObject
                        let! _ = producer.ProduceAsync(FsCodec.StreamName.toString stream, json) in () }
                Propulsion.Streams.StreamsProjector.Start(log, maxReadAhead, maxConcurrentStreams, handle, stats, stats.StatsInterval)
            let transformOrFilter = Propulsion.CosmosStore.EquinoxNewtonsoftParser.enumStreamEvents
            use observer = Propulsion.CosmosStore.CosmosStoreSource.CreateObserver(log, sink.StartIngester, Seq.collect transformOrFilter)
            Propulsion.CosmosStore.CosmosStoreSource.Run
              ( log, monitored, leases, group, observer,
                startFromTail = startFromTail, ?maxItems = maxItems, ?lagReportFreq = maybeLogLagInterval)
            |> Async.RunSynchronously
        | _ -> failwith "Please specify a valid subcommand :- init or project"
        0
    with :? Argu.ArguParseException as e -> eprintfn "%s" e.Message; 1
        | MissingArg msg -> eprintfn "%s" msg; 1
        | e -> eprintfn "%s" e.Message; 1
