module Propulsion.Tool.Program

open Argu
open Propulsion.CosmosStore.Infrastructure // AwaitKeyboardInterruptAsTaskCancelledException
open Serilog
open System

[<NoEquality; NoComparison>]
type Parameters =
    | [<AltCommandLine("-V")>]              Verbose
    | [<AltCommandLine("-C")>]              VerboseConsole
    | [<AltCommandLine("-S")>]              VerboseStore
    | [<CliPrefix(CliPrefix.None); Last; Unique>] Init of ParseResults<InitAuxParameters>
    | [<CliPrefix(CliPrefix.None); Last; Unique>] Checkpoint of ParseResults<CheckpointParameters>
    | [<CliPrefix(CliPrefix.None); Last; Unique>] Project of ParseResults<ProjectParameters>
    interface IArgParserTemplate with
        member a.Usage = a |> function
            | Verbose ->                    "Include low level logging regarding specific test runs."
            | VerboseConsole ->             "Include low level test and store actions logging in on-screen output to console."
            | VerboseStore ->               "Include low level Store logging"
            | Init _ ->                     "Initialize auxiliary store (presently only relevant for `cosmos`, when you intend to run the Projector)."
            | Checkpoint _ ->               "Display or override checkpoints in Cosmos or Dynamo"
            | Project _ ->                  "Project from store specified as the last argument, storing state in the specified `aux` Store (see init)."

and [<NoComparison; NoEquality>] InitAuxParameters =
    | [<AltCommandLine("-ru"); Mandatory>]  Rus of int
    | [<AltCommandLine("-s")>]              Suffix of string
    | [<CliPrefix(CliPrefix.None)>]         Cosmos of ParseResults<Args.Cosmos.Parameters>
    interface IArgParserTemplate with
        member a.Usage = a |> function
            | Rus _ ->                      "Specify RU/s level to provision for the Aux Container."
            | Suffix _ ->                   "Specify Container Name suffix (default: `-aux`)."
            | Cosmos _ ->                   "Cosmos Connection parameters."

and [<NoEquality; NoComparison>] CheckpointParameters =
    | [<AltCommandLine "-s"; Mandatory>]    Source of Propulsion.Feed.SourceId
    | [<AltCommandLine "-t"; Mandatory>]    Tranche of Propulsion.Feed.TrancheId
    | [<AltCommandLine "-g"; Mandatory>]    Group of string
    | [<AltCommandLine "-p"; Unique>]       OverridePosition of Propulsion.Feed.Position

    | [<CliPrefix(CliPrefix.None)>]         Cosmos of ParseResults<Args.Cosmos.Parameters>
    | [<CliPrefix(CliPrefix.None)>]         Dynamo of ParseResults<Args.Dynamo.Parameters>
    interface IArgParserTemplate with
        member a.Usage = a |> function
            | Group _ ->                    "Consumer Group"
            | Source _ ->                   "Specify source to override"
            | Tranche _ ->                  "Specify tranche to override"
            | OverridePosition _ ->         "(optional) Override to specified position"

            | Cosmos _ ->                   "Specify CosmosDB parameters."
            | Dynamo _ ->                   "Specify DynamoDB parameters."

and [<NoComparison; NoEquality; RequireSubcommand>] ProjectParameters =
    | [<AltCommandLine "-g"; Mandatory>]    ConsumerGroupName of string
    | [<AltCommandLine("-Z"); Unique>]      FromTail
    | [<AltCommandLine("-m"); Unique>]      MaxItems of int

    | [<CliPrefix(CliPrefix.None); Last>]   Stats of ParseResults<StatsParameters>
    | [<CliPrefix(CliPrefix.None); Last>]   Kafka of ParseResults<KafkaParameters>
    interface IArgParserTemplate with
        member a.Usage = a |> function
            | ConsumerGroupName _ ->        "Projector instance context name."
            | FromTail _ ->                 "(iff fresh projection) - force starting from present Position. Default: Ensure each and every event is projected from the start."
            | MaxItems _ ->                 "Maximum item count to supply to ChangeFeed Api when querying. Default: Unlimited"

            | Stats _ ->                    "Do not emit events, only stats."
            | Kafka _ ->                    "Project to Kafka."
and [<NoComparison; NoEquality>] KafkaParameters =
    | [<AltCommandLine("-t"); Unique; MainCommand>] Topic of string
    | [<AltCommandLine("-b"); Unique>]      Broker of string
    | [<CliPrefix(CliPrefix.None); Last>]   Cosmos of ParseResults<Args.Cosmos.Parameters>
    | [<CliPrefix(CliPrefix.None); Last>]   Dynamo of ParseResults<Args.Dynamo.Parameters>
    interface IArgParserTemplate with
        member a.Usage = a |> function
            | Topic _ ->                    "Specify target topic. Default: Use $env:PROPULSION_KAFKA_TOPIC"
            | Broker _ ->                   "Specify target broker. Default: Use $env:PROPULSION_KAFKA_BROKER"
            | Cosmos _ ->                   "Specify CosmosDB parameters."
            | Dynamo _ ->                   "Specify DynamoDB parameters."
and [<NoComparison; NoEquality>] StatsParameters =
    | [<CliPrefix(CliPrefix.None); Last; Unique>] Cosmos of ParseResults<Args.Cosmos.Parameters>
    | [<CliPrefix(CliPrefix.None); Last; Unique>] Dynamo of ParseResults<Args.Dynamo.Parameters>
    interface IArgParserTemplate with
        member a.Usage = a |> function
            | Cosmos _ ->                   "Specify CosmosDB parameters."
            | Dynamo _ ->                   "Specify DynamoDB parameters."

let [<Literal>] appName = "propulsion-tool"

module CosmosInit =

    let aux (c, a : ParseResults<InitAuxParameters>) = async {
        match a.TryGetSubCommand() with
        | Some (InitAuxParameters.Cosmos sa) ->
            let args = Args.Cosmos.Arguments(c, sa)
            let client = args.ConnectLeases()
            let rus = a.GetResult(InitAuxParameters.Rus)
            Log.Information("Provisioning Leases Container for {rus:n0} RU/s", rus)
            return! Equinox.CosmosStore.Core.Initialization.initAux client.Database.Client (client.Database.Id, client.Id) rus
        | _ -> return failwith "please specify a `cosmos` endpoint" }

module Checkpoints =

    type Arguments(c, a : ParseResults<CheckpointParameters>) =

        member val StoreArgs =
            match a.GetSubCommand() with
            | CheckpointParameters.Cosmos p -> Choice1Of2 (Args.Cosmos.Arguments (c, p))
            | CheckpointParameters.Dynamo p -> Choice2Of2 (Args.Dynamo.Arguments (c, p))
            | _ -> Args.missingArg "Must specify `cosmos` or `dynamo` store"

    let readOrOverride (c, a : ParseResults<CheckpointParameters>) = async {
        let args = Arguments(c, a)
        let source, tranche, group = a.GetResult Source, a.GetResult Tranche, a.GetResult Group
        let! store, storeSpecFragment, overridePosition = async {
            let cache = Equinox.Cache (appName, sizeMb = 1)
            match args.StoreArgs with
            | Choice1Of2 a ->
                let! store = a.CreateCheckpointStore(group, cache, Log.forMetrics)
                return (store : Propulsion.Feed.IFeedCheckpointStore), "cosmos", fun pos -> store.Override(source, tranche, pos)
            | Choice2Of2 a ->
                let store = a.CreateCheckpointStore(group, cache, Log.forMetrics)
                return store, $"dynamo -t {a.IndexTable}", fun pos -> store.Override(source, tranche, pos) }
        Log.Information("Checkpoint Source {source} Tranche {tranche} Consumer Group {group}", source, tranche, group)
        match a.TryGetResult OverridePosition with
        | None ->
            let! interval, pos = store.Start(source, tranche)
            Log.Information("Checkpoint position {pos}; Checkpoint event frequency {checkpointEventIntervalM:f0}m", pos, interval.TotalMinutes)
        | Some pos ->
            Log.Warning("Checkpoint Overriding to {pos}...", pos)
            do! overridePosition pos
        let sn = Propulsion.Feed.ReaderCheckpoint.streamName (source, tranche, group)
        let cmd = $"eqx dump '{sn}' {storeSpecFragment}"
        Log.Information("Inspect via 👉 {cmd}", cmd) }

module Project =

    type KafkaArguments(c, a : ParseResults<KafkaParameters>) =
        member _.Broker =                   a.TryGetResult Broker |> Option.defaultWith (fun () -> c.KafkaBroker)
        member _.Topic =                    a.TryGetResult Topic |> Option.defaultWith (fun () -> c.KafkaTopic)
        member val StoreArgs =
            match a.GetSubCommand() with
            | KafkaParameters.Cosmos p -> Choice1Of2 (Args.Cosmos.Arguments (c, p))
            | KafkaParameters.Dynamo p -> Choice2Of2 (Args.Dynamo.Arguments (c, p))
            | _ -> Args.missingArg "Must specify `cosmos` or `dynamo` store"

    type StatsArguments(c, a : ParseResults<StatsParameters>) =
        member val StoreArgs =
            match a.GetSubCommand() with
            | StatsParameters.Cosmos p -> Choice1Of2 (Args.Cosmos.Arguments (c, p))
            | StatsParameters.Dynamo p -> Choice2Of2 (Args.Dynamo.Arguments (c, p))

    type Arguments(c, a : ParseResults<ProjectParameters>) =
        member val IdleDelay =              TimeSpan.FromMilliseconds 10.
        member val StoreArgs =
            match a.GetSubCommand() with
            | Kafka a -> KafkaArguments(c, a).StoreArgs
            | Stats a -> StatsArguments(c, a).StoreArgs
            | x -> Args.missingArg $"Invalid subcommand %A{x}"

    type Stats(statsInterval, statesInterval, logExternalStats) =
        inherit Propulsion.Streams.Stats<unit>(Log.Logger, statsInterval = statsInterval, statesInterval = statesInterval)
        member val StatsInterval = statsInterval
        override _.HandleOk(_log) = ()
        override _.HandleExn(_log, _exn) = ()
        override _.DumpStats() =
            base.DumpStats()
            logExternalStats Log.Logger

    let run (c : Args.Configuration, a : ParseResults<ProjectParameters>) = async {
        let args = Arguments(c, a)
        let storeArgs, dumpStoreStats =
            match args.StoreArgs with
            | Choice1Of2 sa -> Choice1Of2 sa, Equinox.CosmosStore.Core.Log.InternalMetrics.dump
            | Choice2Of2 sa -> Choice2Of2 sa, Equinox.DynamoStore.Core.Log.InternalMetrics.dump
        let group, startFromTail, maxItems = a.GetResult ConsumerGroupName, a.Contains FromTail, a.TryGetResult MaxItems
        match maxItems with None -> () | Some bs -> Log.Information("ChangeFeed Max items Count {changeFeedMaxItems}", bs)
        if startFromTail then Log.Warning("ChangeFeed (If new projector group) Skipping projection of all existing events.")
        let producer =
            match a.GetSubCommand() with
            | Kafka a ->
                let a = KafkaArguments(c, a)
                let linger = FsKafka.Batching.BestEffortSerial (TimeSpan.FromMilliseconds 100.)
                let cfg = FsKafka.KafkaProducerConfig.Create(appName, a.Broker, Confluent.Kafka.Acks.Leader, linger, Confluent.Kafka.CompressionType.Lz4)
                let p = FsKafka.KafkaProducer.Create(Log.Logger, cfg, a.Topic)
                Some p
            | Stats _ -> None
            | x -> Args.missingArg $"Invalid subcommand %A{x}"
        let stats = Stats(TimeSpan.FromMinutes 1., TimeSpan.FromMinutes 5., logExternalStats = dumpStoreStats)
        let sink =
            let maxReadAhead, maxConcurrentStreams = 2, 16
            let handle (stream : FsCodec.StreamName, span : Propulsion.Streams.StreamSpan<_>) = async {
                match producer with
                | None -> ()
                | Some producer ->
                    let json = Propulsion.Codec.NewtonsoftJson.RenderedSpan.ofStreamSpan stream span |> Newtonsoft.Json.JsonConvert.SerializeObject
                    let! _ = producer.ProduceAsync(FsCodec.StreamName.toString stream, json) in () }
            Propulsion.Streams.StreamsProjector.Start(Log.Logger, maxReadAhead, maxConcurrentStreams, handle, stats, stats.StatsInterval, idleDelay = args.IdleDelay)
        let source =
            match storeArgs with
            | Choice1Of2 sa ->
                let monitored = sa.MonitoredContainer()
                let leases = sa.ConnectLeases()
                let maybeLogLagInterval = sa.MaybeLogLagInterval
                let transformOrFilter = Propulsion.CosmosStore.EquinoxSystemTextJsonParser.enumStreamEvents
                let observer = Propulsion.CosmosStore.CosmosStoreSource.CreateObserver(Log.Logger, sink.StartIngester, Seq.collect transformOrFilter)
                Propulsion.CosmosStore.CosmosStoreSource.Start
                  ( Log.Logger, monitored, leases, group, observer,
                    startFromTail = startFromTail, ?maxItems = maxItems, ?lagReportFreq = maybeLogLagInterval)
            | Choice2Of2 sa ->
                let indexStore, maybeHydrate = sa.MonitoringParams()
                let checkpoints =
                    let cache = Equinox.Cache (appName, sizeMb = 1)
                    sa.CreateCheckpointStore(group, cache, Log.forMetrics)
                let loadMode =
                    match maybeHydrate with
                    | Some (context, streamsDop) ->
                        let nullFilter _ = true
                        Propulsion.DynamoStore.LoadMode.Hydrated (nullFilter, streamsDop, context)
                    | None -> Propulsion.DynamoStore.LoadMode.All
                Propulsion.DynamoStore.DynamoStoreSource(
                    Log.Logger, stats.StatsInterval,
                    indexStore, defaultArg maxItems 100, TimeSpan.FromSeconds 0.5,
                    checkpoints, sink, loadMode, fromTail = startFromTail, storeLog = Log.forMetrics
                ).Start()
        let work = [
            Async.AwaitKeyboardInterruptAsTaskCancelledException()
            sink.AwaitWithStopOnCancellation()
            source.AwaitWithStopOnCancellation() ]
        return! work |> Async.Parallel |> Async.Ignore<unit[]> }

/// Parse the commandline; can throw exceptions in response to missing arguments and/or `-h`/`--help` args
let parseCommandLine argv =
    let programName = Reflection.Assembly.GetEntryAssembly().GetName().Name
    let parser = ArgumentParser.Create<Parameters>(programName=programName)
    parser.ParseCommandLine argv

[<EntryPoint>]
let main argv =
    let args = parseCommandLine argv
    let verbose, verboseConsole, verboseStore = args.Contains Verbose, args.Contains VerboseConsole, args.Contains VerboseStore
    let metrics = Sinks.equinoxMetricsOnly
    Log.Logger <- LoggerConfiguration().Configure(verbose).Sinks(metrics, verboseConsole, verboseStore).CreateLogger()

    try try let c = Args.Configuration(Environment.GetEnvironmentVariable >> Option.ofObj)
            try match args.GetSubCommand() with
                | Init a ->         CosmosInit.aux (c, a) |> Async.Ignore<Microsoft.Azure.Cosmos.Container> |> Async.RunSynchronously
                | Checkpoint a ->   Checkpoints.readOrOverride (c, a) |> Async.RunSynchronously
                | Project a ->      Project.run (c, a) |> Async.RunSynchronously
                | _ ->              Args.missingArg "Please specify a valid subcommand :- init, checkpoint or project"
                0
            with e when not (e :? Args.MissingArg) -> Log.Fatal(e, "Exiting"); 2
        finally Log.CloseAndFlush()
    with Args.MissingArg msg -> eprintfn $"%s{msg}"; 1
        | :? ArguParseException as e -> eprintfn $"%s{e.Message}"; 1
        | e -> eprintfn $"Exception %s{e.Message}"; 1
