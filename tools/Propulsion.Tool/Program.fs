﻿module Propulsion.Tool.Program

open Argu
open Propulsion.CosmosStore.Infrastructure // AwaitKeyboardInterruptAsTaskCancelledException
open Propulsion.Tool.Args
open Serilog
open System

module CosmosInit = Equinox.CosmosStore.Core.Initialization

[<NoEquality; NoComparison>]
type Parameters =
    | [<AltCommandLine "-V">]               Verbose
    | [<AltCommandLine "-C">]               VerboseConsole
    | [<AltCommandLine "-S">]               VerboseStore
    | [<CliPrefix(CliPrefix.None); Last; Unique>] Init of ParseResults<InitAuxParameters>
    | [<CliPrefix(CliPrefix.None); Last; Unique>] Index of ParseResults<IndexParameters>
    | [<CliPrefix(CliPrefix.None); Last; Unique>] Checkpoint of ParseResults<CheckpointParameters>
    | [<CliPrefix(CliPrefix.None); Last; Unique>] Project of ParseResults<ProjectParameters>
    interface IArgParserTemplate with
        member a.Usage = a |> function
            | Verbose ->                    "Include low level logging regarding specific test runs."
            | VerboseConsole ->             "Include low level test and store actions logging in on-screen output to console."
            | VerboseStore ->               "Include low level Store logging"
            | Init _ ->                     "Initialize auxiliary store (Supported for `cosmos` Only)."
            | Index _ ->                    "Walk a DynamoDB S3 export, ingesting events not already present in the index."
            | Checkpoint _ ->               "Display or override checkpoints in Cosmos or Dynamo"
            | Project _ ->                  "Project from store specified as the last argument, storing state in the specified `aux` Store (see init)."

and [<NoComparison; NoEquality>] InitAuxParameters =
    | [<AltCommandLine "-ru"; Unique>]      Rus of int
    | [<AltCommandLine "-A"; Unique>]       Autoscale
    | [<AltCommandLine "-m"; Unique>]       Mode of CosmosModeType
    | [<AltCommandLine "-s">]               Suffix of string
    | [<CliPrefix(CliPrefix.None)>]         Cosmos of ParseResults<Args.Cosmos.Parameters>
    interface IArgParserTemplate with
        member a.Usage = a |> function
            | Rus _ ->                      "Specify RU/s level to provision for the Aux Container. (with AutoScale, the value represents the maximum RU/s to AutoScale based on)."
            | Autoscale ->                  "Autoscale provisioned throughput. Use --rus to specify the maximum RU/s."
            | Mode _ ->                     "Configure RU mode to use Container-level RU, Database-level RU, or Serverless allocations (Default: Use Container-level allocation)."
            | Suffix _ ->                   "Specify Container Name suffix (default: `-aux`)."
            | Cosmos _ ->                   "Cosmos Connection parameters."
and CosmosModeType = Container | Db | Serverless
and CosmosInitArguments(p : ParseResults<InitAuxParameters>) =
    let rusOrDefault value = p.GetResult(Rus, value)
    let throughput auto = if auto then CosmosInit.Throughput.Autoscale (rusOrDefault 4000) else CosmosInit.Throughput.Manual (rusOrDefault 400)
    member val ProvisioningMode =
        match p.GetResult(Mode, CosmosModeType.Container), p.Contains Autoscale with
        | CosmosModeType.Container, auto -> CosmosInit.Provisioning.Container (throughput auto)
        | CosmosModeType.Db, auto ->        CosmosInit.Provisioning.Database (throughput auto)
        | CosmosModeType.Serverless, auto when auto || p.Contains Rus -> missingArg "Cannot specify RU/s or Autoscale in Serverless mode"
        | CosmosModeType.Serverless, _ ->   CosmosInit.Provisioning.Serverless

and [<NoEquality; NoComparison>] IndexParameters =
    | [<AltCommandLine "-j"; MainCommand; Mandatory>] Source of string
    | [<AltCommandLine "-t"; Unique>]       TrancheId of int
    | [<AltCommandLine "-m"; Unique>]       MinSizeK of int
    | [<AltCommandLine "-b"; Unique>]       EventsPerBatch of int

    | [<CliPrefix(CliPrefix.None)>]         Dynamo of ParseResults<Args.Dynamo.Parameters>
    interface IArgParserTemplate with
        member a.Usage = a |> function
            | Source _ ->                   "Specify source DynamoDB JSON filename"
            | TrancheId _ ->                "Specify destination TrancheId. Default 0"
            | MinSizeK _ ->                 "Specify Index Minimum Item size in KiB. Default 48"
            | EventsPerBatch _ ->           "Specify Maximum Events to Ingest as a single batch. Default 10000"

            | Dynamo _ ->                   "Specify DynamoDB parameters."

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
    | [<AltCommandLine "-Z"; Unique>]       FromTail
    | [<AltCommandLine "-m"; Unique>]       MaxItems of int

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
    | [<AltCommandLine "-t"; Unique; MainCommand>] Topic of string
    | [<AltCommandLine "-b"; Unique>]       Broker of string
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

    let aux (c, p : ParseResults<InitAuxParameters>) =
        match p.GetSubCommand() with
        | InitAuxParameters.Cosmos sa ->
            let mode, a = (CosmosInitArguments p).ProvisioningMode, Args.Cosmos.Arguments(c, sa)
            let client = a.ConnectLeases()
            match mode with
            | Equinox.CosmosStore.Core.Initialization.Provisioning.Container throughput ->
                match throughput with
                | Equinox.CosmosStore.Core.Initialization.Throughput.Autoscale rus ->
                    Log.Information("Provisioning Leases Container with Autoscale throughput of up to {rus:n0} RU/s", rus)
                | Equinox.CosmosStore.Core.Initialization.Throughput.Manual rus ->
                    Log.Information("Provisioning Leases Container with {rus:n0} RU/s", rus)
            | Equinox.CosmosStore.Core.Initialization.Provisioning.Database throughput ->
                let modeStr = "Database"
                match throughput with
                | Equinox.CosmosStore.Core.Initialization.Throughput.Autoscale rus ->
                    Log.Information("Provisioning Leases Container at {modeStr:l} level with Autoscale throughput of up to {rus:n0} RU/s", modeStr, rus)
                | Equinox.CosmosStore.Core.Initialization.Throughput.Manual rus ->
                    Log.Information("Provisioning Leases Container at {modeStr:l} level with {rus:n0} RU/s", modeStr, rus)
            | Equinox.CosmosStore.Core.Initialization.Provisioning.Serverless ->
                let modeStr = "Serverless"
                Log.Information("Provisioning Leases Container in {modeStr:l} mode with automatic throughput RU/s as configured in account", modeStr)
            Equinox.CosmosStore.Core.Initialization.initAux client.Database.Client (client.Database.Id, client.Id) mode
        | x -> missingArg $"unexpected subcommand %A{x}"

module Checkpoints =

    type Arguments(c, p : ParseResults<CheckpointParameters>) =
        member val StoreArgs =
            match p.GetSubCommand() with
            | CheckpointParameters.Cosmos p -> Choice1Of2 (Args.Cosmos.Arguments (c, p))
            | CheckpointParameters.Dynamo p -> Choice2Of2 (Args.Dynamo.Arguments (c, p))
            | _ -> missingArg "Must specify `cosmos` or `dynamo` store"

    let readOrOverride (c, p : ParseResults<CheckpointParameters>) = async {
        let a = Arguments(c, p)
        let source, tranche, group = p.GetResult Source, p.GetResult Tranche, p.GetResult Group
        let! store, storeSpecFragment, overridePosition = async {
            let cache = Equinox.Cache (appName, sizeMb = 1)
            match a.StoreArgs with
            | Choice1Of2 a ->
                let! store = a.CreateCheckpointStore(group, cache, Log.forMetrics)
                return (store : Propulsion.Feed.IFeedCheckpointStore), "cosmos", fun pos -> store.Override(source, tranche, pos)
            | Choice2Of2 a ->
                let store = a.CreateCheckpointStore(group, cache, Log.forMetrics)
                return store, $"dynamo -t {a.IndexTable}", fun pos -> store.Override(source, tranche, pos) }
        Log.Information("Checkpoint Source {source} Tranche {tranche} Consumer Group {group}", source, tranche, group)
        match p.TryGetResult OverridePosition with
        | None ->
            let! interval, pos = store.Start(source, tranche)
            Log.Information("Checkpoint position {pos}; Checkpoint event frequency {checkpointEventIntervalM:f0}m", pos, interval.TotalMinutes)
        | Some pos ->
            Log.Warning("Checkpoint Overriding to {pos}...", pos)
            do! overridePosition pos
        let sn = Propulsion.Feed.ReaderCheckpoint.streamName (source, tranche, group)
        let cmd = $"eqx dump '{sn}' {storeSpecFragment}"
        Log.Information("Inspect via 👉 {cmd}", cmd) }

module Indexer =

    open Propulsion.DynamoStore

    type Arguments(c, p : ParseResults<IndexParameters>) =
//        member val IdleDelay =              TimeSpan.FromMilliseconds 10.
        member val SourcePath =             p.GetResult IndexParameters.Source
        member val TrancheId =              p.TryGetResult IndexParameters.TrancheId
                                            |> Option.map AppendsTrancheId.parse
                                            |> Option.defaultValue AppendsTrancheId.wellKnownId
        // Larger optimizes for not needing to use TransactWriteItems as frequently
        // Smaller will trigger more items and reduce read costs for Sources reading fro the tail
        member val MinItemSize =            p.GetResult(IndexParameters.MinSizeK, 48)
        member val EventsPerBatch =         p.GetResult(IndexParameters.EventsPerBatch, 10000)
        member val StoreArgs =
            match p.GetSubCommand() with
            | IndexParameters.Dynamo p -> Args.Dynamo.Arguments (c, p)
            | x -> missingArg $"unexpected subcommand %A{x}"

        member x.WriterParams() =           x.StoreArgs.WriterParams x.MinItemSize
(*
    type Stats(statsInterval, statesInterval, logExternalStats) =
        inherit Propulsion.Streams.Stats<unit>(Log.Logger, statsInterval = statsInterval, statesInterval = statesInterval)
        member val StatsInterval = statsInterval
        override _.HandleOk(_log) = ()
        override _.HandleExn(_log, _exn) = ()
        override _.DumpStats() =
            base.DumpStats()
            logExternalStats Log.Logger
*)
    let run (c : Args.Configuration, p : ParseResults<IndexParameters>) = async {
        let a = Arguments(c, p)
        let ctx = a.WriterParams()
        (*
        let sa, dumpStoreStats = a.StoreArgs, Equinox.DynamoStore.Core.Log.InternalMetrics.dump
        let stats = Stats(TimeSpan.FromMinutes 1., TimeSpan.FromMinutes 5., logExternalStats = dumpStoreStats)
        let sink =
            let maxReadAhead, maxConcurrentStreams = 2, 16
            let handle (stream : FsCodec.StreamName, span : Propulsion.Streams.StreamSpan<_>) = async {
                // TODO
                let json = Propulsion.Codec.NewtonsoftJson.RenderedSpan.ofStreamSpan stream span |> Newtonsoft.Json.JsonConvert.SerializeObject
                let! _ = producer.ProduceAsync(FsCodec.StreamName.toString stream, json) in () }
            Propulsion.Streams.StreamsProjector.Start(Log.Logger, maxReadAhead, maxConcurrentStreams, handle, stats, stats.StatsInterval, idleDelay = a.IdleDelay)
        let source =
            let indexStore, maybeHydrate = sa.MonitoringParams()
            let checkpoints =
                let cache = Equinox.Cache (appName, sizeMb = 1)
                sa.CreateCheckpointStore(group, cache, Log.forMetrics)
            let loadMode = Propulsion.DynamoStore.LoadMode.All
            Propulsion.DynamoStore.DynamoStoreSource(
                Log.Logger, stats.StatsInterval,
                indexStore, defaultArg maxItems 100, TimeSpan.FromSeconds 0.5,
                checkpoints, sink, loadMode, fromTail = startFromTail, storeLog = Log.forMetrics
            ).Start()
        let work = [
            Async.AwaitKeyboardInterruptAsTaskCancelledException()
            sink.AwaitWithStopOnCancellation()
            source.AwaitWithStopOnCancellation() ]
        return! work |> Async.Parallel |> Async.Ignore<unit[]> *)
        let indexer = DynamoExportIngester.Importer(Log.Logger, ctx)
        return! indexer.ImportDynamoDbJsonFile(a.TrancheId, a.SourcePath, a.EventsPerBatch) }

module Project =

    type KafkaArguments(c, p : ParseResults<KafkaParameters>) =
        member _.Broker =                   p.TryGetResult Broker |> Option.defaultWith (fun () -> c.KafkaBroker)
        member _.Topic =                    p.TryGetResult Topic |> Option.defaultWith (fun () -> c.KafkaTopic)
        member val StoreArgs =
            match p.GetSubCommand() with
            | KafkaParameters.Cosmos p -> Choice1Of2 (Args.Cosmos.Arguments (c, p))
            | KafkaParameters.Dynamo p -> Choice2Of2 (Args.Dynamo.Arguments (c, p))
            | x -> missingArg $"unexpected subcommand %A{x}"

    type StatsArguments(c, p : ParseResults<StatsParameters>) =
        member val StoreArgs =
            match p.GetSubCommand() with
            | StatsParameters.Cosmos p -> Choice1Of2 (Args.Cosmos.Arguments (c, p))
            | StatsParameters.Dynamo p -> Choice2Of2 (Args.Dynamo.Arguments (c, p))

    type Arguments(c, p : ParseResults<ProjectParameters>) =
        member val IdleDelay =              TimeSpan.FromMilliseconds 10.
        member val StoreArgs =
            match p.GetSubCommand() with
            | Kafka a -> KafkaArguments(c, a).StoreArgs
            | Stats a -> StatsArguments(c, a).StoreArgs
            | x -> missingArg $"unexpected subcommand %A{x}"

    type Stats(statsInterval, statesInterval, logExternalStats) =
        inherit Propulsion.Streams.Stats<unit>(Log.Logger, statsInterval = statsInterval, statesInterval = statesInterval)
        member val StatsInterval = statsInterval
        override _.HandleOk(_log) = ()
        override _.HandleExn(_log, _exn) = ()
        override _.DumpStats() =
            base.DumpStats()
            logExternalStats Log.Logger

    let run (c : Args.Configuration, p : ParseResults<ProjectParameters>) = async {
        let a = Arguments(c, p)
        let storeArgs, dumpStoreStats =
            match a.StoreArgs with
            | Choice1Of2 sa -> Choice1Of2 sa, Equinox.CosmosStore.Core.Log.InternalMetrics.dump
            | Choice2Of2 sa -> Choice2Of2 sa, Equinox.DynamoStore.Core.Log.InternalMetrics.dump
        let group, startFromTail, maxItems = p.GetResult ConsumerGroupName, p.Contains FromTail, p.TryGetResult MaxItems
        match maxItems with None -> () | Some bs -> Log.Information("ChangeFeed Max items Count {changeFeedMaxItems}", bs)
        if startFromTail then Log.Warning("ChangeFeed (If new projector group) Skipping projection of all existing events.")
        let producer =
            match p.GetSubCommand() with
            | Kafka a ->
                let a = KafkaArguments(c, a)
                let linger = FsKafka.Batching.BestEffortSerial (TimeSpan.FromMilliseconds 100.)
                let cfg = FsKafka.KafkaProducerConfig.Create(appName, a.Broker, Confluent.Kafka.Acks.Leader, linger, Confluent.Kafka.CompressionType.Lz4)
                let p = FsKafka.KafkaProducer.Create(Log.Logger, cfg, a.Topic)
                Some p
            | Stats _ -> None
            | x -> missingArg $"unexpected subcommand %A{x}"
        let stats = Stats(TimeSpan.FromMinutes 1., TimeSpan.FromMinutes 5., logExternalStats = dumpStoreStats)
        let sink =
            let maxReadAhead, maxConcurrentStreams = 2, 16
            let handle (stream : FsCodec.StreamName, span : Propulsion.Streams.StreamSpan<_>) = async {
                match producer with
                | None -> ()
                | Some producer ->
                    let json = Propulsion.Codec.NewtonsoftJson.RenderedSpan.ofStreamSpan stream span |> Newtonsoft.Json.JsonConvert.SerializeObject
                    let! _ = producer.ProduceAsync(FsCodec.StreamName.toString stream, json) in () }
            Propulsion.Streams.StreamsProjector.Start(Log.Logger, maxReadAhead, maxConcurrentStreams, handle, stats, stats.StatsInterval, idleDelay = a.IdleDelay)
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
    try let a = parseCommandLine argv
        let verbose, verboseConsole, verboseStore = a.Contains Verbose, a.Contains VerboseConsole, a.Contains VerboseStore
        let metrics = Sinks.equinoxMetricsOnly
        try Log.Logger <- LoggerConfiguration().Configure(verbose).Sinks(metrics, verboseConsole, verboseStore).CreateLogger()
            let c = Args.Configuration(Environment.GetEnvironmentVariable >> Option.ofObj)
            try match a.GetSubCommand() with
                | Init a ->         CosmosInit.aux (c, a) |> Async.Ignore<Microsoft.Azure.Cosmos.Container> |> Async.RunSynchronously
                | Checkpoint a ->   Checkpoints.readOrOverride (c, a) |> Async.RunSynchronously
                | Index a ->        Indexer.run (c, a) |> Async.RunSynchronously
                | Project a ->      Project.run (c, a) |> Async.RunSynchronously
                | x ->              missingArg $"unexpected subcommand %A{x}"
                0
            with e when not (e :? MissingArg || e :? ArguParseException) -> Log.Fatal(e, "Exiting"); 2
        finally Log.CloseAndFlush()
    with :? ArguParseException as e -> eprintfn $"%s{e.Message}"; 1
        | MissingArg msg -> eprintfn $"ERROR: %s{msg}"; 1
        | e -> eprintfn $"EXCEPTION: %s{e.Message}"; 1
