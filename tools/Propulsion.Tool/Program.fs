module Propulsion.Tool.Program

open Argu
open Propulsion.Internal // AwaitKeyboardInterruptAsTaskCanceledException
open Serilog

module CosmosInit = Equinox.CosmosStore.Core.Initialization

[<NoEquality; NoComparison>]
type Parameters =
    | [<AltCommandLine "-V">]               Verbose
    | [<AltCommandLine "-C">]               VerboseConsole
    | [<AltCommandLine "-S">]               VerboseStore
    | [<CliPrefix(CliPrefix.None); Last; Unique>] Init of ParseResults<InitAuxParameters>
    | [<CliPrefix(CliPrefix.None); Last; Unique>] InitPg of ParseResults<Args.Mdb.Parameters>
    | [<CliPrefix(CliPrefix.None); Last; Unique>] Index of ParseResults<IndexParameters>
    | [<CliPrefix(CliPrefix.None); Last; Unique>] Checkpoint of ParseResults<CheckpointParameters>
    | [<CliPrefix(CliPrefix.None); Last; Unique>] Project of ParseResults<ProjectParameters>
    interface IArgParserTemplate with
        member a.Usage = a |> function
            | Verbose ->                    "Include low level logging regarding specific test runs."
            | VerboseConsole ->             "Include low level test and store actions logging in on-screen output to console."
            | VerboseStore ->               "Include low level Store logging"
            | Init _ ->                     "Initialize auxiliary store (Supported for `cosmos` Only)."
            | InitPg _ ->                   "Initialize a postgres checkpoint store"
            | Index _ ->                    "Validate index (optionally, ingest events from a DynamoDB JSON S3 export to remediate missing events)."
            | Checkpoint _ ->               "Display or override checkpoints in Cosmos or Dynamo"
            | Project _ ->                  "Project from store specified as the last argument."
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
and CosmosInitArguments(p: ParseResults<InitAuxParameters>) =
    let rusOrDefault (value: int) = p.GetResult(Rus, value)
    let throughput auto = if auto then CosmosInit.Throughput.Autoscale (rusOrDefault 4000)
                                  else CosmosInit.Throughput.Manual (rusOrDefault 400)
    member val ProvisioningMode =
        match p.GetResult(Mode, CosmosModeType.Container), p.Contains Autoscale with
        | CosmosModeType.Container, auto -> CosmosInit.Provisioning.Container (throughput auto)
        | CosmosModeType.Db, auto ->        CosmosInit.Provisioning.Database (throughput auto)
        | CosmosModeType.Serverless, auto when auto || p.Contains Rus -> p.Raise "Cannot specify RU/s or Autoscale in Serverless mode"
        | CosmosModeType.Serverless, _ ->   CosmosInit.Provisioning.Serverless

and [<NoEquality; NoComparison>] IndexParameters =
    | [<AltCommandLine "-p"; Unique>]       IndexPartitionId of int
    | [<AltCommandLine "-j"; MainCommand>]  DynamoDbJson of string
    | [<AltCommandLine "-m"; Unique>]       MinSizeK of int
    | [<AltCommandLine "-b"; Unique>]       EventsPerBatch of int
    | [<AltCommandLine "-g"; Unique>]       GapsLimit of int

    | [<CliPrefix(CliPrefix.None)>]         Dynamo of ParseResults<Args.Dynamo.Parameters>
    interface IArgParserTemplate with
        member a.Usage = a |> function
            | IndexPartitionId _ ->         "PartitionId to verify/import into. (optional, omitting displays partitions->epochs list)"
            | DynamoDbJson _ ->             "Source DynamoDB JSON filename(s) to import (optional, omitting displays current state)"
            | MinSizeK _ ->                 "Index Stream minimum Item size in KiB. Default 48"
            | EventsPerBatch _ ->           "Maximum Events to Ingest as a single batch. Default 10000"
            | GapsLimit _ ->                "Max Number of gaps to output to console. Default 10"

            | Dynamo _ ->                   "Specify DynamoDB parameters."

and [<NoEquality; NoComparison>] CheckpointParameters =
    | [<AltCommandLine "-s"; Mandatory>]    Source of Propulsion.Feed.SourceId
    | [<AltCommandLine "-t"; Mandatory>]    Tranche of Propulsion.Feed.TrancheId
    | [<AltCommandLine "-g"; Mandatory>]    Group of string
    | [<AltCommandLine "-p"; Unique>]       OverridePosition of Propulsion.Feed.Position

    | [<CliPrefix(CliPrefix.None)>]         Cosmos of ParseResults<Args.Cosmos.Parameters>
    | [<CliPrefix(CliPrefix.None)>]         Dynamo of ParseResults<Args.Dynamo.Parameters>
    | [<CliPrefix(CliPrefix.None)>]         Pg     of ParseResults<Args.Mdb.Parameters>
    interface IArgParserTemplate with
        member a.Usage = a |> function
            | Group _ ->                    "Consumer Group"
            | Source _ ->                   "Specify source to override"
            | Tranche _ ->                  "Specify tranche to override"
            | OverridePosition _ ->         "(optional) Override to specified position"

            | Cosmos _ ->                   "Specify CosmosDB parameters."
            | Dynamo _ ->                   "Specify DynamoDB parameters."
            | Pg _ ->                      "Specify MessageDb parameters."

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
            | MaxItems _ ->                 "Controls checkpointing granularity by adjusting the batch size being loaded from the feed. Default: Unlimited"

            | Stats _ ->                    "Do not emit events, only stats."
            | Kafka _ ->                    "Project to Kafka."
and [<NoComparison; NoEquality>] KafkaParameters =
    | [<AltCommandLine "-t"; Unique; MainCommand>] Topic of string
    | [<AltCommandLine "-b"; Unique>]       Broker of string
    | [<CliPrefix(CliPrefix.None); Last>]   Cosmos of ParseResults<Args.Cosmos.Parameters>
    | [<CliPrefix(CliPrefix.None); Last>]   Dynamo of ParseResults<Args.Dynamo.Parameters>
    | [<CliPrefix(CliPrefix.None); Last>]   Mdb    of ParseResults<Args.Mdb.Parameters>
    interface IArgParserTemplate with
        member a.Usage = a |> function
            | Topic _ ->                    "Specify target topic. Default: Use $env:PROPULSION_KAFKA_TOPIC"
            | Broker _ ->                   "Specify target broker. Default: Use $env:PROPULSION_KAFKA_BROKER"
            | Cosmos _ ->                   "Specify CosmosDB parameters."
            | Dynamo _ ->                   "Specify DynamoDB parameters."
            | Mdb _ ->                      "Specify MessageDb parameters."
and [<NoComparison; NoEquality>] StatsParameters =
    | [<CliPrefix(CliPrefix.None); Last; Unique>] Cosmos of ParseResults<Args.Cosmos.Parameters>
    | [<CliPrefix(CliPrefix.None); Last; Unique>] Dynamo of ParseResults<Args.Dynamo.Parameters>
    | [<CliPrefix(CliPrefix.None); Last; Unique>] Mdb    of ParseResults<Args.Mdb.Parameters>
    interface IArgParserTemplate with
        member a.Usage = a |> function
            | Cosmos _ ->                   "Specify CosmosDB parameters."
            | Dynamo _ ->                   "Specify DynamoDB parameters."
            | Mdb _ ->                      "Specify MessageDb parameters."

let [<Literal>] AppName = "propulsion-tool"

module CosmosInit =

    let aux (c, p: ParseResults<InitAuxParameters>) =
        match p.GetSubCommand() with
        | InitAuxParameters.Cosmos sa ->
            let mode, a = (CosmosInitArguments p).ProvisioningMode, Args.Cosmos.Arguments(c, sa)
            let container = a.CreateLeasesContainer()
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
            Equinox.CosmosStore.Core.Initialization.initAux container.Database.Client (container.Database.Id, container.Id) mode
        | x -> Args.missingArg $"unexpected subcommand %A{x}"

module Checkpoints =

    type Arguments(c, p: ParseResults<CheckpointParameters>) =
        member val StoreArgs =
            match p.GetSubCommand() with
            | CheckpointParameters.Cosmos p -> Choice1Of3 (Args.Cosmos.Arguments (c, p))
            | CheckpointParameters.Dynamo p -> Choice2Of3 (Args.Dynamo.Arguments (c, p))
            | CheckpointParameters.Pg p ->    Choice3Of3 (Args.Mdb.Arguments (c, p))
            | x -> Args.missingArg $"unexpected subcommand %A{x}"

    let readOrOverride (c, p: ParseResults<CheckpointParameters>, ct) = task {
        let a = Arguments(c, p)
        let source, tranche, group = p.GetResult Source, p.GetResult Tranche, p.GetResult Group
        let! store, storeSpecFragment, overridePosition = task {
            let cache = Equinox.Cache (AppName, sizeMb = 1)
            match a.StoreArgs with
            | Choice1Of3 a ->
                let! store = a.CreateCheckpointStore(group, cache, Metrics.log)
                return (store: Propulsion.Feed.IFeedCheckpointStore), "cosmos", fun pos -> store.Override(source, tranche, pos, ct)
            | Choice2Of3 a ->
                let store = a.CreateCheckpointStore(group, cache, Metrics.log)
                return store, $"dynamo -t {a.IndexTable}", fun pos -> store.Override(source, tranche, pos, ct)
            | Choice3Of3 a ->
                let store = a.CreateCheckpointStore(group)
                return store, null, fun pos -> store.Override(source, tranche, pos, ct) }
        Log.Information("Checkpoint Source {source} Tranche {tranche} Consumer Group {group}", source, tranche, group)
        match p.TryGetResult OverridePosition with
        | None ->
            let! pos = store.Start(source, tranche, None, ct)
            Log.Information("Checkpoint position {pos}", pos)
        | Some pos ->
            Log.Warning("Checkpoint Overriding to {pos}...", pos)
            do! overridePosition pos
        if storeSpecFragment <> null then
            let sn = Propulsion.Feed.ReaderCheckpoint.Stream.name (source, tranche, group)
            let cmd = $"eqx dump '{sn}' {storeSpecFragment}"
            Log.Information("Inspect via 👉 {cmd}", cmd) }

module Indexer =

    open Propulsion.DynamoStore

    type Arguments(c, p: ParseResults<IndexParameters>) =
        member val GapsLimit =              p.GetResult(IndexParameters.GapsLimit, 10)
        member val ImportJsonFiles =        p.GetResults IndexParameters.DynamoDbJson
        member val TrancheId =              p.TryGetResult IndexParameters.IndexPartitionId |> Option.map (string >> AppendsPartitionId.parse)
        // Larger optimizes for not needing to use TransactWriteItems as frequently
        // Smaller will trigger more items and reduce read costs for Sources reading from the tail
        member val MinItemSize =            p.GetResult(IndexParameters.MinSizeK, 48)
        member val EventsPerBatch =         p.GetResult(IndexParameters.EventsPerBatch, 10000)

        member val StoreArgs =
            match p.GetSubCommand() with
            | IndexParameters.Dynamo p -> Args.Dynamo.Arguments (c, p)
            | x -> Args.missingArg $"unexpected subcommand %A{x}"
        member x.CreateContext() =          x.StoreArgs.CreateContext x.MinItemSize

    let dumpSummary gapsLimit streams spanCount =
        let mutable totalS, totalE, queuing, buffered, gapped = 0, 0L, 0, 0, 0
        for KeyValue (stream, v: DynamoStoreIndex.BufferStreamState) in streams do
            totalS <- totalS + 1
            totalE <- totalE + int64 v.writePos
            if v.spans.Length > 0 then
                match v.spans[0].Index - v.writePos with
                | 0 ->
                    if v.spans.Length > 1 then queuing <- queuing + 1 // There's a gap within the queue
                    else buffered <- buffered + 1 // Everything is fine, just not written yet
                | gap ->
                    gapped <- gapped + 1
                    if gapped < gapsLimit then
                        Log.Warning("Gapped stream {stream}@{wp}: Missing {gap} events before {successorEventTypes}", stream, v.writePos, gap, v.spans[0].c)
                    elif gapped = gapsLimit then
                        Log.Error("Gapped Streams Dump limit ({gapsLimit}) reached; use commandline flag to show more", gapsLimit)
        let level = if gapped > 0 then LogEventLevel.Warning else LogEventLevel.Information
        Log.Write(level, "Index {events:n0} events {streams:n0} streams ({spans:n0} spans) Buffered {buffered} Queueing {queuing} Gapped {gapped:n0}",
                  totalE, totalS, spanCount, buffered, queuing, gapped)

    let run (c: Args.Configuration, p: ParseResults<IndexParameters>) = async {
        let a = Arguments(c, p)
        let context = a.CreateContext()

        match a.TrancheId with
        | None when (not << List.isEmpty) a.ImportJsonFiles ->
            Args.missingArg "Must specify a trancheId parameter to import into"
        | None ->
            let index = AppendsIndex.Reader.create Metrics.log context
            let! state = index.Read()
            Log.Information("Current Partitions / Active Epochs {summary}",
                            seq { for kvp in state -> struct (kvp.Key, kvp.Value) } |> Seq.sortBy (fun struct (t, _) -> t))

            let storeSpecFragment = $"dynamo -t {a.StoreArgs.IndexTable}"
            let dumpCmd sn opts = $"eqx -C dump '{sn}' {opts}{storeSpecFragment}"
            Log.Information("Inspect Index Partitions list events 👉 {cmd}",
                            dumpCmd (AppendsIndex.Stream.name ()) "")

            let pid, eid = AppendsPartitionId.wellKnownId, FSharp.UMX.UMX.tag<appendsEpochId> 2
            Log.Information("Inspect Batches in Epoch {epoch} of Index Partition {partition} 👉 {cmd}",
                            eid, pid, dumpCmd (AppendsEpoch.Stream.name (pid, eid)) "-B ")
        | Some trancheId ->
            let! buffer, indexedSpans = DynamoStoreIndex.Reader.loadIndex (Log.Logger, Metrics.log, context) trancheId a.GapsLimit
            let dump ingestedCount = dumpSummary a.GapsLimit buffer.Items (indexedSpans + ingestedCount)
            dump 0

            match a.ImportJsonFiles with
            | [] -> ()
            | files ->

            Log.Information("Ingesting {files}...", files)

            let ingest =
                let ingester = DynamoStoreIngester(Log.Logger, context, storeLog = Metrics.log)
                fun batch -> ingester.Service.IngestWithoutConcurrency(trancheId, batch)
            let import = DynamoDbExport.Importer(buffer, ingest, dump)
            for file in files do
                let! stats = import.IngestDynamoDbJsonFile(file, a.EventsPerBatch)
                Log.Information("Merged {file}: {items:n0} items {events:n0} events", file, stats.items, stats.events)
            do! import.Flush()
        Equinox.DynamoStore.Core.Log.InternalMetrics.dump Log.Logger }

module Project =

    type KafkaArguments(c, p: ParseResults<KafkaParameters>) =
        member _.Broker =                   p.TryGetResult Broker |> Option.defaultWith (fun () -> c.KafkaBroker)
        member _.Topic =                    p.TryGetResult Topic |> Option.defaultWith (fun () -> c.KafkaTopic)
        member val StoreArgs =
            match p.GetSubCommand() with
            | KafkaParameters.Cosmos    p -> Choice1Of3 (Args.Cosmos.Arguments    (c, p))
            | KafkaParameters.Dynamo    p -> Choice2Of3 (Args.Dynamo.Arguments    (c, p))
            | KafkaParameters.Mdb p -> Choice3Of3 (Args.Mdb.Arguments (c, p))
            | x -> Args.missingArg $"unexpected subcommand %A{x}"

    type StatsArguments(c, p: ParseResults<StatsParameters>) =
        member val StoreArgs =
            match p.GetSubCommand() with
            | StatsParameters.Cosmos    p -> Choice1Of3 (Args.Cosmos.Arguments    (c, p))
            | StatsParameters.Dynamo    p -> Choice2Of3 (Args.Dynamo.Arguments    (c, p))
            | StatsParameters.Mdb p -> Choice3Of3 (Args.Mdb.Arguments (c, p))

    type Arguments(c, p: ParseResults<ProjectParameters>) =
        member val IdleDelay =              TimeSpan.ms 10.
        member val StoreArgs =
            match p.GetSubCommand() with
            | Kafka a -> KafkaArguments(c, a).StoreArgs
            | Stats a -> StatsArguments(c, a).StoreArgs
            | x -> Args.missingArg $"unexpected subcommand %A{x}"

    type Stats(statsInterval, statesInterval, logExternalStats) =
        inherit Propulsion.Streams.Stats<unit>(Log.Logger, statsInterval = statsInterval, statesInterval = statesInterval)
        member val StatsInterval = statsInterval
        override _.HandleOk(()) = ()
        override _.HandleExn(_log, _exn) = ()
        override _.DumpStats() =
            base.DumpStats()
            logExternalStats Log.Logger

    let run (c: Args.Configuration, p: ParseResults<ProjectParameters>) = async {
        let a = Arguments(c, p)
        let storeArgs, dumpStoreStats =
            match a.StoreArgs with
            | Choice1Of3 sa -> Choice1Of3 sa, Equinox.CosmosStore.Core.Log.InternalMetrics.dump
            | Choice2Of3 sa -> Choice2Of3 sa, Equinox.DynamoStore.Core.Log.InternalMetrics.dump
            | Choice3Of3 sa -> Choice3Of3 sa, (fun _ -> ())
        let group, startFromTail, maxItems = p.GetResult ConsumerGroupName, p.Contains FromTail, p.TryGetResult MaxItems
        let producer =
            match p.GetSubCommand() with
            | Kafka a ->
                let a = KafkaArguments(c, a)
                let linger = FsKafka.Batching.BestEffortSerial (TimeSpan.ms 100.)
                let cfg = FsKafka.KafkaProducerConfig.Create(AppName, a.Broker, Confluent.Kafka.Acks.Leader, linger, Confluent.Kafka.CompressionType.Lz4)
                let p = FsKafka.KafkaProducer.Create(Log.Logger, cfg, a.Topic)
                Some p
            | Stats _ -> None
            | x -> Args.missingArg $"unexpected subcommand %A{x}"
        let stats = Stats(TimeSpan.minutes 1., TimeSpan.minutes 5., logExternalStats = dumpStoreStats)
        let sink =
            let maxReadAhead, maxConcurrentStreams = 2, 16
            let handle (stream: FsCodec.StreamName) (span: Propulsion.Sinks.Event[]) = async {
                match producer with
                | None -> ()
                | Some producer ->
                    let json = Propulsion.Codec.NewtonsoftJson.RenderedSpan.ofStreamSpan stream span |> Propulsion.Codec.NewtonsoftJson.Serdes.Serialize
                    do! producer.ProduceAsync(FsCodec.StreamName.toString stream, json) |> Async.Ignore
                return Propulsion.Sinks.AllProcessed, () }
            Propulsion.Sinks.Factory.StartConcurrent(Log.Logger, maxReadAhead, maxConcurrentStreams, handle, stats, idleDelay = a.IdleDelay)
        let source =
            match storeArgs with
            | Choice1Of3 sa ->
                let monitored, leases = sa.ConnectFeed() |> Async.RunSynchronously
                let parseFeedDoc = Propulsion.CosmosStore.EquinoxSystemTextJsonParser.whereStream (fun _sn -> true)
                let observer = Propulsion.CosmosStore.CosmosStoreSource.CreateObserver(Log.Logger, sink.StartIngester, Seq.collect parseFeedDoc)
                Propulsion.CosmosStore.CosmosStoreSource.Start
                  ( Log.Logger, monitored, leases, group, observer,
                    startFromTail = startFromTail, ?maxItems = maxItems, ?lagReportFreq = sa.MaybeLogLagInterval)
            | Choice2Of3 sa ->
                let (indexContext, indexFilter), loadMode = sa.MonitoringParams()
                let checkpoints =
                    let cache = Equinox.Cache (AppName, sizeMb = 1)
                    sa.CreateCheckpointStore(group, cache, Metrics.log)
                Propulsion.DynamoStore.DynamoStoreSource(
                    Log.Logger, stats.StatsInterval,
                    indexContext, defaultArg maxItems 100, TimeSpan.seconds 0.5,
                    checkpoints, sink, loadMode, startFromTail = startFromTail, storeLog = Metrics.log,
                    ?trancheIds = indexFilter
                ).Start()
            | Choice3Of3 sa ->
                let categories, client = sa.CreateClient()
                let checkpoints = sa.CreateCheckpointStore(group)
                Propulsion.MessageDb.MessageDbSource(
                    Log.Logger, stats.StatsInterval,
                    client, defaultArg maxItems 100, TimeSpan.seconds 0.5,
                    checkpoints, sink, categories
                ).Start()
        let work = [
            Async.AwaitKeyboardInterruptAsTaskCanceledException()
            sink.AwaitWithStopOnCancellation()
            source.AwaitWithStopOnCancellation() ]
        return! work |> Async.Parallel |> Async.Ignore<unit[]> }

/// Parse the commandline; can throw exceptions in response to missing arguments and/or `-h`/`--help` args
let parseCommandLine argv =
    let programName = System.Reflection.Assembly.GetEntryAssembly().GetName().Name
    let parser = ArgumentParser.Create<Parameters>(programName = programName)
    parser.ParseCommandLine argv

[<EntryPoint>]
let main argv =
    try let a = parseCommandLine argv
        let verbose, verboseConsole, verboseStore = a.Contains Verbose, a.Contains VerboseConsole, a.Contains VerboseStore
        let metrics = Sinks.equinoxMetricsOnly
        try Log.Logger <- LoggerConfiguration().Configure(verbose).Sinks(metrics, verboseConsole, verboseStore).CreateLogger()
            let c = Args.Configuration(System.Environment.GetEnvironmentVariable >> Option.ofObj)
            try match a.GetSubCommand() with
                | Init a ->         CosmosInit.aux (c, a) |> Async.Ignore<Microsoft.Azure.Cosmos.Container> |> Async.RunSynchronously
                | InitPg a ->       Args.Mdb.Arguments(c, a).CreateCheckpointStoreTable().Wait()
                | Checkpoint a ->   Checkpoints.readOrOverride(c, a, CancellationToken.None).Wait()
                | Index a ->        Indexer.run (c, a) |> Async.RunSynchronously
                | Project a ->      Project.run (c, a) |> Async.RunSynchronously
                | x ->              Args.missingArg $"unexpected subcommand %A{x}"
                0
            with e when not (e :? Args.MissingArg || e :? ArguParseException || e :? System.Threading.Tasks.TaskCanceledException) -> Log.Fatal(e, "Exiting"); 2
        finally Log.CloseAndFlush()
    with :? ArguParseException as e -> eprintfn $"%s{e.Message}"; 1
        | Args.MissingArg msg -> eprintfn $"ERROR: %s{msg}"; 1
        | e -> eprintfn $"EXCEPTION: %s{e.Message}"; 1
