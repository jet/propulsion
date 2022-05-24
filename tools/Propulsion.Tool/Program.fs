module Propulsion.Tool.Program

open Argu
open Propulsion.CosmosStore.Infrastructure // AwaitKeyboardInterruptAsTaskCancelledException
open Serilog
open Serilog.Events
open System

[<NoEquality; NoComparison>]
type Parameters =
    | [<AltCommandLine("-V")>]              Verbose
    | [<AltCommandLine("-C")>]              VerboseConsole
    | [<AltCommandLine("-S")>]              LocalSeq
    | [<CliPrefix(CliPrefix.None); Last; Unique>] Init of ParseResults<InitAuxParameters>
    | [<CliPrefix(CliPrefix.None); Last; Unique>] Checkpoint of ParseResults<CheckpointParameters>
    | [<CliPrefix(CliPrefix.None); Last; Unique>] Project of ParseResults<ProjectParameters>
    interface IArgParserTemplate with
        member a.Usage = a |> function
            | Verbose ->                    "Include low level logging regarding specific test runs."
            | VerboseConsole ->             "Include low level test and store actions logging in on-screen output to console."
            | LocalSeq ->                   "Configures writing to a local Seq endpoint at http://localhost:5341, see https://getseq.net"
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
    | [<CliPrefix(CliPrefix.None); Last>]   Cosmos of ParseResults<Args.Cosmos.Parameters>
    interface IArgParserTemplate with
        member a.Usage = a |> function
            | Topic _ ->                    "Specify target topic. Default: Use $env:PROPULSION_KAFKA_TOPIC"
            | Broker _ ->                   "Specify target broker. Default: Use $env:PROPULSION_KAFKA_BROKER"
            | Cosmos _ ->                   "Cosmos Connection parameters."
and [<NoComparison; NoEquality>] StatsTarget =
    | [<CliPrefix(CliPrefix.None); Last; Unique>] Cosmos of ParseResults<Args.Cosmos.Parameters>
    interface IArgParserTemplate with
        member a.Usage = a |> function
            | Cosmos _ ->                   "Cosmos Connection parameters."

let createLog verbose verboseConsole maybeSeqEndpoint =
    let c = LoggerConfiguration().Destructure.FSharpTypes().Enrich.FromLogContext()
    let c = if verbose then c.MinimumLevel.Debug() else c
    let c = c.WriteTo.Sink(Equinox.CosmosStore.Core.Log.InternalMetrics.Stats.LogSink())
    let outputTemplate =
        let full = "{Timestamp:T} {Level:u1} {Message:l} {Properties}{NewLine}{Exception}"
        if verbose && verboseConsole then full else full.Replace("{Properties}", null)
    let c = c.WriteTo.Console((if verboseConsole then LogEventLevel.Debug else LogEventLevel.Information), outputTemplate, theme = Sinks.SystemConsole.Themes.AnsiConsoleTheme.Code)
    let c = match maybeSeqEndpoint with None -> c | Some endpoint -> c.WriteTo.Seq(endpoint)
    c.CreateLogger()

let [<Literal>] appName = "propulsion-tool"

module CosmosInit =

    let aux (log : ILogger) (a : ParseResults<InitAuxParameters>) = async {
        match a.TryGetSubCommand() with
        | Some (InitAuxParameters.Cosmos sa) ->
            let args = Cosmos.Arguments(sa)
            let client = args.ConnectLeases(log)
            let rus = a.GetResult(InitAuxParameters.Rus)
            log.Information("Provisioning Leases Container for {rus:n0} RU/s", rus)
            return! Equinox.CosmosStore.Core.Initialization.initAux client.Database.Client (client.Database.Id, client.Id) rus
        | _ -> return failwith "please specify a `cosmos` endpoint" }

module Checkpoints =

    type CheckpointArguments(c, a : ParseResults<CheckpointParameters>) =

        member val StoreArgs =
            match a.GetSubCommand() with
            | CheckpointParameters.Cosmos cosmos -> Choice1Of2 (Args.Cosmos.Arguments (c, cosmos))
            | CheckpointParameters.Dynamo dynamo -> Choice2Of2 (Args.Dynamo.Arguments (c, dynamo))
            | _ -> Args.missingArg "Must specify `cosmos` or `dynamo` store"

    let readOrOverride (log : ILogger) (c, a : ParseResults<CheckpointParameters>) = async {
        let args = CheckpointArguments(c, a)
        let source, tranche, group = a.GetResult Source, a.GetResult Tranche, a.GetResult Group
        Log.Information("Checkpoint Target Source {source} Tranche {tranche} for {group}", source, tranche, group)

        let! store, overridePos = async {
            match args.StoreArgs with
            | Choice1Of2 cosmos ->
                let! store = cosmos.CreateCheckpointStore(log, group, Equinox.Cache (appName, sizeMb = 1))
                return (store : Propulsion.Feed.IFeedCheckpointStore), fun pos -> store.Override(source, tranche, pos)
            | Choice2Of2 dynamo ->
                let store = dynamo.CreateCheckpointStore(log, group, Equinox.Cache (appName, sizeMb = 1))
                return store, fun pos -> store.Override(source, tranche, pos) }
        match a.TryGetResult OverridePosition with
        | None ->
            let! interval, pos = store.Start(source, tranche)
            log.Information("Checkpoint position {pos}; Checkpoint event frequency {checkpointEventIntervalM:f0}m", pos, interval.TotalMinutes)
        | Some pos ->
            log.Warning("Resetting Checkpoint position to {pos}", pos)
            do! overridePos pos }


type Stats(log, statsInterval, statesInterval) =
    inherit Propulsion.Streams.Stats<unit>(log, statsInterval = statsInterval, statesInterval = statesInterval)
    member val StatsInterval = statsInterval
    override _.HandleOk(_log) = ()
    override _.HandleExn(_log, _exn) = ()

[<EntryPoint>]
let main argv =
    let programName = System.Reflection.Assembly.GetEntryAssembly().GetName().Name
    let parser = ArgumentParser.Create<Parameters>(programName = programName)
    try
        let args = parser.ParseCommandLine argv
        let verboseConsole = args.Contains VerboseConsole
        let maybeSeq = if args.Contains LocalSeq then Some "http://localhost:5341" else None
        let verbose = args.Contains Verbose
        let log = createLog verbose verboseConsole maybeSeq
        let c = Args.Configuration(Environment.GetEnvironmentVariable >> Option.ofObj)
        match args.GetSubCommand() with
        | Init a -> CosmosInit.aux log a |> Async.Ignore<Microsoft.Azure.Cosmos.Container> |> Async.RunSynchronously
        | Project pargs ->
            let group, startFromTail, maxItems = pargs.GetResult ConsumerGroupName, pargs.Contains FromTail, pargs.TryGetResult MaxItems
            maxItems |> Option.iter (fun bs -> log.Information("ChangeFeed Max items Count {changeFeedMaxItems}", bs))
            if startFromTail then Log.Warning("ChangeFeed (If new projector group) Skipping projection of all existing events.")
            let maybeLogLagInterval = pargs.TryGetResult LagFreqM |> Option.map TimeSpan.FromMinutes
            let broker, topic, storeArgs =
                match pargs.GetSubCommand() with
                | Kafka kargs -> Some c.KafkaBroker, Some c.KafkaTopic, kargs.GetResult KafkaTarget.Cosmos
                | Stats sargs -> None, None, sargs.GetResult StatsTarget.Cosmos
                | x -> failwithf "Invalid subcommand %A" x
            let args = Args.Cosmos.Arguments(c, storeArgs)
            let monitored = args.MonitoredContainer(log)
            let leases = args.ConnectLeases(log)

            let producer =
                match broker, topic with
                | Some b, Some t ->
                    let linger = FsKafka.Batching.BestEffortSerial (TimeSpan.FromMilliseconds 100.)
                    let cfg = FsKafka.KafkaProducerConfig.Create(appName, b, Confluent.Kafka.Acks.Leader, linger, Confluent.Kafka.CompressionType.Lz4)
                    let p = FsKafka.KafkaProducer.Create(log, cfg, t)
                    Some p
                | _ -> None
            let sink =
                let stats = Stats(log, TimeSpan.FromMinutes 1., TimeSpan.FromMinutes 5.)
                let maxReadAhead, maxConcurrentStreams = 2, 16
                let handle (stream : FsCodec.StreamName, span : Propulsion.Streams.StreamSpan<_>) = async {
                    match producer with
                    | None -> ()
                    | Some producer ->
                        let json = Propulsion.Codec.NewtonsoftJson.RenderedSpan.ofStreamSpan stream span |> Newtonsoft.Json.JsonConvert.SerializeObject
                        let! _ = producer.ProduceAsync(FsCodec.StreamName.toString stream, json) in () }
                Propulsion.Streams.StreamsProjector.Start(log, maxReadAhead, maxConcurrentStreams, handle, stats, stats.StatsInterval)
            let source =
                let transformOrFilter = Propulsion.CosmosStore.EquinoxSystemTextJsonParser.enumStreamEvents
                let observer = Propulsion.CosmosStore.CosmosStoreSource.CreateObserver(log, sink.StartIngester, Seq.collect transformOrFilter)
                Propulsion.CosmosStore.CosmosStoreSource.Start
                  ( log, monitored, leases, group, observer,
                    startFromTail = startFromTail, ?maxItems = maxItems, ?lagReportFreq = maybeLogLagInterval)
            [   Async.AwaitKeyboardInterruptAsTaskCancelledException()
                sink.AwaitWithStopOnCancellation()
                source.AwaitWithStopOnCancellation() ]
            |> Async.Parallel
            |> Async.Ignore<unit[]>
            |> Async.RunSynchronously
        | _ -> failwith "Please specify a valid subcommand :- init, checkpoint or project"
        0
    with :? ArguParseException as e -> eprintfn "%s" e.Message; 1
        | Args.MissingArg msg -> eprintfn "%s" msg; 1
        | e -> eprintfn "%s" e.Message; 1
