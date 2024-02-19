module Propulsion.Tool.Project

open Argu
open Infrastructure
open Propulsion.Internal
open Serilog

type [<NoEquality; NoComparison; RequireSubcommand>] Parameters =
    | [<AltCommandLine "-g"; Unique>]       ConsumerGroupName of string
    | [<AltCommandLine "-r"; Unique>]       MaxReadAhead of int
    | [<AltCommandLine "-w"; Unique>]       MaxWriters of int
    | [<AltCommandLine "-Z"; Unique>]       FromTail
    | [<AltCommandLine "-F"; Unique>]       Follow
    | [<AltCommandLine "-b"; Unique>]       MaxItems of int
    | [<CliPrefix(CliPrefix.None); Last>]   Stats of ParseResults<SourceParameters>
    | [<CliPrefix(CliPrefix.None); Last>]   Kafka of ParseResults<KafkaParameters>
    interface IArgParserTemplate with
        member a.Usage = a |> function
            | ConsumerGroupName _ ->        "Projector instance context name. Optional if source is JSON"
            | MaxReadAhead _ ->             "maximum number of batches to let processing get ahead of completion. Default: File: 32768 Other: 2."
            | MaxWriters _ ->               "maximum number of concurrent streams on which to process at any time. Default: 8 (Sync: 16)."
            | FromTail ->                   "(iff fresh projection) - force starting from present Position. Default: Ensure each and every event is projected from the start."
            | Follow ->                     "Stop when the Tail is reached."
            | MaxItems _ ->                 "Controls checkpointing granularity by adjusting the batch size being loaded from the feed. Default: Unlimited"
            | Stats _ ->                    "Do not emit events, only stats."
            | Kafka _ ->                    "Project to Kafka."
and [<NoEquality; NoComparison; RequireSubcommand>] KafkaParameters =
    | [<AltCommandLine "-t"; Unique; MainCommand>] Topic of string
    | [<AltCommandLine "-b"; Unique>]       Broker of string
    | [<CliPrefix(CliPrefix.None); Last>]   Source of ParseResults<SourceParameters>
    interface IArgParserTemplate with
        member a.Usage = a |> function
            | Topic _ ->                    "Specify target topic. Default: Use $env:PROPULSION_KAFKA_TOPIC"
            | Broker _ ->                   "Specify target broker. Default: Use $env:PROPULSION_KAFKA_BROKER"
            | Source _ ->                   "Specify Source."
and [<NoEquality; NoComparison; RequireSubcommand>] SourceParameters =
    | [<CliPrefix(CliPrefix.None); Last; Unique>] Cosmos of ParseResults<Args.Cosmos.Parameters>
    | [<CliPrefix(CliPrefix.None); Last; Unique>] Dynamo of ParseResults<Args.Dynamo.Parameters>
    | [<CliPrefix(CliPrefix.None); Last; Unique>] Mdb    of ParseResults<Args.Mdb.Parameters>
    | [<CliPrefix(CliPrefix.None); Last; Unique>] Json   of ParseResults<Args.Json.Parameters>
    interface IArgParserTemplate with
        member a.Usage = a |> function
            | Cosmos _ ->                   "Specify CosmosDB parameters."
            | Dynamo _ ->                   "Specify DynamoDB parameters."
            | Mdb _ ->                      "Specify MessageDb parameters."
            | Json _ ->                     "Specify JSON file parameters."
type [<NoEquality; NoComparison>] SourceArgs =
    | Cosmos of Args.Cosmos.Arguments
    | Dynamo of Args.Dynamo.Arguments
    | Mdb of Args.Mdb.Arguments
    | Json of Args.Json.Arguments
type KafkaArguments(c: Args.Configuration, p: ParseResults<KafkaParameters>) =
    member val Broker =                     p.GetResult(Broker, fun () -> c.KafkaBroker)
    member val Topic =                      p.GetResult(Topic, fun () -> c.KafkaTopic)
    member val Source =                     SourceArguments(c, p.GetResult KafkaParameters.Source)
and SourceArguments(c, p: ParseResults<SourceParameters>) =
    member val StoreArgs =
        match p.GetSubCommand() with
        | SourceParameters.Cosmos p ->      Cosmos (Args.Cosmos.Arguments (c, p))
        | SourceParameters.Dynamo p ->      Dynamo (Args.Dynamo.Arguments (c, p))
        | SourceParameters.Mdb p ->         Mdb (Args.Mdb.Arguments (c, p))
        | SourceParameters.Json p ->        Json (Args.Json.Arguments (c, p))

type Arguments(c, p: ParseResults<Parameters>) =
    member val IdleDelay =                  TimeSpan.ms 10.
    member val StoreArgs =
        match p.GetSubCommand() with
        | Kafka a -> KafkaArguments(c, a).Source.StoreArgs
        | Stats a -> SourceArguments(c, a).StoreArgs
        | x -> p.Raise $"unexpected subcommand %A{x}"

type Stats(statsInterval, statesInterval, logExternalStats) =
    inherit Propulsion.Streams.Stats<unit>(Log.Logger, statsInterval = statsInterval, statesInterval = statesInterval)
    member val StatsInterval = statsInterval
    override _.HandleOk(()) = ()
    override _.HandleExn(_log, _exn) = ()
    override _.DumpStats() =
        base.DumpStats()
        logExternalStats Log.Logger

let eofSignalException = System.Threading.Tasks.TaskCanceledException "Stopping; FeedMonitor wait completed"
let run appName (c: Args.Configuration, p: ParseResults<Parameters>) = async {
    let a = Arguments(c, p)
    let dumpStoreStats =
        match a.StoreArgs with
        | Cosmos _ -> Equinox.CosmosStore.Core.Log.InternalMetrics.dump
        | Dynamo _ -> Equinox.DynamoStore.Core.Log.InternalMetrics.dump
        | Mdb _ -> ignore
        | Json _ -> ignore
    let group =
        match p.TryGetResult ConsumerGroupName, a.StoreArgs with
        | Some x, _ -> x
        | None, Json _ -> System.Guid.NewGuid() |> _.ToString("N")
        | None, _ -> p.Raise "ConsumerGroupName is mandatory, unless consuming from a JSON file"
    let startFromTail, follow, maxItems = p.Contains FromTail, p.Contains Follow, p.TryGetResult MaxItems
    let producer =
        match p.GetSubCommand() with
        | Kafka a ->
            let a = KafkaArguments(c, a)
            let linger = FsKafka.Batching.BestEffortSerial (TimeSpan.ms 100.)
            let cfg = FsKafka.KafkaProducerConfig.Create(appName, a.Broker, Confluent.Kafka.Acks.Leader, linger, Confluent.Kafka.CompressionType.Lz4)
            let p = FsKafka.KafkaProducer.Create(Log.Logger, cfg, a.Topic)
            Some p
        | Stats _ -> None
        | x -> p.Raise $"unexpected subcommand %A{x}"
    let isFileSource = match a.StoreArgs with Json _ -> true | _ -> true
    let maxReadAhead = p.GetResult(MaxReadAhead, if isFileSource then 32768 else 2)
    let maxConcurrentProcessors = p.GetResult(MaxWriters, 8)
    let stats = Stats(TimeSpan.minutes 1., TimeSpan.minutes 5., logExternalStats = dumpStoreStats)
    let sink =
        let handle (stream: FsCodec.StreamName) (span: Propulsion.Sinks.Event[]) = async {
            match producer with
            | None -> ()
            | Some producer ->
                let json = Propulsion.Codec.NewtonsoftJson.RenderedSpan.ofStreamSpan stream span |> Propulsion.Codec.NewtonsoftJson.Serdes.Serialize
                do! producer.ProduceAsync(FsCodec.StreamName.toString stream, json) |> Async.Ignore
            return Propulsion.Sinks.AllProcessed, () }
        Propulsion.Sinks.Factory.StartConcurrent(Log.Logger, maxReadAhead, maxConcurrentProcessors, handle, stats, idleDelay = a.IdleDelay)
    let source =
        let parseFeedDoc = Propulsion.CosmosStore.EquinoxSystemTextJsonParser.whereStream (fun _sn -> true)
        match a.StoreArgs with
        | Cosmos sa ->
            let monitored, leases = sa.ConnectFeed() |> Async.RunSynchronously
            Propulsion.CosmosStore.CosmosStoreSource(
                Log.Logger, stats.StatsInterval, monitored, leases, group, parseFeedDoc, sink,
                startFromTail = startFromTail, ?maxItems = maxItems, ?lagEstimationInterval = sa.MaybeLogLagInterval
            ).Start()
        | Dynamo sa ->
            let (indexContext, indexFilter), loadMode = sa.MonitoringParams()
            let checkpoints =
                let cache = Equinox.Cache (appName, sizeMb = 1)
                sa.CreateCheckpointStore(group, cache, Metrics.log)
            Propulsion.DynamoStore.DynamoStoreSource(
                Log.Logger, stats.StatsInterval,
                indexContext, defaultArg maxItems 100, TimeSpan.seconds 0.5,
                checkpoints, sink, loadMode, startFromTail = startFromTail, storeLog = Metrics.log,
                ?trancheIds = indexFilter
            ).Start()
        | Mdb sa ->
            let categories, client = sa.CreateClient()
            let checkpoints = sa.CreateCheckpointStore(group)
            Propulsion.MessageDb.MessageDbSource(
                Log.Logger, stats.StatsInterval,
                client, defaultArg maxItems 100, TimeSpan.seconds 0.5,
                checkpoints, sink, categories
            ).Start()
        | Json sa ->
            CosmosDumpSource.Start(Log.Logger, stats.StatsInterval, sa.Filepath, sa.Skip, parseFeedDoc, sink, ?truncateTo = sa.Trunc)

    let work = [
        Async.AwaitKeyboardInterruptAsTaskCanceledException()
        if follow then
            source.AwaitWithStopOnCancellation()
        else async {
            let initialWait = TimeSpan.seconds 10
            do! source.Monitor.AwaitCompletion(initialWait, awaitFullyCaughtUp = true, logInterval = stats.StatsInterval / 2.) |> Async.ofTask
            source.Stop()
            do! source.Await() // Let it emit the stats
            do! source.Flush() |> Async.Ignore<Propulsion.Feed.TranchePositions> // flush checkpoints (currently a no-op)
            raise eofSignalException } // trigger tear down of sibling waits
        sink.AwaitWithStopOnCancellation() ]
    return! work |> Async.Parallel |> Async.Ignore<unit[]> }
