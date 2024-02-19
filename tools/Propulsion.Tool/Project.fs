module Propulsion.Tool.Project

open Argu
open Infrastructure
open Propulsion.Internal
open Serilog

type [<NoEquality; NoComparison; RequireSubcommand>] Parameters =
    | [<AltCommandLine "-g"; Mandatory>]    ConsumerGroupName of string
    | [<AltCommandLine "-Z"; Unique>]       FromTail
    | [<AltCommandLine "-F"; Unique>]       Follow
    | [<AltCommandLine "-b"; Unique>]       MaxItems of int
    | [<CliPrefix(CliPrefix.None); Last>]   Stats of ParseResults<SourceParameters>
    | [<CliPrefix(CliPrefix.None); Last>]   Kafka of ParseResults<KafkaParameters>
    interface IArgParserTemplate with
        member a.Usage = a |> function
            | ConsumerGroupName _ ->        "Projector instance context name."
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
    interface IArgParserTemplate with
        member a.Usage = a |> function
            | Cosmos _ ->                   "Specify CosmosDB parameters."
            | Dynamo _ ->                   "Specify DynamoDB parameters."
            | Mdb _ ->                      "Specify MessageDb parameters."
type [<NoEquality; NoComparison>] SourceArgs =
    | Cosmos of Args.Cosmos.Arguments
    | Dynamo of Args.Dynamo.Arguments
    | Mdb of Args.Mdb.Arguments
type KafkaArguments(c: Args.Configuration, p: ParseResults<KafkaParameters>) =
    member val Broker =                  p.GetResult(Broker, fun () -> c.KafkaBroker)
    member val Topic =                   p.GetResult(Topic, fun () -> c.KafkaTopic)
    member val Source =                  SourceArguments(c, p.GetResult KafkaParameters.Source)
and SourceArguments(c, p: ParseResults<SourceParameters>) =
    member val StoreArgs =
        match p.GetSubCommand() with
        | SourceParameters.Cosmos p ->   Cosmos (Args.Cosmos.Arguments (c, p))
        | SourceParameters.Dynamo p ->   Dynamo (Args.Dynamo.Arguments (c, p))
        | SourceParameters.Mdb p ->      Mdb (Args.Mdb.Arguments (c, p))

type Arguments(c, p: ParseResults<Parameters>) =
    member val IdleDelay =              TimeSpan.ms 10.
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

let run appName (c: Args.Configuration, p: ParseResults<Parameters>) = async {
    let a = Arguments(c, p)
    let dumpStoreStats =
        match a.StoreArgs with
        | Cosmos _ -> Equinox.CosmosStore.Core.Log.InternalMetrics.dump
        | Dynamo _ -> Equinox.DynamoStore.Core.Log.InternalMetrics.dump
        | Mdb _ -> ignore
    let group, startFromTail, follow, maxItems = p.GetResult ConsumerGroupName, p.Contains FromTail, p.Contains Follow, p.TryGetResult MaxItems
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
        match a.StoreArgs with
        | Cosmos sa ->
            let monitored, leases = sa.ConnectFeed() |> Async.RunSynchronously
            let parseFeedDoc = Propulsion.CosmosStore.EquinoxSystemTextJsonParser.whereStream (fun _sn -> true)
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
            raise <| System.Threading.Tasks.TaskCanceledException "Stopping; FeedMonitor wait completed" } // trigger tear down of sibling waits
        sink.AwaitWithStopOnCancellation() ]
    return! work |> Async.Parallel |> Async.Ignore<unit[]> }
