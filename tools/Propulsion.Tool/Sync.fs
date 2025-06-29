module Propulsion.Tool.Sync

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
    | [<AltCommandLine "-A"; Unique>]       RequireAll
    | [<AltCommandLine "-E"; Unique>]       EventsOnly
    | [<AltCommandLine "-C"; Unique>]       Categorize
    | [<AltCommandLine "-b"; Unique>]       MaxItems of int

    | [<AltCommandLine "-N";    AltCommandLine "--exclude-system"; Unique>] ExcSys
    | [<AltCommandLine "-cat";  AltCommandLine "--include-category">]   IncCat of    regex: string
    | [<AltCommandLine "-ncat"; AltCommandLine "--exclude-category">]   ExcCat of    regex: string
    | [<AltCommandLine "-sn";   AltCommandLine "--include-streamname">] IncStream of regex: string
    | [<AltCommandLine "-nsn";  AltCommandLine "--exclude-streamname">] ExcStream of regex: string
    | [<AltCommandLine "-et";   AltCommandLine "--include-eventtype">]  IncEvent of  regex: string
    | [<AltCommandLine "-net";  AltCommandLine "--exclude-eventtype">]  ExcEvent of  regex: string

    | [<CliPrefix(CliPrefix.None); Last>]   Stats of ParseResults<SourceParameters>
    | [<CliPrefix(CliPrefix.None); Last>]   Kafka of ParseResults<KafkaParameters>
    | [<CliPrefix(CliPrefix.None); Last>]   Cosmos of ParseResults<CosmosParameters>
    interface IArgParserTemplate with
        member a.Usage = a |> function
            | ConsumerGroupName _ ->        "Projector instance context name. Optional if source is JSON"
            | MaxReadAhead _ ->             "maximum number of batches to let processing get ahead of completion. Default: File: 32768 Other: 4."
            | MaxWriters _ ->               "maximum number of concurrent streams on which to process at any time. Default: 8 (Cosmos: 16)."
            | FromTail ->                   "(iff fresh projection) - force starting from present Position. Default: Ensure each and every event is projected from the start."
            | Follow ->                     "Stop when the Tail is reached."
            | RequireAll ->                 "Wait for out of order events to arrive (including waiting for event 0 per stream) before dispatching for any stream. " +
                                            "NOTE normally a large `MaxReadAhead` and `cosmos -b` is required to avoid starving the scheduler. " +
                                            "NOTE This mode does not make sense to apply unless the ProcessorName is fresh; if the consumer group name is not fresh (and hence items are excluded from the feed), there will inevitably be missing events, and processing will stall. " +
                                            "Default: assume events arrive from the changefeed (and/or the input JSON file) without any gaps or out of order deliveries for any stream."
            | EventsOnly ->                 "Exclude Unfolds from processing. Default: Unfolds are read, parsed and processed"
            | Categorize ->                 "Gather handler latency stats by category"
            | MaxItems _ ->                 "Limits RU consumption when reading; impacts checkpointing granularity by adjusting the batch size being loaded from the feed. Default (Sync): 9999. Default: 100 (CosmosClient default)"

            | ExcSys ->                     "Exclude System streams. Default: Include Index Streams, identified by a $ prefix."
            | IncCat _ ->                   "Allow Stream Category. Multiple values are combined with OR. Default: include all, subject to Category Deny and Stream Deny rules."
            | ExcCat _ ->                   "Deny  Stream Category. Specified values/regexes are applied after the Category Allow rule(s)."
            | IncStream _ ->                "Allow Stream Name. Multiple values are combined with OR. Default: Allow all streams that pass the category Allow test, Fail the Category and Stream deny tests."
            | ExcStream _ ->                "Deny  Stream Name. Specified values/regexes are applied after the IncCat, ExcCat and IncStream filters."

            | IncEvent _ ->                 "Allow Event Type Name. Multiple values are combined with OR. Applied only after Category and Stream filters. Default: include all."
            | ExcEvent _ ->                 "Deny  Event Type Name. Specified values/regexes are applied after the Event Type Name Allow rule(s)."

            | Stats _ ->                    "Do not emit events, only stats."
            | Kafka _ ->                    "Project to Kafka."
            | Cosmos _ ->                   "Feed Events into specified Cosmos Store."
and Arguments(c, p: ParseResults<Parameters>) =
    member val Filters = Propulsion.StreamFilter(
                                            allowCats = p.GetResults IncCat, denyCats = p.GetResults ExcCat,
                                            allowSns = p.GetResults IncStream, denySns = p.GetResults ExcStream,
                                            includeSystem = not (p.Contains ExcSys),
                                            allowEts = p.GetResults IncEvent, denyEts = p.GetResults ExcEvent)
    member val Categorize =                 p.Contains Categorize
    member val EventsOnly =                 p.Contains EventsOnly
    member val Command =
        match p.GetSubCommand() with
        | Kafka a ->                        KafkaArguments(c, a) |> SubCommand.Kafka
        | Stats a ->                        SourceArguments(c, a) |> SubCommand.Stats
        | Parameters.Cosmos a ->            CosmosArguments(c, a) |> SubCommand.Sync
        | x ->                              p.Raise $"unexpected subcommand %A{x}"
    member val StatsInterval =              TimeSpan.minutes 1
    member val StateInterval =              TimeSpan.minutes 5
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
and SourceArguments(c, p: ParseResults<SourceParameters>) =
    member val Store = p.GetSubCommand () |> function
        | SourceParameters.Cosmos p ->      Cosmos (Args.Cosmos.Arguments (c, p))
        | SourceParameters.Dynamo p ->      Dynamo (Args.Dynamo.Arguments (c, p))
        | SourceParameters.Mdb p ->         Mdb (Args.Mdb.Arguments (c, p))
        | SourceParameters.Json p ->        Json (Args.Json.Arguments p)
and [<NoEquality; NoComparison>] StoreArgs =
    | Cosmos of Args.Cosmos.Arguments
    | Dynamo of Args.Dynamo.Arguments
    | Mdb of Args.Mdb.Arguments
    | Json of Args.Json.Arguments
and [<NoEquality; NoComparison; RequireSubcommand>] KafkaParameters =
    | [<AltCommandLine "-t"; Unique; MainCommand>] Topic of string
    | [<AltCommandLine "-b"; Unique>]       Broker of string
    | [<CliPrefix(CliPrefix.None); Last>]   From of ParseResults<SourceParameters>
    interface IArgParserTemplate with
        member a.Usage = a |> function
            | Topic _ ->                    "Specify target topic. Default: Use $env:PROPULSION_KAFKA_TOPIC"
            | Broker _ ->                   "Specify target broker. Default: Use $env:PROPULSION_KAFKA_BROKER"
            | From _ ->                     "Specify Source."
and KafkaArguments(c: Args.Configuration, p: ParseResults<KafkaParameters>) =
    member val Broker =                     p.GetResult(Broker, fun () -> c.KafkaBroker)
    member val Topic =                      p.GetResult(Topic, fun () -> c.KafkaTopic)
    member val Source =                     SourceArguments(c, p.GetResult KafkaParameters.From)
and [<NoEquality; NoComparison; RequireSubcommand>] CosmosParameters =
    | [<AltCommandLine "-m">]               ConnectionMode of Microsoft.Azure.Cosmos.ConnectionMode
    | [<AltCommandLine "-s">]               Connection of string
    | [<AltCommandLine "-d">]               Database of string
    | [<AltCommandLine "-c"; Mandatory>]    Container of string
    | [<AltCommandLine "-a">]               LeaseContainerId of string
    | [<AltCommandLine "-r">]               Retries of int
    | [<AltCommandLine "-rt">]              RetriesWaitTime of float
    | [<AltCommandLine "-kb">]              MaxKiB of int
    | [<CliPrefix(CliPrefix.None); Last>]   From of ParseResults<SourceParameters>
    interface IArgParserTemplate with
        member a.Usage = a |> function
            | ConnectionMode _ ->           "override the connection mode. Default: Direct."
            | Connection _ ->               $"""specify a connection string for the destination Cosmos account.
                                            Default (From Cosmos): Same as Source.
                                            Default (From Json): optional if environment variable {Args.Configuration.Cosmos.CONNECTION} specified"""
            | Database _ ->                 $"""specify a database name for store.
                                            Default (From Cosmos): Same as Source.
                                            Default (From Json): optional if environment variable {Args.Configuration.Cosmos.DATABASE} specified"""
            | Container _ ->                "specify a container name for store."
            | LeaseContainerId _ ->         "store leases in Sync target DB (default: use `-aux` adjacent to the Source Container). Enables the Source to be read via a ReadOnly connection string."
            | Retries _ ->                  "specify operation retries. Default: 2."
            | RetriesWaitTime _ ->          "specify max wait-time for retry when being throttled by Cosmos in seconds. Default: 5."
            | MaxKiB _ ->                   "specify maximum size in KiB to pass to the Sync stored proc (reduce if Malformed Streams due to 413 RequestTooLarge responses). Default: 128."
            | From _ ->                     "Specify Source."
and CosmosArguments(c: Args.Configuration, p: ParseResults<CosmosParameters>) =
    let source =                            SourceArguments(c, p.GetResult CosmosParameters.From)
    let connection =                        match source.Store with
                                            | Cosmos c -> p.GetResult(Connection, fun () -> c.Connection)
                                            | Json _ -> p.GetResult(Connection, fun () -> c.CosmosConnection)
                                            | x -> p.Raise $"unexpected subcommand %A{x}"
    let connector =
        let retries =                       p.GetResult(Retries, 2)
        let maxRetryWaitTime =              p.GetResult(RetriesWaitTime, 5) |> TimeSpan.seconds
        let mode =                          p.TryGetResult ConnectionMode
        Equinox.CosmosStore.CosmosStoreConnector(Equinox.CosmosStore.Discovery.ConnectionString connection, retries, maxRetryWaitTime, ?mode = mode)
    let database =                          match source.Store with
                                            | Cosmos c -> p.GetResult(Database, fun () -> c.Database)
                                            | Json _ -> p.GetResult(Database, fun () -> c.CosmosDatabase)
                                            | x -> p.Raise $"unexpected subcommand %A{x}"
    let container =                         p.GetResult Container
    member val Source =                     source
    member val MaxBytes =                   p.GetResult(MaxKiB, 128) * 1024
    member x.Connect() =                    connector.ConnectContext("Destination", database, container, maxEvents = 128)
    member x.ConnectEvents() = async {      let! context = x.Connect()
                                            return Equinox.CosmosStore.Core.EventsContext(context, Metrics.log) }
    member x.ConnectFeed() =                let source = match source.Store with StoreArgs.Cosmos c -> c | _ -> p.Raise "unexpected"
                                            match p.TryGetResult LeaseContainerId with
                                            | Some localAuxContainerId -> source.ConnectFeedReadOnly(connector.CreateUninitialized(), database, localAuxContainerId)
                                            | None -> source.ConnectFeed()
and [<NoEquality; NoComparison; RequireQualifiedAccess>] SubCommand =
    | Kafka of KafkaArguments
    | Stats of SourceArguments
    | Sync of CosmosArguments
    member x.Source: StoreArgs = x |> function
        | SubCommand.Kafka a -> a.Source.Store
        | SubCommand.Stats a -> a.Store
        | SubCommand.Sync a -> a.Source.Store

[<AbstractClass>]
type StatsBase<'outcome>(log, statsInterval, stateInterval, logExternalStats) =
    inherit Propulsion.Streams.Stats<'outcome>(log, statsInterval, stateInterval, logExternalStats = logExternalStats)

    override _.HandleExn(log, exn) =
        log.Information(exn, "Unhandled")

type Outcome = (struct (string * (struct (string * System.TimeSpan))[] * (string * int)[]))
module Outcome =
    let private eventType (x: Propulsion.Sinks.Event) = x.EventType
    let private eventCounts = Array.countBy eventType
    let private create sn ham spam: Outcome = struct (FsCodec.StreamName.Category.ofStreamName sn, ham, spam)
    let render_ sn ham spam elapsedS =
        let share = TimeSpan.seconds (match Array.length ham with 0 -> 0 | count -> elapsedS / float count)
        create sn (ham |> Array.map (fun x -> struct (eventType x, share))) (eventCounts spam)

type Stats(log: ILogger, statsInterval, stateInterval, logExternalStats) =
    inherit StatsBase<Outcome>(log, statsInterval, stateInterval, logExternalStats)
    let mutable handled, ignored = 0, 0
    let accHam, accSpam = Stats.CategoryCounters(), Stats.CategoryCounters()
    let intervalLats, accEventTypeLats = Stats.EventTypeLatencies(), Stats.EventTypeLatencies()
    override _.HandleOk((category, ham, spam)) =
        accHam.Ingest(category, ham |> Seq.countBy ValueTuple.fst)
        accSpam.Ingest(category, spam)
        handled <- handled + Array.length ham
        ignored <- ignored + Array.sumBy snd spam
        for eventType, latency in ham do
            intervalLats.Record(category, eventType, latency)
            accEventTypeLats.Record(category, eventType, latency)
    override _.DumpStats() =
        if handled > 0 || ignored > 0 then
            if ignored > 0 then log.Information(" Handled {count}, skipped {skipped}", handled, ignored)
            handled <- 0; ignored <- 0
        base.DumpStats()
        intervalLats.Dump(log, "EVENTS")
        intervalLats.Clear()
    override _.DumpState purge =
        base.DumpState purge
        for cat in Seq.append accHam.Categories accSpam.Categories |> Seq.distinct |> Seq.sort do
            let ham, spam = accHam.StatsDescending(cat) |> Array.ofSeq, accSpam.StatsDescending cat |> Array.ofSeq
            if ham.Length > 0 then log.Information(" Category {cat} handled {@ham}", cat, ham)
            if spam.Length <> 0 then log.Information(" Category {cat} ignored {@spam}", cat, spam)
        accEventTypeLats.Dump(log, "ΣEVENTS")
        if purge then
            accHam.Clear(); accSpam.Clear()
            accEventTypeLats.Clear()

let private handle isValidEvent stream (events: Propulsion.Sinks.Event[]): Async<Outcome * int64> = async {
    let ham, spam = events |> Array.partition isValidEvent
    return Outcome.render_ stream ham spam 0, Propulsion.Sinks.Events.next events }

let eofSignalException = System.Threading.Tasks.TaskCanceledException "Stopping; FeedMonitor wait completed"
let run appName (c: Args.Configuration, p: ParseResults<Parameters>) = async {
    let a = Arguments(c, p)
    let dumpStoreStats =
        match a.Command.Source with
        | Cosmos _ -> Equinox.CosmosStore.Core.Log.InternalMetrics.dump
        | Dynamo _ -> Equinox.DynamoStore.Core.Log.InternalMetrics.dump
        | Mdb _ -> ignore
        | Json _ -> match a.Command with SubCommand.Sync _ -> Equinox.CosmosStore.Core.Log.InternalMetrics.dump | _ -> ignore
    let group =
        match p.TryGetResult ConsumerGroupName, a.Command.Source with
        | Some x, _ -> x
        | None, Json _ -> System.Guid.NewGuid() |> _.ToString("N")
        | None, _ -> p.Raise "ConsumerGroupName is mandatory, unless consuming from a JSON file"
    let startFromTail, follow, requireAll = p.Contains FromTail, p.Contains Follow, p.Contains RequireAll
    let maxItems = match a.Command with SubCommand.Sync _ -> p.GetResult(MaxItems, 9999) |> Some | _ -> p.TryGetResult MaxItems
    let producer =
        match a.Command with
        | SubCommand.Kafka a ->
            let linger = FsKafka.Batching.BestEffortSerial (TimeSpan.ms 100.)
            let cfg = FsKafka.KafkaProducerConfig.Create(appName, a.Broker, Confluent.Kafka.Acks.Leader, linger, Confluent.Kafka.CompressionType.Lz4)
            let p = FsKafka.KafkaProducer.Create(Log.Logger, cfg, a.Topic)
            Some p
        | SubCommand.Stats _ | SubCommand.Sync _ -> None
    let isFileSource = match a.Command.Source with Json _ -> true | _ -> false
    let parse =
        a.Filters.CreateStreamFilter()
        |> if a.EventsOnly then Propulsion.CosmosStore.EquinoxSystemTextJsonParser.whereStream
           else Propulsion.CosmosStore.EquinoxSystemTextJsonParser.eventsAndUnfoldsWhereStream
    let statsInterval, stateInterval = a.StatsInterval, a.StateInterval
    let maxReadAhead = p.GetResult(MaxReadAhead, if isFileSource then 32768 else 4)
    let maxConcurrentProcessors = p.GetResult(MaxWriters, 8)
    let sink =
        match a.Command with
        | SubCommand.Kafka _ | SubCommand.Stats _ ->
            let stats = Stats(Log.Logger, statsInterval, stateInterval, logExternalStats = dumpStoreStats, Categorize = a.Categorize)
            let handle isValidEvent (stream: FsCodec.StreamName) (events: Propulsion.Sinks.Event[]) = async {
                let ham, spam = events |> Array.partition isValidEvent
                match producer with
                | None -> ()
                | Some producer ->
                    let json = Propulsion.Codec.NewtonsoftJson.RenderedSpan.ofStreamSpan stream events
                               |> Propulsion.Codec.NewtonsoftJson.Serdes.Serialize
                    do! producer.ProduceAsync(FsCodec.StreamName.toString stream, json) |> Async.Ignore
                return Outcome.render_ stream ham spam 0, Propulsion.Sinks.Events.next events }
            let eventFilter = a.Filters.CreateEventFilter()
            Propulsion.Sinks.Factory.StartConcurrent(Log.Logger, maxReadAhead, maxConcurrentProcessors, handle eventFilter, stats,
                                                     requireAll = requireAll)
        | SubCommand.Sync sa ->
            let eventsContext = sa.ConnectEvents() |> Async.RunSynchronously
            let stats = Propulsion.CosmosStore.CosmosStoreSinkStats(Log.Logger, statsInterval, stateInterval, storeLog = Metrics.log,
                                                                    logExternalStats = dumpStoreStats, Categorize = a.Categorize)
            Propulsion.CosmosStore.CosmosStoreSink.Start(Log.Logger, maxReadAhead, eventsContext, maxConcurrentProcessors, stats,
                                                         maxBytes = sa.MaxBytes, requireAll = requireAll,
                                                         ?purgeInterval = if requireAll then None else Some (TimeSpan.hours 1))
    let source =
        match a.Command.Source with
        | Cosmos sa ->
            let monitored, leases =
                match a.Command with
                | SubCommand.Sync a -> a.ConnectFeed() |> Async.RunSynchronously
                | SubCommand.Kafka _ | SubCommand.Stats _ -> sa.ConnectFeed() |> Async.RunSynchronously
            Propulsion.CosmosStore.CosmosStoreSource(
                Log.Logger, statsInterval, monitored, leases, group, parse, sink,
                startFromTail = startFromTail, ?maxItems = maxItems, ?lagEstimationInterval = sa.MaybeLogLagInterval
            ).Start()
        | Dynamo sa ->
            let (indexContext, indexFilter), loadMode = sa.MonitoringParams()
            let checkpoints =
                let cache = Equinox.Cache (appName, sizeMb = 1)
                sa.CreateCheckpointStore(group, cache, Metrics.log)
            Propulsion.DynamoStore.DynamoStoreSource(
                Log.Logger, statsInterval,
                indexContext, defaultArg maxItems 100, TimeSpan.seconds 0.5,
                checkpoints, sink, loadMode, startFromTail = startFromTail, storeLog = Metrics.log,
                ?trancheIds = indexFilter
            ).Start()
        | Mdb sa ->
            let categories, client = sa.CreateClient()
            let checkpoints = sa.CreateCheckpointStore(group)
            Propulsion.MessageDb.MessageDbSource(
                Log.Logger, statsInterval,
                client, defaultArg maxItems 100, TimeSpan.seconds 0.5,
                checkpoints, sink, categories
            ).Start()
        | Json sa ->
            let checkpoints = Propulsion.Feed.ReaderCheckpoint.MemoryStore.createNull ()
            let filePath = Propulsion.Feed.JsonSource.DownloadIfHttpUri sa.Filepath |> Async.ofTask |> Async.RunSynchronously
            Propulsion.Feed.JsonSource.Start(Log.Logger, statsInterval, filePath, sa.Skip, parse, checkpoints, sink, ?truncateTo = sa.Trunc)

    let pipeline = [
        Async.AwaitKeyboardInterruptAsTaskCanceledException()
        if follow then
            source.AwaitWithStopOnCancellation()
        else async {
            let initialWait = TimeSpan.seconds 30
            do! source.Monitor.AwaitCompletion(initialWait, awaitFullyCaughtUp = true, logInterval = statsInterval / 2.) |> Async.ofTask
            source.Stop()
            do! source.Await() // Let it emit the stats
            do! source.Flush() |> Async.Ignore<Propulsion.Feed.TranchePositions> // flush checkpoints (currently a no-op)
            raise eofSignalException } // trigger tear down of sibling waits
        sink.AwaitWithStopOnCancellation() ]
    return! pipeline |> Async.Parallel |> Async.Ignore<unit[]> }
