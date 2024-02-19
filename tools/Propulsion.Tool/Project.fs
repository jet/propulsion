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

    | [<AltCommandLine "-I";    AltCommandLine "--include-indexes"; Unique>] IncIdx
    | [<AltCommandLine "-cat";  AltCommandLine "--include-category">]   IncCat of    regex: string
    | [<AltCommandLine "-ncat"; AltCommandLine "--exclude-category">]   ExcCat of    regex: string
    | [<AltCommandLine "-sn";   AltCommandLine "--include-streamname">] IncStream of regex: string
    | [<AltCommandLine "-nsn";  AltCommandLine "--exclude-streamname">] ExcStream of regex: string
    | [<AltCommandLine "-et";   AltCommandLine "--include-eventtype">]  IncEvent of  regex: string
    | [<AltCommandLine "-net";  AltCommandLine "--exclude-eventtype">]  ExcEvent of  regex: string

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

            | IncIdx ->                     "Include Index streams. Default: Exclude Index Streams, identified by a $ prefix."
            | IncCat _ ->                   "Allow Stream Category. Multiple values are combined with OR. Default: include all, subject to Category Deny and Stream Deny rules."
            | ExcCat _ ->                   "Deny  Stream Category. Specified values/regexes are applied after the Category Allow rule(s)."
            | IncStream _ ->                "Allow Stream Name. Multiple values are combined with OR. Default: Allow all streams that pass the category Allow test, Fail the Category and Stream deny tests."
            | ExcStream _ ->                "Deny  Stream Name. Specified values/regexes are applied after the IncCat, ExcCat and IncStream filters."

            | IncEvent _ ->                 "Allow Event Type Name. Multiple values are combined with OR. Applied only after Category and Stream filters. Default: include all."
            | ExcEvent _ ->                 "Deny  Event Type Name. Specified values/regexes are applied after the Event Type Name Allow rule(s)."

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
and SourceArguments(c, p: ParseResults<SourceParameters>) =
    member val StoreArgs =
        match p.GetSubCommand() with
        | SourceParameters.Cosmos p ->      Cosmos (Args.Cosmos.Arguments (c, p))
        | SourceParameters.Dynamo p ->      Dynamo (Args.Dynamo.Arguments (c, p))
        | SourceParameters.Mdb p ->         Mdb (Args.Mdb.Arguments (c, p))
        | SourceParameters.Json p ->        Json (Args.Json.Arguments (c, p))
and [<NoEquality; NoComparison>] SourceArgs =
    | Cosmos of Args.Cosmos.Arguments
    | Dynamo of Args.Dynamo.Arguments
    | Mdb of Args.Mdb.Arguments
    | Json of Args.Json.Arguments
and KafkaArguments(c: Args.Configuration, p: ParseResults<KafkaParameters>) =
    member val Broker =                     p.GetResult(Broker, fun () -> c.KafkaBroker)
    member val Topic =                      p.GetResult(Topic, fun () -> c.KafkaTopic)
    member val Source =                     SourceArguments(c, p.GetResult KafkaParameters.Source)
type StreamFilterArguments(p: ParseResults<Parameters>) =
    let allowCats, denyCats = p.GetResults IncCat, p.GetResults ExcCat
    let allowSns, denySns = p.GetResults IncStream, p.GetResults ExcStream
    let incIndexes = p.Contains IncIdx
    let allowEts, denyEts = p.GetResults IncEvent, p.GetResults ExcEvent
    let isPlain = Seq.forall (fun x -> System.Char.IsLetterOrDigit x || x = '_')
    let asRe = Seq.map (fun x -> if isPlain x then $"^{x}$" else x)
    let (|Filter|) exprs =
        let values, pats = List.partition isPlain exprs
        let valuesContains = let set = System.Collections.Generic.HashSet(values) in set.Contains
        let aPatternMatches (x: string) = pats |> List.exists (fun p -> System.Text.RegularExpressions.Regex.IsMatch(x, p))
        fun cat -> valuesContains cat || aPatternMatches cat
    let filter map (allow, deny) =
        match allow, deny with
        | [], [] -> fun _ -> true
        | Filter includes, Filter excludes -> fun x -> let x = map x in (List.isEmpty allow || includes x) && not (excludes x)
    let validStream = filter FsCodec.StreamName.toString (allowSns, denySns)
    let isTransactionalStream (sn: FsCodec.StreamName) = let sn = FsCodec.StreamName.toString sn in not (sn.StartsWith('$'))
    member _.CreateStreamFilter(maybeCategories) =
        let handlerCats = match maybeCategories with Some xs -> List.ofArray xs | None -> List.empty
        let allowCats = handlerCats @ allowCats
        let validCat = filter FsCodec.StreamName.Category.ofStreamName (allowCats, denyCats)
        let allowCats = match allowCats with [] -> [ ".*" ] | xs -> xs
        let denyCats = denyCats @ [ if not incIndexes then "^\$" ]
        let allowSns, denySns = match allowSns, denySns with [], [] -> [".*"], [] | x -> x
        let allowEts, denyEts = match allowEts, denyEts with [], [] -> [".*"], [] | x -> x
        Log.Information("Categories ☑️ {@allowCats} 🚫{@denyCats} Streams ☑️ {@allowStreams} 🚫{denyStreams} Events ☑️ {allowEts} 🚫{@denyEts}",
                        asRe allowCats, asRe denyCats, asRe allowSns, asRe denySns, asRe allowEts, asRe denyEts)
        fun sn ->
            validCat sn
            && validStream sn
            && (incIndexes || isTransactionalStream sn)
    member val EventFilter = filter (fun (x: Propulsion.Sinks.Event) -> x.EventType) (allowEts, denyEts)

type Arguments(c, p: ParseResults<Parameters>) =
    member val StatsInterval =              TimeSpan.minutes 1
    member val StateInterval =              TimeSpan.minutes 5
    member val IdleDelay =                  TimeSpan.ms 10.
    member val Filters =                    StreamFilterArguments(p)
    member val Command =
        match p.GetSubCommand() with
        | Kafka a -> KafkaArguments(c, a) |> SubCommand.Kafka
        | Stats a -> SourceArguments(c, a) |> SubCommand.Stats
        | x -> p.Raise $"unexpected subcommand %A{x}"
and [<NoEquality; NoComparison; RequireQualifiedAccess>] SubCommand =
    | Kafka of KafkaArguments
    | Stats of SourceArguments
    member x.SourceStore = x |> function
        | SubCommand.Kafka a -> a.Source.StoreArgs
        | SubCommand.Stats a -> a.StoreArgs

[<AbstractClass>]
type StatsBase<'outcome>(log, statsInterval, stateInterval, verboseStore, logExternalStats) =
    inherit Propulsion.Streams.Stats<'outcome>(log, statsInterval, stateInterval)

    override _.DumpStats() =
        base.DumpStats()
        logExternalStats Log.Logger

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

type CategoryCounters() =
    let cats = System.Collections.Generic.Dictionary<string, Propulsion.Internal.Stats.Counters>()
    member _.Ingest(category, counts) =
        let cat =
            match cats.TryGetValue category with
            | false, _ -> let acc = Propulsion.Internal.Stats.Counters() in cats.Add(category, acc); acc
            | true, acc -> acc
        for event, count : int in counts do cat.Ingest(event, count)
    member _.Categories = cats.Keys
    member _.StatsDescending cat =
        match cats.TryGetValue cat with
        | true, acc -> acc.StatsDescending
        | false, _ -> Seq.empty
    member _.DumpGrouped(log: ILogger, totalLabel) =
        if cats.Count <> 0 then
            Propulsion.Internal.Stats.dumpCounterSet log totalLabel cats
    member _.Clear() = cats.Clear()

type EventTypeLatencies() =
    let inner = Propulsion.Internal.Stats.LatencyStatsSet()
    member _.Record(category: string, eventType: string, latency) =
        let key = $"{category}/{eventType}"
        inner.Record(key, latency)
    member _.Dump(log, totalLabel) =
        let inline catFromKey (key: string) = key.Substring(0, key.IndexOf '/')
        inner.DumpGrouped(catFromKey, log, totalLabel = totalLabel)
        inner.Dump log
    member _.Clear() = inner.Clear()

type Stats(log, statsInterval, stateInterval, verboseStore, logExternalStats) =
    inherit StatsBase<Outcome>(log, statsInterval, stateInterval, verboseStore, logExternalStats)
    let mutable handled, ignored = 0, 0
    let accHam, accSpam = CategoryCounters(), CategoryCounters()
    let intervalLats, accEventTypeLats = EventTypeLatencies(), EventTypeLatencies()
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
            intervalLats.Dump(log, "EVENTS")
            intervalLats.Clear()
    override _.DumpState purge =
        accEventTypeLats.Dump(log, "ΣEVENTS")
        for cat in Seq.append accHam.Categories accSpam.Categories |> Seq.distinct |> Seq.sort do
            let ham, spam = accHam.StatsDescending(cat) |> Array.ofSeq, accSpam.StatsDescending cat |> Array.ofSeq
            if ham.Length > 00 then log.Information(" Category {cat} handled {@ham}", cat, ham)
            if spam.Length <> 0 then log.Information(" Category {cat} ignored {@spam}", cat, spam)
        if purge then
            accHam.Clear(); accSpam.Clear()
            accEventTypeLats.Clear()

let private handle isValidEvent stream (events: Propulsion.Sinks.Event[]): Async<_ * Outcome> = async {
    let ham, spam = events |> Array.partition isValidEvent
    return Propulsion.Sinks.StreamResult.AllProcessed, Outcome.render_ stream ham spam 0 }

let eofSignalException = System.Threading.Tasks.TaskCanceledException "Stopping; FeedMonitor wait completed"
let run appName (c: Args.Configuration, p: ParseResults<Parameters>) = async {
    let a = Arguments(c, p)
    let dumpStoreStats =
        match a.Command.SourceStore with
        | Cosmos _ -> Equinox.CosmosStore.Core.Log.InternalMetrics.dump
        | Dynamo _ -> Equinox.DynamoStore.Core.Log.InternalMetrics.dump
        | Mdb _ -> ignore
        | Json _ -> ignore
    let group =
        match p.TryGetResult ConsumerGroupName, a.Command.SourceStore with
        | Some x, _ -> x
        | None, Json _ -> System.Guid.NewGuid() |> _.ToString("N")
        | None, _ -> p.Raise "ConsumerGroupName is mandatory, unless consuming from a JSON file"
    let startFromTail, follow, maxItems = p.Contains FromTail, p.Contains Follow, p.TryGetResult MaxItems
    let producer =
        match a.Command with
        | SubCommand.Kafka a ->
            let linger = FsKafka.Batching.BestEffortSerial (TimeSpan.ms 100.)
            let cfg = FsKafka.KafkaProducerConfig.Create(appName, a.Broker, Confluent.Kafka.Acks.Leader, linger, Confluent.Kafka.CompressionType.Lz4)
            let p = FsKafka.KafkaProducer.Create(Log.Logger, cfg, a.Topic)
            Some p
        | SubCommand.Stats _ -> None
    let isFileSource = match a.Command.SourceStore with Json _ -> true | _ -> true
    let parse = a.Filters.CreateStreamFilter None |> Propulsion.CosmosStore.EquinoxSystemTextJsonParser.whereStream
    let statsInterval, stateInterval = a.StatsInterval, a.StateInterval
    let maxReadAhead = p.GetResult(MaxReadAhead, if isFileSource then 32768 else 2)
    let maxConcurrentProcessors = p.GetResult(MaxWriters, 8)
    let sink =
        let stats = Stats(Log.Logger, statsInterval, stateInterval, verboseStore = false, logExternalStats = dumpStoreStats)
        let handle isValidEvent (stream: FsCodec.StreamName) (events: Propulsion.Sinks.Event[]) = async {
            let ham, spam = events |> Array.partition isValidEvent
            match producer with
            | None -> ()
            | Some producer ->
                let json = Propulsion.Codec.NewtonsoftJson.RenderedSpan.ofStreamSpan stream events |> Propulsion.Codec.NewtonsoftJson.Serdes.Serialize
                do! producer.ProduceAsync(FsCodec.StreamName.toString stream, json) |> Async.Ignore
            return Propulsion.Sinks.StreamResult.AllProcessed, Outcome.render_ stream ham spam 0 }
        Propulsion.Sinks.Factory.StartConcurrent(Log.Logger, maxReadAhead, maxConcurrentProcessors, handle a.Filters.EventFilter, stats, idleDelay = a.IdleDelay)
    let source =
        match a.Command.SourceStore with
        | Cosmos sa ->
            let monitored, leases =  sa.ConnectFeed() |> Async.RunSynchronously
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
            CosmosDumpSource.Start(Log.Logger, statsInterval, sa.Filepath, sa.Skip, parse, sink, ?truncateTo = sa.Trunc)

    let work = [
        Async.AwaitKeyboardInterruptAsTaskCanceledException()
        if follow then
            source.AwaitWithStopOnCancellation()
        else async {
            let initialWait = TimeSpan.seconds 10
            do! source.Monitor.AwaitCompletion(initialWait, awaitFullyCaughtUp = true, logInterval = statsInterval / 2.) |> Async.ofTask
            source.Stop()
            do! source.Await() // Let it emit the stats
            do! source.Flush() |> Async.Ignore<Propulsion.Feed.TranchePositions> // flush checkpoints (currently a no-op)
            raise eofSignalException } // trigger tear down of sibling waits
        sink.AwaitWithStopOnCancellation() ]
    return! work |> Async.Parallel |> Async.Ignore<unit[]> }
