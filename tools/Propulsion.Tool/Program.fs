module Propulsion.Tool.Program

open Argu
open Equinox.Core // Stopwatch.Time
open FsKafka
open Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessing
open Serilog
open Serilog.Events
open Propulsion.Codec.NewtonsoftJson
open Propulsion.Cosmos
open Propulsion.Tool.Infrastructure
open Propulsion.Streams
open System
open System.Collections.Generic
open System.Diagnostics

module Config =
    let validateBrokerUri (broker : Uri) =
        if not broker.IsAbsoluteUri then invalidArg "broker" "should be of 'host:port' format"
        if String.IsNullOrEmpty broker.Authority then
            // handle a corner case in which Uri instances are erroneously putting the hostname in the `scheme` field.
            if System.Text.RegularExpressions.Regex.IsMatch(string broker, "^\S+:[0-9]+$") then string broker
            else invalidArg "broker" "should be of 'host:port' format"

        else broker.Authority

exception MissingArg of string

let private getEnvVarForArgumentOrThrow varName argName =
    match Environment.GetEnvironmentVariable varName with
    | null -> raise (MissingArg(sprintf "Please provide a %s, either as an argument or via the %s environment variable" argName varName))
    | x -> x
let private defaultWithEnvVar varName argName = function None -> getEnvVarForArgumentOrThrow varName argName | Some x -> x

module Cosmos =
    type [<NoEquality; NoComparison>] Arguments =
        | [<AltCommandLine("-V")>]          VerboseStore
        | [<AltCommandLine("-m")>]          ConnectionMode of Equinox.Cosmos.ConnectionMode
        | [<AltCommandLine("-o")>]          Timeout of float
        | [<AltCommandLine("-r")>]          Retries of int
        | [<AltCommandLine("-rt")>]         RetriesWaitTime of float
        | [<AltCommandLine("-s")>]          Connection of string
        | [<AltCommandLine("-d")>]          Database of string
        | [<AltCommandLine("-c")>]          Container of string
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
    type Info(args : ParseResults<Arguments>) =
        member __.Mode =                    args.GetResult(ConnectionMode,Equinox.Cosmos.ConnectionMode.Direct)
        member __.Connection =              args.TryGetResult Connection  |> defaultWithEnvVar "EQUINOX_COSMOS_CONNECTION" "Connection"
        member __.Database =                args.TryGetResult Database    |> defaultWithEnvVar "EQUINOX_COSMOS_DATABASE"   "Database"
        member __.Container =               args.TryGetResult Container   |> defaultWithEnvVar "EQUINOX_COSMOS_CONTAINER"  "Container"

        member __.Timeout =                 args.GetResult(Timeout,5.) |> TimeSpan.FromSeconds
        member __.Retries =                 args.GetResult(Retries,1)
        member __.MaxRetryWaitTime =        args.GetResult(RetriesWaitTime, 5.) |> TimeSpan.FromSeconds

    open Equinox.Cosmos

    let connection (log: ILogger, storeLog: ILogger) (a : Info) =
        let (Discovery.UriAndKey (endpointUri,_)) as discovery = a.Connection|> Discovery.FromConnectionString
        log.Information("CosmosDb {mode} {connection} Database {database} Container {container}",
            a.Mode, endpointUri, a.Database, a.Container)
        log.Information("CosmosDb timeout {timeout}s; Throttling retries {retries}, max wait {maxRetryWaitTime}s",
            (let t = a.Timeout in t.TotalSeconds), a.Retries, a.MaxRetryWaitTime)
        discovery, a.Database, a.Container, Connector(a.Timeout, a.Retries, a.MaxRetryWaitTime, log=storeLog, mode=a.Mode)

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
    | [<AltCommandLine("-s"); Unique>]      Suffix of string
    | [<AltCommandLine("-Z"); Unique>]      FromTail
    | [<AltCommandLine("-md"); Unique>]     MaxDocuments of int
    | [<AltCommandLine("-l"); Unique>]      LagFreqM of float
    | [<CliPrefix(CliPrefix.None); Last>]   Stats of ParseResults<StatsTarget>
    | [<CliPrefix(CliPrefix.None); Last>]   Kafka of ParseResults<KafkaTarget>
    interface IArgParserTemplate with
        member a.Usage = a |> function
            | ConsumerGroupName _ ->        "Projector instance context name."
            | Suffix _ ->                   "Specify Container Name suffix (default: `-aux`)."
            | FromTail _ ->                 "(iff `suffix` represents a fresh projection) - force starting from present Position. Default: Ensure each and every event is projected from the start."
            | MaxDocuments _ ->             "Maximum item count to supply to Changefeed Api when querying. Default: Unlimited"
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

let createStoreLog verbose verboseConsole maybeSeqEndpoint =
    let c = LoggerConfiguration().Destructure.FSharpTypes()
    let c = if verbose then c.MinimumLevel.Debug() else c
    let c = c.WriteTo.Sink(Equinox.Cosmos.Store.Log.InternalMetrics.Stats.LogSink())
    let c = c.WriteTo.Console((if verbose && verboseConsole then LogEventLevel.Debug else LogEventLevel.Warning), theme=Sinks.SystemConsole.Themes.AnsiConsoleTheme.Code)
    let c = match maybeSeqEndpoint with None -> c | Some endpoint -> c.WriteTo.Seq(endpoint)
    c.CreateLogger() :> ILogger

let createDomainLog verbose verboseConsole maybeSeqEndpoint =
    let c = LoggerConfiguration().Destructure.FSharpTypes().Enrich.FromLogContext()
    let c = if verbose then c.MinimumLevel.Debug() else c
    let c = c.WriteTo.Sink(Equinox.Cosmos.Store.Log.InternalMetrics.Stats.LogSink())
    let c = c.WriteTo.Console((if verboseConsole then LogEventLevel.Debug else LogEventLevel.Information), theme=Sinks.SystemConsole.Themes.AnsiConsoleTheme.Code)
    let c = match maybeSeqEndpoint with None -> c | Some endpoint -> c.WriteTo.Seq(endpoint)
    c.CreateLogger()

let [<Literal>] appName = "propulsion-tool"

module CosmosInit =
    open Equinox.Cosmos.Store.Sync.Initialization
    let aux (log: ILogger, verboseConsole, maybeSeq) (iargs: ParseResults<InitAuxArguments>) = async {
        match iargs.TryGetSubCommand() with
        | Some (InitAuxArguments.Cosmos sargs) ->
            let storeLog = createStoreLog (sargs.Contains Cosmos.Arguments.VerboseStore) verboseConsole maybeSeq
            let discovery, dbName, baseContainerName, connector = Cosmos.connection (log,storeLog) (Cosmos.Info sargs)
            let auxContainerName = let containerSuffix = iargs.GetResult(InitAuxArguments.Suffix,"-aux") in baseContainerName + containerSuffix
            let rus = iargs.GetResult(InitAuxArguments.Rus)
            log.Information("Provisioning Lease/`aux` Container {containe} for {rus:n0} RU/s", auxContainerName, rus)
            let! conn = connector.Connect(appName, discovery)
            return! initAux conn.Client (dbName,auxContainerName) rus
        | _ -> failwith "please specify a `cosmos` endpoint" }

[<EntryPoint>]
let main argv =
    let programName = System.Reflection.Assembly.GetEntryAssembly().GetName().Name
    let parser = ArgumentParser.Create<Arguments>(programName=programName)
    try
        let args = parser.ParseCommandLine argv
        let verboseConsole = args.Contains VerboseConsole
        let maybeSeq = if args.Contains LocalSeq then Some "http://localhost:5341" else None
        let verbose = args.Contains Verbose
        let log = createDomainLog verbose verboseConsole maybeSeq
        match args.GetSubCommand() with
        | Init iargs -> CosmosInit.aux (log, verboseConsole, maybeSeq) iargs |> Async.RunSynchronously
        | Project pargs ->
            let broker, topic, storeArgs =
                match pargs.GetSubCommand() with
                | Kafka kargs ->
                    let broker = kargs.TryGetResult Broker |> defaultWithEnvVar "PROPULSION_KAFKA_BROKER" "Broker"
                    let topic =  kargs.TryGetResult Topic  |> defaultWithEnvVar "PROPULSION_KAFKA_TOPIC"  "Topic"
                    Some broker, Some topic,kargs.GetResult KafkaTarget.Cosmos
                | Stats sargs -> None, None, sargs.GetResult StatsTarget.Cosmos
                | x -> failwithf "Invalid subcommand %A" x
            let storeLog = createStoreLog (storeArgs.Contains Cosmos.Arguments.VerboseStore) verboseConsole maybeSeq
            let discovery, dbName, containerName, connector = Cosmos.connection (log, storeLog) (Cosmos.Info storeArgs)
            pargs.TryGetResult MaxDocuments |> Option.iter (fun bs -> log.Information("Requesting ChangeFeed Maximum Document Count {changeFeedMaxItemCount}", bs))
            pargs.TryGetResult LagFreqM |> Option.iter (fun s -> log.Information("Dumping lag stats at {lagS:n0}m intervals", s))
            let auxContainerName = containerName + pargs.GetResult(ProjectArguments.Suffix,"-aux")
            let group = pargs.GetResult ConsumerGroupName
            log.Information("Processing using Group Name {group} in Aux Container {auxContainerName}", group, auxContainerName)
            if pargs.Contains FromTail then log.Warning("(If new projection prefix) Skipping projection of all existing events.")
            let source = { database = dbName; container = containerName }
            let aux = { database = dbName; container = auxContainerName }

            let buildRangeProjector _context =
                let sw = Stopwatch.StartNew() // we'll report the warmup/connect time on the first batch
                let producer, disposeProducer =
                    match broker,topic with
                    | Some b,Some t ->
                        let linger = FsKafka.Batching.BestEffortSerial (TimeSpan.FromMilliseconds 100.)
                        let cfg = KafkaProducerConfig.Create(appName, b, Confluent.Kafka.Acks.Leader, linger, Confluent.Kafka.CompressionType.Lz4)
                        let p = BatchedProducer.Create(log, cfg, t)
                        Some p, (p :> IDisposable).Dispose
                    | _ -> None, id
                let projectBatch (log : ILogger) (ctx : IChangeFeedObserverContext) (docs : IReadOnlyList<Microsoft.Azure.Documents.Document>) = async {
                    sw.Stop() // Stop the clock after CFP hands off to us
                    let render (e: StreamEvent<_>) = RenderedSpan.ofStreamSpan e.stream { StreamSpan.index = e.event.Index; events=[| e.event |] }
                    let pt, events = (fun () -> docs |> Seq.collect EquinoxCosmosParser.enumStreamEvents |> Seq.map render |> Array.ofSeq) |> Stopwatch.Time
                    let! et = async {
                        match producer with
                        | None ->
                            let! et,() = ctx.Checkpoint() |> Stopwatch.Time
                            return et
                        | Some producer ->
                            let es = [| for e in events -> e.s, Newtonsoft.Json.JsonConvert.SerializeObject e |]
                            let! et,() = async {
                                let! _ = producer.ProduceBatch es
                                return! ctx.Checkpoint() } |> Stopwatch.Time
                            return et }

                    if log.IsEnabled LogEventLevel.Debug then log.Debug("Response Headers {0}", let hs = ctx.FeedResponse.ResponseHeaders in [for h in hs -> h, hs.[h]])
                    let r = ctx.FeedResponse
                    log.Information("Reader {partitionId} {token,9} {requestCharge:n0}RU {count} docs {l:n1}s; Parse: s {streams} e {events} {p:n3}s; Emit: {e:n1}s",
                        ctx.PartitionKeyRangeId, r.ResponseContinuation.Trim[|'"'|], r.RequestCharge, docs.Count, float sw.ElapsedMilliseconds / 1000.,
                        events.Length, (let e = pt.Elapsed in e.TotalSeconds), (let e = et.Elapsed in e.TotalSeconds))
                    sw.Restart() // restart the clock as we handoff back to the CFP
                }
                ChangeFeedObserver.Create(log, projectBatch, dispose = disposeProducer)

            let run = async {
                let logLag (interval : TimeSpan) remainingWork = async {
                    let logLevel = if remainingWork |> Seq.exists (fun (_r,rw) -> rw <> 0L) then Events.LogEventLevel.Information else Events.LogEventLevel.Debug
                    log.Write(logLevel, "Lags {@rangeLags} <- [Range Id, documents count] ", remainingWork)
                    return! Async.Sleep(int interval.TotalMilliseconds) }
                let maybeLogLag = pargs.TryGetResult LagFreqM |> Option.map (TimeSpan.FromMinutes >> logLag)
                let! _cfp =
                    ChangeFeedProcessor.Start
                      ( log, connector.CreateClient(appName, discovery), source, aux, group, buildRangeProjector,
                        startFromTail = pargs.Contains FromTail,
                        ?maxDocuments = pargs.TryGetResult MaxDocuments,
                        ?reportLagAndAwaitNextEstimation = maybeLogLag)
                return! Async.AwaitKeyboardInterrupt() }
            Async.RunSynchronously run
        | _ -> failwith "Please specify a valid subcommand :- init or project"
        0
    with :? Argu.ArguParseException as e -> eprintfn "%s" e.Message; 1
        | MissingArg msg -> eprintfn "%s" msg; 1
        | e -> eprintfn "%s" e.Message; 1
