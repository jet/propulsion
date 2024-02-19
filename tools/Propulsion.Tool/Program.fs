module Propulsion.Tool.Program

open Argu
open Propulsion.Internal // AwaitKeyboardInterruptAsTaskCanceledException
open Serilog

[<NoEquality; NoComparison; RequireSubcommand>]
type Parameters =
    | [<AltCommandLine "-V">]               Verbose
    | [<AltCommandLine "-C">]               VerboseConsole
    | [<AltCommandLine "-S">]               VerboseStore
    | [<CliPrefix(CliPrefix.None); Last; Unique>] Init of ParseResults<Args.Cosmos.InitParameters>
    | [<CliPrefix(CliPrefix.None); Last; Unique>] InitPg of ParseResults<Args.Mdb.Parameters>
    | [<CliPrefix(CliPrefix.None); Last; Unique>] Index of ParseResults<Args.Dynamo.IndexParameters>
    | [<CliPrefix(CliPrefix.None); Last; Unique>] Checkpoint of ParseResults<CheckpointParameters>
    | [<CliPrefix(CliPrefix.None); Last; Unique>] Project of ParseResults<Project.Parameters>
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
and [<NoEquality; NoComparison; RequireSubcommand>] CheckpointParameters =
    | [<AltCommandLine "-s"; Mandatory>]    Source of Propulsion.Feed.SourceId
    | [<AltCommandLine "-t"; Mandatory>]    Tranche of Propulsion.Feed.TrancheId
    | [<AltCommandLine "-g"; Mandatory>]    Group of string
    | [<AltCommandLine "-p"; Unique>]       OverridePosition of Propulsion.Feed.Position
    | [<CliPrefix(CliPrefix.None)>]         Cosmos of ParseResults<Args.Cosmos.Parameters>
    | [<CliPrefix(CliPrefix.None)>]         Dynamo of ParseResults<Args.Dynamo.Parameters>
    | [<CliPrefix(CliPrefix.None)>]         Mdb    of ParseResults<Args.Mdb.Parameters>
    interface IArgParserTemplate with
        member a.Usage = a |> function
            | Group _ ->                    "Consumer Group"
            | Source _ ->                   "Specify source to override"
            | Tranche _ ->                  "Specify tranche to override"
            | OverridePosition _ ->         "(optional) Override to specified position"
            | Cosmos _ ->                   "Specify CosmosDB parameters."
            | Dynamo _ ->                   "Specify DynamoDB parameters."
            | Mdb _ ->                      "Specify MessageDb parameters."

let [<Literal>] AppName = "propulsion-tool"

module Checkpoints =

    type Arguments(c, p: ParseResults<CheckpointParameters>) =
        member val StoreArgs =
            match p.GetSubCommand() with
            | CheckpointParameters.Cosmos p ->  Choice1Of3 (Args.Cosmos.Arguments (c, p))
            | CheckpointParameters.Dynamo p ->  Choice2Of3 (Args.Dynamo.Arguments (c, p))
            | CheckpointParameters.Mdb p ->     Choice3Of3 (Args.Mdb.Arguments (c, p))
            | x -> p.Raise $"unexpected subcommand %A{x}"

    let readOrOverride (c, p: ParseResults<CheckpointParameters>, ct) = task {
        let a = Arguments(c, p)
        let source, tranche, group = p.GetResult CheckpointParameters.Source, p.GetResult Tranche, p.GetResult Group
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

type Arguments(c: Args.Configuration, p: ParseResults<Parameters>) =
    member val Verbose = p.Contains Verbose
    member val VerboseConsole = p.Contains VerboseConsole
    member val VerboseStore = p.Contains VerboseStore
    member _.ExecuteSubCommand() = async {
        match p.GetSubCommand() with
        | Init a ->         do! Args.Cosmos.initAux (c, a) |> Async.Ignore<Microsoft.Azure.Cosmos.Container>
        | InitPg a ->       do! Args.Mdb.Arguments(c, a).CreateCheckpointStoreTable() |> Async.ofTask
        | Checkpoint a ->   do! Checkpoints.readOrOverride(c, a, CancellationToken.None) |> Async.ofTask
        | Index a ->        do! Args.Dynamo.index (c, a)
        | Project a ->      do! Project.run AppName (c, a)
        | x ->              p.Raise $"unexpected subcommand %A{x}" }
    static member Parse argv =
        let parseResults = ArgumentParser.Create().ParseCommandLine argv
        Arguments(Args.Configuration(EnvVar.tryGet, EnvVar.getOr parseResults.Raise), parseResults)

let isExpectedShutdownSignalException: exn -> bool = function
    | :? ArguParseException -> true // Via Arguments.Parse and/or Configuration.tryGet
    | :? System.Threading.Tasks.TaskCanceledException -> true // via AwaitKeyboardInterruptAsTaskCanceledException
    | _ -> false

[<EntryPoint>]
let main argv =
    try let a = Arguments.Parse argv
        try Log.Logger <- LoggerConfiguration().Configure(a.Verbose).Sinks(Sinks.equinoxMetricsOnly, a.VerboseConsole, a.VerboseStore).CreateLogger()
            try a.ExecuteSubCommand() |> Async.RunSynchronously; 0
            with e when not (isExpectedShutdownSignalException e) -> Log.Fatal(e, "Exiting"); 2
        finally Log.CloseAndFlush()
    with :? ArguParseException as e -> eprintfn $"%s{e.Message}"; 1
        | e -> eprintfn $"EXCEPTION: %s{e.Message}"; 1
