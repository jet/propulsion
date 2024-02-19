module Propulsion.Tool.Args

open Argu
open Propulsion.Internal
open Serilog

module Configuration =

    module Cosmos =

        let [<Literal>] CONNECTION =                "EQUINOX_COSMOS_CONNECTION"
        let [<Literal>] DATABASE =                  "EQUINOX_COSMOS_DATABASE"
        let [<Literal>] CONTAINER =                 "EQUINOX_COSMOS_CONTAINER"

    module Dynamo =

        let [<Literal>] REGION =                    "EQUINOX_DYNAMO_REGION"
        let [<Literal>] SERVICE_URL =               "EQUINOX_DYNAMO_SERVICE_URL"
        let [<Literal>] ACCESS_KEY =                "EQUINOX_DYNAMO_ACCESS_KEY_ID"
        let [<Literal>] SECRET_KEY =                "EQUINOX_DYNAMO_SECRET_ACCESS_KEY"
        let [<Literal>] TABLE =                     "EQUINOX_DYNAMO_TABLE"
        let [<Literal>] INDEX_TABLE =               "EQUINOX_DYNAMO_TABLE_INDEX"

    module Kafka =

        let [<Literal>] BROKER =                    "PROPULSION_KAFKA_BROKER"
        let [<Literal>] TOPIC =                     "PROPULSION_KAFKA_TOPIC"

    module Mdb =
        let [<Literal>] CONNECTION_STRING =         "MDB_CONNECTION_STRING"
        let [<Literal>] SCHEMA =                    "MDB_SCHEMA"

type Configuration(tryGet: string -> string option, get: string -> string) =
    member _.CosmosConnection =                     get Configuration.Cosmos.CONNECTION
    member _.CosmosDatabase =                       get Configuration.Cosmos.DATABASE
    member _.CosmosContainer =                      get Configuration.Cosmos.CONTAINER

    member _.DynamoRegion =                         tryGet Configuration.Dynamo.REGION
    member _.DynamoServiceUrl =                     get Configuration.Dynamo.SERVICE_URL
    member _.DynamoAccessKey =                      get Configuration.Dynamo.ACCESS_KEY
    member _.DynamoSecretKey =                      get Configuration.Dynamo.SECRET_KEY
    member _.DynamoTable =                          get Configuration.Dynamo.TABLE
    member _.DynamoIndexTable =                     tryGet Configuration.Dynamo.INDEX_TABLE

    member _.KafkaBroker =                          get Configuration.Kafka.BROKER
    member _.KafkaTopic =                           get Configuration.Kafka.TOPIC

    member _.MdbConnectionString =                  get Configuration.Mdb.CONNECTION_STRING
    member _.MdbSchema =                            get Configuration.Mdb.SCHEMA

module Cosmos =

    open Configuration.Cosmos

    type [<NoEquality; NoComparison>] Parameters =
        | [<AltCommandLine "-m">]           ConnectionMode of Microsoft.Azure.Cosmos.ConnectionMode
        | [<AltCommandLine "-s">]           Connection of string
        | [<AltCommandLine "-d">]           Database of string
        | [<AltCommandLine "-c">]           Container of string
        | [<AltCommandLine "-o">]           Timeout of float
        | [<AltCommandLine "-r">]           Retries of int
        | [<AltCommandLine "-rt">]          RetriesWaitTime of float
        | [<AltCommandLine "-a"; Unique>]   LeaseContainer of string
        | [<AltCommandLine "-as"; Unique>]  Suffix of string

        | [<AltCommandLine "-l"; Unique>]   LagFreqM of float
        interface IArgParserTemplate with
            member a.Usage = a |> function
                | ConnectionMode _ ->       "override the connection mode. Default: Direct."
                | Connection _ ->           "specify a connection string for a Cosmos account. (optional if environment variable " + CONNECTION + " specified)"
                | Database _ ->             "specify a database name for Cosmos store. (optional if environment variable " + DATABASE + " specified)"
                | Container _ ->            "specify a container name for Cosmos store. (optional if environment variable " + CONTAINER + " specified)"
                | Timeout _ ->              "specify operation timeout in seconds (default: 5)."
                | Retries _ ->              "specify operation retries (default: 1)."
                | RetriesWaitTime _ ->      "specify max wait-time for retry when being throttled by Cosmos in seconds (default: 5)"
                | LeaseContainer _ ->       "Specify full Lease Container Name (default: Container + Suffix)."
                | Suffix _ ->               "Specify Container Name suffix (default: `-aux`, if LeaseContainer not specified)."
                | LagFreqM _ ->             "Specify frequency to dump lag stats. Default: off"

    type Arguments(c: Configuration, p: ParseResults<Parameters>) =
        let connector =
            let discovery =                 p.GetResult(Connection, fun () -> c.CosmosConnection) |> Equinox.CosmosStore.Discovery.ConnectionString
            let timeout =                   p.GetResult(Timeout, 5.) |> TimeSpan.seconds
            let retries =                   p.GetResult(Retries, 1)
            let maxRetryWaitTime =          p.GetResult(RetriesWaitTime, 5.) |> TimeSpan.seconds
            let mode =                      p.TryGetResult ConnectionMode
            Equinox.CosmosStore.CosmosStoreConnector(discovery, timeout, retries, maxRetryWaitTime, ?mode = mode)
        let databaseId =                    p.GetResult(Database, fun () -> c.CosmosDatabase)
        let containerId =                   p.GetResult(Container, fun () -> c.CosmosContainer)
        let leasesContainerName =           p.GetResult(LeaseContainer, fun () -> containerId + p.GetResult(Suffix, "-aux"))
        let checkpointInterval =            TimeSpan.hours 1.
        member val MaybeLogLagInterval =    p.TryGetResult(LagFreqM, TimeSpan.minutes)
        member _.CreateLeasesContainer() =  connector.CreateLeasesContainer(databaseId, leasesContainerName)
        member _.ConnectFeed() =            connector.ConnectFeed(databaseId, containerId, leasesContainerName)
        member x.CreateCheckpointStore(group, cache, storeLog) = async {
            let! context = connector.ConnectContext("Checkpoints", databaseId, containerId, 256)
            return Propulsion.Feed.ReaderCheckpoint.CosmosStore.create storeLog (group, checkpointInterval) (context, cache) }

    open Equinox.CosmosStore.Core.Initialization
    type [<NoEquality; NoComparison; RequireSubcommand>] InitParameters =
        | [<AltCommandLine "-ru"; Unique>]      Rus of int
        | [<AltCommandLine "-A"; Unique>]       Autoscale
        | [<AltCommandLine "-m"; Unique>]       Mode of ModeType
        | [<AltCommandLine "-s">]               Suffix of string
        | [<CliPrefix(CliPrefix.None)>]         Cosmos of ParseResults<Parameters>
        interface IArgParserTemplate with
            member a.Usage = a |> function
                | Rus _ ->                      "Specify RU/s level to provision for the Aux Container. (with AutoScale, the value represents the maximum RU/s to AutoScale based on)."
                | Autoscale ->                  "Autoscale provisioned throughput. Use --rus to specify the maximum RU/s."
                | Mode _ ->                     "Configure RU mode to use Container-level RU, Database-level RU, or Serverless allocations (Default: Use Container-level allocation)."
                | Suffix _ ->                   "Specify Container Name suffix (default: `-aux`)."
                | Cosmos _ ->                   "Cosmos Connection parameters."
    and ModeType = Container | Db | Serverless
    type InitArguments(p: ParseResults<InitParameters>) =
        let rusOrDefault (value: int) = p.GetResult(Rus, value)
        let throughput auto = if auto then Throughput.Autoscale (rusOrDefault 4000)
                                      else Throughput.Manual (rusOrDefault 400)
        member val ProvisioningMode =
            match p.GetResult(Mode, ModeType.Container), p.Contains Autoscale with
            | ModeType.Container, auto -> Provisioning.Container (throughput auto)
            | ModeType.Db, auto ->        Provisioning.Database (throughput auto)
            | ModeType.Serverless, auto when auto || p.Contains Rus -> p.Raise "Cannot specify RU/s or Autoscale in Serverless mode"
            | ModeType.Serverless, _ ->   Provisioning.Serverless

    let initAux (c, p: ParseResults<InitParameters>) =
        match p.GetSubCommand() with
        | InitParameters.Cosmos sa ->
            let mode, a = (InitArguments p).ProvisioningMode, Arguments(c, sa)
            let container = a.CreateLeasesContainer()
            match mode with
            | Provisioning.Container throughput ->
                match throughput with
                | Throughput.Autoscale rus ->
                    Log.Information("Provisioning Leases Container with Autoscale throughput of up to {rus:n0} RU/s", rus)
                | Throughput.Manual rus ->
                    Log.Information("Provisioning Leases Container with {rus:n0} RU/s", rus)
            | Provisioning.Database throughput ->
                let modeStr = "Database"
                match throughput with
                | Throughput.Autoscale rus ->
                    Log.Information("Provisioning Leases Container at {modeStr:l} level with Autoscale throughput of up to {rus:n0} RU/s", modeStr, rus)
                | Throughput.Manual rus ->
                    Log.Information("Provisioning Leases Container at {modeStr:l} level with {rus:n0} RU/s", modeStr, rus)
            | Provisioning.Serverless ->
                let modeStr = "Serverless"
                Log.Information("Provisioning Leases Container in {modeStr:l} mode with automatic throughput RU/s as configured in account", modeStr)
            initAux container.Database.Client (container.Database.Id, container.Id) mode
        | x -> p.Raise $"unexpected subcommand %A{x}"

module Dynamo =

    open Configuration.Dynamo

    type Equinox.DynamoStore.DynamoStoreConnector with

        member private x.LogConfiguration() =
            Log.Information("DynamoStore {endpoint} Timeout {timeoutS}s Retries {retries}",
                            x.Endpoint, (let t = x.Timeout in t.TotalSeconds), x.Retries)
        member x.CreateStoreClient() =
            x.LogConfiguration()
            x.CreateDynamoStoreClient()

    type Equinox.DynamoStore.DynamoStoreClient with

        member x.CreateContext(role, table, ?queryMaxItems, ?maxBytes, ?archiveTableName: string) =
            let c = Equinox.DynamoStore.DynamoStoreContext(x, table, ?queryMaxItems = queryMaxItems, ?maxBytes = maxBytes, ?archiveTableName = archiveTableName)
            Log.Information("DynamoStore {role:l} Table {table} Archive {archive} Tip thresholds: {maxTipBytes}b {maxTipEvents}e Query paging {queryMaxItems} items",
                            role, table, Option.toObj archiveTableName, c.TipOptions.MaxBytes, Option.toNullable c.TipOptions.MaxEvents, c.QueryOptions.MaxItems)
            c

    type [<NoEquality; NoComparison>] Parameters =
        | [<AltCommandLine "-sr">]          RegionProfile of string
        | [<AltCommandLine "-su">]          ServiceUrl of string
        | [<AltCommandLine "-sa">]          AccessKey of string
        | [<AltCommandLine "-ss">]          SecretKey of string
        | [<AltCommandLine "-t">]           Table of string
        | [<AltCommandLine "-r">]           Retries of int
        | [<AltCommandLine "-rt">]          RetriesTimeoutS of float
        | [<AltCommandLine "-i">]           IndexTable of string
        | [<AltCommandLine "-is">]          IndexSuffix of string
        | [<AltCommandLine "-d">]           StreamsDop of int
        | [<AltCommandLine "-ip">]          IndexPartition of Propulsion.Feed.TrancheId
        interface IArgParserTemplate with
            member a.Usage = a |> function
                | RegionProfile _ ->        "specify an AWS Region (aka System Name, e.g. \"us-east-1\") to connect to using the implicit AWS SDK/tooling config and/or environment variables etc. Optional if:\n" +
                                            "1) $" + REGION + " specified OR\n" +
                                            "2) Explicit `ServiceUrl`/$" + SERVICE_URL + "+`AccessKey`/$" + ACCESS_KEY + "+`Secret Key`/$" + SECRET_KEY + " specified.\n" +
                                            "See https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-envvars.html for details"
                | ServiceUrl _ ->           "specify a server endpoint for a Dynamo account. (Not applicable if `ServiceRegion`/$" + REGION + " specified; Optional if $" + SERVICE_URL + " specified)"
                | AccessKey _ ->            "specify an access key id for a Dynamo account. (Not applicable if `ServiceRegion`/$" + REGION + " specified; Optional if $" + ACCESS_KEY + " specified)"
                | SecretKey _ ->            "specify a secret access key for a Dynamo account. (Not applicable if `ServiceRegion`/$" + REGION + " specified; Optional if $" + SECRET_KEY + " specified)"
                | Table _ ->                "specify a table name for the primary store. (optional if environment variable " + TABLE + ", or `IndexTable` specified)"
                | Retries _ ->              "specify operation retries (default: 1)."
                | RetriesTimeoutS _ ->      "specify max wait-time including retries in seconds (default: 10)"
                | IndexTable _ ->           "specify a table name for the index store. (optional if environment variable " + INDEX_TABLE + " specified. default: `Table`+`IndexSuffix`)"
                | IndexSuffix _ ->          "specify a suffix for the index store. (not relevant if `Table` or `IndexTable` specified. default: \"-index\")"
                | StreamsDop _ ->           "parallelism when loading events from Store Feed Source. Default: Don't load events"
                | IndexPartition _ ->       "Constrain Index Partitions to load. Default: Load all indexed partitions"
    type Arguments(c: Configuration, p: ParseResults<Parameters>) =
        let conn =                          match p.TryGetResult RegionProfile |> Option.orElseWith (fun () -> c.DynamoRegion) with
                                            | Some systemName ->
                                                Choice1Of2 systemName
                                            | None ->
                                                let serviceUrl =   p.GetResult(ServiceUrl, fun () -> c.DynamoServiceUrl)
                                                let accessKey =    p.GetResult(AccessKey,  fun () -> c.DynamoAccessKey)
                                                let secretKey =    p.GetResult(SecretKey,  fun () -> c.DynamoSecretKey)
                                                Choice2Of2 (serviceUrl, accessKey, secretKey)
        let mkConnector timeout retries =   match conn with
                                            | Choice1Of2 systemName -> Equinox.DynamoStore.DynamoStoreConnector(systemName, timeout, retries)
                                            | Choice2Of2 (serviceUrl, accessKey, secretKey) -> Equinox.DynamoStore.DynamoStoreConnector(serviceUrl, accessKey, secretKey, timeout, retries)
        let connector (defTimeout: int) (defRetries: int) =
                                            let c = mkConnector (p.GetResult(RetriesTimeoutS, defTimeout) |> TimeSpan.seconds) (p.GetResult(Retries, defRetries))
                                            lazy c.CreateDynamoStoreClient() // lazy to trigger logging exactly once at the right time
        let readClient =                    connector 10 1
        let indexPartitions =               p.GetResults IndexPartition
        let writeClient =                   connector 120 10
        let tableNameWithIndexSuffix () =   c.DynamoTable + p.GetResult(IndexSuffix, "-index")
        let indexTable =                    p.GetResult(IndexTable, fun () -> c.DynamoIndexTable |> Option.defaultWith tableNameWithIndexSuffix)
        let indexReadContext =              lazy readClient.Value.CreateContext("Index", indexTable)
        let streamsDop =                    p.TryGetResult StreamsDop
        let checkpointInterval =            TimeSpan.hours 1.
        member val IndexTable =             indexTable
        member x.MonitoringParams() =
            let indexProps =
                let c = indexReadContext.Value
                match List.toArray indexPartitions with
                | [||] ->
                    Log.Information "DynamoStoreSource Partitions (All)"
                    (c, None)
                | xs ->
                    Log.Information("DynamoStoreSource Partition Filter {partitionIds}", xs)
                    (c, Some xs)
            let loadMode =
                match streamsDop with
                | None ->
                    Log.Information("DynamoStoreSource IndexOnly mode")
                    Propulsion.DynamoStore.EventLoadMode.IndexOnly
                | Some streamsDop ->
                    Log.Information("DynamoStoreSource WithData, parallelism limit {streamsDop}", streamsDop)
                    let table = p.GetResult(Table, fun () -> c.DynamoTable)
                    let context = readClient.Value.CreateContext("Store", table)
                    Propulsion.DynamoStore.EventLoadMode.WithData (streamsDop, context)
            indexProps, loadMode
        member _.CreateContext(minItemSizeK) =
            let queryMaxItems = 100
            let client = writeClient.Value
            Log.Information("DynamoStore QueryMaxItems {queryMaxItems} MinItemSizeK {minItemSizeK}", queryMaxItems, minItemSizeK)
            client.CreateContext("Index", indexTable, queryMaxItems = queryMaxItems, maxBytes = minItemSizeK * 1024)
        member _.CreateCheckpointStore(group, cache, storeLog) =
            Propulsion.Feed.ReaderCheckpoint.DynamoStore.create storeLog (group, checkpointInterval) (indexReadContext.Value, cache)

    type [<NoEquality; NoComparison; RequireSubcommand>] IndexParameters =
        | [<AltCommandLine "-p"; Unique>]       IndexPartitionId of int
        | [<AltCommandLine "-j"; MainCommand>]  DynamoDbJson of string
        | [<AltCommandLine "-m"; Unique>]       MinSizeK of int
        | [<AltCommandLine "-b"; Unique>]       EventsPerBatch of int
        | [<AltCommandLine "-g"; Unique>]       GapsLimit of int
        | [<CliPrefix(CliPrefix.None)>]         Dynamo of ParseResults<Parameters>
        interface IArgParserTemplate with
            member a.Usage = a |> function
                | IndexPartitionId _ ->         "PartitionId to verify/import into. (optional, omitting displays partitions->epochs list)"
                | DynamoDbJson _ ->             "Source DynamoDB JSON filename(s) to import (optional, omitting displays current state)"
                | MinSizeK _ ->                 "Index Stream minimum Item size in KiB. Default 48"
                | EventsPerBatch _ ->           "Maximum Events to Ingest as a single batch. Default 10000"
                | GapsLimit _ ->                "Max Number of gaps to output to console. Default 10"
                | Dynamo _ ->                   "Specify DynamoDB parameters."

    open Propulsion.DynamoStore

    type IndexerArguments(c, p: ParseResults<IndexParameters>) =
        member val GapsLimit =              p.GetResult(IndexParameters.GapsLimit, 10)
        member val ImportJsonFiles =        p.GetResults IndexParameters.DynamoDbJson
        member val TrancheId =              p.TryGetResult(IndexParameters.IndexPartitionId, string >> AppendsPartitionId.parse)
        // Larger optimizes for not needing to use TransactWriteItems as frequently
        // Smaller will trigger more items and reduce read costs for Sources reading from the tail
        member val MinItemSize =            p.GetResult(IndexParameters.MinSizeK, 48)
        member val EventsPerBatch =         p.GetResult(IndexParameters.EventsPerBatch, 10000)

        member val StoreArgs =
            match p.GetSubCommand() with
            | IndexParameters.Dynamo p ->   Arguments (c, p)
            | x ->                          p.Raise $"unexpected subcommand %A{x}"
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

    let index (c: Configuration, p: ParseResults<IndexParameters>) = async {
        let a = IndexerArguments(c, p)
        let context = a.CreateContext()

        match a.TrancheId with
        | None when (not << List.isEmpty) a.ImportJsonFiles ->
            p.Raise "Must specify a trancheId parameter to import into"
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

module Mdb =

    open Configuration.Mdb

    type [<NoEquality; NoComparison>] Parameters =
        | [<AltCommandLine "-c">]           ConnectionString of string
        | [<AltCommandLine "-cc">]          CheckpointConnectionString of string
        | [<AltCommandLine "-cs">]          CheckpointSchema of string
        | [<AltCommandLine "-cat">]         Category of string
        interface IArgParserTemplate with
            member a.Usage = a |> function
                | ConnectionString _ ->     $"Connection string for the postgres database housing message-db. (Optional if environment variable {CONNECTION_STRING} is defined)"
                | CheckpointConnectionString _ -> "Connection string used for the checkpoint store. If not specified, defaults to the connection string argument"
                | CheckpointSchema _ ->     $"Schema that should contain the checkpoints table Optional if environment variable {SCHEMA} is defined"
                | Category _ ->             "The message-db category to load (must specify >1 when projecting)"

    type Arguments(c: Configuration, p: ParseResults<Parameters>) =
        let connectionString () = p.GetResult(ConnectionString, fun () -> c.MdbConnectionString)
        let checkpointConnectionString () = p.GetResult(CheckpointConnectionString, connectionString)
        let schema = p.GetResult(CheckpointSchema, fun () -> c.MdbSchema)

        member _.CreateClient() =
            Array.ofList (p.GetResults Category), connectionString ()

        member _.CreateCheckpointStore(group) =
            Propulsion.MessageDb.ReaderCheckpoint.CheckpointStore(checkpointConnectionString (), schema, group)

        member x.CreateCheckpointStoreTable([<O; D null>] ?ct) = task {
            let connStringWithoutPassword = Npgsql.NpgsqlConnectionStringBuilder(checkpointConnectionString (), Password = null)
            Log.Information("Authenticating with postgres using {connectionString}", connStringWithoutPassword.ToString())
            Log.Information("Creating checkpoints table as {table}", $"{schema}.{Propulsion.MessageDb.ReaderCheckpoint.TableName}")
            let checkpointStore = x.CreateCheckpointStore("nil")
            do! checkpointStore.CreateSchemaIfNotExists(?ct = ct)
            Log.Information("Table created") }

module Json =

    type [<NoEquality; NoComparison>] Parameters =
        | [<AltCommandLine "-f"; Mandatory; MainCommand>] Path of filename: string
        | [<AltCommandLine "-s"; Unique>]   Skip of lines: int
        | [<AltCommandLine "-eof"; Unique>] Truncate of lines: int
        | [<AltCommandLine "-pos"; Unique>] LineNo of int
        interface IArgParserTemplate with
            member p.Usage = p |> function
                | Path _ ->                 "specify file path"
                | Skip _ ->                 "specify number of lines to skip"
                | Truncate _ ->             "specify line number to pretend is End of File"
                | LineNo _ ->               "specify line number to start (1-based)"
    and Arguments(c: Configuration, p: ParseResults<Parameters>) =
        member val Filepath =               p.GetResult Path
        member val Skip =                   p.TryGetResult(LineNo, fun l -> l - 1) |> Option.defaultWith (fun () -> p.GetResult(Skip, 0))
        member val Trunc =                  p.TryGetResult Truncate
