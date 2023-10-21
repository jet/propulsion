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
    member x.CosmosConnection =                     get Configuration.Cosmos.CONNECTION
    member x.CosmosDatabase =                       get Configuration.Cosmos.DATABASE
    member x.CosmosContainer =                      get Configuration.Cosmos.CONTAINER

    member x.DynamoRegion =                         tryGet Configuration.Dynamo.REGION
    member x.DynamoServiceUrl =                     get Configuration.Dynamo.SERVICE_URL
    member x.DynamoAccessKey =                      get Configuration.Dynamo.ACCESS_KEY
    member x.DynamoSecretKey =                      get Configuration.Dynamo.SECRET_KEY
    member x.DynamoTable =                          get Configuration.Dynamo.TABLE
    member x.DynamoIndexTable =                     tryGet Configuration.Dynamo.INDEX_TABLE

    member x.KafkaBroker =                          get Configuration.Kafka.BROKER
    member x.KafkaTopic =                           get Configuration.Kafka.TOPIC

    member x.MdbConnectionString =                  get Configuration.Mdb.CONNECTION_STRING
    member x.MdbSchema =                            get Configuration.Mdb.SCHEMA

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
        member val MaybeLogLagInterval =    p.TryGetResult LagFreqM |> Option.map TimeSpan.minutes
        member _.CreateLeasesContainer() =  connector.CreateLeasesContainer(databaseId, leasesContainerName)
        member _.ConnectFeed() =            connector.ConnectFeed(databaseId, containerId, leasesContainerName)
        member x.CreateCheckpointStore(group, cache, storeLog) = async {
            let! context = connector.ConnectContext("Checkpoints", databaseId, containerId, 256)
            return Propulsion.Feed.ReaderCheckpoint.CosmosStore.create storeLog (group, checkpointInterval) (context, cache) }

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

module Mdb =
    open Configuration.Mdb
    open Npgsql
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

        member x.CreateClient() =
            Array.ofList (p.GetResults Category), connectionString ()

        member x.CreateCheckpointStore(group) =
            Propulsion.MessageDb.ReaderCheckpoint.CheckpointStore(checkpointConnectionString (), schema, group)

        member x.CreateCheckpointStoreTable([<O; D null>] ?ct) = task {
            let connStringWithoutPassword = NpgsqlConnectionStringBuilder(checkpointConnectionString (), Password = null)
            Log.Information("Authenticating with postgres using {connectionString}", connStringWithoutPassword.ToString())
            Log.Information("Creating checkpoints table as {table}", $"{schema}.{Propulsion.MessageDb.ReaderCheckpoint.TableName}")
            let checkpointStore = x.CreateCheckpointStore("nil")
            do! checkpointStore.CreateSchemaIfNotExists(?ct = ct)
            Log.Information("Table created") }
