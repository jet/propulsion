namespace Propulsion.DynamoStore.Indexer

open Amazon.Lambda.Core
open Amazon.Lambda.DynamoDBEvents
open Equinox.DynamoStore
open Propulsion.DynamoStore
open Serilog

[<assembly: LambdaSerializer(typeof<Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer>)>] ()

type Configuration(?tryGet) =
    let envVarTryGet = System.Environment.GetEnvironmentVariable >> Option.ofObj
    let tryGet = defaultArg tryGet envVarTryGet
    let get key = match tryGet key with Some value -> value | None -> failwithf "Missing Argument/Environment Variable %s" key

    member _.DynamoRegion =             tryGet Propulsion.DynamoStore.Lambda.Args.Dynamo.REGION
    member _.DynamoServiceUrl =         get Propulsion.DynamoStore.Lambda.Args.Dynamo.SERVICE_URL
    member _.DynamoAccessKey =          get Propulsion.DynamoStore.Lambda.Args.Dynamo.ACCESS_KEY
    member _.DynamoSecretKey =          get Propulsion.DynamoStore.Lambda.Args.Dynamo.SECRET_KEY
    member _.DynamoIndexTable =         get Propulsion.DynamoStore.Lambda.Args.Dynamo.INDEX_TABLE
    member _.OnlyWarnGap =              tryGet Propulsion.DynamoStore.Lambda.Args.Dynamo.ONLY_WARN_GAP |> Option.map bool.Parse

type Store(connector: DynamoStoreConnector, table, dynamoItemSizeCutoffBytes) =
    let queryMaxItems = 100

    let storeClient = connector.CreateDynamoDbClient() |> DynamoStoreClient
    member val Context = DynamoStoreContext(storeClient, table, maxBytes = dynamoItemSizeCutoffBytes, queryMaxItems = queryMaxItems)

    new(c: Configuration, requestTimeout, retries, dynamoItemSizeCutoffBytes) =
        let conn =
            match c.DynamoRegion with
            | Some r -> DynamoStoreConnector(r, requestTimeout, retries)
            | None -> DynamoStoreConnector(c.DynamoServiceUrl, c.DynamoAccessKey, c.DynamoSecretKey, requestTimeout, retries)
        Store(conn, c.DynamoIndexTable, dynamoItemSizeCutoffBytes)

type Function() =

    // Larger optimizes for not needing to use TransactWriteItems as frequently
    // Smaller will trigger more items and reduce read costs for Sources reading fro the tail
    let itemCutoffKiB = 48
    let config = Configuration()
    let store = Store(config, requestTimeout = System.TimeSpan.FromSeconds 120., retries = 10, dynamoItemSizeCutoffBytes = itemCutoffKiB * 1024)
    // TOCONSIDER surface metrics from write activities to prometheus by wiring up Metrics Sink (for now we log them instead)
    let removeMetrics (e : Serilog.Events.LogEvent) = e.RemovePropertyIfPresent(Equinox.DynamoStore.Core.Log.PropertyTag)
    let template = "{Level:u1} {Message} {Properties}{NewLine}{Exception}"
    let log =
        LoggerConfiguration()
            .Enrich.With({ new Serilog.Core.ILogEventEnricher with member _.Enrich(evt, _) = removeMetrics evt })
            .WriteTo.Console(outputTemplate = template)
            .CreateLogger()
    let ingester = DynamoStoreIngester(log, store.Context, ?onlyWarnOnGap = config.OnlyWarnGap)

    member _.Handle(dynamoEvent : DynamoDBEvent, _context : ILambdaContext) : System.Threading.Tasks.Task =
        Handler.handle log ingester.Service dynamoEvent
