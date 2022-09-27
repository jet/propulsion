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

    let [<Literal>] SYSTEM_NAME =       "EQUINOX_DYNAMO_SYSTEM_NAME"
    let [<Literal>] SERVICE_URL =       "EQUINOX_DYNAMO_SERVICE_URL"
    let [<Literal>] ACCESS_KEY =        "EQUINOX_DYNAMO_ACCESS_KEY_ID"
    let [<Literal>] SECRET_KEY =        "EQUINOX_DYNAMO_SECRET_ACCESS_KEY"
    let [<Literal>] TABLE_INDEX =       "EQUINOX_DYNAMO_TABLE_INDEX"

    member _.DynamoSystemName =         tryGet SYSTEM_NAME
    member _.DynamoServiceUrl =         get SERVICE_URL
    member _.DynamoAccessKey =          get ACCESS_KEY
    member _.DynamoSecretKey =          get SECRET_KEY
    member _.DynamoIndexTable =         get TABLE_INDEX

type Store(connector : DynamoStoreConnector, table, dynamoItemSizeCutoffBytes) =
    let queryMaxItems = 100

    let client = connector.CreateClient()
    let storeClient = DynamoStoreClient(client, table)
    let context = DynamoStoreContext(storeClient, maxBytes = dynamoItemSizeCutoffBytes, queryMaxItems = queryMaxItems)

    new (c : Configuration, requestTimeout, retries, dynamoItemSizeCutoffBytes) =
        let conn =
            match c.DynamoSystemName with
            | Some r -> DynamoStoreConnector(r, requestTimeout, retries)
            | None -> DynamoStoreConnector(c.DynamoServiceUrl, c.DynamoAccessKey, c.DynamoSecretKey, requestTimeout, retries)
        Store(conn, c.DynamoIndexTable, dynamoItemSizeCutoffBytes)

    member _.Context = context

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
    let ingester = DynamoStoreIngester(log, store.Context)

    member _.Handle(dynamoEvent : DynamoDBEvent, _context : ILambdaContext) : System.Threading.Tasks.Task =
        Handler.handle log ingester.Service dynamoEvent
