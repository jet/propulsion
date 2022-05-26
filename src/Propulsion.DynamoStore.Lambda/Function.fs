namespace Propulsion.DynamoStore.Lambda

open Amazon.Lambda.Core
open Amazon.Lambda.DynamoDBEvents
open Equinox.DynamoStore
open Propulsion.DynamoStore
open Serilog

[<assembly: LambdaSerializer(typeof<Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer>)>]
()

type Configuration(?tryGet) =
    let envVarTryGet = System.Environment.GetEnvironmentVariable >> Option.ofObj
    let tryGet = defaultArg tryGet envVarTryGet
    let get key =
        match tryGet key with
        | Some value -> value
        | None -> failwithf "Missing Argument/Environment Variable %s" key

    let [<Literal>] SERVICE_URL =       "EQUINOX_DYNAMO_SERVICE_URL"
    let [<Literal>] ACCESS_KEY =        "EQUINOX_DYNAMO_ACCESS_KEY_ID"
    let [<Literal>] SECRET_KEY =        "EQUINOX_DYNAMO_SECRET_ACCESS_KEY"
    let [<Literal>] TABLE_INDEX =       "EQUINOX_DYNAMO_TABLE_INDEX"

    member _.DynamoServiceUrl =         get SERVICE_URL
    member _.DynamoAccessKey =          get ACCESS_KEY
    member _.DynamoSecretKey =          get SECRET_KEY
    member _.DynamoIndexTable =         get TABLE_INDEX

type Connector(serviceUrl, accessKey, secretKey, table, requestTimeout, retries, dynamoItemSizeCutoffBytes) =
    let queryMaxItems = 100

    let conn = DynamoStoreConnector(serviceUrl, accessKey, secretKey, requestTimeout, retries)
    let client = conn.CreateClient()
    let storeClient = DynamoStoreClient(client, table)
    let context = DynamoStoreContext(storeClient, maxBytes = dynamoItemSizeCutoffBytes, queryMaxItems = queryMaxItems)

    new (c : Configuration, requestTimeout, retries, dynamoItemSizeCutoffBytes) =
        Connector(c.DynamoServiceUrl, c.DynamoAccessKey, c.DynamoSecretKey, c.DynamoIndexTable, requestTimeout, retries, dynamoItemSizeCutoffBytes)

    member _.Context = context

type Function() =

    // Larger optimizes for not needing to use TransactWriteItems as frequently
    // Smaller will trigger more items and reduce read costs for Sources reading fro the tail
    let itemCutoffKiB = 48
    // Values up to 5 work reasonably, but side effects are:
    // - read usage is more 'lumpy'
    // - readers need more memory to hold the state
    // - Lambda startup time increases
    let epochCutoffMiB = 1
    // Should be large enough to accomodate state of 2 epochs
    // Note the backing memory is not preallocated, so the effects of this being too large will not be immediately apparent
    // (Overusage will hasten the Lambda being killed due to excess memory usage)
    let maxCacheMiB = 5
    let cfg = Configuration()
    let conn = Connector(cfg, requestTimeout = (System.TimeSpan.FromSeconds 120.), retries = 10, dynamoItemSizeCutoffBytes = itemCutoffKiB * 1024)
    let cache = Equinox.Cache("indexer", sizeMb = maxCacheMiB)
    // TOCONSIDER surface metrics from write activities to prometheus by wiring up Metrics Sink (for now we log them instead)
    let removeMetrics (e : Serilog.Events.LogEvent) = e.RemovePropertyIfPresent(Equinox.DynamoStore.Core.Log.PropertyTag)
    let template = "{Level:u1} {Message} {Properties}{NewLine}{Exception}"
    let log =
        LoggerConfiguration()
            .Enrich.With({ new Serilog.Core.ILogEventEnricher with member _.Enrich(evt,_) = removeMetrics evt })
            .WriteTo.Console(outputTemplate = template)
            .CreateLogger()
    let service = DynamoStoreIndexer(log, conn.Context, cache, epochBytesCutoff = epochCutoffMiB * 1024 * 1024) //

    member _.FunctionHandler(dynamoEvent : DynamoDBEvent, _context : ILambdaContext) =
        DynamoStreamsLambda.ingest log service dynamoEvent |> Async.StartImmediateAsTask
