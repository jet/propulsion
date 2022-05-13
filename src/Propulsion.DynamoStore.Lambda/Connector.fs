namespace Propulsion.DynamoStore.Lambda

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

open Equinox.DynamoStore

type Connector(serviceUrl, accessKey, secretKey, table) =
    let retries, timeout, queryMaxItems, maxBytes = 10, System.TimeSpan.FromSeconds 60., 50, 64*1024

    let conn = DynamoStoreConnector(serviceUrl, accessKey, secretKey, retries, timeout)
    let client = conn.CreateClient()
    let storeClient = DynamoStoreClient(client, table)
    let context = DynamoStoreContext(storeClient, maxBytes = maxBytes, queryMaxItems = queryMaxItems)

    new (c : Configuration) = Connector(c.DynamoServiceUrl, c.DynamoAccessKey, c.DynamoSecretKey, c.DynamoIndexTable)

    member _.Context = context
