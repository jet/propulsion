namespace Propulsion.DynamoStore.Lambda

type Configuration(?tryGet) =
    let envVarTryGet = System.Environment.GetEnvironmentVariable >> Option.ofObj
    let tryGet = defaultArg tryGet envVarTryGet
    let get key =
        match tryGet key with
        | Some value -> value
        | None -> failwith $"Missing Argument/Environment Variable %s{key}"

    let [<Literal>] SERVICE_URL =       "EQUINOX_DYNAMO_SERVICE_URL"
    let [<Literal>] ACCESS_KEY =        "EQUINOX_DYNAMO_ACCESS_KEY_ID"
    let [<Literal>] SECRET_KEY =        "EQUINOX_DYNAMO_SECRET_ACCESS_KEY"
    let [<Literal>] TABLE =             "EQUINOX_DYNAMO_TABLE"
//  let [<Literal>] ARCHIVE_TABLE =     "EQUINOX_DYNAMO_TABLE_ARCHIVE"

    member _.DynamoServiceUrl =         get SERVICE_URL
    member _.DynamoAccessKey =          get ACCESS_KEY
    member _.DynamoSecretKey =          get SECRET_KEY
    member _.DynamoTable =              get TABLE
//  member _.DynamoArchiveTable =       get ARCHIVE_TABLE

open Equinox.DynamoStore

type Connector(serviceUrl, accessKey, secretKey, table) =
    let conn = DynamoStoreConnector(serviceUrl, accessKey, secretKey)
    let client = conn.CreateClient()
    let storeClient = DynamoStoreClient(client, table)
    let context = DynamoStoreContext(storeClient)

    new (c : Configuration) = Connector(c.DynamoServiceUrl, c.DynamoAccessKey, c.DynamoSecretKey, c.DynamoTable)

    member _.Log(log : Serilog.ILogger, name) = log.Information("DynamoDB Connecting {name} to {endpoint}", name, serviceUrl)
    member _.Context = context
