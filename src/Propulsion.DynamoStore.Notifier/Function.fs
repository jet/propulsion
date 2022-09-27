namespace Propulsion.DynamoStore.Notifier

open Amazon.Lambda.Core
open Amazon.Lambda.DynamoDBEvents
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

type Function() =

    // TOCONSIDER surface metrics from write activities to prometheus by wiring up Metrics Sink (for now we log them instead)
    let removeMetrics (e : Serilog.Events.LogEvent) = e.RemovePropertyIfPresent(Equinox.DynamoStore.Core.Log.PropertyTag)
    let template = "{Level:u1} {Message} {Properties}{NewLine}{Exception}"
    let log =
        LoggerConfiguration()
            .Enrich.With({ new Serilog.Core.ILogEventEnricher with member _.Enrich(evt, _) = removeMetrics evt })
            .WriteTo.Console(outputTemplate = template)
            .CreateLogger()

    member _.Handle(dynamoEvent : DynamoDBEvent, _context : ILambdaContext) : System.Threading.Tasks.Task =
        let client = Handler.SnsClient(unbox ())
        Handler.handle log client dynamoEvent
