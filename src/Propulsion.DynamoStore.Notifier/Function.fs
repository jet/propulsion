namespace Propulsion.DynamoStore.Notifier

open Amazon.Lambda.Core
open Amazon.Lambda.DynamoDBEvents
open Serilog

[<assembly: LambdaSerializer(typeof<Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer>)>] ()

type Configuration(?tryGet) =
    let envVarTryGet = System.Environment.GetEnvironmentVariable >> Option.ofObj
    let tryGet = defaultArg tryGet envVarTryGet
    let get key = match tryGet key with Some value -> value | None -> failwithf "Missing Argument/Environment Variable %s" key

    let [<Literal>] SNS_ARN =           "SNS_TOPIC_ARN"

    member val SnsTopicArn =            get SNS_ARN

type Function() =

    let config = Configuration()
    let snsClient = Handler.SnsClient(config.SnsTopicArn)
    let template = "{Level:u1} {Message} {Properties}{NewLine}{Exception}"
    let log =
        LoggerConfiguration()
            .WriteTo.Console(outputTemplate = template)
            .CreateLogger()

    member _.Handle(dynamoEvent : DynamoDBEvent, _context : ILambdaContext) : System.Threading.Tasks.Task =
        Handler.handle log snsClient dynamoEvent
