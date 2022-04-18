module Propulsion.DynamoStore.DynamoStreamsLambda


open Amazon.Lambda.Core
open Amazon.Lambda.DynamoDBEvents

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[<assembly: LambdaSerializer(typeof<Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer>)>]
()

type Function() =

    member _.Handler(dynamoEvent : DynamoDBEvent, context : ILambdaContext) =


        let processRecord (record: DynamoDBEvent.DynamodbStreamRecord) =
            context.Logger.LogInformation(sprintf "Event ID: %s" record.EventID)
            context.Logger.LogInformation(sprintf "Event Name: %s" record.EventName.Value)
            context.Logger.LogInformation(sprintf "Event Name: %s" record.Dynamodb.StreamViewType.Value) // keys only is fine
            context.Logger.LogInformation(sprintf "Event Name: %s" record.Dynamodb.Keys["i"].N)
            context.Logger.LogInformation(sprintf "Event Name: %s" record.Dynamodb.Keys["p"].S)
            context.Logger.LogInformation(sprintf "Event Name: %s" record.Dynamodb.NewImage["a"].N)
            context.Logger.LogInformation(sprintf "Event Name: %s" record.Dynamodb.NewImage["c"].NS)
            // TODO: Add business logic processing the record.Dynamodb object.

        dynamoEvent.Records
        |> Seq.iter processRecord

        context.Logger.LogInformation("Stream processing complete.")
