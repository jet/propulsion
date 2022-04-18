module Propulsion.DynamoStore.DynamoStreamsLambda


open Amazon.DynamoDBv2
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
            match record.Dynamodb.StreamViewType with
            | x when x = StreamViewType.NEW_IMAGE || x = StreamViewType.NEW_AND_OLD_IMAGES -> ()
            | x -> invalidOp (sprintf "Unexpected StreamViewType %O" x)

            let sn, i = IndexStreamId.ofP record.Dynamodb.Keys["p"].S, int record.Dynamodb.Keys["i"].N
            let appendedLen = int record.Dynamodb.NewImage["a"].N
            let appendedSpanEventTypes =
                let batchEventTypes = record.Dynamodb.NewImage["c"].NS
                batchEventTypes.GetRange(batchEventTypes.Count - appendedLen, appendedLen)

            context.Logger.LogInformation(sprintf "Span %O @ %d : %A" sn i appendedSpanEventTypes)
            // TODO: Add business logic processing the record.Dynamodb object.

        dynamoEvent.Records
        |> Seq.iter processRecord

        context.Logger.LogInformation("Stream processing complete.")
