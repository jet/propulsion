module Propulsion.DynamoStore.DynamoStreamsLambda

open Amazon.DynamoDBv2
open Amazon.Lambda.Core
open Amazon.Lambda.DynamoDBEvents

let ingest (service : DynamoStoreIndexer.Service) (dynamoEvent : DynamoDBEvent) (context : ILambdaContext) : Async<unit> =

    let spans = ResizeArray<AppendsEpoch.Events.StreamSpan>()

    let processRecord (record: DynamoDBEvent.DynamodbStreamRecord) =
        context.Logger.LogInformation(sprintf "Event ID: %s" record.EventID)
        context.Logger.LogInformation(sprintf "Event Name: %s" record.EventName.Value)

        match record.Dynamodb.StreamViewType with
        | x when x = StreamViewType.NEW_IMAGE || x = StreamViewType.NEW_AND_OLD_IMAGES -> ()
        | x -> invalidOp (sprintf "Unexpected StreamViewType %O" x)

        let sn, i = IndexStreamId.ofP record.Dynamodb.Keys["p"].S, int record.Dynamodb.Keys["i"].N
        let appendedLen = int record.Dynamodb.NewImage["a"].N
        let appendedSpanEventTypes =
            let batchEventTypes = record.Dynamodb.NewImage["c"].NS
            batchEventTypes.GetRange(batchEventTypes.Count - appendedLen, appendedLen).ToArray()

        spans.Add { p = sn; i = i; c = appendedSpanEventTypes }

        context.Logger.LogInformation(sprintf "Span %O @ %d : %A" sn i appendedSpanEventTypes)

    dynamoEvent.Records
    |> Seq.iter processRecord

    service.Ingest(AppendsTrancheId.wellKnownId, spans.ToArray())
