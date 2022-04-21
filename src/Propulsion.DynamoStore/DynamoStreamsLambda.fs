module Propulsion.DynamoStore.DynamoStreamsLambda

open Amazon.DynamoDBv2
open Amazon.Lambda.Core
open Amazon.Lambda.DynamoDBEvents
open Propulsion.Infrastructure // Async.Raise
open System.Text

let ingest (service : DynamoStoreIndexer.Service) (dynamoEvent : DynamoDBEvent) (context : ILambdaContext) : Async<unit> =

    let spans, summary = ResizeArray(), StringBuilder()
    let mutable indexStream, noEvents = 0, 0
    try for record in dynamoEvent.Records do
            match record.Dynamodb.StreamViewType with
            | x when x = StreamViewType.NEW_IMAGE || x = StreamViewType.NEW_AND_OLD_IMAGES -> ()
            | x -> invalidOp (sprintf "Unexpected StreamViewType %O" x)

            summary.Append(record.EventName.Value[0]) |> ignore

            let updated = record.Dynamodb.NewImage
            match record.EventName with
            | et when et = OperationType.REMOVE -> ()
            | et when et = OperationType.INSERT || et = OperationType.MODIFY ->
                let p = record.Dynamodb.Keys["p"].S
                let sn, n = IndexStreamId.ofP p, int64 updated["n"].N
                let appendedLen = int updated["a"].N
                if p.StartsWith(AppendsEpoch.Category) || p.StartsWith(AppendsIndex.Category) then indexStream <- indexStream + 1
                elif appendedLen = 0 then noEvents <- noEvents + 1
                else
                    let allBatchEventTypes = updated["c"].L.ToArray() |> Seq.map (fun x -> x.S) |> Array.ofSeq
                    match allBatchEventTypes |> Array.skip (allBatchEventTypes.Length - appendedLen) with
                    | [||] -> ()
                    | appendedSpanEventTypes ->
                        let i = n - allBatchEventTypes.LongLength
                        spans.Add({ p = sn; i = i; c = appendedSpanEventTypes } : AppendsEpoch.Events.StreamSpan)
                        let et =
                            match appendedSpanEventTypes with
                            | [| et |] -> ":" + et
                            | xs -> sprintf ":%s+%d" xs[0] (xs.Length - 1)
                        summary.Append(p).Append(et).Append(if i = 0 then " " else sprintf "@%d " i) |> ignore
            | et -> invalidOp (sprintf "Unknown OperationType %s" et.Value)
        match spans.ToArray() with
        | [||] -> async { context.Logger.LogInformation(sprintf "Index %d NoEvents %d Spans 0 %O" indexStream noEvents summary) }
        | spans ->
            context.Logger.LogInformation(sprintf "Index %d NoEvents %d Spans %d %O" indexStream noEvents spans.Length summary)
            service.Ingest(AppendsTrancheId.wellKnownId, spans)
    with e -> async {
        context.Logger.LogWarning(summary.ToString())
        return! Async.Raise e }
