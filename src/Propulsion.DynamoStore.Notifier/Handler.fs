module Propulsion.DynamoStore.Notifier.Handler

open Amazon.DynamoDBv2
open Amazon.Lambda.DynamoDBEvents
open Amazon.SimpleNotificationService
open Amazon.SimpleNotificationService.Model
open Propulsion.DynamoStore
open Propulsion.Internal
open System.Collections.Generic
open System.Net
open System.Threading

let private parse (log : Serilog.ILogger) (dynamoEvent : DynamoDBEvent) : KeyValuePair<Propulsion.Feed.TrancheId, Propulsion.Feed.Position> array =
    let tails = Dictionary()
    let updateTails trancheId checkpoint =
        match tails.TryGetValue trancheId with
        | false, _ -> tails.Add(trancheId, checkpoint)
        | true, cur -> if checkpoint > cur then tails[trancheId] <- checkpoint
    let summary = System.Text.StringBuilder()
    let mutable indexStream, otherStream, noEvents = 0, 0, 0
    try for record in dynamoEvent.Records do
            match record.Dynamodb.StreamViewType with
            | x when x = StreamViewType.NEW_IMAGE || x = StreamViewType.NEW_AND_OLD_IMAGES -> ()
            | x -> invalidOp (sprintf "Unexpected StreamViewType %O" x)

            summary.Append(record.EventName.Value[0]) |> ignore

            let updated = record.Dynamodb.NewImage
            match record.EventName with
            | ot when ot = OperationType.REMOVE -> ()
            | ot when ot = OperationType.INSERT || ot = OperationType.MODIFY ->
                let p = record.Dynamodb.Keys["p"].S
                match FsCodec.StreamName.parse p with
                | AppendsEpoch.StreamName (trancheId, epochId) ->
                    match int64 updated["a"].N with
                    | 0L -> noEvents <- noEvents + 1
                    | appendedLen ->
                        let n = int64 updated["n"].N
                        let i = n - appendedLen
                        summary.Append(trancheId).Append('/').Append(epochId).Append(appendedLen).Append('@').Append(i) |> ignore
                        let eventTypes = updated["c"].L
                        let isClosed = eventTypes[eventTypes.Count - 1].S |> AppendsEpoch.Events.isEventTypeClosed
                        let checkpoint = Checkpoint.positionOfEpochClosedAndVersion epochId isClosed n
                        updateTails trancheId checkpoint
                | _ ->
                    if p.StartsWith AppendsIndex.Category then indexStream <- indexStream + 1
                    else otherStream <- otherStream + 1
            | et -> invalidOp (sprintf "Unknown OperationType %s" et.Value)
        log.Information("Index {indexCount} Other {otherCount} NoEvents {noEventCount} Tails {tails} {summary}",
                        indexStream, otherStream, noEvents, Seq.map ValueTuple.ofKvp tails, summary)
        Array.ofSeq tails
    with e ->
        log.Warning(e, "Failed {summary}", summary)
        reraise ()

type SnsClient(topicArn) =

    let client : IAmazonSimpleNotificationService = new AmazonSimpleNotificationServiceClient()

    let mkRequest messages =
        let req = PublishBatchRequest(TopicArn = topicArn)
        for struct (trancheId, pos) in messages do
            let entry = PublishBatchRequestEntry(Subject = trancheId, Message = pos, MessageGroupId = trancheId, MessageDeduplicationId = trancheId + pos)
            entry.MessageAttributes.Add("TrancheId", MessageAttributeValue(StringValue = trancheId))
            entry.MessageAttributes.Add("Position", MessageAttributeValue(StringValue = pos))
            req.PublishBatchRequestEntries.Add(entry)
        req

    let publishBatch (log : Serilog.ILogger) ct (req : PublishBatchRequest) = task {
        let! res = client.PublishBatchAsync(req, ct)
        if res.HttpStatusCode <> HttpStatusCode.OK || res.Failed.Count <> 0 then
            let fails = [| for x in res.Failed -> struct (x.Code, x.SenderFault, x.Message) |]
            log.Warning("PublishBatchAsync {res}. Fails: {fails}", res.HttpStatusCode, fails)
            failwithf "PublishBatchAsync result %A %A" res.HttpStatusCode fails }

    member _.Publish(log : Serilog.ILogger, messageGroupsAndMessages) = task {
        for b in messageGroupsAndMessages |> Seq.chunkBySize 10 |> Seq.map mkRequest do
           do! publishBatch log CancellationToken.None b }

let handle log (client : SnsClient) dynamoEvent = task {
    match parse log dynamoEvent with
    | [||] -> ()
    | spans -> do! client.Publish(log, seq { for m in spans -> Propulsion.Feed.TrancheId.toString m.Key, Propulsion.Feed.Position.toString m.Value }) }
