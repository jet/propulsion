module Propulsion.DynamoStore.Notifier.Handler

open Amazon.DynamoDBStreams
open Amazon.Lambda.DynamoDBEvents
open Amazon.SimpleNotificationService
open Amazon.SimpleNotificationService.Model
open Propulsion.DynamoStore
open Propulsion.Internal
open System.Collections.Generic
open System.Net
open System.Threading

let private parse (log: Serilog.ILogger) (dynamoEvent: DynamoDBEvent): KeyValuePair<AppendsPartitionId, Propulsion.Feed.Position>[]  =
    let tails = Dictionary()
    let updateTails partitionId checkpoint =
        match tails.TryGetValue partitionId with
        | false, _ -> tails.Add(partitionId, checkpoint)
        | true, cur -> if checkpoint > cur then tails[partitionId] <- checkpoint
    let summary = System.Text.StringBuilder()
    let mutable indexStream, otherStream, noEvents = 0, 0, 0
    try for record in dynamoEvent.Records do
            match record.Dynamodb.StreamViewType with
            | x when x = string StreamViewType.NEW_IMAGE || x = string StreamViewType.NEW_AND_OLD_IMAGES -> ()
            | x -> invalidOp $"Unexpected StreamViewType {x}"

            if summary.Length <> 0 then summary.Append ' ' |> ignore
            summary.Append(record.EventName) |> ignore

            let updated = record.Dynamodb.NewImage
            match record.EventName with
            | ot when ot = "REMOVE" -> ()
            | ot when ot = "INSERT" || ot = "MODIFY" ->
                let p = record.Dynamodb.Keys["p"].S
                match FsCodec.StreamName.parse p with
                | AppendsEpoch.Stream.For (partitionId, epochId) ->
                    // Calf writes won't have an "a" field
                    let appendedLen = match updated.TryGetValue "a" with true, v -> int64 v.N | false, _ -> 0
                    // Tip writes may not actually have added events, if the sync was transmuted to an update of the unfolds only
                    // In such cases, we would not want to trigger a downstream write
                    // (This should not actually manifest for AppendsEpoch streams; mentioning for completeness)
                    if appendedLen = 0 then noEvents <- noEvents + 1 else

                    let n = int64 updated["n"].N
                    let i = n - appendedLen
                    summary.Append(partitionId).Append('/').Append(epochId).Append(' ').Append(appendedLen).Append('@').Append(i) |> ignore
                    let eventTypes = updated["c"].L
                    let isClosed = eventTypes[eventTypes.Count - 1].S |> AppendsEpoch.Events.isEventTypeClosed
                    let checkpoint = Checkpoint.positionOfEpochClosedAndVersion epochId isClosed n
                    updateTails partitionId checkpoint
                | _ ->
                    if p.StartsWith AppendsIndex.Stream.Category then indexStream <- indexStream + 1
                    else otherStream <- otherStream + 1
            | et -> invalidOp $"Unknown OperationType %s{et}"
        log.Information("Index {indexCount} Other {otherCount} NoEvents {noEventCount} Tails {tails} {summary:l}",
                        indexStream, otherStream, noEvents, Seq.map ValueTuple.ofKvp tails, summary)
        Array.ofSeq tails
    with e ->
        log.Warning(e, "Failed {summary}", summary)
        reraise ()

let private mkRequest topicArn messages =
    let req = PublishBatchRequest(TopicArn = topicArn)
    messages |> Seq.iteri (fun i struct (partitionId, pos) ->
        let e = PublishBatchRequestEntry(Id = string i, Subject = partitionId, Message = pos, MessageGroupId = partitionId, MessageDeduplicationId = partitionId + pos)
        e.MessageAttributes.Add("Partition", MessageAttributeValue(StringValue = partitionId, DataType="String"))
        e.MessageAttributes.Add("Position", MessageAttributeValue(StringValue = pos, DataType="String"))
        req.PublishBatchRequestEntries.Add(e))
    req

let private publishBatch (client: IAmazonSimpleNotificationService) (log: Serilog.ILogger) ct (req: PublishBatchRequest) = task {
    let! res = client.PublishBatchAsync(req, ct)
    if res.HttpStatusCode <> HttpStatusCode.OK || res.Failed.Count <> 0 then
        let fails = [| for x in res.Failed -> struct (x.Code, x.SenderFault, x.Message) |]
        log.Warning("PublishBatchAsync {res}. Fails: {fails}", res.HttpStatusCode, fails)
        failwithf $"PublishBatchAsync result {res.HttpStatusCode} %A{fails}" }

type SnsClient(topicArn) =

    let client: IAmazonSimpleNotificationService = new AmazonSimpleNotificationServiceClient()

    member _.Publish(log: Serilog.ILogger, messageGroupsAndMessages) = task {
        for b in messageGroupsAndMessages |> Seq.chunkBySize 10 |> Seq.map (mkRequest topicArn) do
           do! publishBatch client log CancellationToken.None b }

let handle log (client: SnsClient) dynamoEvent = task {
    match parse log dynamoEvent with
    | [||] -> ()
    | spans -> do! client.Publish(log, seq { for m in spans -> AppendsPartitionId.toString m.Key, Propulsion.Feed.Position.toString m.Value }) }
