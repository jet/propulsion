namespace Propulsion.DynamoStore.Lambda

open Amazon.Lambda.SQSEvents
open Propulsion.Internal
open Serilog
open System.Collections.Generic

/// Each queued Notifier message conveyed to the Lambda represents a Target Position on an Index Tranche
type SqsNotificationBatch(event: SQSEvent) =
    let inputs = [|
        for r in event.Records ->
            let trancheId = r.MessageAttributes["Partition"].StringValue |> Propulsion.Feed.TrancheId.parse
            let position = r.MessageAttributes["Position"].StringValue |> int64 |> Propulsion.Feed.Position.parse
            struct (trancheId, position, r.MessageId) |]

    member val Count = inputs.Length
    /// Yields the set of Index Partitions on which we are anticipating there to be work available
    member val Tranches = seq { for trancheId, _, _ in inputs -> trancheId } |> Seq.distinct |> Seq.toArray
    /// Correlates the achieved Tranche Positions with those that triggered the work; requeue any not yet acknowledged as processed
    member _.FailuresForPositionsNotReached(updated: IReadOnlyDictionary<_, _>) =
        let res = SQSBatchResponse()
        let incomplete = ResizeArray()
        for trancheId, pos, messageId in inputs do
            match updated.TryGetValue trancheId with
            | true, pos' when pos' >= pos -> ()
            | _ ->
                res.BatchItemFailures.Add(SQSBatchResponse.BatchItemFailure(ItemIdentifier = messageId))
                incomplete.Add(struct (trancheId, pos))
        struct (res, incomplete.ToArray())

module SqsNotificationBatch =

    let parse (event: SQSEvent) =
        let req = SqsNotificationBatch(event)
        Log.Information("SqsBatch {count} notifications, {tranches} tranches", req.Count, req.Tranches.Length)
        req

    let batchResponseWithFailuresForPositionsNotReached (req: SqsNotificationBatch) checkpoints: SQSBatchResponse =
        let struct (res, requeued) = req.FailuresForPositionsNotReached(checkpoints)
        if requeued.Length > 0 then Log.Information("SqsBatch requeued {requeued}", requeued)
        res
