namespace Propulsion.CosmosStore

open Propulsion.Internal
open System

type internal ChangeFeedProcessor =
    static member Start
        (   monitored: Microsoft.Azure.Cosmos.Container, leases: Microsoft.Azure.Cosmos.Container, processorName: string, stats: Stats, leaseOwnerId,
            ingest, trancheCapacity,
            startFromTail, feedPollDelay, leaseAcquireInterval, leaseRenewInterval, leaseTtl, ?maxItems, ?notifyError, ?customize, ?lagEstimationInterval) =
        stats.LogStart(leaseAcquireInterval, leaseTtl, leaseRenewInterval, feedPollDelay, startFromTail, ?maxItems = maxItems)
        let processorName_ = processorName + ":"
        let (|RangeId|) (leaseToken: string) = leaseToken.Trim[|'"'|] |> int
        let processor =
            let handler (context: Microsoft.Azure.Cosmos.ChangeFeedProcessorContext) (changes: ChangeFeedItems) (checkpointAsync: Func<Task>) ct: Task = task {
                let (RangeId rangeId) = context.LeaseToken
                let log: exn -> unit = function
                    | :? OperationCanceledException -> () // Shutdown via .Stop triggers this
                    | e -> stats.LogHandlerExn(rangeId, e)
                try let ctx = { group = processorName
                                epoch = context.Headers.ContinuationToken.Trim[|'"'|] |> int64
                                timestamp = changes |> Seq.last |> ChangeFeedItem.timestamp
                                rangeId = rangeId
                                requestCharge = context.Headers.RequestCharge }
                    return! ingest (ctx, changes, checkpointAsync, ct)
                with Exception.Log log () -> () }
            let notifyError =
                let log = match notifyError with Some f -> f | None -> Action<_, _>(fun i ex -> stats.LogExn(i, ex))
                fun (RangeId rangeId) ex -> log.Invoke(rangeId, ex); Task.CompletedTask
            let logStateChange acquired (RangeId rangeId) = stats.LogStateChange(rangeId, acquired); Task.CompletedTask
            monitored
                .GetChangeFeedProcessorBuilderWithManualCheckpoint(processorName_, handler)
                .WithLeaseContainer(leases)
                .WithPollInterval(feedPollDelay)
                .WithLeaseConfiguration(acquireInterval = leaseAcquireInterval, expirationInterval = leaseTtl, renewInterval = leaseRenewInterval)
                .WithInstanceName(leaseOwnerId)
                .WithLeaseAcquireNotification(logStateChange true)
                .WithLeaseReleaseNotification(logStateChange false)
                .WithErrorNotification(notifyError)
                |> fun b -> if startFromTail then b else let minTime = DateTime.MinValue in b.WithStartTime(minTime.ToUniversalTime()) // fka StartFromBeginning
                |> fun b -> match maxItems with Some mi -> b.WithMaxItems(mi) | None -> b
                |> fun b -> match customize with Some c -> c b | None -> b
                |> fun b -> b.Build()
        let estimator = monitored.GetChangeFeedEstimator(processorName_, leases)
        let fetchEstimatorStates (map: Microsoft.Azure.Cosmos.ChangeFeedProcessorState -> 'u) ct: Task<'u[]>  = task {
            use query = estimator.GetCurrentStateIterator()
            let result = ResizeArray()
            while query.HasMoreResults do
                let! res = query.ReadNextAsync(ct)
                for x in res do result.Add(map x)
            return result.ToArray() }
        let runEstimation ct = task {
            let! leasesStates = fetchEstimatorStates (fun s -> struct ((|RangeId|) s.LeaseToken, s.EstimatedLag)) ct
            Array.sortInPlaceBy ValueTuple.fst leasesStates
            stats.ReportEstimation leasesStates }
        let pumpEstimation interval ct =
            stats.ReportEstimationInterval(interval)
            Task.periodically (fun ct -> runEstimation ct) interval ct
        let children = seq {
            match lagEstimationInterval with None -> () | Some interval -> pumpEstimation interval
            (fun ct -> stats.PumpStats(trancheCapacity, runEstimation, ct)) }
        Propulsion.PipelineFactory.Start(stats.Log, Task.ofUnitTask << processor.StartAsync, children, Task.ofUnitTask << processor.StopAsync)
