namespace Propulsion.CosmosStore

open Propulsion.Internal
open System

type internal ChangeFeedProcessor =
    static member Start
        (   monitored: Microsoft.Azure.Cosmos.Container, leases: Microsoft.Azure.Cosmos.Container, processorName: string, stats: Stats, leaseOwnerId, ingest,
            startFromTail, feedPollDelay, leaseAcquireInterval, leaseRenewInterval, leaseTtl, ?maxItems, ?notifyError, ?customize, ?lagReportFrequency) =
        stats.LogStart(leaseAcquireInterval, leaseTtl, leaseRenewInterval, feedPollDelay, startFromTail, ?maxItems = maxItems)
        let processorName_ = processorName + ":"
        let leaseTokenToPartitionId (leaseToken: string) = int (leaseToken.Trim[|'"'|])
        let processor =
            let handler (context: Microsoft.Azure.Cosmos.ChangeFeedProcessorContext) (changes: ChangeFeedItems) (checkpointAsync: Func<Task>) ct: Task = task {
                let log: exn -> unit = function
                    | :? OperationCanceledException -> () // Shutdown via .Stop triggers this
                    | e -> stats.LogHandlerExn(leaseTokenToPartitionId context.LeaseToken, e)
                try let ctx = { group = processorName
                                epoch = context.Headers.ContinuationToken.Trim[|'"'|] |> int64
                                timestamp = changes |> Seq.last |> ChangeFeedItem.timestamp
                                rangeId = leaseTokenToPartitionId context.LeaseToken
                                requestCharge = context.Headers.RequestCharge }
                    return! ingest (ctx, changes, checkpointAsync, ct)
                with Exception.Log log () -> () }
            let logStateChange state leaseToken = stats.LogStateChange(leaseTokenToPartitionId leaseToken, state); Task.CompletedTask
            let notifyError =
                let log = match notifyError with Some f -> f | None -> Action<_, _>(fun i ex -> stats.LogExn(i, ex))
                fun leaseToken ex -> log.Invoke(leaseTokenToPartitionId leaseToken, ex); Task.CompletedTask
            monitored
                .GetChangeFeedProcessorBuilderWithManualCheckpoint(processorName_, handler)
                .WithLeaseContainer(leases)
                .WithPollInterval(feedPollDelay)
                .WithLeaseConfiguration(acquireInterval = leaseAcquireInterval, expirationInterval = leaseTtl, renewInterval = leaseRenewInterval)
                .WithInstanceName(leaseOwnerId)
                .WithLeaseAcquireNotification(logStateChange "Acquire")
                .WithLeaseReleaseNotification(logStateChange "Release")
                .WithErrorNotification(notifyError)
                |> fun b -> if startFromTail then b else let minTime = DateTime.MinValue in b.WithStartTime(minTime.ToUniversalTime()) // fka StartFromBeginning
                |> fun b -> match maxItems with Some mi -> b.WithMaxItems(mi) | None -> b
                |> fun b -> match customize with Some c -> c b | None -> b
                |> fun b -> b.Build()
        let maybePumpMetrics =
            lagReportFrequency
            |> Option.map (fun interval ->
                let lagMonitorCallback (remainingWork: struct (int * int64)[]) ct = task {
                    stats.ReportEstimation(remainingWork)
                    return! Task.Delay(TimeSpan.toMs interval, ct) }
                let estimator = monitored.GetChangeFeedEstimator(processorName_, leases)
                let fetchEstimatorStates (map: Microsoft.Azure.Cosmos.ChangeFeedProcessorState -> 'u) ct: Task<'u[]>  = task {
                    use query = estimator.GetCurrentStateIterator()
                    let result = ResizeArray()
                    while query.HasMoreResults do
                        let! res = query.ReadNextAsync(ct)
                        for x in res do result.Add(map x)
                    return result.ToArray() }
                fun (ct: CancellationToken) -> task {
                    stats.ReportEstimationInterval(interval)
                    while not ct.IsCancellationRequested do
                        let! leasesStates = fetchEstimatorStates (fun s -> struct (leaseTokenToPartitionId s.LeaseToken, s.EstimatedLag)) ct
                        Array.sortInPlaceBy ValueTuple.fst leasesStates
                        do! lagMonitorCallback leasesStates ct } )
        Propulsion.PipelineFactory.Start(stats.Log, Task.ofUnitTask << processor.StartAsync, maybePumpMetrics, Task.ofUnitTask << processor.StopAsync)
