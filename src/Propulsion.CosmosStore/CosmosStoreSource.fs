namespace Propulsion.CosmosStore

open Propulsion.Internal
open Serilog
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

/// Wraps the Microsoft.Azure.Cosmos ChangeFeedProcessor and ChangeFeedProcessorEstimator
/// See https://docs.microsoft.com/en-us/azure/cosmos-db/how-to-use-change-feed-estimator
type CosmosStoreSource
    (   log: ILogger,
        monitored: Microsoft.Azure.Cosmos.Container,
        // The non-partitioned (i.e., PartitionKey is "id") Container holding the partition leases.
        // Should always read from the write region to keep the number of write conflicts to a minimum when the sdk
        // updates the leases. Since the non-write region(s) might lag behind due to us using non-strong consistency, during
        // fail over we are likely to reprocess some messages, but that's okay since processing has to be idempotent in any case
        leases: Microsoft.Azure.Cosmos.Container,
        // Identifier to disambiguate multiple independent feed processor positions (akin to a 'consumer group')
        processorName, parseFeedDoc, sink: Propulsion.Sinks.Sink,
        // Limit on items to take in a batch when querying for changes (in addition to 4MB response size limit). Default Unlimited.
        // Max Items is not emphasized as a control mechanism as it can only be used meaningfully when events are highly regular in size.
        [<O; D null>] ?maxItems,
        // Delay before re-polling a partition after backlog has been drained. Default: 1s
        [<O; D null>] ?tailSleepInterval,
        // (NB Only applies if this is the first time this leasePrefix is presented)
        // Specify `true` to request starting of projection from the present write position.
        // Default: false (projecting all events from start beforehand)
        [<O; D null>] ?startFromTail,
        [<O; D null>] ?lagReportFreq: TimeSpan,
        // Enables reporting or other processing of Exception conditions as per <c>WithErrorNotification</c>
        [<O; D null>] ?notifyError: Action<int, exn>,
        // Admits customizations in the ChangeFeedProcessorBuilder chain
        [<O; D null>] ?customize,
        // Frequency to check for partitions without a processor. Default: 5s
        [<O; D null>] ?leaseAcquireInterval,
        // Frequency to renew leases held by processors under our control. Default 3s
        [<O; D null>] ?leaseRenewInterval,
        // Duration to take lease when acquired/renewed. Default 10s
        [<O; D null>] ?leaseTtl) =
    let leaseOwnerId =
        // If k>1 processes share an owner id, then they will compete for same partitions.
        // In that scenario, redundant processing happen on assigned partitions, but checkpoint will process on only 1 consumer.
        // Including the processId should eliminate the possibility that a broken process manager causes k>1 scenario to happen.
        // The only downside is that upon redeploy, lease expiration / TTL would have to be observed before a consumer can pick it up.
        $"%s{Environment.MachineName}-%s{System.Reflection.Assembly.GetEntryAssembly().GetName().Name}-%d{System.Diagnostics.Process.GetCurrentProcess().Id}"
    let stats = Stats(log, monitored.Database.Id, monitored.Id, processorName)
    let observer = new Observers<_>(stats, sink.StartIngester, Seq.collect parseFeedDoc)
    member _.Flush() = (observer: IDisposable).Dispose()
    member _.Start() =
        ChangeFeedProcessor.Start(
            monitored, leases, processorName, stats, leaseOwnerId, observer.Ingest,
            ?notifyError = notifyError, ?customize = customize, ?maxItems = maxItems,
            feedPollDelay = defaultArg tailSleepInterval (TimeSpan.seconds 1.),
            leaseAcquireInterval = defaultArg leaseAcquireInterval (TimeSpan.seconds 5),
            leaseRenewInterval = defaultArg leaseRenewInterval (TimeSpan.seconds 5),
            leaseTtl = defaultArg leaseTtl (TimeSpan.seconds 10),
            startFromTail = defaultArg startFromTail false,
            ?lagReportFrequency = lagReportFreq)
