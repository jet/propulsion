namespace Propulsion.CosmosStore

open Propulsion.Internal
open Serilog
open System

type internal Observers<'Items>(stats, startIngester: ILogger * int -> Propulsion.Ingestion.Ingester<'Items>, mapContent: ChangeFeedItems -> 'Items) =

    // Its important we don't risk >1 instance https://andrewlock.net/making-getoradd-on-concurrentdictionary-thread-safe-using-lazy/
    // while it would be safe, there would be a risk of incurring the cost of multiple initialization loops
    let forTranche = System.Collections.Concurrent.ConcurrentDictionary<int, Lazy<Observer<_>>>()
    let build trancheId = lazy (new Observer<'Items>(stats, startIngester (stats.Log, trancheId), mapContent))
    let getOrAddForTranche trancheId = forTranche.GetOrAdd(trancheId, build).Value

    member _.Ingest(context, docs, checkpoint, ct) =
        let trancheObserver = getOrAddForTranche context.rangeId
        trancheObserver.Ingest(context, docs, checkpoint, ct)
    interface IDisposable with
        member _.Dispose() =
            for x in forTranche.Values do
                (x.Value : IDisposable).Dispose()

//// Wraps the V3 ChangeFeedProcessor and [`ChangeFeedProcessorEstimator`](https://docs.microsoft.com/en-us/azure/cosmos-db/how-to-use-change-feed-estimator)
type internal ChangeFeedProcessor =

    static member Start
        (   monitored: Microsoft.Azure.Cosmos.Container,
            // The non-partitioned (i.e., PartitionKey is "id") Container holding the partition leases.
            // Should always read from the write region to keep the number of write conflicts to a minimum when the sdk
            // updates the leases. Since the non-write region(s) might lag behind due to us using non-strong consistency, during
            // fail over we are likely to reprocess some messages, but that's okay since processing has to be idempotent in any case
            leases: Microsoft.Azure.Cosmos.Container,
            // Identifier to disambiguate multiple independent feed processor positions (akin to a 'consumer group')
            processorName: string,
            // Callback responsible for
            // - handling ingestion of a batch of documents (potentially offloading work to another control path)
            // - ceding control as soon as commencement of the next batch retrieval is desired
            // - triggering marking of progress via an invocation of `ctx.Checkpoint()` (can be immediate, but can also be deferred and performed asynchronously)
            // NB emitting an exception will not trigger a retry, and no progress writing will take place without explicit calls to `ctx.Checkpoint`
            ingest,
            stats: Stats,
            ?leaseOwnerId: string,
            // (NB Only applies if this is the first time this leasePrefix is presented)
            // Specify `true` to request starting of projection from the present write position.
            // Default: false (projecting all events from start beforehand)
            ?startFromTail: bool,
            // Frequency to check for partitions without a processor. Default 1s
            ?leaseAcquireInterval: TimeSpan,
            // Frequency to renew leases held by processors under our control. Default 3s
            ?leaseRenewInterval: TimeSpan,
            // Duration to take lease when acquired/renewed. Default 10s
            ?leaseTtl: TimeSpan,
            // Delay before re-polling a partition after backlog has been drained
            ?feedPollDelay: TimeSpan,
            // Limit on items to take in a batch when querying for changes (in addition to 4MB response size limit). Default Unlimited.
            // Max Items is not emphasized as a control mechanism as it can only be used meaningfully when events are highly regular in size.
            ?maxItems: int,
            // Enables reporting or other processing of Exception conditions as per <c>WithErrorNotification</c>
            ?notifyError: Action<int, exn>,
            // Admits customizations in the ChangeFeedProcessorBuilder chain
            ?customize) =
        let leaseOwnerId = leaseOwnerId |> Option.defaultWith ChangeFeedProcessor.mkLeaseOwnerIdForProcess
        let feedPollDelay = defaultArg feedPollDelay (TimeSpan.FromSeconds 1.)
        let leaseAcquireInterval = defaultArg leaseAcquireInterval (TimeSpan.FromSeconds 1.)
        let leaseRenewInterval = defaultArg leaseRenewInterval (TimeSpan.FromSeconds 3.)
        let leaseTtl = defaultArg leaseTtl (TimeSpan.FromSeconds 10.)
        let startFromTail = defaultArg startFromTail false

        stats.LogStart(leaseAcquireInterval, leaseTtl, leaseRenewInterval, feedPollDelay, startFromTail, ?maxItems = maxItems)
        let processorName_ = processorName + ":"
        let leaseTokenToPartitionId (leaseToken: string) = int (leaseToken.Trim[|'"'|])
        let processor =
            let handler =
                let aux (context: Microsoft.Azure.Cosmos.ChangeFeedProcessorContext) (changes: ChangeFeedItems) (checkpointAsync: CancellationToken -> Task<unit>) ct = task {
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
                fun ctx chg (chk: Func<Task>) ct ->
                    let chk' _ct = task { do! chk.Invoke() }
                    aux ctx chg chk' ct :> Task
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
            stats.EstimationReportLoop()
            |> Option.map (fun lagMonitorCallback ->
                let estimator = monitored.GetChangeFeedEstimator(processorName_, leases)
                let fetchEstimatorStates (map: Microsoft.Azure.Cosmos.ChangeFeedProcessorState -> 'u) ct: Task<'u[]>  = task {
                    use query = estimator.GetCurrentStateIterator()
                    let result = ResizeArray()
                    while query.HasMoreResults do
                        let! res = query.ReadNextAsync(ct)
                        for x in res do result.Add(map x)
                    return result.ToArray() }
                fun (ct: CancellationToken) -> task {
                    while not ct.IsCancellationRequested do
                        let! leasesStates = fetchEstimatorStates (fun s -> struct (leaseTokenToPartitionId s.LeaseToken, s.EstimatedLag)) ct
                        Array.sortInPlaceBy ValueTuple.fst leasesStates
                        do! lagMonitorCallback leasesStates ct } )
        SourcePipeline.Start(stats.Log, Task.ofUnitTask << processor.StartAsync, maybePumpMetrics, Task.ofUnitTask << processor.StopAsync)
    static member private mkLeaseOwnerIdForProcess() =
        // If k>1 processes share an owner id, then they will compete for same partitions.
        // In that scenario, redundant processing happen on assigned partitions, but checkpoint will process on only 1 consumer.
        // Including the processId should eliminate the possibility that a broken process manager causes k>1 scenario to happen.
        // The only downside is that upon redeploy, lease expiration / TTL would have to be observed before a consumer can pick it up.
        $"%s{Environment.MachineName}-%s{System.Reflection.Assembly.GetEntryAssembly().GetName().Name}-%d{System.Diagnostics.Process.GetCurrentProcess().Id}"

type CosmosStoreSource
    (   log: ILogger,
        monitored: Microsoft.Azure.Cosmos.Container, leases: Microsoft.Azure.Cosmos.Container, processorName, parseFeedDoc, sink: Propulsion.Sinks.Sink,
        [<O; D null>] ?maxItems,
        [<O; D null>] ?tailSleepInterval,
        [<O; D null>] ?startFromTail,
        [<O; D null>] ?lagReportFreq: TimeSpan,
        [<O; D null>] ?notifyError,
        [<O; D null>] ?customize) =
    let stats = Stats(log, monitored.Database.Id, monitored.Id, processorName, ?lagReportFreq = lagReportFreq)
    let observer = new Observers<_>(stats, sink.StartIngester, Seq.collect parseFeedDoc)
    member _.Flush() = (observer: IDisposable).Dispose()
    member _.Start() =
        ChangeFeedProcessor.Start(
            monitored, leases, processorName, observer.Ingest, stats, ?notifyError = notifyError, ?customize = customize,
            ?maxItems = maxItems, ?feedPollDelay = tailSleepInterval,
            startFromTail = defaultArg startFromTail false,
            leaseAcquireInterval = TimeSpan.seconds 5., leaseRenewInterval = TimeSpan.seconds 5., leaseTtl = TimeSpan.seconds 10.)
