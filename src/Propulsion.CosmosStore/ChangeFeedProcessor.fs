namespace Propulsion.CosmosStore

open Microsoft.Azure.Cosmos
open Propulsion.Internal
open System
type IReadOnlyCollection<'T> = System.Collections.Generic.IReadOnlyCollection<'T>

[<NoComparison>]
type ChangeFeedObserverContext = { source: Container; group: string; epoch: int64; timestamp: DateTime; rangeId: int; requestCharge: float }

type IChangeFeedObserver =
    inherit IDisposable

    /// Callback responsible for
    /// - handling ingestion of a batch of documents (potentially offloading work to another control path)
    /// - ceding control as soon as commencement of the next batch retrieval is desired
    /// - triggering marking of progress via an invocation of `ctx.Checkpoint()` (can be immediate, but can also be deferred and performed asynchronously)
    /// NB emitting an exception will not trigger a retry, and no progress writing will take place without explicit calls to `ctx.Checkpoint`
#if COSMOSV3
    abstract member Ingest: context: ChangeFeedObserverContext * tryCheckpointAsync: (CancellationToken -> Task<unit>) * docs: IReadOnlyCollection<Newtonsoft.Json.Linq.JObject> * CancellationToken -> Task<unit>
#else
    abstract member Ingest: context: ChangeFeedObserverContext * tryCheckpointAsync: (CancellationToken -> Task<unit>) * docs: IReadOnlyCollection<System.Text.Json.JsonDocument> * CancellationToken -> Task<unit>
#endif

type internal SourcePipeline =

    static member Start(log: Serilog.ILogger, start, maybeStartChild, stop) =
        let machine, triggerStop = Propulsion.PipelineFactory.PrepareSource2(log, start, maybeStartChild, stop)
        new Propulsion.Pipeline(Task.run machine, triggerStop)

module Log =

    type [<Struct>] MetricContext = { database: string; container: string; group: string }
    type ReadMetric =       { context: MetricContext; rangeId: int; token: int64; latency: TimeSpan; rc: float; age: TimeSpan; docs: int }
    type IngestMetric =     { context: MetricContext; rangeId: int; ingestLatency: TimeSpan; ingestQueued: int }
    type LagMetric =        { context: MetricContext; rangeLags: struct (int * int64)[] }
    [<RequireQualifiedAccess; NoEquality; NoComparison>]
    type Metric =
        | Read of ReadMetric
        | Wait of IngestMetric
        | Lag of LagMetric

    let [<Literal>] PropertyTag = "propulsionCosmosEvent"
    /// Attach a property to the captured event record to hold the metric information
    let internal withMetric (value: Metric) = Log.withScalarProperty PropertyTag value
    let [<return: Struct>] (|MetricEvent|_|) (logEvent: Serilog.Events.LogEvent): Metric voption =
        let mutable p = Unchecked.defaultof<_>
        logEvent.Properties.TryGetValue(PropertyTag, &p) |> ignore
        match p with Log.ScalarValue (:? Metric as e) -> ValueSome e | _ -> ValueNone

    type Stats(log: Serilog.ILogger, databaseId, containerId, processorName: string, ?lagReportFreq: TimeSpan) =
        let context = { database = databaseId; container = containerId; group = processorName }
        let metricsLog = log.ForContext("isMetric", true)
        member val Log = log
        member _.LogStateChange(rangeId: int, state: string) =
            log.Information("Reader {partition} {state}", rangeId, state)
        member _.LogExn(rangeId: int, ex: exn) =
            log.Error(ex, "Reader {partition} error", rangeId)
        member _.LogHandlerExn(rangeId: int, ex: exn) =
            log.Error(ex, "Reader {processorName}/{partition} Handler Threw", processorName, rangeId)
        member _.LogStart(leaseAcquireInterval: TimeSpan, leaseTtl: TimeSpan, leaseRenewInterval: TimeSpan, feedPollDelay: TimeSpan, startFromTail: bool, ?maxItems) =
            log.Information("ChangeFeed {processorName} Lease acquire {leaseAcquireIntervalS:n0}s ttl {ttlS:n0}s renew {renewS:n0}s feedPollDelay {feedPollDelayS:n0}s Items limit {maxItems} fromTail {fromTail}",
                            processorName, leaseAcquireInterval.TotalSeconds, leaseTtl.TotalSeconds, leaseRenewInterval.TotalSeconds, feedPollDelay.TotalSeconds, Option.toNullable maxItems, startFromTail)

        member _.ReportRead(rangeId: int, lastWait: TimeSpan, epoch, requestCharge, batchTimestamp, latency, itemCount, batchesInFlight, maxReadAhead) =
            let age = DateTime.UtcNow - batchTimestamp
            let m = Metric.Read { context = context; rangeId = rangeId; token = epoch; latency = latency; rc = requestCharge; age = age; docs = itemCount }
            (log |> withMetric m).Information("Reader {partition} {token,9} age {age:dddd\.hh\:mm\:ss} {count,4} docs {requestCharge,6:f1}RU {l,5:f1}s Wait {pausedS:f3}s Ahead {cur}/{max}",
                                              rangeId, epoch, age, itemCount, requestCharge, latency, batchesInFlight, maxReadAhead, lastWait.TotalSeconds, batchesInFlight, maxReadAhead)
        member _.ReportWait(rangeId: int, waitElapsed, batchesInFlight, maxReadAhead) =
            if metricsLog.IsEnabled LogEventLevel.Information then
                let m = Metric.Wait { context = context; rangeId = rangeId; ingestLatency = waitElapsed; ingestQueued = batchesInFlight }
                // NOTE: Write to metrics log (App wiring has logic to also emit it to Console when in verboseStore mode, but main purpose is to feed to Prometheus ASAP)
                (metricsLog |> withMetric m).Information("Reader {partition} Wait {pausedS:f3}s Ahead {cur}/{max}", rangeId, waitElapsed.TotalSeconds, batchesInFlight, maxReadAhead)

        member _.ReportEstimation(remainingWork: struct (int * int64)[]) =
            let mutable synced, lagged, count, total = ResizeArray(), ResizeArray(), 0, 0L
            for partitionId, gap as partitionAndGap in remainingWork do
                total <- total + gap
                count <- count + 1
                if gap = 0L then synced.Add partitionId else lagged.Add partitionAndGap
            let m = Metric.Lag { context = context; rangeLags = remainingWork }
            (log |> withMetric m).Information("ChangeFeed {processorName} Lag Partitions {partitions} Gap {gapDocs:n0} docs {@laggingPartitions} Synced {@syncedPartitions}",
                processorName, count, total, lagged, synced)

        member x.EstimationReportLoop() =
            match lagReportFreq with
            | None -> None
            | Some interval ->
                log.Information("ChangeFeed {processorName} Lag stats interval {lagReportIntervalS:n0}s", processorName, interval.TotalSeconds)
                let logAndWait (remainingWork: struct (int * int64)[]) ct = task {
                    x.ReportEstimation(remainingWork)
                    return! Task.Delay(TimeSpan.toMs interval, ct) }
                Some logAndWait

//// Wraps the V3 ChangeFeedProcessor and [`ChangeFeedProcessorEstimator`](https://docs.microsoft.com/en-us/azure/cosmos-db/how-to-use-change-feed-estimator)
type ChangeFeedProcessor =

    static member Start
        (   monitored: Container,
            // The non-partitioned (i.e., PartitionKey is "id") Container holding the partition leases.
            // Should always read from the write region to keep the number of write conflicts to a minimum when the sdk
            // updates the leases. Since the non-write region(s) might lag behind due to us using non-strong consistency, during
            // failover we are likely to reprocess some messages, but that's okay since processing has to be idempotent in any case
            leases: Container,
            // Identifier to disambiguate multiple independent feed processor positions (akin to a 'consumer group')
            processorName: string,
            // Observers to forward documents to (Disposal is tied to stopping of the Source)
            observer: IChangeFeedObserver,
            stats: Log.Stats,
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
                let aux (context: ChangeFeedProcessorContext)
#if COSMOSV3
                        (changes: IReadOnlyCollection<Newtonsoft.Json.Linq.JObject>)
#else
                        (changes: IReadOnlyCollection<System.Text.Json.JsonDocument>)
#endif
                        (checkpointAsync: CancellationToken -> Task<unit>) ct = task {
                    let log: exn -> unit = function
                        | :? OperationCanceledException -> () // Shutdown via .Stop triggers this
                        | e -> stats.LogHandlerExn(leaseTokenToPartitionId context.LeaseToken, e)
                    try let ctx = { source = monitored; group = processorName
                                    epoch = context.Headers.ContinuationToken.Trim[|'"'|] |> int64
#if COSMOSV3
                                    timestamp = changes |> Seq.last |> EquinoxNewtonsoftParser.timestamp
#else
                                    timestamp = changes |> Seq.last |> EquinoxSystemTextJsonParser.timestamp
#endif
                                    rangeId = leaseTokenToPartitionId context.LeaseToken
                                    requestCharge = context.Headers.RequestCharge }
                        return! observer.Ingest(ctx, checkpointAsync, changes, ct)
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
                let fetchEstimatorStates (map: ChangeFeedProcessorState -> 'u) ct: Task<'u[]>  = task {
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
