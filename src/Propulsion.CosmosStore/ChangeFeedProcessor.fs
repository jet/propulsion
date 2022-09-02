namespace Propulsion.CosmosStore

open FSharp.Control
open Microsoft.Azure.Cosmos
open Propulsion.Infrastructure // AwaitTaskCorrect
open Serilog
open System
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks

[<NoComparison>]
type ChangeFeedObserverContext = { source : Container; group : string; epoch : int64; timestamp : DateTime; rangeId : int; requestCharge : float }

type IChangeFeedObserver =
    inherit IDisposable

    /// Callback responsible for
    /// - handling ingestion of a batch of documents (potentially offloading work to another control path)
    /// - ceding control as soon as commencement of the next batch retrieval is desired
    /// - triggering marking of progress via an invocation of `ctx.Checkpoint()` (can be immediate, but can also be deferred and performed asynchronously)
    /// NB emitting an exception will not trigger a retry, and no progress writing will take place without explicit calls to `ctx.Checkpoint`
#if COSMOSV2 || COSMOSV3
    abstract member Ingest: context : ChangeFeedObserverContext * tryCheckpointAsync : Async<unit> * docs : IReadOnlyCollection<Newtonsoft.Json.Linq.JObject> -> Async<unit>
#else
    abstract member Ingest: context : ChangeFeedObserverContext * tryCheckpointAsync : Async<unit> * docs : IReadOnlyCollection<System.Text.Json.JsonDocument> -> Task<unit>
#endif

type internal SourcePipeline =

    static member Start(log : ILogger, start, maybeStartChild, stop, observer : IDisposable) =
        let cts = new CancellationTokenSource()
        let triggerStop () =
            let level = if cts.IsCancellationRequested then Events.LogEventLevel.Debug else Events.LogEventLevel.Information
            log.Write(level, "Source stopping...")
            observer.Dispose()
            cts.Cancel()
        let ct = cts.Token

        let tcs = TaskCompletionSource<unit>()

        let machine = async {
            do! start ()
            // external cancellation should yield a success result
            use _ = ct.Register(fun _ -> tcs.TrySetResult () |> ignore)

            match maybeStartChild with
            | None -> ()
            | Some child -> let! _ = Async.StartChild child in ()

            do! Async.AwaitTaskCorrect tcs.Task // aka base.AwaitShutdown()
            do! stop ()
            log.Information("... source stopped") }

        let task = Async.StartAsTask machine

        new Propulsion.Pipeline(task, triggerStop)

//// Wraps the V3 ChangeFeedProcessor and [`ChangeFeedProcessorEstimator`](https://docs.microsoft.com/en-us/azure/cosmos-db/how-to-use-change-feed-estimator)
type ChangeFeedProcessor =

    static member Start
        (   log : ILogger, monitored : Container,
            // The non-partitioned (i.e., PartitionKey is "id") Container holding the partition leases.
            // Should always read from the write region to keep the number of write conflicts to a minimum when the sdk
            // updates the leases. Since the non-write region(s) might lag behind due to us using non-strong consistency, during
            // failover we are likely to reprocess some messages, but that's okay since processing has to be idempotent in any case
            leases : Container,
            // Identifier to disambiguate multiple independent feed processor positions (akin to a 'consumer group')
            processorName : string,
            // Observers to forward documents to (Disposal is tied to stopping of the Source)
            observer : IChangeFeedObserver,
            ?leaseOwnerId : string,
            // (NB Only applies if this is the first time this leasePrefix is presented)
            // Specify `true` to request starting of projection from the present write position.
            // Default: false (projecting all events from start beforehand)
            ?startFromTail : bool,
            // Frequency to check for partitions without a processor. Default 1s
            ?leaseAcquireInterval : TimeSpan,
            // Frequency to renew leases held by processors under our control. Default 3s
            ?leaseRenewInterval : TimeSpan,
            // Duration to take lease when acquired/renewed. Default 10s
            ?leaseTtl : TimeSpan,
            // Delay before re-polling a partition after backlog has been drained
            ?feedPollDelay : TimeSpan,
            // Limit on items to take in a batch when querying for changes (in addition to 4MB response size limit). Default Unlimited.
            // Max Items is not emphasized as a control mechanism as it can only be used meaningfully when events are highly regular in size.
            ?maxItems : int,
            // Continuously fed per-partition lag information until parent Async completes
            // callback should Async.Sleep until next update is desired
            ?reportLagAndAwaitNextEstimation,
            // Enables reporting or other processing of Exception conditions as per <c>WithErrorNotification</c>
            ?notifyError : int -> exn -> unit,
            // Admits customizations in the ChangeFeedProcessorBuilder chain
            ?customize) =
        let leaseOwnerId = defaultArg leaseOwnerId (ChangeFeedProcessor.mkLeaseOwnerIdForProcess())
        let feedPollDelay = defaultArg feedPollDelay (TimeSpan.FromSeconds 1.)
        let leaseAcquireInterval = defaultArg leaseAcquireInterval (TimeSpan.FromSeconds 1.)
        let leaseRenewInterval = defaultArg leaseRenewInterval (TimeSpan.FromSeconds 3.)
        let leaseTtl = defaultArg leaseTtl (TimeSpan.FromSeconds 10.)

        log.Information("ChangeFeed {processorName} Lease acquire {leaseAcquireIntervalS:n0}s ttl {ttlS:n0}s renew {renewS:n0}s feedPollDelay {feedPollDelayS:n0}s",
                        processorName, leaseAcquireInterval.TotalSeconds, leaseTtl.TotalSeconds, leaseRenewInterval.TotalSeconds, feedPollDelay.TotalSeconds)
        let processorName_ = processorName + ":"
        let leaseTokenToPartitionId (leaseToken : string) = int (leaseToken.Trim[|'"'|])
        let processor =
            let handler =
                let aux (context : ChangeFeedProcessorContext)
#if COSMOSV2 || COSMOSV3
                        (changes : IReadOnlyCollection<Newtonsoft.Json.Linq.JObject>)
#else
                        (changes : IReadOnlyCollection<System.Text.Json.JsonDocument>)
#endif
                        (checkpointAsync : Func<Task>) = async {
                    let checkpoint = async { return! checkpointAsync.Invoke() |> Async.AwaitTaskCorrect }
                    try let ctx = { source = monitored; group = processorName
                                    epoch = context.Headers.ContinuationToken.Trim[|'"'|] |> int64
#if COSMOSV2 || COSMOSV3
                                    timestamp = changes |> Seq.last |> EquinoxNewtonsoftParser.timestamp
#else
                                    timestamp = changes |> Seq.last |> EquinoxSystemTextJsonParser.timestamp
#endif
                                    rangeId = leaseTokenToPartitionId context.LeaseToken
                                    requestCharge = context.Headers.RequestCharge }
#if COSMOSV2 || COSMOSV3
                        return! observer.Ingest(ctx, checkpoint, changes)
#else
                        return! observer.Ingest(ctx, checkpoint, changes) |> Async.AwaitTask
#endif
                    with e ->
                        log.Error(e, "Reader {processorName}/{partitionId} Handler Threw", processorName, context.LeaseToken)
                        do! Async.Raise e }
                fun ctx chg chk ct -> Async.StartAsTask(aux ctx chg chk, cancellationToken = ct) :> Task
            let acquireAsync leaseToken = log.Information("Reader {partitionId} Assigned", leaseTokenToPartitionId leaseToken); Task.CompletedTask
            let releaseAsync leaseToken = log.Information("Reader {partitionId} Revoked", leaseTokenToPartitionId leaseToken); Task.CompletedTask
            let notifyError =
                notifyError
                |> Option.defaultValue (fun i ex -> log.Error(ex, "Reader {partitionId} error", i))
                |> fun f -> fun leaseToken ex -> f (leaseTokenToPartitionId leaseToken) ex; Task.CompletedTask
            monitored
                .GetChangeFeedProcessorBuilderWithManualCheckpoint(processorName_, Container.ChangeFeedHandlerWithManualCheckpoint handler)
                .WithLeaseContainer(leases)
                .WithPollInterval(feedPollDelay)
                .WithLeaseConfiguration(acquireInterval = leaseAcquireInterval, expirationInterval = leaseTtl, renewInterval = leaseRenewInterval)
                .WithInstanceName(leaseOwnerId)
                .WithLeaseAcquireNotification(Container.ChangeFeedMonitorLeaseAcquireDelegate acquireAsync)
                .WithLeaseReleaseNotification(Container.ChangeFeedMonitorLeaseReleaseDelegate releaseAsync)
                .WithErrorNotification(Container.ChangeFeedMonitorErrorDelegate notifyError)
                |> fun b -> if startFromTail = Some true then b else let minTime = DateTime.MinValue in b.WithStartTime(minTime.ToUniversalTime()) // fka StartFromBeginning
                |> fun b -> match maxItems with Some mi -> b.WithMaxItems(mi) | None -> b
                |> fun b -> match customize with Some c -> c b | None -> b
                |> fun b -> b.Build()
        let maybePumpMetrics =
            reportLagAndAwaitNextEstimation
            |> Option.map (fun lagMonitorCallback ->
                let estimator = monitored.GetChangeFeedEstimator(processorName_, leases)
                let rec emitLagMetrics () = async {
                    let feedIteratorMap (map : 't -> 'u) (query : FeedIterator<'t>) : AsyncSeq<'u> =
                        let rec loop () : AsyncSeq<'u> = asyncSeq {
                            if not query.HasMoreResults then () else
                            let! ct = Async.CancellationToken
                            let! (res : FeedResponse<'t>) = query.ReadNextAsync(ct) |> Async.AwaitTaskCorrect
                            for x in res do yield map x
                            if query.HasMoreResults then
                                yield! loop () }
                        // earlier versions, such as 3.9.0, do not implement IDisposable; see linked issue for detail on when SDK team added it
                        use __ = query // see https://github.com/jet/equinox/issues/225 - in the Cosmos V4 SDK, all this is managed IAsyncEnumerable
                        loop ()
                    let! leasesState =
                        estimator.GetCurrentStateIterator()
                        |> feedIteratorMap (fun s -> leaseTokenToPartitionId s.LeaseToken, s.EstimatedLag)
                        |> AsyncSeq.toArrayAsync
                    do! lagMonitorCallback (Seq.sortBy fst leasesState |> List.ofSeq)
                    return! emitLagMetrics () }
                emitLagMetrics ())
        let wrap (f : unit -> Task) () = f () |> Async.AwaitTaskCorrect
        SourcePipeline.Start(log, wrap processor.StartAsync, maybePumpMetrics, wrap processor.StopAsync, observer)
    static member private mkLeaseOwnerIdForProcess() =
        // If k>1 processes share an owner id, then they will compete for same partitions.
        // In that scenario, redundant processing happen on assigned partitions, but checkpoint will process on only 1 consumer.
        // Including the processId should eliminate the possibility that a broken process manager causes k>1 scenario to happen.
        // The only downside is that upon redeploy, lease expiration / TTL would have to be observed before a consumer can pick it up.
        let processName = System.Reflection.Assembly.GetEntryAssembly().GetName().Name
        let processId = System.Diagnostics.Process.GetCurrentProcess().Id
        let hostName = Environment.MachineName
        sprintf "%s-%s-%d" hostName processName processId
