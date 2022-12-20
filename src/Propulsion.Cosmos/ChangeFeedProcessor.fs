namespace Propulsion.Cosmos

open Microsoft.Azure.Documents
open Microsoft.Azure.Documents.Client
open Microsoft.Azure.Documents.ChangeFeedProcessor
open Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessing
open Propulsion.Internal
open Propulsion.Infrastructure // AwaitTaskCorrect
open Serilog
open System
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks

type ContainerId = { database : string; container : string }

type ChangeFeedObserverContext = { source : ContainerId; leasePrefix : string }

/// Provides F#-friendly wrapping to compose a ChangeFeedObserver from functions
type ChangeFeedObserver =
    static member Create
      ( /// Base logger context; will be decorated with a `partition` property when passed to `assign`, `init` and `ingest`
        log : ILogger,
        /// Callback responsible for
        /// - handling ingestion of a batch of documents (potentially offloading work to another control path)
        /// - ceding control as soon as commencement of the next batch retrieval is desired
        /// - triggering marking of progress via an invocation of `ctx.Checkpoint()` (can be immediate, but can also be deferred and performed asynchronously)
        /// NB emitting an exception will not trigger a retry, and no progress writing will take place without explicit calls to `ctx.CheckpointAsync`
        ingest : ILogger -> IChangeFeedObserverContext * IReadOnlyList<Document> * CancellationToken -> Task<unit>,
        /// Called when this Observer is being created (triggered before `assign`)
        ?init : ILogger -> int -> unit,
        /// Called when a lease is won and the observer is being spun up (0 or more `ingest` calls will follow). Overriding inhibits default logging.
        ?assign : ILogger -> int -> Async<unit>,
        /// Called when a lease is revoked [and the observer is about to be `Dispose`d], overriding inhibits default logging.
        ?revoke : ILogger -> Async<unit>,
        /// Called when this Observer is being destroyed due to the revocation of a lease (triggered after `revoke`)
        ?dispose : unit -> unit) =
        let mutable log = log
        let _open (ctx : IChangeFeedObserverContext) = async {
            log <- log.ForContext("partition", ctx.PartitionKeyRangeId)
            let rangeId = int ctx.PartitionKeyRangeId
            match init with
            | Some f -> f log rangeId
            | None ->  ()
            match assign with
            | Some f -> return! f log rangeId
            | None -> log.Information("Reader {partition} Assigned", ctx.PartitionKeyRangeId) }
        let _process (ctx : IChangeFeedObserverContext, docs, ct) = task {
            let logExn (e : exn) = log.Error(e, "Reader {partition} Handler Threw", ctx.PartitionKeyRangeId)
            try do! ingest log (ctx, docs, ct)
            with Exception.Log logExn () -> () }
        let _close (ctx : IChangeFeedObserverContext, reason) = async {
            log.Warning "Closing" // Added to enable diagnosing underlying CFP issues; will be removed eventually
            match revoke with
            | Some f -> return! f log
            | None -> log.Information("Reader {partition} Revoked {reason}", ctx.PartitionKeyRangeId, reason) }
        { new IChangeFeedObserver with
            member _.OpenAsync ctx = Async.StartAsTask(_open ctx) :> _
            member _.ProcessChangesAsync(ctx, docs, ct) = _process(ctx, docs, ct) :> _
            member _.CloseAsync (ctx, reason) = Async.StartAsTask(_close (ctx, reason)) :> _
          interface IDisposable with
            member _.Dispose() =
                log.Warning "Disposing" // Added to enable diagnosing correct Disposal; will be removed eventually
                match dispose with
                | Some f -> f ()
                | None -> () }

type ChangeFeedObserverFactory =
    static member FromFunction (f : unit -> #IChangeFeedObserver) =
        { new IChangeFeedObserverFactory with member _.CreateObserver () = f () :> _ }

//// Wraps the [Azure CosmosDb ChangeFeedProcessor library](https://github.com/Azure/azure-documentdb-changefeedprocessor-dotnet)
type ChangeFeedProcessor =
    static member Start
        (   log : ILogger, client : DocumentClient, source : ContainerId,
            /// The aux, non-partitioned container holding the partition leases.
            // Aux container should always read from the write region to keep the number of write conflicts to a minimum when the sdk
            // updates the leases. Since the non-write region(s) might lag behind due to us using non-strong consistency, during
            // failover we are likely to reprocess some messages, but that's okay since processing has to be idempotent in any case
            aux : ContainerId,
            /// Identifier to disambiguate multiple independent feed processor positions (akin to a 'consumer group')
            leasePrefix : string,
            createObserver : ChangeFeedObserverContext -> IChangeFeedObserver,
            ?leaseOwnerId : string,
            /// Used to specify an endpoint/account key for the aux container, where that varies from that of the source container. Default: use `client`
            ?auxClient : DocumentClient,
            /// (NB Only applies if this is the first time this leasePrefix is presented)
            /// Specify `true` to request starting of projection from the present write position.
            /// Default: false (projecting all events from start beforehand)
            ?startFromTail : bool,
            /// Frequency to check for partitions without a processor. Default 1s
            ?leaseAcquireInterval : TimeSpan,
            /// Frequency to renew leases held by processors under our control. Default 3s
            ?leaseRenewInterval : TimeSpan,
            /// Duration to take lease when acquired/renewed. Default 10s
            ?leaseTtl : TimeSpan,
            /// Delay before re-polling a partition after backlog has been drained
            ?feedPollDelay : TimeSpan,
            /// Limit on items to take in a batch when querying for changes (in addition to 4MB response size limit). Default Unlimited
            ?maxDocuments : int,
            /// Continuously fed per-partition lag information until parent Async completes
            /// callback should Async.Sleep until next update is desired
            ?reportLagAndAwaitNextEstimation) = async {

        let leaseOwnerId = defaultArg leaseOwnerId (ChangeFeedProcessor.mkLeaseOwnerIdForProcess())
        let feedPollDelay = defaultArg feedPollDelay (TimeSpan.FromSeconds 1.)
        let leaseAcquireInterval = defaultArg leaseAcquireInterval (TimeSpan.FromSeconds 1.)
        let leaseRenewInterval = defaultArg leaseRenewInterval (TimeSpan.FromSeconds 3.)
        let leaseTtl = defaultArg leaseTtl (TimeSpan.FromSeconds 10.)

        log.Information("ChangeFeed Lease acquire {leaseAcquireIntervalS:n0}s ttl {ttlS:n0}s renew {renewS:n0}s feedPollDelay {feedPollDelayS:n0}s",
                        leaseAcquireInterval.TotalSeconds, leaseTtl.TotalSeconds, leaseRenewInterval.TotalSeconds, feedPollDelay.TotalSeconds)

        let builder =
            let feedProcessorOptions =
                ChangeFeedProcessorOptions(
                    StartFromBeginning=not (defaultArg startFromTail false),
                    LeaseAcquireInterval=leaseAcquireInterval, LeaseExpirationInterval=leaseTtl, LeaseRenewInterval=leaseRenewInterval,
                    FeedPollDelay=feedPollDelay,
                    LeasePrefix=leasePrefix + ":")
            // As of CFP 2.2.5, the default behavior does not afford any useful characteristics when the processing is erroring:-
            // a) progress gets written regardless of whether the handler completes with an Exception or not
            // b) no retries happen while the processing is online
            // ... as a result the checkpointing logic is turned off.
            // NB for lag reporting to work correctly, it is of course still important that the writing take place, and that it be written via the CFP lib
            feedProcessorOptions.CheckpointFrequency.ExplicitCheckpoint <- true
            // Max Items is not emphasized as a control mechanism as it can only be used meaningfully when events are highly regular in size
            maxDocuments |> Option.iter (fun mi -> feedProcessorOptions.MaxItemCount <- mi)
            let mkD cid (dc : DocumentClient) =
                DocumentCollectionInfo(Uri=dc.ServiceEndpoint,ConnectionPolicy=dc.ConnectionPolicy,DatabaseName=cid.database,CollectionName=cid.container)
            ChangeFeedProcessorBuilder()
                .WithHostName(leaseOwnerId)
                .WithFeedCollection(mkD source client)
                .WithLeaseCollection(mkD aux (defaultArg auxClient client))
                .WithFeedDocumentClient(client)
                .WithLeaseDocumentClient(defaultArg auxClient client)
                .WithProcessorOptions(feedProcessorOptions)
        match reportLagAndAwaitNextEstimation with
        | None -> ()
        | Some (lagMonitorCallback : _ -> Task<unit>) ->
            let! estimator = builder.BuildEstimatorAsync() |> Async.AwaitTaskCorrect
            let! ct = Async.CancellationToken
            let emitLagMetrics () = task {
                while not ct.IsCancellationRequested do
                    let! remainingWork = estimator.GetEstimatedRemainingWorkPerPartitionAsync()
                    do! lagMonitorCallback <| List.ofSeq (seq { for r in remainingWork -> int (r.PartitionKeyRangeId.Trim[|'"'|]),r.RemainingWork } |> Seq.sortBy fst) }
            Task.start emitLagMetrics
        let context : ChangeFeedObserverContext = { source = source; leasePrefix = leasePrefix }
        let! processor = builder.WithObserverFactory(ChangeFeedObserverFactory.FromFunction (fun () -> createObserver context)).BuildAsync() |> Async.AwaitTaskCorrect
        do! processor.StartAsync() |> Async.AwaitTaskCorrect
        return processor }
    static member private mkLeaseOwnerIdForProcess() =
        // If k>1 processes share an owner id, then they will compete for same partitions.
        // In that scenario, redundant processing happen on assigned partitions, but checkpoint will process on only 1 consumer.
        // Including the processId should eliminate the possibility that a broken process manager causes k>1 scenario to happen.
        // The only downside is that upon redeploy, lease expiration / TTL would have to be observed before a consumer can pick it up.
        let processName = System.Reflection.Assembly.GetEntryAssembly().GetName().Name
        let processId = System.Diagnostics.Process.GetCurrentProcess().Id
        let hostName = Environment.MachineName
        sprintf "%s-%s-%d" hostName processName processId
