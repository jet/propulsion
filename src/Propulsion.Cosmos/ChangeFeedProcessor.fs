namespace Propulsion.Cosmos

open Microsoft.Azure.Documents
open Microsoft.Azure.Documents.Client
open Microsoft.Azure.Documents.ChangeFeedProcessor
open Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessing
open Serilog
open System
open System.Collections.Generic

[<AutoOpen>]
module IChangeFeedObserverContextExtensions =
    /// Provides F#-friendly wrapping for the `CheckpointAsync` function, which typically makes sense to pass around in `Async` form
    type Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessing.IChangeFeedObserverContext with
        /// Triggers `CheckpointAsync()`; Mark the the full series up to and including this batch as having been confirmed consumed.
        member __.Checkpoint() = async {
            return! __.CheckpointAsync() |> Async.AwaitTaskCorrect }

/// Provides F#-friendly wrapping to compose a ChangeFeedObserver from functions
type ChangeFeedObserver =
    static member Create
      ( /// Base logger context; will be decorated with a `partitionKeyRangeId` property when passed to `assign`, `init` and `processBatch`
        log : ILogger,
        /// Callback responsible for
        /// - handling ingestion of a batch of documents (potentially offloading work to another control path)
        /// - ceding control as soon as commencement of the next batch retrieval is desired
        /// - triggering marking of progress via an invocation of `ctx.Checkpoint()` (can be immediate, but can also be deferred and performed asynchronously)
        /// NB emitting an exception will not trigger a retry, and no progress writing will take place without explicit calls to `ctx.CheckpointAsync`
        ingest : ILogger -> IChangeFeedObserverContext -> IReadOnlyList<Document> -> Async<unit>,
        /// Called when this Observer is being created (triggered before `assign`)
        ?init : ILogger -> int -> unit,
        /// Called when a lease is won and the observer is being spun up (0 or more `processBatch` calls will follow). Overriding inhibits default logging.
        ?assign : ILogger -> int -> Async<unit>,
        /// Called when a lease is revoked [and the observer is about to be `Dispose`d], overriding inhibits default logging.
        ?revoke : ILogger -> Async<unit>,
        /// Called when this Observer is being destroyed due to the revocation of a lease (triggered after `revoke`)
        ?dispose : unit -> unit) =
        let mutable log = log
        let _open (ctx : IChangeFeedObserverContext) = async {
            log <- log.ForContext("partitionKeyRangeId",ctx.PartitionKeyRangeId)
            let rangeId = int ctx.PartitionKeyRangeId
            match init with
            | Some f -> f log rangeId
            | None ->  ()
            match assign with
            | Some f -> return! f log rangeId
            | None -> log.Information("Range {partitionKeyRangeId} Assigned", ctx.PartitionKeyRangeId) }
        let _process (ctx, docs) = async {
            try do! ingest log ctx docs
            with e ->
                log.Error(e, "Range {partitionKeyRangeId} Handler Threw", ctx.PartitionKeyRangeId)
                do! Async.Raise e }
        let _close (ctx : IChangeFeedObserverContext, reason) = async {
            log.Warning "Closing" // Added to enable diagnosing underlying CFP issues; will be removed eventually
            match revoke with
            | Some f -> return! f log
            | None -> log.Information("Range {partitionKeyRangeId} Revoked {reason}", ctx.PartitionKeyRangeId, reason) }
        { new IChangeFeedObserver with
            member __.OpenAsync ctx = Async.StartAsTask(_open ctx) :> _
            member __.ProcessChangesAsync(ctx, docs, ct) = Async.StartAsTask(_process(ctx, docs), cancellationToken=ct) :> _
            member __.CloseAsync (ctx, reason) = Async.StartAsTask(_close (ctx, reason)) :> _
          interface IDisposable with
            member __.Dispose() =
                log.Warning "Disposing" // Added to enable diagnosing correct Disposal; will be removed eventually
                match dispose with
                | Some f -> f ()
                | None -> () }

type ChangeFeedObserverFactory =
    static member FromFunction (f : unit -> #IChangeFeedObserver) =
        { new IChangeFeedObserverFactory with member __.CreateObserver () = f () :> _ }

type ContainerId = { database : string; container : string }

//// Wraps the [Azure CosmosDb ChangeFeedProcessor library](https://github.com/Azure/azure-documentdb-changefeedprocessor-dotnet)
type ChangeFeedProcessor =
    static member Start
        (   log : ILogger, client : DocumentClient, source : ContainerId,
            /// The aux, non-partitioned container holding the partition leases.
            // Aux container should always read from the write region to keep the number of write conflicts to a minimum when the sdk
            // updates the leases. Since the non-write region(s) might lag behind due to us using non-strong consistency, during
            // failover we are likely to reprocess some messages, but that's okay since processing has to be idempotent in any case
            aux : ContainerId,
            createObserver : unit -> IChangeFeedObserver,
            ?leaseOwnerId : string,
            /// Used to specify an endpoint/account key for the aux container, where that varies from that of the source container. Default: use `client`
            ?auxClient : DocumentClient,
            /// Identifier to disambiguate multiple independent feed processor positions (akin to a 'consumer group')
            ?leasePrefix : string,
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

        let inline s (x : TimeSpan) = x.TotalSeconds
        log.Information("Changefeed Lease acquire {leaseAcquireIntervalS:n0}s ttl {ttlS:n0}s renew {renewS:n0}s feedPollDelay {feedPollDelayS:n0}s",
            s leaseAcquireInterval, s leaseTtl, s leaseRenewInterval, s feedPollDelay)

        let builder =
            let feedProcessorOptions =
                ChangeFeedProcessorOptions(
                    StartFromBeginning=not (defaultArg startFromTail false),
                    LeaseAcquireInterval=leaseAcquireInterval, LeaseExpirationInterval=leaseTtl, LeaseRenewInterval=leaseRenewInterval,
                    FeedPollDelay=feedPollDelay)
            // As of CFP 2.2.5, the default behavior does not afford any useful characteristics when the processing is erroring:-
            // a) progress gets written regardless of whether the handler completes with an Exception or not
            // b) no retries happen while the processing is online
            // ... as a result the checkpointing logic is turned off.
            // NB for lag reporting to work correctly, it is of course still important that the writing take place, and that it be written via the CFP lib
            feedProcessorOptions.CheckpointFrequency.ExplicitCheckpoint <- true
            leasePrefix |> Option.iter (fun lp -> feedProcessorOptions.LeasePrefix <- lp + ":")
            // Max Items is not emphasized as a control mechanism as it can only be used meaningfully when events are highly regular in size
            maxDocuments |> Option.iter (fun mi -> feedProcessorOptions.MaxItemCount <- Nullable mi)
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
        | Some lagMonitorCallback ->
            let! estimator = builder.BuildEstimatorAsync() |> Async.AwaitTaskCorrect
            let rec emitLagMetrics () = async {
                let! remainingWork = estimator.GetEstimatedRemainingWorkPerPartitionAsync() |> Async.AwaitTaskCorrect
                do! lagMonitorCallback <| List.ofSeq (seq { for r in remainingWork -> int (r.PartitionKeyRangeId.Trim[|'"'|]),r.RemainingWork } |> Seq.sortBy fst)
                return! emitLagMetrics () }
            let! _ = Async.StartChild(emitLagMetrics ()) in ()
        let! processor = builder.WithObserverFactory(ChangeFeedObserverFactory.FromFunction createObserver).BuildAsync() |> Async.AwaitTaskCorrect
        do! processor.StartAsync() |> Async.AwaitTaskCorrect
        return processor }
    static member private mkLeaseOwnerIdForProcess() =
        // If k>1 processes share an owner id, then they will compete for same partitions.
        // In that scenario, redundant processing happen on assigned partitions, but checkpoint will process on only 1 consumer.
        // Including the processId should eliminate the possibility that a broken process manager causes k>1 scenario to happen.
        // The only downside is that upon redeploy, lease expiration / TTL would have to be observed before a consumer can pick it up.
        let processName = System.Reflection.Assembly.GetEntryAssembly().GetName().Name
        let processId = System.Diagnostics.Process.GetCurrentProcess().Id
        let hostName = System.Environment.MachineName
        sprintf "%s-%s-%d" hostName processName processId
