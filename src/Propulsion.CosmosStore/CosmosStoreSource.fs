﻿namespace Propulsion.CosmosStore

open Propulsion.Internal
open System

/// Wraps the Microsoft.Azure.Cosmos ChangeFeedProcessor and ChangeFeedProcessorEstimator
/// See https://docs.microsoft.com/en-us/azure/cosmos-db/how-to-use-change-feed-estimator
type CosmosStoreSource
    (   log: Serilog.ILogger, statsInterval,
        monitored: Microsoft.Azure.Cosmos.Container,
        // The non-partitioned (i.e., PartitionKey is "id") Container holding the partition leases.
        // Should always read from the write region to keep the number of write conflicts to a minimum when the sdk
        // updates the leases. Since the non-write region(s) might lag behind due to us using non-strong consistency, during
        // fail over we are likely to reprocess some messages, but that's okay since processing has to be idempotent in any case
        leases: Microsoft.Azure.Cosmos.Container,
        // Identifier to disambiguate multiple independent feed processor positions (akin to a 'consumer group')
        processorName, parseFeedDoc, sink: Propulsion.Sinks.SinkPipeline,
        // Limit on items to take in a batch when querying for changes (in addition to 4MB response size limit). Default Unlimited.
        // Max Items is not emphasized as a control mechanism as it can only be used meaningfully when events are highly regular in size.
        [<O; D null>] ?maxItems,
        // Delay before re-polling a partition after backlog has been drained. Default: 1s
        [<O; D null>] ?tailSleepInterval,
        // (NB Only applies if this is the first time this leasePrefix is presented)
        // Specify `true` to request starting of projection from the present write position.
        // Default: false (projecting all events from start beforehand)
        [<O; D null>] ?startFromTail,
        [<O; D null>] ?lagEstimationInterval: TimeSpan,
        // Enables reporting or other processing of Exception conditions as per <c>WithErrorNotification</c>
        [<O; D null>] ?notifyError: Action<int, exn>,
        // Admits customizations in the ChangeFeedProcessorBuilder chain
        [<O; D null>] ?customize,
        // Frequency to check for partitions without a processor. Default: 5s
        [<O; D null>] ?leaseAcquireInterval,
        // Frequency to renew leases held by processors under our control. Default 5s
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
    let startIngester trancheId = sink.StartIngester(log, trancheId |> Propulsion.Feed.TrancheId.toString |> int)
    let buildObserver trancheId = lazy (
        let startIngester () = startIngester trancheId
        new Observer<seq<Propulsion.Sinks.StreamEvent>>(stats, startIngester, Seq.collect parseFeedDoc))
    let observers = new Observers<seq<Propulsion.Sinks.StreamEvent>>(log, processorName, buildObserver)

    member _.Start(): Propulsion.Feed.Core.SourcePipeline =
        let machine, triggerStop, outcomeTask =
            ChangeFeedProcessor.Start(
                monitored, leases, processorName, leaseOwnerId, log, stats, statsInterval, observers,
                defaultArg startFromTail false, feedPollInterval = defaultArg tailSleepInterval (TimeSpan.seconds 1),
                leaseAcquireInterval = defaultArg leaseAcquireInterval (TimeSpan.seconds 5),
                leaseRenewInterval = defaultArg leaseRenewInterval (TimeSpan.seconds 5),
                leaseTtl = defaultArg leaseTtl (TimeSpan.seconds 10),
                ?maxItems = maxItems, ?notifyError = notifyError, ?customize = customize, ?lagEstimationInterval = lagEstimationInterval)
        let fetchPositions () = Propulsion.Feed.Core.SourcePositions.current observers
        let monitor = lazy Propulsion.Feed.Core.FeedMonitor(log, fetchPositions, sink, fun () -> outcomeTask.IsCompleted)
        let flush () = Propulsion.Feed.Core.SourcePositions.completed observers |> Task.FromResult
        new Propulsion.SourcePipeline<_, _>(Task.run machine, flush, triggerStop, monitor)
