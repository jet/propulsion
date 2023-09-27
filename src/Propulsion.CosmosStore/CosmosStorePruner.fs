// Implements a Sink that removes every submitted event (and all preceding events)     from the relevant stream
namespace Propulsion.CosmosStore

open Propulsion.Internal
open Propulsion.Sinks
open Propulsion.Streams
open Serilog
open System

module Pruner =

    type Outcome =
        | Ok of completed : int * deferred : int
        | Nop of int

    // Per set of accumulated events per stream (selected via `selectExpired`), attempt to prune up to the high water mark
    let handle pruneUntil stream (span: Event[]) ct = task {
        // The newest event eligible for deletion defines the cutoff point
        let untilIndex = span[span.Length - 1].Index
        // Depending on the way the events are batched, requests break into three groupings:
        // 1. All requested events already deleted, no writes took place
        //    (if trimmedPos is beyond requested Index, Propulsion will discard the requests via the OverrideNextIndex)
        // 2. All events deleted as requested
        //    (N events over M batches were removed)
        // 3. Some deletions deferred
        //    (requested trim point was in the middle of a batch; touching it would put the batch out of order)
        //    in this case, we mark the event as handled and await a successor event triggering another attempt
        let! deleted, deferred, trimmedPos = pruneUntil (stream, untilIndex, ct)
        // Categorize the outcome so the stats handler can summarize the work being carried out
        let res = if deleted = 0 && deferred = 0 then Nop span.Length else Ok (deleted, deferred)
        // For case where we discover events have already been deleted beyond our requested position, signal to reader to drop events
        let writePos = max trimmedPos (untilIndex + 1L)
        return struct (writePos, res) }

type CosmosStorePrunerStats(log, statsInterval, stateInterval, [<O; D null>] ?failThreshold) =
    inherit Propulsion.Streams.Stats<Pruner.Outcome>(log, statsInterval, stateInterval, ?failThreshold = failThreshold)

    let mutable nops, totalRedundant, ops, totalDeletes, totalDeferred = 0, 0, 0, 0, 0
    override _.HandleOk outcome =
        match outcome with
        | Pruner.Outcome.Nop count ->
            nops <- nops + 1
            totalRedundant <- totalRedundant + count
        | Pruner.Outcome.Ok (completed, deferred) ->
            ops <- ops + 1
            totalDeletes <- totalDeletes + completed
            totalDeferred <- totalDeferred + deferred
    override _.DumpStats() =
        log.Information("Deleted {ops}r {deletedCount}e Deferred {deferred}e Redundant {nops}r {nopCount}e",
            ops, totalDeletes, totalDeferred, nops, totalRedundant)
        ops <- 0; totalDeletes <- 0; nops <- 0; totalDeferred <- totalDeferred; totalRedundant <- 0
        base.DumpStats()
        Equinox.CosmosStore.Core.Log.InternalMetrics.dump log

    override x.Classify e =
        match e with
        | Equinox.CosmosStore.Exceptions.RateLimited ->     OutcomeKind.RateLimited
        | Equinox.CosmosStore.Exceptions.RequestTimeout ->  OutcomeKind.Timeout
        | e ->                                              base.Classify e
    override _.HandleExn(log, exn) = log.Warning(exn, "Unhandled")

/// DANGER: <c>CosmosPruner</c> DELETES events - use with care
type CosmosStorePruner =

    /// DANGER: this API DELETES events - use with care
    /// Starts a <c>Sink</c> that prunes _all submitted events from the supplied <c>context</c>_
    static member Start
        (   log : ILogger, maxReadAhead, context, maxConcurrentStreams, stats: CosmosStorePrunerStats,
            ?purgeInterval, ?wakeForResults, ?idleDelay,
            // Defaults to statsInterval
            ?ingesterStatsInterval)
        : Sink =
        let dispatcher =
#if COSMOSV3
            let inline pruneUntil (sn, index, ct) = Equinox.CosmosStore.Core.Events.pruneUntil context (FsCodec.StreamName.toString sn) index |> Async.executeAsTask ct
#else
            let inline pruneUntil (sn, index, ct) = Equinox.CosmosStore.Core.Events.pruneUntil context sn index |> Async.executeAsTask ct
#endif
            let interpret _stream span =
                let metrics = StreamSpan.metrics Event.storedSize span
                struct (metrics, span)
            Dispatcher.Concurrent<_, _, _, _>.Create(maxConcurrentStreams, interpret, Pruner.handle pruneUntil, (fun _ r -> r))
        let dumpStreams logStreamStates _log = logStreamStates Event.storedSize
        let scheduler = Scheduling.Engine(dispatcher, stats, dumpStreams, pendingBufferSize = 5,
                                          ?purgeInterval = purgeInterval, ?wakeForResults = wakeForResults, ?idleDelay = idleDelay)
        Projector.Pipeline.Start(log, scheduler.Pump, maxReadAhead, scheduler, ingesterStatsInterval = defaultArg ingesterStatsInterval stats.StatsInterval.Period)
