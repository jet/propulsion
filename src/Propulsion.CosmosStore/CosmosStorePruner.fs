// Implements a Sink that removes every submitted event (and all preceding events)     from the relevant stream
namespace Propulsion.CosmosStore

open Propulsion.Streams
open Serilog
open System
open System.Collections.Generic

module Pruner =

    type [<RequireQualifiedAccess>] ExceptionKind = TimedOut | RateLimited | Other

    let (|TimedOutMessage|RateLimitedMessage|Other|) (e : exn) =
        match e with
        | (:? Microsoft.Azure.Cosmos.CosmosException as ce) when ce.StatusCode = System.Net.HttpStatusCode.TooManyRequests -> RateLimitedMessage
        | e when e.GetType().FullName = "Microsoft.Azure.Documents.RequestTimeoutException" -> TimedOutMessage
        | _ -> Other
    let classify = function
        | RateLimitedMessage -> ExceptionKind.RateLimited
        | TimedOutMessage -> ExceptionKind.TimedOut
        | Other -> ExceptionKind.Other

    type Outcome =
        | Ok of completed : int * deferred : int
        | Nop of int

    type Stats(log, statsInterval, stateInterval) =
        inherit Propulsion.Streams.Projector.Stats<Outcome>(log, statsInterval, stateInterval)

        let mutable nops, totalRedundant, ops, totalDeletes, totalDeferred = 0, 0, 0, 0, 0

        let rateLimited, timedOut = ref 0, ref 0
        let rlStreams, toStreams = HashSet(), HashSet()

        override __.HandleOk outcome =
            match outcome with
            | Nop count ->
                nops <- nops + 1
                totalRedundant <- totalRedundant + count
            | Ok (completed, deferred) ->
                ops <- ops + 1
                totalDeletes <- totalDeletes + completed
                totalDeferred <- totalDeferred + deferred

        /// Used to render exceptions that don't fall into the rate-limiting or timed-out categories
        override __.HandleExn(log, exn) =
            match classify exn with
            | ExceptionKind.RateLimited | ExceptionKind.TimedOut ->
                () // Outcomes are already included in the statistics - no logging is warranted
            | ExceptionKind.Other ->
                log.Warning(exn, "Unhandled")

        /// Gather stats pertaining to and/or filter exceptions pertaining to timeouts or rate-limiting
        override __.Handle message =
            let inline adds x (set:HashSet<_>) = set.Add x |> ignore
            base.Handle message
            match message with
            | Scheduling.InternalMessage.Result (_duration, (stream, Choice2Of2 (_, exn))) ->
                match classify exn with
                | ExceptionKind.RateLimited ->
                    adds stream rlStreams; incr rateLimited
                | ExceptionKind.TimedOut ->
                    adds stream toStreams; incr timedOut
                | ExceptionKind.Other -> ()
            | _ -> ()

        override __.DumpStats() =
            log.Information("Deleted {ops}r {deletedCount}e Deferred {deferred}e Redundant {nops}r {nopCount}e",
                ops, totalDeletes, totalDeferred, nops, totalRedundant)
            ops <- 0; totalDeletes <- 0; nops <- 0; totalDeferred <- totalDeferred; totalRedundant <- 0
            if !rateLimited <> 0 || !timedOut <> 0 then
                let transients = !rateLimited + !timedOut
                log.Warning("Transients {transients} Rate-limited {rateLimited:n0}r {rlStreams:n0}s Timed out {toCount:n0}r {toStreams:n0}s",
                    transients, !rateLimited, rlStreams.Count, !timedOut, toStreams.Count)
                rateLimited := 0; timedOut := 0; rlStreams.Clear(); toStreams.Clear()
            base.DumpStats()
            Equinox.CosmosStore.Core.Log.InternalMetrics.dump log

    // Per set of accumulated events per stream (selected via `selectExpired`), attempt to prune up to the high water mark
    let handle pruneUntil (stream, span: Propulsion.Streams.StreamSpan<_>) = async {
        // The newest event eligible for deletion defines the cutoff point
        let beforeIndex = span.events.[span.events.Length - 1].Index
        // Depending on the way the events are batched, requests break into three groupings:
        // 1. All requested events already deleted, no writes took place
        //    (if trimmedPos is beyond requested Index, Propulsion will discard the requests via the OverrideWritePosition)
        // 2. All events deleted as requested
        //    (N events over M batches were removed)
        // 3. Some deletions deferred
        //    (requested trim point was in the middle of a batch; touching it would put the batch out of order)
        //    in this case, we mark the event as handled and await a successor event triggering another attempt
        let! deleted, deferred, trimmedPos = pruneUntil (FsCodec.StreamName.toString stream) beforeIndex
        // Categorize the outcome so the stats handler can summarize the work being carried out
        let res = if deleted = 0 && deferred = 0 then Nop span.events.Length else Ok (deleted, deferred)
        // For case where we discover events have already been deleted beyond our requested position, signal to reader to drop events
        let writePos = max trimmedPos beforeIndex
        return writePos, res
    }

    type StreamSchedulingEngine =

        static member Create(pruneUntil, itemDispatcher, stats : Stats, dumpStreams, ?maxBatches, ?idleDelay)
            : Scheduling.StreamSchedulingEngine<_, _, _> =
            let attemptWrite (item : Scheduling.DispatchItem<_>) = async {
                let stats = Buffering.StreamSpan.stats item.span
                try let! index', res = handle pruneUntil (item.stream, item.span)
                    return Choice1Of2 (index', stats, res)
                with e -> return Choice2Of2 (stats, e) }
            let interpretProgress _streams _stream : Choice<int64 * 'Metrics * 'Outcome, 'Metrics * exn> -> int64 option * Choice<'Metrics * 'Outcome, 'Metrics * exn> = function
                | Choice1Of2 (index, stats, outcome) -> Some index, Choice1Of2 (stats, outcome)
                | Choice2Of2 (stats, exn) -> None, Choice2Of2 (stats, exn)
            let dispatcher = Scheduling.MultiDispatcher<_, _, _>(itemDispatcher, attemptWrite, interpretProgress, stats, dumpStreams)
            Scheduling.StreamSchedulingEngine(dispatcher, enableSlipstreaming=false, ?maxBatches=maxBatches, ?idleDelay=idleDelay)

/// DANGER: <c>CosmosPruner</c> DELETES events - use with care
type CosmosStorePruner =

    /// DANGER: this API DELETES events - use with care
    /// Starts a <c>StreamsProjectorPipeline</c> that prunes _all submitted events from the supplied <c>context</c>_
    static member Start
        (   log : ILogger, maxReadAhead, context, maxConcurrentStreams,
            /// Default 5m
            ?statsInterval,
            /// Default 5m
            ?stateInterval, ?ingesterStatsInterval, ?maxSubmissionsPerPartition, ?pumpInterval,
            /// Delay when no items available. Default 10ms.
            ?idleDelay)
        : Propulsion.ProjectorPipeline<_> =
        let idleDelay = defaultArg idleDelay (TimeSpan.FromMilliseconds 10.)
        let statsInterval, stateInterval = defaultArg statsInterval (TimeSpan.FromMinutes 5.), defaultArg stateInterval (TimeSpan.FromMinutes 5.)
        let stats = Pruner.Stats(log.ForContext<Pruner.Stats>(), statsInterval, stateInterval)
        let dispatcher = Propulsion.Streams.Scheduling.ItemDispatcher<_>(maxConcurrentStreams)
        let dumpStreams (s : Scheduling.StreamStates<_>) l = s.Dump(l, Propulsion.Streams.Buffering.StreamState.eventsSize)
        let pruneUntil stream index = Equinox.CosmosStore.Core.Events.pruneUntil context stream index
        let streamScheduler = Pruner.StreamSchedulingEngine.Create(pruneUntil, dispatcher, stats, dumpStreams, idleDelay=idleDelay)
        Propulsion.Streams.Projector.StreamsProjectorPipeline.Start(
            log, dispatcher.Pump(), streamScheduler.Pump, maxReadAhead, streamScheduler.Submit, statsInterval,
            ?ingesterStatsInterval=ingesterStatsInterval, ?maxSubmissionsPerPartition=maxSubmissionsPerPartition, ?pumpInterval=pumpInterval)
