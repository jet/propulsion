// Implements a Sink that removes every submitted event (and all preceding events)     from the relevant stream
namespace Propulsion.Cosmos

open Propulsion.Streams
open Serilog
open System
open System.Collections.Generic

module Pruner =

    type [<RequireQualifiedAccess>] ExceptionKind = TimedOut | RateLimited | Other

    let (|TimedOutMessage|RateLimitedMessage|Other|) (e : exn) =
        match e.GetType().FullName with
        | "Microsoft.Azure.Documents.RequestTimeoutException" -> TimedOutMessage
        | "Microsoft.Azure.Documents.RequestRateTooLargeException" -> RateLimitedMessage
        | _ -> Other

    let classify = function
        | RateLimitedMessage -> ExceptionKind.RateLimited
        | TimedOutMessage -> ExceptionKind.TimedOut
        | Other -> ExceptionKind.Other

    type Outcome =
        | Ok of completed : int * deferred : int
        | Nop of int

    type Stats(log, statsInterval, stateInterval) =
        inherit Propulsion.Streams.Stats<Outcome>(log, statsInterval, stateInterval)

        let mutable nops, totalRedundant, ops, totalDeletes, totalDeferred = 0, 0, 0, 0, 0

        let mutable rateLimited, timedOut = 0, 0
        let rlStreams, toStreams = HashSet(), HashSet()

        override _.HandleOk outcome =
            match outcome with
            | Nop count ->
                nops <- nops + 1
                totalRedundant <- totalRedundant + count
            | Ok (completed, deferred) ->
                ops <- ops + 1
                totalDeletes <- totalDeletes + completed
                totalDeferred <- totalDeferred + deferred

        /// Used to render exceptions that don't fall into the rate-limiting or timed-out categories
        override _.HandleExn(log, exn) =
            match classify exn with
            | ExceptionKind.RateLimited | ExceptionKind.TimedOut ->
                () // Outcomes are already included in the statistics - no logging is warranted
            | ExceptionKind.Other ->
                log.Warning(exn, "Unhandled")

        /// Gather stats pertaining to and/or filter exceptions pertaining to timeouts or rate-limiting
        override _.Handle message =
            let inline adds x (set:HashSet<_>) = set.Add x |> ignore
            base.Handle message
            match message with
            | { stream = stream; result = Choice2Of2 (_, exn) } ->
                match classify exn with
                | ExceptionKind.RateLimited ->
                    adds stream rlStreams; rateLimited <- rateLimited + 1
                | ExceptionKind.TimedOut ->
                    adds stream toStreams; timedOut <- timedOut + 1
                | ExceptionKind.Other -> ()
            | _ -> ()

        override _.DumpStats() =
            log.Information("Deleted {ops}r {deletedCount}e Deferred {deferred}e Redundant {nops}r {nopCount}e",
                ops, totalDeletes, totalDeferred, nops, totalRedundant)
            ops <- 0; totalDeletes <- 0; nops <- 0; totalDeferred <- totalDeferred; totalRedundant <- 0
            if rateLimited <> 0 || timedOut <> 0 then
                let transients = rateLimited + timedOut
                log.Warning("Transients {transients} Rate-limited {rateLimited:n0}r {rlStreams:n0}s Timed out {toCount:n0}r {toStreams:n0}s",
                    transients, rateLimited, rlStreams.Count, timedOut, toStreams.Count)
                rateLimited <- 0; timedOut <- 0; rlStreams.Clear(); toStreams.Clear()
            base.DumpStats()
            Equinox.Cosmos.Store.Log.InternalMetrics.dump log

    // Per set of accumulated events per stream (selected via `selectExpired`), attempt to prune up to the high water mark
    let handle pruneUntil struct (stream, span: StreamSpan<_>) = async {
        // The newest event eligible for deletion defines the cutoff point
        let untilIndex = span[span.Length - 1].Index
        // Depending on the way the events are batched, requests break into three groupings:
        // 1. All requested events already deleted, no writes took place
        //    (if trimmedPos is beyond requested Index, Propulsion will discard the requests via the OverrideWritePosition)
        // 2. All events deleted as requested
        //    (N events over M batches were removed)
        // 3. Some deletions deferred
        //    (requested trim point was in the middle of a batch; touching it would put the batch out of order)
        //    in this case, we mark the event as handled and await a successor event triggering another attempt
        let! deleted, deferred, trimmedPos = pruneUntil (FsCodec.StreamName.toString stream) untilIndex
        // Categorize the outcome so the stats handler can summarize the work being carried out
        let res = if deleted = 0 && deferred = 0 then Nop span.Length else Ok (deleted, deferred)
        // For case where we discover events have already been deleted beyond our requested position, signal to reader to drop events
        let writePos = max trimmedPos (untilIndex + 1L)
        return struct (writePos, res)
    }

    type StreamSchedulingEngine =

        static member Create(pruneUntil, maxDop, stats : Stats, dumpStreams, ?purgeInterval, ?wakeForResults, ?idleDelay)
            : Scheduling.Engine<_, _, _, _> =
            let interpret struct (stream, span) =
                let metrics = StreamSpan.metrics Default.eventSize span
                struct (metrics, struct (stream, span))
            let dispatcher = Dispatcher.Concurrent<_, _, _, _>.Create(maxDop, interpret, handle pruneUntil, (fun _ -> id))
            Scheduling.Engine(
                dispatcher, stats, dumpStreams, maxIngest = 5,
                ?purgeInterval = purgeInterval, ?wakeForResults = wakeForResults, ?idleDelay = idleDelay)

/// DANGER: <c>CosmosPruner</c> DELETES events - use with care
type CosmosPruner =

    /// DANGER: this API DELETES events - use with care
    /// Starts a <c>Sink</c> that prunes _all submitted events from the supplied <c>context</c>_
    static member Start
        (   log : ILogger, maxReadAhead, context, maxConcurrentStreams,
            // Default 5m
            ?statsInterval,
            // Default 5m
            ?stateInterval,
            ?purgeInterval, ?wakeForResults, ?idleDelay,
            // Defaults to statsInterval
            ?ingesterStatsInterval)
        : Default.Sink =
        let statsInterval, stateInterval = defaultArg statsInterval (TimeSpan.FromMinutes 5.), defaultArg stateInterval (TimeSpan.FromMinutes 5.)
        let stats = Pruner.Stats(log.ForContext<Pruner.Stats>(), statsInterval, stateInterval)
        let dumpStreams logStreamStates _log = logStreamStates Default.eventSize
        let pruneUntil stream index = Equinox.Cosmos.Core.Events.pruneUntil context stream index
        let streamScheduler =
            Pruner.StreamSchedulingEngine.Create(
                pruneUntil, maxConcurrentStreams, stats, dumpStreams,
                ?purgeInterval = purgeInterval, ?wakeForResults = wakeForResults, ?idleDelay = idleDelay)
        Projector.Pipeline.Start(log, streamScheduler.Pump, maxReadAhead, streamScheduler, statsInterval, ?ingesterStatsInterval = ingesterStatsInterval)
