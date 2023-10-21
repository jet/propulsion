namespace Propulsion.Feed.Core

open Propulsion
open Propulsion.Feed
open Propulsion.Internal
open System
open System.Collections.Generic

/// Intercepts receipt and completion of batches, recording the read and completion positions
type TranchePositions() =
    let positions = System.Collections.Concurrent.ConcurrentDictionary<TrancheId, TrancheState>()

    member _.Intercept(trancheId) =
        positions.GetOrAdd(trancheId, fun _trancheId -> { read = ValueNone; isTail = false; completed = ValueNone }) |> ignore
        fun (batch: Ingestion.Batch<'Items>) ->
            let p = positions[trancheId]
            p.read <- ValueSome batch.epoch
            p.isTail <- batch.isTail
            let onCompletion () =
                batch.onCompletion()
                positions[trancheId].completed <- ValueSome batch.epoch
            { batch with onCompletion = onCompletion }
    member _.Current() = positions.ToArray()
    member x.Completed(): IReadOnlyDictionary<TrancheId, Position> =
        seq { for kv in x.Current() do match kv.Value.completed with ValueNone -> () | ValueSome c -> (kv.Key, Position.parse c) }
        |> readOnlyDict

/// Represents the current state of a Tranche of the source's processing
and TrancheState =
    { mutable read: int64 voption; mutable isTail: bool; mutable completed: int64 voption }
    member x.IsEmpty = x.completed = x.read

and [<Struct; NoComparison; NoEquality>] private WaitMode = OriginalWorkOnly | IncludeSubsequent | AwaitFullyCaughtUp
and FeedMonitor(log: Serilog.ILogger, positions: TranchePositions, sink: Propulsion.Sinks.Sink, sourceIsCompleted) =

    let notEol () = not sink.IsCompleted && not (sourceIsCompleted ())
    let choose f (xs: KeyValuePair<_, _>[]) = [| for x in xs do match f x.Value with ValueNone -> () | ValueSome v' -> struct (x.Key, v') |]
    // Waits for up to propagationDelay, returning the opening tranche positions observed (or empty if the wait has timed out)
    let awaitPropagation sleep (propagationDelay: TimeSpan) positions ct = task {
        let timeout = IntervalTimer propagationDelay
        let mutable startPositions = positions ()
        while Array.isEmpty startPositions && not timeout.IsDue && notEol () do
            do! Task.delay sleep ct
            startPositions <- positions ()
        return startPositions }
    // Waits for up to lingerTime for work to arrive. If any work arrives, it waits for activity (including extra work that arrived during the wait) to quiesce.
    // If the lingerTime expires without work having completed, returns the start positions from when activity commenced
    let awaitLinger sleep (lingerTime: TimeSpan) positions ct = task {
        let timeout = IntervalTimer lingerTime
        let mutable startPositions = positions ()
        let mutable worked = false
        while (Array.any startPositions || not worked) && not timeout.IsDue && notEol () do
            do! Task.delay sleep ct
            let current = positions ()
            if not worked && Array.any current then startPositions <- current; worked <- true // Starting: Record start position (for if we exit due to timeout)
            elif worked && Array.isEmpty current then startPositions <- current // Finished now: clear starting position record, triggering normal exit of loop
        return startPositions }
    let isTrancheDrained (s: TrancheState) = s.isTail && s.IsEmpty
    let isDrained: KeyValuePair<_, TrancheState>[] -> bool = Array.forall (fun (KeyValue (_t, s)) -> isTrancheDrained s)
    let awaitCompletion (sleep, logInterval) (sw: System.Diagnostics.Stopwatch) startReadPositions waitMode ct = task {
        let logInterval = IntervalTimer logInterval
        let logWaitStatusUpdateNow () =
            let current = positions.Current()
            let currentRead, completed = current |> choose (fun v -> v.read), current |> choose (fun v -> v.completed)
            match waitMode with
            | OriginalWorkOnly ->   log.Information("FeedMonitor {totalTime:n1}s Awaiting Started {starting} Completed {completed}",
                                                    sw.ElapsedSeconds, startReadPositions, completed)
            | IncludeSubsequent ->  log.Information("FeedMonitor {totalTime:n1}s Awaiting Running. Current {current} Completed {completed} Starting {starting}",
                                                    sw.ElapsedSeconds, currentRead, completed, startReadPositions)
            | AwaitFullyCaughtUp -> let draining = current |> choose (fun v -> if isTrancheDrained v then ValueNone else ValueSome ()) |> Array.map ValueTuple.fst
                                    log.Information("FeedMonitor {totalTime:n1}s Awaiting Tails {tranches}. Current {current} Completed {completed} Starting {starting}",
                                                    sw.ElapsedSeconds, draining, currentRead, completed, startReadPositions)
        let busy () =
            let current = positions.Current()
            match waitMode with
            | OriginalWorkOnly ->   let completed = current |> choose (fun v -> v.completed)
                                    let trancheCompletedPos = Dictionary() in for struct (k, v) in completed do trancheCompletedPos.Add(k, v)
                                    let startPosStillPendingCompletion trancheStartPos trancheId =
                                        match trancheCompletedPos.TryGetValue trancheId with
                                        | true, v -> v <= trancheStartPos
                                        | false, _ -> false
                                    startReadPositions |> Seq.exists (fun struct (t, s) -> startPosStillPendingCompletion s t)
            | IncludeSubsequent ->  current |> Array.exists (fun kv -> not kv.Value.IsEmpty) // All work (including follow-on work) completed
            | AwaitFullyCaughtUp -> current |> isDrained |> not
        while busy () && notEol () do
            if logInterval.IfDueRestart() then logWaitStatusUpdateNow()
            do! Task.delay sleep ct }

    /// Waits quasi-deterministically for events to be observed, (for up to the <c>propagationDelay</c>)
    /// If at least one event has been observed, waits for the completion of the Sink's processing
    /// NOTE: Best used in conjunction with MemoryStoreSource.AwaitCompletion, which is deterministic.
    /// This enables one to place minimal waits within integration tests precisely when and where they are necessary.
    member _.AwaitCompletion
        (   // time to wait for arrival of initial events, this should take into account:
            // - time for indexing of the events to complete based on the environment's configuration (e.g. with DynamoStore, the total Lambda and DynamoDB Streams trigger time)
            // - an adjustment to account for the polling interval that the Source is using
            propagationDelay,
            // sleep interval while awaiting completion. Default 1ms.
            ?sleep,
            // interval at which to log status of the Await (to assist in analyzing stuck Sinks). Default 5s.
            ?logInterval,
            // Inhibit waiting for the handling of follow-on events that arrived after the wait commenced.
            ?ignoreSubsequent,
            // Whether to wait for completed work to reach the tail [of all tranches]. Default off.
            ?awaitFullyCaughtUp,
            // Compute time to wait subsequent to processing for trailing events, based on
            // - propagationTimeout: the propagationDelay as supplied
            // - propagation: the observed propagation time
            // - processing: the observed processing time
            // Example:
            //   let lingerTime _isDrained (propagationTimeout: TimeSpan) (propagation: TimeSpan) (processing: TimeSpan) =
            //      max (propagationTimeout.TotalSeconds / 4.) ((propagation.TotalSeconds + processing.TotalSeconds) / 3.) |> TimeSpan.FromSeconds
            ?lingerTime: bool -> TimeSpan -> TimeSpan -> TimeSpan -> TimeSpan,
            ?ct) = task {
        let ct = defaultArg ct CancellationToken.None
        let sw = Stopwatch.start ()
        let sleep = defaultArg sleep (TimeSpan.FromMilliseconds 1)
        let currentCompleted = seq { for kv in positions.Current() -> struct (kv.Key, ValueOption.toNullable kv.Value.completed) }
        let waitMode =
            match ignoreSubsequent, awaitFullyCaughtUp with
            | Some true, Some true -> invalidArg (nameof awaitFullyCaughtUp) "cannot be combined with ignoreSubsequent"
            | _, Some true -> AwaitFullyCaughtUp
            | Some true, _ -> OriginalWorkOnly
            | _ -> OriginalWorkOnly
        let requireTail = match waitMode with AwaitFullyCaughtUp -> true | _ -> false
        let activeTranches () =
            match positions.Current() with
            | xs when xs |> Array.forall (fun (kv: KeyValuePair<_, TrancheState>) -> kv.Value.IsEmpty && (not requireTail || kv.Value.isTail)) -> Array.empty
            | originals -> originals |> choose (fun v -> v.read)
        match! awaitPropagation sleep propagationDelay activeTranches ct with
        | [||] ->
            if propagationDelay = TimeSpan.Zero then log.Debug("FeedSource Wait Skipped; no processing pending. Completed {completed}", currentCompleted)
            else log.Information("FeedMonitor Wait {propagationDelay:n1}s Timeout. Completed {completed}", sw.ElapsedSeconds, currentCompleted)
        | starting ->
            let propUsed = sw.Elapsed
            let logInterval = defaultArg logInterval (TimeSpan.FromSeconds 5.)
            let swProcessing = Stopwatch.start ()
            do! awaitCompletion (sleep, logInterval) swProcessing starting waitMode ct
            let procUsed = swProcessing.Elapsed
            let isDrainedNow () = positions.Current() |> isDrained
            let linger = match lingerTime with None -> TimeSpan.Zero | Some lingerF -> lingerF (isDrainedNow ()) propagationDelay propUsed procUsed
            let skipLinger = linger = TimeSpan.Zero
            let ll = if skipLinger then LogEventLevel.Information else LogEventLevel.Debug
            let originalCompleted = currentCompleted |> Seq.cache
            if log.IsEnabled ll then
                let completed = positions.Current() |> choose (fun v -> v.completed)
                log.Write(ll, "FeedMonitor Wait {totalTime:n1}s Processed Propagate {propagate:n1}s/{propTimeout:n1}s Process {process:n1}s Tail {allAtTail} Starting {starting} Completed {completed}",
                          sw.ElapsedSeconds, propUsed.TotalSeconds, propagationDelay.TotalSeconds, procUsed.TotalSeconds, isDrainedNow (), starting, completed)
            if not skipLinger then
                let swLinger = Stopwatch.start ()
                match! awaitLinger sleep linger activeTranches ct with
                | [||] ->
                    log.Information("FeedMonitor Wait {totalTime:n1}s OK Propagate {propagate:n1}/{propTimeout:n1}s Process {process:n1}s Linger {lingered:n1}/{linger:n1}s Tail {allAtTail}. Starting {starting} Completed {completed}",
                                    sw.ElapsedSeconds, propUsed.TotalSeconds, propagationDelay.TotalSeconds, procUsed.TotalSeconds, swLinger.ElapsedSeconds, linger.TotalSeconds, isDrainedNow (), starting, originalCompleted)
                | lingering ->
                    do! awaitCompletion (sleep, logInterval) swProcessing lingering waitMode ct
                    log.Information("FeedMonitor Wait {totalTime:n1}s Lingered Propagate {propagate:n1}/{propTimeout:n1}s Process {process:n1}s Linger {lingered:n1}/{linger:n1}s Tail {allAtTail}. Starting {starting} Lingering {lingering} Completed {completed}",
                                    sw.ElapsedSeconds, propUsed.TotalSeconds, propagationDelay.TotalSeconds, procUsed.TotalSeconds, swLinger.ElapsedSeconds, linger, isDrainedNow (), starting, lingering, currentCompleted)
            // If the sink Faulted, let the awaiter observe the associated Exception that triggered the shutdown
            if sink.IsCompleted && not sink.RanToCompletion then
                return! sink.Wait() }

module FeedMonitor =
    /// Pumps to the Sink until either the specified timeout has been reached, or all items in the Source have been fully consumed
    let runUntilCaughtUp
            (start: unit -> SourcePipeline<FeedMonitor>)
            (checkpoint: CancellationToken -> Task<'R>)
            (timeout: TimeSpan, statsInterval: IntervalTimer) = task {
        let sw = Stopwatch.start ()
        // Kick off reading from the source (Disposal will Stop it if we're exiting due to a timeout; we'll spin up a fresh one when re-triggered)
        use pipeline = start ()

        try // In the case of sustained activity and/or catch-up scenarios, proactively trigger an orderly shutdown of the Source
            // in advance of the Lambda being killed (no point starting new work or incurring DynamoDB CU consumption that won't finish)
            Task.Delay(timeout).ContinueWith(fun _ -> pipeline.Stop()) |> ignore

            // If for some reason we're not provisioned well enough to read something within 1m, no point for paying for a full lambda timeout
            let initialReaderTimeout = TimeSpan.FromMinutes 1.
            do! pipeline.Monitor.AwaitCompletion(initialReaderTimeout, awaitFullyCaughtUp = true, logInterval = TimeSpan.FromSeconds 30)
            // Shut down all processing (we create a fresh Source per Lambda invocation)
            pipeline.Stop()

            if sw.ElapsedSeconds > 2 then statsInterval.Trigger()
            // force a final attempt to flush (normally checkpointing is at 5s intervals)
            return! checkpoint CancellationToken.None
        finally statsInterval.SleepUntilTriggerCleared() }
