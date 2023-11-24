namespace Propulsion.Feed.Core

open Propulsion
open Propulsion.Feed
open Propulsion.Internal
open System

type ITranchePosition =
    abstract member IsTail: bool with get
    abstract member ReadPos: Position voption with get
    abstract member CompletedPos: Position voption with get
    /// Intercepts receipt and completion of batches, recording the read and completion positions
    abstract member Decorate: Ingestion.Batch<'Items> -> Ingestion.Batch<'Items>
type TranchePosition() =
    [<DefaultValue>] val mutable read: Position voption
    [<DefaultValue>] val mutable completed: Position voption
    [<DefaultValue>] val mutable isTail: bool
    interface ITranchePosition with
        member x.IsTail = x.isTail
        member x.ReadPos = x.read
        member x.CompletedPos = x.completed
        member x.Decorate(batch) =
            x.read <- Position.parse batch.epoch |> ValueSome
            x.isTail <- batch.isTail
            let onCompletion () =
                batch.onCompletion()
                x.completed <- Position.parse batch.epoch |> ValueSome
            { batch with onCompletion = onCompletion }
module TranchePosition =
    let isEmpty (x: #ITranchePosition) = x.ReadPos = x.CompletedPos
    let isDrained (s: #ITranchePosition) = s.IsTail && isEmpty s

type ISourcePositions<'T> =
    abstract member For: TrancheId -> 'T
    abstract member Current: unit -> struct (TrancheId * 'T)[]
module SourcePositions =
    let current (x: ISourcePositions<#ITranchePosition>) =
        [| for k, v in x.Current() -> struct (k, v :> ITranchePosition) |]
    let completed (x: ISourcePositions<#ITranchePosition>): TranchePositions =
        [| for k, v in x.Current() do match v.CompletedPos with ValueNone -> () | ValueSome c -> struct (k, c) |]
type SourcePositions<'T when 'T :> ITranchePosition>(createTranche: Func<TrancheId, Lazy<'T>>) =
    // Its important we don't risk >1 instance https://andrewlock.net/making-getoradd-on-concurrentdictionary-thread-safe-using-lazy/
    // while it would be safe, there would be a risk of incurring the cost of multiple initialization loops
    let tranches = System.Collections.Concurrent.ConcurrentDictionary<TrancheId, Lazy<'T>>()
    member _.Iter(f) = for x in tranches.Values do f x.Value
    interface ISourcePositions<'T> with
        member _.For trancheId = tranches.GetOrAdd(trancheId, createTranche).Value
        member _.Current() = tranches.ToArray() |> Array.map (fun kv -> (kv.Key, kv.Value.Value))

and [<Struct; NoComparison; NoEquality>] private WaitMode = OriginalWorkOnly | IncludeSubsequent | AwaitFullyCaughtUp
and FeedMonitor(log: Serilog.ILogger, fetchPositions: unit -> struct (TrancheId * ITranchePosition)[], sink: Propulsion.Sinks.SinkPipeline, sourceIsCompleted) =

    let allDrained = Array.forall (ValueTuple.snd >> TranchePosition.isDrained)
    let notEol () = not sink.IsCompleted && not (sourceIsCompleted ())
    let choose f (xs: struct (_ * _)[]) = [| for k, v in xs do match f v with ValueNone -> () | ValueSome v' -> struct (k, v') |]
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
    let awaitCompletion (sleep, logInterval) (sw: System.Diagnostics.Stopwatch) startReadPositions waitMode ct = task {
        let logInterval = IntervalTimer logInterval
        let logWaitStatusUpdateNow () =
            let current = fetchPositions ()
            let currentRead, completed = current |> choose (fun v -> v.ReadPos), current |> choose (fun v -> v.CompletedPos)
            match waitMode with
            | OriginalWorkOnly ->   log.Information("FeedMonitor {totalTime:n1}s Awaiting Started {starting} Completed {completed}",
                                                    sw.ElapsedSeconds, startReadPositions, completed)
            | IncludeSubsequent ->  log.Information("FeedMonitor {totalTime:n1}s Awaiting Running. Current {current} Completed {completed} Starting {starting}",
                                                    sw.ElapsedSeconds, currentRead, completed, startReadPositions)
            | AwaitFullyCaughtUp -> let draining = current |> choose (fun v -> if TranchePosition.isDrained v then ValueNone else ValueSome ()) |> Array.map ValueTuple.fst
                                    log.Information("FeedMonitor {totalTime:n1}s Awaiting Tails {tranches}. Current {current} Completed {completed} Starting {starting}",
                                                    sw.ElapsedSeconds, draining, currentRead, completed, startReadPositions)
        let busy () =
            let current = fetchPositions ()
            match waitMode with
            | OriginalWorkOnly ->   let completed = current |> choose (fun v -> v.CompletedPos)
                                    let trancheCompletedPos = System.Collections.Generic.Dictionary(completed |> Seq.map ValueTuple.toKvp)
                                    let startPosStillPendingCompletion trancheStartPos trancheId =
                                        match trancheCompletedPos.TryGetValue trancheId with
                                        | true, v -> v <= trancheStartPos
                                        | false, _ -> false
                                    startReadPositions |> Seq.exists (fun struct (t, s) -> startPosStillPendingCompletion s t)
            | IncludeSubsequent ->  current |> Array.exists (not << TranchePosition.isEmpty << ValueTuple.snd) // All work (including follow-on work) completed
            | AwaitFullyCaughtUp -> current |> allDrained |> not
        while busy () && notEol () do
            if logInterval.IfDueRestart() then logWaitStatusUpdateNow ()
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
            //      max (propagationTimeout.TotalSeconds / 4.) ((propagation.TotalSeconds + processing.TotalSeconds) / 3.) |> TimeSpan.seconds
            ?lingerTime: bool -> TimeSpan -> TimeSpan -> TimeSpan -> TimeSpan,
            ?ct) = task {
        let ct = defaultArg ct CancellationToken.None
        let sw = Stopwatch.start ()
        let sleep = defaultArg sleep (TimeSpan.FromMilliseconds 1)
        let currentCompleted = seq { for k, v in fetchPositions () -> struct (k, ValueOption.toNullable v.CompletedPos) }
        let waitMode =
            match ignoreSubsequent, awaitFullyCaughtUp with
            | Some true, Some true -> invalidArg (nameof awaitFullyCaughtUp) $"cannot be combined with {nameof ignoreSubsequent}"
            | _, Some true -> AwaitFullyCaughtUp
            | Some true, _ -> OriginalWorkOnly
            | _ -> OriginalWorkOnly
        let requireTail = match waitMode with AwaitFullyCaughtUp -> true | _ -> false
        let orDummyValue = ValueOption.orElse (ValueSome (Position.parse -1L))
        let activeTranches () =
            match fetchPositions () with
            | xs when Array.any xs && requireTail && xs |> Array.forall (ValueTuple.snd >> TranchePosition.isDrained) ->
                xs |> choose (fun v -> v.ReadPos |> orDummyValue)
            | xs when xs |> Array.forall (fun struct (_, v) -> TranchePosition.isEmpty v && (not requireTail || v.IsTail)) -> Array.empty
            | originals -> originals |> choose (fun v -> v.ReadPos)
        match! awaitPropagation sleep propagationDelay activeTranches ct with
        | [||] ->
            if propagationDelay = TimeSpan.Zero then log.Debug("FeedSource Wait Skipped; no processing pending. Completed {completed}", currentCompleted)
            else log.Warning("FeedMonitor Wait {propagationDelay:n1}s Timeout. Completed {completed}", sw.ElapsedSeconds, currentCompleted)
        | starting ->
            let propUsed = sw.Elapsed
            let logInterval = defaultArg logInterval (TimeSpan.seconds 5)
            let swProcessing = Stopwatch.start ()
            do! awaitCompletion (sleep, logInterval) swProcessing starting waitMode ct
            let procUsed = swProcessing.Elapsed
            let isDrainedNow = fetchPositions >> allDrained
            let linger = match lingerTime with None -> TimeSpan.Zero | Some lingerF -> lingerF (isDrainedNow ()) propagationDelay propUsed procUsed
            let skipLinger = linger = TimeSpan.Zero
            let ll = if skipLinger then LogEventLevel.Information else LogEventLevel.Debug
            let originalCompleted = currentCompleted |> Seq.cache
            if log.IsEnabled ll then
                let completed = fetchPositions () |> choose (fun v -> v.CompletedPos |> orDummyValue)
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

type SourcePipeline = Propulsion.SourcePipeline<FeedMonitor, TranchePositions>

module FeedMonitor =

    /// Pumps to the Sink until either the specified timeout has been reached, or all items in the Source have been fully consumed
    let runUntilCaughtUp
            (start: unit -> SourcePipeline)
            (timeout: TimeSpan, statsInterval: IntervalTimer) = task {
        let sw = Stopwatch.start ()
        // Kick off reading from the source (Disposal will Stop it if we're exiting due to a timeout; we'll spin up a fresh one when re-triggered)
        use pipeline = start ()

        try // In the case of sustained activity and/or catch-up scenarios, proactively trigger an orderly shutdown of the Source
            // in advance of the Lambda being killed (no point starting new work or incurring DynamoDB CU consumption that won't finish)
            Task.Delay(timeout).ContinueWith(fun _ -> pipeline.Stop()) |> ignore

            // If for some reason we're not provisioned well enough to read something within 1m, no point for paying for a full lambda timeout
            let initialReaderTimeout = TimeSpan.FromMinutes 1.
            do! pipeline.Monitor.AwaitCompletion(initialReaderTimeout, awaitFullyCaughtUp = true, logInterval = TimeSpan.seconds 30)
            // Shut down all processing (we create a fresh Source per Lambda invocation)
            pipeline.Stop()
            do! pipeline.Wait()
            let! res = pipeline.Flush() // TOCONSIDER should also go in a finally and/or have an IAsyncDisposable mon the SourcePipeline manage it
            if sw.ElapsedSeconds > 2 then statsInterval.Trigger()
            return res
            // force a final attempt to flush (normally checkpointing is at 5s intervals)
        finally statsInterval.SleepUntilTriggerCleared() }
