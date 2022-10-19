namespace Propulsion.Feed.Core

open FSharp.Control
open Propulsion
open Propulsion.Feed
open Propulsion.Internal
open System
open System.Collections.Generic

/// Drives reading and checkpointing for a set of feeds (tranches) of a custom source feed
type FeedSourceBase internal
    (   log : Serilog.ILogger, statsInterval : TimeSpan, sourceId,
        checkpoints : IFeedCheckpointStore, establishOrigin : (TrancheId -> Async<Position>) option,
        sink : Propulsion.Streams.Default.Sink,
        renderPos : Position -> string,
        ?logCommitFailure, ?stopAtTail) =
    let log = log.ForContext("source", sourceId)
    let logForTranche trancheId = log.ForContext("tranche", trancheId)
    let positions = TranchePositions()
    let pumpPartition crawl (ingester : Ingestion.Ingester<_>) trancheId = async {
        let log = logForTranche trancheId
        let ingest = positions.Intercept(trancheId) >> ingester.Ingest
        let reader = FeedReader(log, sourceId, trancheId, statsInterval, crawl trancheId, ingest, checkpoints.Commit, renderPos,
                                ?logCommitFailure = logCommitFailure, ?stopAtTail = stopAtTail)
        try try let! freq, pos = checkpoints.Start(sourceId, trancheId, ?establishOrigin = (establishOrigin |> Option.map (fun f -> f trancheId)))
                log.Information("Reading {source:l}/{tranche:l} From {pos} Checkpoint Event interval {checkpointFreq:n1}m",
                                sourceId, trancheId, renderPos pos, freq.TotalMinutes)
                return! reader.Pump(pos)
            with e ->
                log.Warning(e, "Exception encountered while running reader, exiting loop")
                return! Async.Raise e
        finally ingester.Stop() }
    let mutable ingesters = Array.empty<Ingestion.Ingester<_>>

    member val internal Positions = positions

    /// Runs checkpointing functions for any batches with unwritten checkpoints
    /// Yields current Tranche Positions
    member _.Checkpoint() : Async<IReadOnlyDictionary<TrancheId, Position>> = async {
        let! ct = Async.CancellationToken
        do! Async.Parallel(seq { for x in ingesters -> Async.AwaitTask(x.FlushProgress ct) }) |> Async.Ignore<unit array>
        return positions.Completed() }

    /// Propagates exceptions raised by <c>readTranches</c> or <c>crawl</c>,
    member internal _.Pump
        (   readTranches : unit -> Async<TrancheId[]>,
            // Responsible for managing retries and back offs; yielding an exception will result in abend of the read loop
            crawl : TrancheId -> bool * Position -> AsyncSeq<struct (TimeSpan * Batch<_>)>) = async {
        // TODO implement behavior to pick up newly added tranches by periodically re-running readTranches
        // TODO when that's done, remove workaround in readTranches
        let! (tranches : TrancheId array) = readTranches ()
        log.Information("Starting {tranches} tranche readers...", tranches.Length)
        ingesters <- tranches |> Array.mapi (fun partitionId trancheId -> sink.StartIngester(logForTranche trancheId, partitionId))
        let trancheWorkflows = (ingesters, tranches) ||> Seq.map2 (pumpPartition crawl)
        return! Async.Parallel trancheWorkflows |> Async.Ignore<unit[]> }

    member x.Start(pump) =
        let ct, stop =
            let cts = new System.Threading.CancellationTokenSource()
            let stop () =
                if not cts.IsCancellationRequested then log.Information "Source stopping..."
                cts.Cancel()
            cts.Token, stop

        let supervise, markCompleted, outcomeTask =
            let tcs = System.Threading.Tasks.TaskCompletionSource<unit>()
            let recordExn (e : exn) = tcs.TrySetException e |> ignore
            // first exception from a supervised task becomes the outcome if that happens
            let supervise inner = async {
                try do! inner
                with e ->
                    log.Warning(e, "Exception encountered while running source, exiting loop")
                    recordExn e }
            supervise, (fun () -> tcs.TrySetResult () |> ignore), tcs.Task

        let supervise () = task {
            // external cancellation should yield a success result (in the absence of failures from the supervised tasks)
            use _ = ct.Register(fun _t -> markCompleted ())

            // Start the work on an independent task; if it fails, it'll flow via the TCS.TrySetException into outcomeTask's Result
            Async.Start(supervise pump, cancellationToken = ct)

            try return! outcomeTask
            finally log.Information "... source stopped" }

        let monitor = lazy FeedMonitor(log, positions, sink, fun () -> Task.isCompleted outcomeTask)
        new SourcePipeline<_>(Task.run supervise, stop, monitor)

/// Intercepts receipt and completion of batches, recording the read and completion positions
and internal TranchePositions() =
    let positions = System.Collections.Concurrent.ConcurrentDictionary<TrancheId, TrancheState>()

    member _.Intercept(trancheId) =
        positions.GetOrAdd(trancheId, fun _trancheId -> { read = ValueNone; isTail = false; completed = ValueNone }) |> ignore
        fun (batch : Ingestion.Batch<_>) ->
            let p = positions[trancheId]
            p.read <- ValueSome batch.epoch
            p.isTail <- batch.isTail
            let onCompletion () =
                batch.onCompletion()
                positions[trancheId].completed <- ValueSome batch.epoch
            { batch with onCompletion = onCompletion }
    member _.Current() = positions.ToArray()
    member x.Completed() : IReadOnlyDictionary<TrancheId, Position> =
        seq { for kv in x.Current() do match kv.Value.completed with ValueNone -> () | ValueSome c -> (kv.Key, Position.parse c) }
        |> readOnlyDict

/// Represents the current state of a Tranche of the source's processing
and internal TrancheState =
    { mutable read : int64 voption; mutable isTail : bool; mutable completed : int64 voption }
    member x.IsEmpty = x.completed = x.read

and [<Struct; NoComparison; NoEquality>] private WaitMode = OriginalWorkOnly | IncludeSubsequent | AwaitFullyCaughtUp
and FeedMonitor internal (log : Serilog.ILogger, positions : TranchePositions, sink : Propulsion.Streams.Default.Sink, completed) =

    let notEol () = not sink.IsCompleted && not (completed ())
    let choose f (xs : KeyValuePair<_, _> array) = [| for x in xs do match f x.Value with ValueNone -> () | ValueSome v' -> struct (x.Key, v') |]
    let activeReadPositions () =
        match positions.Current() with
        | xs when xs |> Array.forall (fun (kv : KeyValuePair<_, TrancheState>)  -> kv.Value.IsEmpty) -> Array.empty
        | originals -> originals |> choose (fun v -> v.read)
    // Waits for up to propagationDelay, returning the opening tranche positions observed (or empty if the wait has timed out)
    let awaitPropagation (sleepMs : int) (propagationDelay : TimeSpan) = async {
        let timeout = IntervalTimer propagationDelay
        let mutable startPositions = activeReadPositions ()
        while Array.isEmpty startPositions && not timeout.IsDue && notEol () do
            do! Async.Sleep sleepMs
            startPositions <- activeReadPositions ()
        return startPositions }
    // Waits for up to lingerTime for work to arrive. If any work arrives, it waits for activity (including extra work that arrived during the wait) to quiesce.
    // If the lingerTime expires without work having completed, returns the start positions from when activity commenced
    let awaitLinger (sleepMs : int) (lingerTime : TimeSpan) = async {
        let timeout = IntervalTimer lingerTime
        let mutable startPositions = activeReadPositions ()
        let mutable worked = false
        while (Array.any startPositions || not worked) && not timeout.IsDue && notEol () do
            do! Async.Sleep sleepMs
            let current = activeReadPositions ()
            if not worked && Array.any current then startPositions <- current; worked <- true // Starting: Record start position (for if we exit due to timeout)
            elif worked && Array.isEmpty current then startPositions <- current // Finished now: clear starting position record, triggering normal exit of loop
        return startPositions }
    let isDrained : KeyValuePair<_, TrancheState> array -> bool = Array.forall (fun (KeyValue (_t, s)) -> s.isTail && s.IsEmpty)
    let awaitCompletion (sleepMs : int, logInterval) (sw : System.Diagnostics.Stopwatch) startReadPositions waitMode = async {
        let logInterval = IntervalTimer logInterval
        let logWaitStatusUpdateNow () =
            let current = positions.Current()
            let currentRead, completed = current |> choose (fun v -> v.read), current |> choose (fun v -> v.completed)
            match waitMode with
            | OriginalWorkOnly ->   log.Information("FeedMonitor {totalTime:n1}s Awaiting Started {starting} Completed {completed}",
                                                    sw.ElapsedSeconds, startReadPositions, completed)
            | IncludeSubsequent ->  log.Information("FeedMonitor {totalTime:n1}s Awaiting Running. Current {current} Completed {completed} Starting {starting}",
                                                    sw.ElapsedSeconds, currentRead, completed, startReadPositions)
            | AwaitFullyCaughtUp -> let catchingUp = current |> choose (fun v -> if not v.isTail then ValueSome () else ValueNone) |> Array.map ValueTuple.fst
                                    log.Information("FeedMonitor {totalTime:n1}s Awaiting Catchup {tranches}. Current {current} Completed {completed} Starting {starting}",
                                                    sw.ElapsedSeconds, catchingUp, currentRead, completed, startReadPositions)
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
            do! Async.Sleep sleepMs }

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
            ?delay,
            // interval at which to log status of the Await (to assist in analyzing stuck Sinks). Default 5s.
            ?logInterval,
            // Inhibit waiting for the handling of follow-on events that arrived after the wait commenced.
            ?ignoreSubsequent,
            // Whether to wait for completed work to reach the tail [of all tranches]. Default off.
            ?awaitFullyCaughtUp,
            // Compute time to wait subsequent to processing for trailing events, based on:
            // - propagationTimeout: the propagationDelay as supplied
            // - propagation: the observed propagation time
            // - processing: the observed processing time
            // Example:
            //   let lingerTime _isDrained (propagationTimeout : TimeSpan) (propagation : TimeSpan) (processing : TimeSpan) =
            //      max (propagationTimeout.TotalSeconds / 4.) ((propagation.TotalSeconds + processing.TotalSeconds) / 3.) |> TimeSpan.FromSeconds
            ?lingerTime : bool -> TimeSpan -> TimeSpan -> TimeSpan -> TimeSpan) = async {
        let sw = Stopwatch.start ()
        let delayMs = delay |> Option.map (fun (d : TimeSpan) -> int d.TotalMilliseconds) |> Option.defaultValue 1
        let currentCompleted = seq { for kv in positions.Current() -> struct (kv.Key, ValueOption.toNullable kv.Value.completed) }
        match! awaitPropagation delayMs propagationDelay with
        | [||] ->
            if propagationDelay = TimeSpan.Zero then log.Debug("FeedSource Wait Skipped; no processing pending. Completed {completed}", currentCompleted)
            else log.Information("FeedMonitor Wait {propagationDelay:n1}s Timeout. Completed {completed}", sw.ElapsedSeconds, currentCompleted)
        | starting ->
            let propUsed = sw.Elapsed
            let logInterval = defaultArg logInterval (TimeSpan.FromSeconds 5.)
            let swProcessing = Stopwatch.start ()
            let waitMode =
                match ignoreSubsequent, awaitFullyCaughtUp with
                | Some true, Some true -> invalidArg (nameof awaitFullyCaughtUp) "cannot be combine with ignoreSubsequent"
                | _, Some true -> AwaitFullyCaughtUp
                | Some true, _ -> IncludeSubsequent
                | _ -> OriginalWorkOnly
            do! awaitCompletion (delayMs , logInterval) swProcessing starting waitMode
            let procUsed = swProcessing.Elapsed
            let isDrainedNow () = positions.Current() |> isDrained
            let linger = match lingerTime with None -> TimeSpan.Zero | Some lingerF -> lingerF (isDrainedNow ()) propagationDelay propUsed procUsed
            let skipLinger = linger = TimeSpan.Zero
            let ll = if skipLinger then Serilog.Events.LogEventLevel.Information else Serilog.Events.LogEventLevel.Debug
            let originalCompleted = currentCompleted |> Seq.cache
            if log.IsEnabled ll then
                let completed = positions.Current() |> choose (fun v -> v.completed)
                log.Write(ll, "FeedMonitor Wait {totalTime:n1}s Processed Propagate {propagate:n1}s/{propTimeout:n1}s Process {process:n1}s Tail {allAtTail} Starting {starting} Completed {completed}",
                          sw.ElapsedSeconds, propUsed.TotalSeconds, propagationDelay.TotalSeconds, procUsed.TotalSeconds, isDrainedNow (), starting, completed)
            if not skipLinger then
                let swLinger = Stopwatch.start ()
                match! awaitLinger delayMs linger with
                | [||] ->
                    log.Information("FeedMonitor Wait {totalTime:n1}s OK Propagate {propagate:n1}/{propTimeout:n1}s Process {process:n1}s Linger {lingered:n1}/{linger:n1}s Tail {allAtTail}. Starting {starting} Completed {completed}",
                                    sw.ElapsedSeconds, propUsed.TotalSeconds, propagationDelay.TotalSeconds, procUsed.TotalSeconds, swLinger.ElapsedSeconds, linger.TotalSeconds, isDrainedNow (), starting, originalCompleted)
                | lingering ->
                    do! awaitCompletion (delayMs, logInterval) swProcessing lingering waitMode
                    log.Information("FeedMonitor Wait {totalTime:n1}s Lingered Propagate {propagate:n1}/{propTimeout:n1}s Process {process:n1}s Linger {lingered:n1}/{linger:n1}s Tail {allAtTail}. Starting {starting} Lingering {lingering} Completed {completed}",
                                    sw.ElapsedSeconds, propUsed.TotalSeconds, propagationDelay.TotalSeconds, procUsed.TotalSeconds, swLinger.ElapsedSeconds, linger, isDrainedNow (), starting, lingering, currentCompleted)
            // If the sink Faulted, let the awaiter observe the associated Exception that triggered the shutdown
            if sink.IsCompleted && not sink.RanToCompletion then
                return! sink.AwaitShutdown() }

/// Drives reading and checkpointing from a source that contains data from multiple streams
type TailingFeedSource
    (   log : Serilog.ILogger, statsInterval : TimeSpan,
        sourceId, tailSleepInterval : TimeSpan,
        checkpoints : IFeedCheckpointStore, establishOrigin : (TrancheId -> Async<Position>) option, sink : Propulsion.Streams.Default.Sink,
        crawl : TrancheId * Position -> AsyncSeq<struct (TimeSpan * Batch<_>)>,
        renderPos,
        ?logReadFailure, ?readFailureSleepInterval, ?logCommitFailure, ?stopAtTail) =
    inherit FeedSourceBase(log, statsInterval, sourceId, checkpoints, establishOrigin, sink, renderPos, ?logCommitFailure = logCommitFailure, ?stopAtTail = stopAtTail)

    let crawl trancheId (wasLast, startPos) = asyncSeq {
        if wasLast then
            do! Async.Sleep tailSleepInterval
        try let batches = crawl (trancheId, startPos)
            for batch in batches do
                yield batch
        with e -> // Swallow (and sleep, if requested) if there's an issue reading from a tailing log
            match logReadFailure with None -> log.ForContext<TailingFeedSource>().Warning(e, "Read failure") | Some l -> l e
            match readFailureSleepInterval with None -> () | Some interval -> do! Async.Sleep(TimeSpan.toMs interval) }

    member _.Pump(readTranches) =
        base.Pump(readTranches, crawl)

/// Drives reading and checkpointing from a source that aggregates data from multiple streams as a singular source
/// without shards/physical partitions (tranches), such as the SqlStreamStore and EventStoreDB $all feeds
/// Per the API design of such stores, readBatch also only ever yields a single page
type AllFeedSource
    (   log : Serilog.ILogger, statsInterval : TimeSpan,
        sourceId, tailSleepInterval : TimeSpan,
        readBatch : Position -> Async<Batch<_>>,
        checkpoints : IFeedCheckpointStore, sink : Propulsion.Streams.Default.Sink,
        // Custom checkpoint rendering logic
        ?renderPos,
        // Custom logic to derive an origin position if the checkpoint store doesn't have one
        // facilitates implementing a 'startFromTail' behavior
        ?establishOrigin) =
    inherit TailingFeedSource
        (   log, statsInterval, sourceId, tailSleepInterval,
            checkpoints, establishOrigin, sink,
            (fun (_trancheId, pos) -> asyncSeq {
                  let sw = Stopwatch.start ()
                  let! b = readBatch pos
                  yield sw.Elapsed, b } ),
            renderPos = defaultArg renderPos string)

    member internal _.Pump =
        let readTranches () = async { return [| TrancheId.parse "0" |] }
        base.Pump(readTranches)

    member x.Start() =
        base.Start x.Pump

/// Drives reading from the Source until the Tail of each Tranche has been reached
/// Useful for ingestion
type SinglePassFeedSource
    (   log : Serilog.ILogger, statsInterval : TimeSpan,
        sourceId,
        checkpoints : IFeedCheckpointStore, sink : Propulsion.Streams.Default.Sink,
        crawl : TrancheId * Position -> AsyncSeq<struct (TimeSpan * Batch<_>)>,
        renderPos,
        ?logReadFailure, ?readFailureSleepInterval, ?logCommitFailure) =
    inherit TailingFeedSource(log, statsInterval, sourceId, TimeSpan.Zero, checkpoints, None, sink, crawl, renderPos,
                              ?logReadFailure = logReadFailure, ?readFailureSleepInterval = readFailureSleepInterval, ?logCommitFailure = logCommitFailure,
                              stopAtTail = true)

namespace Propulsion.Feed

open FSharp.Control
open Propulsion.Internal
open System

[<NoComparison; NoEquality>]
type Page<'F> = { items : FsCodec.ITimelineEvent<'F>[]; checkpoint : Position; isTail : bool }

/// Drives reading and checkpointing for a set of change feeds (tranches) of a custom data source that can represent their
///   content as an append-only data source with a change feed wherein each <c>FsCodec.ITimelineEvent</c> has a monotonically increasing <c>Index</c>. <br/>
/// Processing concludes if <c>readTranches</c> and <c>readPage</c> throw, in which case the <c>Pump</c> loop terminates, propagating the exception.
type FeedSource
    (   log : Serilog.ILogger, statsInterval : TimeSpan,
        sourceId, tailSleepInterval : TimeSpan,
        checkpoints : IFeedCheckpointStore, sink : Propulsion.Streams.Default.Sink,
        // Responsible for managing retries and back offs; yielding an exception will result in abend of the read loop
        readPage : TrancheId * Position -> Async<Page<_>>,
        ?renderPos) =
    inherit Core.FeedSourceBase(log, statsInterval, sourceId, checkpoints, None, sink, defaultArg renderPos string)

    let crawl trancheId =
        let streamName = FsCodec.StreamName.compose "Messages" [SourceId.toString sourceId; TrancheId.toString trancheId]
        fun (wasLast, pos) -> asyncSeq {
            if wasLast then
                do! Async.Sleep(TimeSpan.toMs tailSleepInterval)
            let readTs = Stopwatch.timestamp ()
            let! page = readPage (trancheId, pos)
            let items' = page.items |> Array.map (fun x -> struct (streamName, x))
            yield struct (Stopwatch.elapsed readTs, ({ items = items'; checkpoint = page.checkpoint; isTail = page.isTail } : Core.Batch<_>))
        }

    /// Drives the continual loop of reading and checkpointing each tranche until a fault occurs. <br/>
    /// The <c>readTranches</c> and <c>readPage</c> functions are expected to manage their own resilience strategies (retries etc). <br/>
    /// Any exception from <c>readTranches</c> or <c>readPage</c> will be propagated in order to enable termination of the overall projector loop
    member _.Pump(readTranches : unit -> Async<TrancheId[]>) =
        base.Pump(readTranches, crawl)
