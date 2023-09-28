namespace Propulsion.Feed.Core

open FSharp.Control
open Microsoft.FSharp.Core
open Propulsion
open Propulsion.Feed
open Propulsion.Internal
open System
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks

/// Drives reading and checkpointing for a set of feeds (tranches) of a custom source feed
type FeedSourceBase internal
    (   log: Serilog.ILogger, statsInterval: TimeSpan, sourceId,
        checkpoints: IFeedCheckpointStore, establishOrigin: Func<TrancheId, CancellationToken, Task<Position>> option,
        sink: Propulsion.Sinks.Sink,
        renderPos: Position -> string,
        ?logCommitFailure, ?readersStopAtTail) =
    let log = log.ForContext("source", sourceId)
    let positions = TranchePositions()
    let pumpPartition (partitionId: int) trancheId struct (ingester: Ingestion.Ingester<_>, reader: FeedReader) ct = task {
        try let log (e: exn) = reader.Log.Warning(e, "Finishing {partition}", partitionId)
            let establishTrancheOrigin (f: Func<_, CancellationToken, _>) = Func<_, _>(fun ct -> f.Invoke(trancheId, ct))
            try let! pos = checkpoints.Start(sourceId, trancheId, establishOrigin = (establishOrigin |> Option.map establishTrancheOrigin), ct = ct)
                reader.Log.Information("Reading {partition} {source:l}/{tranche:l} From {pos}",
                                       partitionId, sourceId, trancheId, renderPos pos)
                return! reader.Pump(pos, ct)
            with Exception.Log log () -> ()
        finally ingester.Stop() }
    let mutable partitions = Array.empty<struct(Ingestion.Ingester<_> * FeedReader)>
    let dumpStats () = for _i, r in partitions do r.DumpStats()
    let rec pumpStats ct: Task = task {
        try do! Task.delay statsInterval ct
        finally dumpStats () // finally is so we do a final write after we are cancelled, which would otherwise stop us after the sleep
        return! pumpStats ct }

    member val internal Positions = positions

    /// Runs checkpointing functions for any batches with unwritten checkpoints
    /// Yields current Tranche Positions
    member _.Checkpoint(ct): Task<IReadOnlyDictionary<TrancheId, Position>> = task {
        do! Task.parallelLimit 4 ct (seq { for i, _r in partitions -> i.FlushProgress }) |> Task.ignore<unit[]>
        return positions.Completed() }

    /// Propagates exceptions raised by <c>readTranches</c> or <c>crawl</c>,
    member internal x.Pump
        (   readTranches: CancellationToken -> Task<TrancheId[]>,
            // Responsible for managing retries and back offs; yielding an exception will result in abend of the read loop
            crawl: TrancheId -> bool * Position -> CancellationToken -> IAsyncEnumerable<struct (TimeSpan * Batch<_>)>,
            ct) = task {
        // TODO implement behavior to pick up newly added tranches by periodically re-running readTranches
        // TODO when that's done, remove workaround in readTranches
        let! (tranches: TrancheId[]) = readTranches ct
        log.Information("Starting {tranches} tranche readers...", tranches.Length)
        partitions <- tranches |> Array.mapi (fun partitionId trancheId ->
            let log = log.ForContext("partition", partitionId).ForContext("tranche", trancheId)
            let ingester = sink.StartIngester(log, partitionId)
            let ingest = positions.Intercept(trancheId) >> ingester.Ingest
            let awaitIngester = if defaultArg readersStopAtTail false then Some ingester.Wait else None
            let reader = FeedReader(log, partitionId, sourceId, trancheId, crawl trancheId, ingest, checkpoints.Commit, renderPos,
                                    ?logCommitFailure = logCommitFailure, ?awaitIngesterShutdown = awaitIngester)
            ingester, reader)
        pumpStats ct |> ignore // loops in background until overall pumping is cancelled
        let trancheWorkflows = (tranches, partitions) ||> Seq.mapi2 pumpPartition
        do! Task.parallelUnlimited ct trancheWorkflows |> Task.ignore<unit[]>
        do! x.Checkpoint(ct) |> Task.ignore }

    member x.Start(pump) =
        let ct, stop =
            let cts = new CancellationTokenSource()
            let stop disposing =
                if not cts.IsCancellationRequested && not disposing then log.Information "Source stopping..."
                cts.Cancel()
            cts.Token, stop

        let supervise, markCompleted, outcomeTask =
            let tcs = System.Threading.Tasks.TaskCompletionSource<unit>()
            let markCompleted () = tcs.TrySetResult () |> ignore
            let recordExn (e: exn) = tcs.TrySetException e |> ignore
            // first exception from a supervised task becomes the outcome if that happens
            let supervise inner () = task {
                try do! inner ct
                    // If the source completes all reading cleanly, declare completion
                    log.Information "Source drained..."
                    markCompleted ()
                with e ->
                    log.Warning(e, "Exception encountered while running source, exiting loop")
                    recordExn e }
            supervise, markCompleted, tcs.Task

        let supervise () = task {
            // external cancellation should yield a success result (in the absence of failures from the supervised tasks)
            use _ = ct.Register(fun _t -> markCompleted ())

            // Start the work on an independent task; if it fails, it'll flow via the TCS.TrySetException into outcomeTask's Result
            Task.start(supervise pump)

            try return! outcomeTask
            finally log.Information "... source completed" }

        let monitor = lazy FeedMonitor(log, positions, sink, fun () -> outcomeTask.IsCompleted)
        new SourcePipeline<_>(Task.run supervise, stop, monitor)

/// Intercepts receipt and completion of batches, recording the read and completion positions
and internal TranchePositions() =
    let positions = System.Collections.Concurrent.ConcurrentDictionary<TrancheId, TrancheState>()

    member _.Intercept(trancheId) =
        positions.GetOrAdd(trancheId, fun _trancheId -> { read = ValueNone; isTail = false; completed = ValueNone }) |> ignore
        fun (batch: Ingestion.Batch<_>) ->
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
and internal TrancheState =
    { mutable read: int64 voption; mutable isTail: bool; mutable completed: int64 voption }
    member x.IsEmpty = x.completed = x.read

and [<Struct; NoComparison; NoEquality>] private WaitMode = OriginalWorkOnly | IncludeSubsequent | AwaitFullyCaughtUp
and FeedMonitor internal (log: Serilog.ILogger, positions: TranchePositions, sink: Propulsion.Sinks.Sink, sourceIsCompleted) =

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

/// Drives reading and checkpointing from a source that contains data from multiple streams
type TailingFeedSource
    (   log: Serilog.ILogger, statsInterval: TimeSpan,
        sourceId, tailSleepInterval: TimeSpan,
        checkpoints: IFeedCheckpointStore, establishOrigin, sink: Propulsion.Sinks.Sink, renderPos,
        crawl: Func<TrancheId, Position, CancellationToken, IAsyncEnumerable<struct (TimeSpan * Batch<Propulsion.Sinks.EventBody>)>>,
        ?logReadFailure, ?readFailureSleepInterval: TimeSpan, ?logCommitFailure, ?readersStopAtTail) =
    inherit FeedSourceBase(log, statsInterval, sourceId, checkpoints, establishOrigin, sink, renderPos,
                           ?logCommitFailure = logCommitFailure, ?readersStopAtTail = readersStopAtTail)

    let crawl trancheId (wasLast, startPos) ct = taskSeq {
        if wasLast then do! Task.delay tailSleepInterval ct
        try let batches = crawl.Invoke(trancheId, startPos, ct)
            for batch in batches do
                yield batch
        with e -> // Swallow (and sleep, if requested) if there's an issue reading from a tailing log
            match logReadFailure with None -> log.ForContext("tranche", trancheId).ForContext<TailingFeedSource>().Warning(e, "Read failure") | Some l -> l e
            match readFailureSleepInterval with None -> () | Some interval -> do! Task.delay interval ct }

    member _.Pump(readTranches, ct) =
        base.Pump(readTranches, crawl, ct)

module TailingFeedSource =

    let readOne (readBatch: _ -> Task<_>) cat pos ct = taskSeq {
        let sw = Stopwatch.start ()
        let! b = readBatch struct (cat, pos, ct)
        yield struct (sw.Elapsed, b) }

/// Drives reading and checkpointing from a source that aggregates data from multiple streams as a singular source
/// without shards/physical partitions (tranches), such as the SqlStreamStore and EventStoreDB $all feeds
/// Per the API design of such stores, readBatch also only ever yields a single page
type AllFeedSource
    (   log: Serilog.ILogger, statsInterval: TimeSpan,
        sourceId, tailSleepInterval: TimeSpan,
        readBatch: Func<Position, CancellationToken, Task<Batch<Propulsion.Sinks.EventBody>>>,
        checkpoints: IFeedCheckpointStore, sink: Propulsion.Sinks.Sink,
        // Custom checkpoint rendering logic
        ?renderPos,
        // Custom logic to derive an origin position if the checkpoint store doesn't have one
        // facilitates implementing a 'startFromTail' behavior
        ?establishOrigin) =
    inherit TailingFeedSource
        (   log, statsInterval, sourceId, tailSleepInterval, checkpoints, establishOrigin, sink, defaultArg renderPos string,
            TailingFeedSource.readOne (fun struct (_cat, p, c) -> readBatch.Invoke(p, c)))

    member internal _.Pump(ct) =
        let readTranches _ct = task { return [| TrancheId.parse "0" |] }
        base.Pump(readTranches, ct)

    member x.Start() =
        base.Start(x.Pump)

/// Drives reading from the Source, stopping when the Tail of each of the Tranches has been reached
type SinglePassFeedSource
    (   log: Serilog.ILogger, statsInterval: TimeSpan,
        sourceId,
        crawl: Func<TrancheId, Position, CancellationToken, IAsyncEnumerable<struct (TimeSpan * Batch<_>)>>,
        checkpoints: IFeedCheckpointStore, sink: Propulsion.Sinks.Sink,
        ?renderPos, ?logReadFailure, ?readFailureSleepInterval, ?logCommitFailure) =
    inherit TailingFeedSource(log, statsInterval, sourceId, (*tailSleepInterval*)TimeSpan.Zero, checkpoints, (*establishOrigin*)None, sink, defaultArg renderPos string,
                              crawl,
                              ?logReadFailure = logReadFailure, ?readFailureSleepInterval = readFailureSleepInterval, ?logCommitFailure = logCommitFailure,
                              readersStopAtTail = true)

    member x.Start(readTranches) =
        base.Start(fun ct -> x.Pump(readTranches, ct))

module Categories =
    let startsWith (p: string) (s: FsCodec.StreamName) = (FsCodec.StreamName.toString s).StartsWith(p)

    let categoryFilter (categories: string[]) =
        let prefixes = categories |> Array.map startsWith
        fun (x: FsCodec.StreamName) -> prefixes |> Array.exists (fun f -> f x)

    let mapFilters categories streamFilter =
        match categories, streamFilter with
        | None, None ->                   fun _ -> true
        | Some categories, None -> categoryFilter categories
        | None, Some (filter: Func<_, bool>) -> filter.Invoke
        | Some categories, Some filter ->
            let categoryFilter = categoryFilter categories
            fun x -> categoryFilter x && filter.Invoke x

namespace Propulsion.Feed

open FSharp.Control
open Propulsion.Internal
open System
open System.Threading
open System.Threading.Tasks

[<NoComparison; NoEquality>]
type Page<'F> = { items: FsCodec.ITimelineEvent<'F>[]; checkpoint: Position; isTail: bool }

/// Drives reading and checkpointing for a set of change feeds (tranches) of a custom data source that can represent their
///   content as an append-only data source with a change feed wherein each <c>FsCodec.ITimelineEvent</c> has a monotonically increasing <c>Index</c>. <br/>
/// Processing concludes if <c>readTranches</c> and <c>readPage</c> throw, in which case the <c>Pump</c> loop terminates, propagating the exception.
type FeedSource
    (   log: Serilog.ILogger, statsInterval: TimeSpan,
        sourceId, tailSleepInterval: TimeSpan,
        checkpoints: IFeedCheckpointStore, sink: Propulsion.Sinks.Sink,
        ?renderPos) =
    inherit Core.FeedSourceBase(log, statsInterval, sourceId, checkpoints, None, sink, defaultArg renderPos string)

    let crawl (readPage: Func<TrancheId,Position,CancellationToken,Task<Page<_>>>) trancheId =
        let streamName = FsCodec.StreamName.compose "Messages" [| SourceId.toString sourceId; TrancheId.toString trancheId |]
        fun (wasLast, pos) ct -> taskSeq {
            if wasLast then
                do! Task.delay tailSleepInterval ct
            let readTs = Stopwatch.timestamp ()
            let! page = readPage.Invoke(trancheId, pos, ct)
            let items' = page.items |> Array.map (fun x -> struct (streamName, x))
            yield struct (Stopwatch.elapsed readTs, ({ items = items'; checkpoint = page.checkpoint; isTail = page.isTail }: Core.Batch<_>)) }

    member internal _.Pump(readTranches: Func<CancellationToken, Task<TrancheId[]>>,
                  readPage: Func<TrancheId, Position, CancellationToken, Task<Page<Propulsion.Sinks.EventBody>>>, ct): Task<unit> =
        base.Pump(readTranches.Invoke, crawl readPage, ct)

    /// Drives the continual loop of reading and checkpointing each tranche until a fault occurs. <br/>
    /// The <c>readTranches</c> and <c>readPage</c> functions are expected to manage their own resilience strategies (retries etc). <br/>
    /// Any exception from <c>readTranches</c> or <c>readPage</c> will be propagated in order to enable termination of the overall projector loop
    member x.StartAsync(readTranches: Func<CancellationToken, Task<TrancheId[]>>,
                        // Responsible for managing retries and back offs; yielding an exception will result in abend of the read loop
                        readPage: Func<TrancheId, Position, CancellationToken, Task<Page<Propulsion.Sinks.EventBody>>>) =
        base.Start(fun ct -> x.Pump(readTranches, readPage, ct))

    member x.Start(readTranches: unit -> Async<TrancheId[]>, readPage: TrancheId -> Position -> Async<Page<Propulsion.Sinks.EventBody>>) =
        x.StartAsync((fun ct -> readTranches () |> Async.executeAsTask ct),
                     (fun t p ct -> readPage t p |> Async.executeAsTask ct))
