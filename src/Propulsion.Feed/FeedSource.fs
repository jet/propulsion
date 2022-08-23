namespace Propulsion.Feed.Core

open FSharp.Control
open Propulsion
open Propulsion.Feed
open System
open System.Collections.Generic
open System.Threading.Tasks

/// Drives reading and checkpointing for a set of feeds (tranches) of a custom source feed
type FeedSourceBase internal
    (   log : Serilog.ILogger, statsInterval : TimeSpan, sourceId,
        checkpoints : IFeedCheckpointStore, establishOrigin : (TrancheId -> Async<Position>) option,
        sink : Propulsion.Streams.Default.Sink,
        renderPos : Position -> string,
        ?logCommitFailure) =

    let log = log.ForContext("source", sourceId)
    let positions = TranchePositions()

    let pumpPartition crawl partitionId trancheId = async {
        let log = log.ForContext("tranche", trancheId)
        let ingester : Ingestion.Ingester<_> = sink.StartIngester(log, partitionId)
        let ingest = positions.Intercept(trancheId) >> ingester.Ingest
        let reader = FeedReader(log, sourceId, trancheId, statsInterval, crawl trancheId, ingest, checkpoints.Commit, renderPos, ?logCommitFailure = logCommitFailure)
        try let! freq, pos = checkpoints.Start(sourceId, trancheId, ?establishOrigin = (establishOrigin |> Option.map (fun f -> f trancheId)))
            log.Information("Reading {source:l}/{tranche:l} From {pos} Checkpoint Event interval {checkpointFreq:n1}m",
                            sourceId, trancheId, renderPos pos, freq.TotalMinutes)
            return! reader.Pump(pos)
        with e ->
            log.Warning(e, "Exception encountered while running reader, exiting loop")
            return! Async.Raise e }
    let pump readTranches crawl = async {
        try let! (tranches : TrancheId array) = readTranches ()

            log.Information("Starting {tranches} tranche readers...", tranches.Length)
            return! Async.Parallel(tranches |> Seq.mapi (pumpPartition crawl)) |> Async.Ignore<unit[]>
        with e ->
            log.Warning(e, "Exception encountered while running source, exiting loop")
            return! Async.Raise e }

    let choose f (xs : KeyValuePair<_, _> array) = [| for x in xs do match f x.Value with ValueNone -> () | ValueSome v' -> struct (x.Key, v') |]
    let elapsedSeconds (x : System.Diagnostics.Stopwatch) = float x.ElapsedMilliseconds / 1000.
    let checkForActivity () =
        match positions.Current() with
        | xs when xs |> Array.forall (fun (kv : KeyValuePair<_, TrancheState>)  -> kv.Value.IsEmpty) -> Array.empty
        | originals -> originals |> choose (fun v -> v.read)
    let awaitPropagation (propagationDelay : TimeSpan) (delayMs : int) = async {
        let sw, max = System.Diagnostics.Stopwatch.StartNew(), int64 propagationDelay.TotalMilliseconds
        let mutable startPositions = checkForActivity ()
        while Array.isEmpty startPositions && not sink.IsCompleted && sw.ElapsedMilliseconds < max do
            do! Async.Sleep delayMs
            startPositions <- checkForActivity ()
        return startPositions }
    let awaitCompletion starting (delayMs : int) includeSubsequent logInterval = async {
        let maybeLog =
            let logDue = Internal.intervalCheck logInterval
            fun () ->
                if logDue () then
                    let currentRead, completed =
                        let current = positions.Current()
                        current |> choose (fun v -> v.read), current |> choose (fun v -> v.completed)
                    if includeSubsequent then
                        log.Information("Feed Awaiting All. Current {current} Completed {completed} Starting {starting}",
                                        currentRead, completed, starting)
                    else log.Information("Feed Awaiting Starting {starting} Completed {completed}", starting, completed)
        let isComplete () =
            let current = positions.Current()
            let completed = current |> choose (fun v -> v.completed)
            let originalStartedAreAllCompleted () =
                let forTranche = Dictionary() in for struct (k, v) in completed do forTranche.Add(k, v)
                let hasPassed origin trancheId =
                    let mutable v = Unchecked.defaultof<_>
                    forTranche.TryGetValue(trancheId, &v) && v > origin
                starting |> Seq.forall (fun struct (t, s) -> hasPassed s t)
            current |> Array.forall (fun kv -> kv.Value.IsEmpty) // All submitted work (including follow-on work), completed
            || (not includeSubsequent && originalStartedAreAllCompleted ())
        while not (isComplete ()) && not sink.IsCompleted do
            maybeLog ()
            do! Async.Sleep delayMs }

    /// Propagates exceptions raised by <c>readTranches</c> or <c>crawl</c>,
    member internal _.Pump
        (   readTranches : unit -> Async<TrancheId[]>,
            // Responsible for managing retries and back offs; yielding an exception will result in abend of the read loop
            crawl : TrancheId -> bool * Position -> AsyncSeq<TimeSpan * Batch<_>>) =
        // TODO implement behavior to pick up newly added tranches by periodically re-running readTranches
        // TODO when that's done, remove workaround in readTranches
        pump readTranches crawl

    /// Waits (for up to the <c>propagationDelay</c>) for events to be observed
    /// If at least one event has been observed, waits for the completion of the Sink's processing
    member _.AwaitCompletion
        (   // time to wait for arrival of initial events, this should take into account:
            // - time for indexing of the events to complete based on the environment's configuration (e.g. with DynamoStore, the total Lambda and DynamoDB Streams trigger time)
            // - an adjustment to account for the polling interval that the Source is using
            propagationDelay,
            // sleep interval while awaiting completion. Default 1ms.
            ?delay,
            // interval at which to log status of the Await (to assist in analyzing stuck Sinks). Default 10s.
            ?logInterval,
            // Also wait for processing of batches that arrived subsequent to the start of the AwaitCompletion call
            ?ignoreSubsequent) = async {
        let sw = System.Diagnostics.Stopwatch.StartNew()
        let delayMs =
            let delay = defaultArg delay (TimeSpan.FromMilliseconds 1.)
            int delay.TotalMilliseconds
        match! awaitPropagation propagationDelay delayMs with
        | [||] ->
            let currentCompleted = seq { for kv in positions.Current() -> struct (kv.Key, kv.Value.completed) }
            if propagationDelay = TimeSpan.Zero then log.Debug("Feed Wait Skipped; no processing pending. Completed Epochs {completed}", currentCompleted)
            else log.Information("Feed Wait Timeout {propagationDelay:n1}s. Completed {completed}", elapsedSeconds sw, currentCompleted)
        | starting ->
            let includeSubsequent = ignoreSubsequent <> Some true
            let logInterval = defaultArg logInterval (TimeSpan.FromSeconds 10.)
            do! awaitCompletion starting delayMs includeSubsequent logInterval
            if log.IsEnabled Serilog.Events.LogEventLevel.Information then
                let completed = positions.Current() |> choose (fun v -> v.completed)
                log.Information("Feed Wait Complete {wait:n1}s Starting {starting} Completed {completed}", elapsedSeconds sw, starting, completed)
            // If the sink Faulted, let the awaiter observe the associated Exception that triggered the shutdown
            if sink.IsCompleted && not sink.RanToCompletion then
                return! sink.AwaitShutdown() }

/// Intercepts receipt and completion of batches, recording the read and completion positions
and TranchePositions() =
    let positions = System.Collections.Concurrent.ConcurrentDictionary<TrancheId, TrancheState>()

    member _.Intercept(trancheId) =
        positions.GetOrAdd(trancheId, fun _trancheId -> { read = ValueNone; completed = ValueNone }) |> ignore
        fun (batch : Ingestion.Batch<_>) ->
            positions[trancheId].read <- ValueSome batch.epoch
            let onCompletion () =
                batch.onCompletion()
                positions[trancheId].completed <- ValueSome batch.epoch
            { batch with onCompletion = onCompletion }
    member _.Current() = positions.ToArray()
/// Represents the current state of a tranche of the source's processing
and TrancheState =
    { mutable read : int64 voption; mutable completed : int64 voption }
    member x.IsEmpty = x.completed = x.read

/// Drives reading and checkpointing from a source that contains data from multiple streams. While a TrancheId is always required,
/// it may have a default value of `"0"` if the underlying source representation does not involve autonomous shards/physical partitions etc
type TailingFeedSource
    (   log : Serilog.ILogger, statsInterval : TimeSpan,
        sourceId, tailSleepInterval : TimeSpan,
        crawl : TrancheId * Position -> AsyncSeq<TimeSpan * Batch<_>>,
        checkpoints : IFeedCheckpointStore, establishOrigin : (TrancheId -> Async<Position>) option, sink : Propulsion.Streams.Default.Sink,
        renderPos,
        ?logReadFailure,
        ?readFailureSleepInterval : TimeSpan,
        ?logCommitFailure) =
    inherit FeedSourceBase(log, statsInterval, sourceId, checkpoints, establishOrigin, sink, renderPos, ?logCommitFailure = logCommitFailure)

    let crawl trancheId (wasLast, startPos) = asyncSeq {
        if wasLast then
            do! Async.Sleep tailSleepInterval
        try let batches = crawl (trancheId, startPos)
            for batch in batches do
                yield batch
        with e -> // Swallow (and sleep, if requested) if there's an issue reading from a tailing log
            match logReadFailure with None -> log.ForContext<TailingFeedSource>().Warning(e, "Read failure") | Some l -> l e
            match readFailureSleepInterval with None -> () | Some interval -> do! Async.Sleep interval }

    member _.Pump(readTranches) =
        base.Pump(readTranches, crawl)

    member x.Start(pump) =
        let cts = new System.Threading.CancellationTokenSource()
        let stop () =
            log.Information "Source stopping..."
            cts.Cancel()
        let ct = cts.Token

        let tcs = System.Threading.Tasks.TaskCompletionSource<unit>()
        let propagateExceptionToPipelineOutcome inner = async { try do! inner with e -> tcs.SetException(e) }

        let startPump () = Async.Start(propagateExceptionToPipelineOutcome pump, cancellationToken = ct)

        let supervise () = task {
            // external cancellation should yield a success result
            use _ = ct.Register(fun _ -> tcs.TrySetResult () |> ignore)

            startPump ()

            try return! tcs.Task // aka base.AwaitShutdown()
            finally log.Information "... source stopped" }
        new Pipeline(Task.Run<unit>(supervise), stop)

/// Drives reading and checkpointing from a source that aggregates data from multiple streams as a singular source
/// without shards/physical partitions (tranches), such as the SqlStreamStore, and EventStoreDB $all feeds
/// Per the API design of such stores, readBatch only ever yields a single page
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
            (fun (_trancheId, pos) -> asyncSeq {
                  let sw = System.Diagnostics.Stopwatch.StartNew()
                  let! b = readBatch pos
                  yield sw.Elapsed, b } ),
            checkpoints, establishOrigin, sink,
            renderPos = defaultArg renderPos string)

    member _.Pump() =
        let readTranches () = async { return [| TrancheId.parse "0" |] }
        base.Pump(readTranches)

    member x.Start() = base.Start(x.Pump())

namespace Propulsion.Feed

open FSharp.Control
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
                do! Async.Sleep tailSleepInterval
            let sw = System.Diagnostics.Stopwatch.StartNew()
            let! page = readPage (trancheId, pos)
            let items' = page.items |> Array.map (fun x -> struct (streamName, x))
            yield sw.Elapsed, ({ items = items'; checkpoint = page.checkpoint; isTail = page.isTail } : Core.Batch<_>)
        }

    /// Drives the continual loop of reading and checkpointing each tranche until a fault occurs. <br/>
    /// The <c>readTranches</c> and <c>readPage</c> functions are expected to manage their own resilience strategies (retries etc). <br/>
    /// Any exception from <c>readTranches</c> or <c>readPage</c> will be propagated in order to enable termination of the overall projector loop
    member _.Pump(readTranches : unit -> Async<TrancheId[]>) =
        base.Pump(readTranches, crawl)
