namespace Propulsion.Feed.Internal

open FSharp.Control
open Propulsion
open Propulsion.Feed
open System
open System.Collections.Generic
open System.Threading.Tasks

/// Intercepts receipt and completion of batches, recording the read and completion positions
type TranchePositions() =
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
and TrancheState = { mutable read : int64 voption; mutable completed : int64 voption }
module TrancheState =

    let isEmpty (x : TrancheState) = x.completed = x.read

/// Drives reading and checkpointing for a set of feeds (tranches) of a custom source feed
type FeedSourceBase internal
    (   log : Serilog.ILogger, statsInterval : TimeSpan, sourceId,
        checkpoints : IFeedCheckpointStore, establishOrigin : (TrancheId -> Async<Position>) option,
        sink : Propulsion.Streams.Sink,
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
            return! Async.Raise e
    }

    /// Propagates exceptions raised by <c>readTranches</c> or <c>crawl</c>,
    member internal _.Pump
        (   readTranches : unit -> Async<TrancheId[]>,
            // Responsible for managing retries and back offs; yielding an exception will result in abend of the read loop
            crawl : TrancheId -> bool * Position -> AsyncSeq<TimeSpan * Batch<byte[]>>) = async {
        // TODO implement behavior to pick up newly added tranches by periodically re-running readTranches
        // TODO when that's done, remove workaround in readTranches
        try let! tranches = readTranches ()
            log.Information("Starting {tranches} tranche readers...", tranches.Length)
            return! Async.Parallel(tranches |> Seq.mapi (pumpPartition crawl)) |> Async.Ignore<unit[]>
        with e ->
            log.Warning(e, "Exception encountered while running source, exiting loop")
            return! Async.Raise e }

    /// Waits until all <c>Ingest</c>ed batches have been successfully processed via the Sink
    /// NOTE this relies on specific guarantees the MemoryStore's Committed event affords us
    /// 1. a Decider's Transact will not return until such time as the Committed events have been handled
    ///      (i.e., we have prepared the batch for submission)
    /// 2. At the point where the caller triggers AwaitCompletion, we can infer that all reactions have been processed
    ///      when checkpointing/completion has passed beyond our starting point
    member _.AwaitCompletion
        (   // sleep interval while awaiting completion. Default 1ms.
            ?delay,
            // interval at which to log status of the Await (to assist in analyzing stuck Sinks). Default 10s.
            ?logInterval,
            // Also wait for processing of batches that arrived subsequent to the start of the AwaitCompletion call
            ?ignoreSubsequent) = async {
        match positions.Current() with
        | xs when xs |> Array.forall (fun kv -> TrancheState.isEmpty kv.Value) ->
            log.Debug("Feed Wait Skipped; no processing pending. Completed Epochs {epochs}", seq { for kv in xs -> struct (kv.Key, kv.Value.completed) })
        | originals ->
            let sw = System.Diagnostics.Stopwatch.StartNew()
            let choose f (xs : KeyValuePair<_,_> array) = [| for x in xs do match f x.Value with ValueNone -> () | ValueSome v' -> struct (x.Key, v') |]
            let starting = originals |> choose (fun v -> v.read)
            let includeSubsequent = ignoreSubsequent <> Some true
            let delayMs =
                let delay = defaultArg delay TimeSpan.FromMilliseconds 1.
                int delay.TotalMilliseconds
            let maybeLog =
                let logInterval = defaultArg logInterval (TimeSpan.FromSeconds 10.)
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
                current |> Array.forall (fun kv -> TrancheState.isEmpty kv.Value) // All submitted work (including follow-on work), completed
                ||  not includeSubsequent && originalStartedAreAllCompleted ()
            while not (isComplete ()) && not sink.IsCompleted do
                maybeLog ()
                do! Async.Sleep delayMs
            if log.IsEnabled Serilog.Events.LogEventLevel.Information then
                let completed = positions.Current() |> choose (fun v -> v.completed)
                let t = sw.Elapsed
                log.Information("Feed Waited {wait:n1}s Starting {starting} Completed {completed}", t.TotalSeconds, starting, completed)

            // If the sink Faulted, let the awaiter observe the associated Exception that triggered the shutdown
            if sink.IsCompleted && not sink.RanToCompletion then
                return! sink.AwaitShutdown()
    }

/// Drives reading and checkpointing from a source that contains data from multiple streams. While a TrancheId is always required,
/// it may have a default value of `"0"` if the underlying source representation does not involve autonomous shards/physical partitions etc
type TailingFeedSource
    (   log : Serilog.ILogger, statsInterval : TimeSpan,
        sourceId, tailSleepInterval : TimeSpan,
        crawl : TrancheId * Position -> AsyncSeq<TimeSpan * Batch<_>>,
        checkpoints : IFeedCheckpointStore, establishOrigin : (TrancheId -> Async<Position>) option, sink : Propulsion.Streams.Sink,
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
        checkpoints : IFeedCheckpointStore, sink : Propulsion.Streams.Sink,
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
open Propulsion
open System

[<NoComparison; NoEquality>]
type Page<'e> = { items : FsCodec.ITimelineEvent<'e>[]; checkpoint : Position; isTail : bool }

/// Drives reading and checkpointing for a set of change feeds (tranches) of a custom data source that can represent their
///   content as an append-only data source with a change feed wherein each <c>FsCodec.ITimelineEvent</c> has a monotonically increasing <c>Index</c>. <br/>
/// Processing concludes if <c>readTranches</c> and <c>readPage</c> throw, in which case the <c>Pump</c> loop terminates, propagating the exception.
type FeedSource
    (   log : Serilog.ILogger, statsInterval : TimeSpan,
        sourceId, tailSleepInterval : TimeSpan,
        checkpoints : IFeedCheckpointStore, sink : Propulsion.Streams.Sink,
        // Responsible for managing retries and back offs; yielding an exception will result in abend of the read loop
        readPage : TrancheId * Position -> Async<Page<byte array>>,
        ?renderPos) =
    inherit Internal.FeedSourceBase(log, statsInterval, sourceId, checkpoints, None, sink, defaultArg renderPos string)

    let crawl trancheId =
        let streamName = FsCodec.StreamName.compose "Messages" [SourceId.toString sourceId; TrancheId.toString trancheId]
        fun (wasLast, pos) -> asyncSeq {
            if wasLast then
                do! Async.Sleep tailSleepInterval
            let sw = System.Diagnostics.Stopwatch.StartNew()
            let! page = readPage (trancheId, pos)
            let items' = page.items |> Array.map (fun x -> struct (streamName, x))
            yield sw.Elapsed, ({ items = items'; checkpoint = page.checkpoint; isTail = page.isTail } : Internal.Batch<_>)
        }

    /// Drives the continual loop of reading and checkpointing each tranche until a fault occurs. <br/>
    /// The <c>readTranches</c> and <c>readPage</c> functions are expected to manage their own resilience strategies (retries etc). <br/>
    /// Any exception from <c>readTranches</c> or <c>readPage</c> will be propagated in order to enable termination of the overall projector loop
    member _.Pump(readTranches : unit -> Async<TrancheId[]>) =
        base.Pump(readTranches, crawl)
