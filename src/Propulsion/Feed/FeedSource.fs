namespace Propulsion.Feed.Core

open FSharp.Control // taskSeq
open Propulsion
open Propulsion.Feed
open Propulsion.Internal
open System

/// Drives reading and checkpointing for a set of feeds (tranches) of a custom source feed
type FeedSourceBase internal
    (   log: Serilog.ILogger, statsInterval: TimeSpan, sourceId,
        checkpoints: IFeedCheckpointStore, establishOrigin: Func<TrancheId, CancellationToken, Task<Position>> option,
        sink: Propulsion.Sinks.SinkPipeline,
        renderPos: Position -> string,
        shutdownTimeout: TimeSpan,
        ?logCommitFailure, ?readersStopAtTail) =
    let log = log.ForContext("source", sourceId)

    let mutable partitions = Array.empty<struct(Ingestion.Ingester<_> * FeedReader)>

    let dumpStats () = for _i, r in partitions do r.DumpStats()

    let pumpPartition trancheId struct (ingester: Ingestion.Ingester<_>, reader: FeedReader) ct = task {
        try let establishTrancheOrigin (f: Func<_, CancellationToken, _>) = Func<_, _>(fun ct -> f.Invoke(trancheId, ct))
            try let! pos = checkpoints.Start(sourceId, trancheId, establishOrigin = (establishOrigin |> Option.map establishTrancheOrigin), ct = ct)
                reader.LogPartitionStarting(pos)
                return! reader.Pump(pos, ct)
            with:? System.Threading.Tasks.TaskCanceledException | :? OperationCanceledException -> ()
                | Exception.Log reader.LogPartitionExn () -> ()
        finally ingester.Stop() }

    let positions: ISourcePositions<ITranchePosition> = SourcePositions(fun _ -> lazy (TranchePosition() :> _))

    /// Runs checkpointing functions for any batches with unwritten checkpoints
    /// Yields current Tranche Positions
    member _.Checkpoint(): Task<TranchePositions> = task {
        use cts = new System.Threading.CancellationTokenSource(shutdownTimeout)
        let backoff = max (TimeSpan.ms 100) (shutdownTimeout / 3.)
        do! Task.parallelLimit 4 cts.Token (seq { for i, _r in partitions -> i.Flush backoff }) |> Task.ignore<unit[]>
        return positions |> SourcePositions.completed }

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
            let instrumentBatch = let t: ITranchePosition = positions.For(trancheId) in t.Decorate
            let reader = FeedReader(log, partitionId, sourceId, trancheId, crawl trancheId, ingester, instrumentBatch, checkpoints.Commit,
                                    renderPos, stopAtTail = defaultArg readersStopAtTail false, ?logCommitFailure = logCommitFailure)
            ingester, reader)
        Task.start (fun () ->  // loops in background until overall pumping is cancelled
            let dumpStats_ _ct = dumpStats (); Task.FromResult ()
            Task.periodically dumpStats_ statsInterval ct)
        let trancheWorkflows = (tranches, partitions) ||> Seq.map2 pumpPartition
        try do! Task.parallelUnlimited ct trancheWorkflows |> Task.ignore<unit[]>
        finally dumpStats () } // Flush the stats when each partition has finished (and flushed)

    /// Would be protected if that existed - derived types are expected to use this in implementing a parameterless `Start()`
    member x.Start(pump): SourcePipeline =
        let stopIngesters () = for i, _ in partitions do i.Stop()
        let machine, triggerStop, outcomeTask = PipelineFactory.PrepareSource(log, pump, stopIngesters)
        let monitor = lazy FeedMonitor(log, positions.Current, sink, fun () -> outcomeTask.IsCompleted)
        new SourcePipeline<_, _>(Task.run machine, x.Checkpoint, triggerStop, monitor)

/// Drives reading and checkpointing from a source that contains data from multiple streams
type TailingFeedSource
    (   log: Serilog.ILogger, statsInterval: TimeSpan,
        sourceId, tailSleepInterval: TimeSpan,
        checkpoints: IFeedCheckpointStore, establishOrigin, sink: Propulsion.Sinks.SinkPipeline, renderPos,
        crawl: Func<TrancheId, Position, CancellationToken, IAsyncEnumerable<struct (TimeSpan * Batch<Propulsion.Sinks.EventBody>)>>,
        ?logReadFailure, ?readFailureSleepInterval: TimeSpan, ?logCommitFailure, ?readersStopAtTail, ?shutdownTimeout) =
    inherit FeedSourceBase(log, statsInterval, sourceId, checkpoints, establishOrigin, sink, renderPos, defaultArg shutdownTimeout (TimeSpan.seconds 5),
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
        checkpoints: IFeedCheckpointStore, sink: Propulsion.Sinks.SinkPipeline,
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

module Categories =

    let private startsWith (prefix: string) (s: FsCodec.StreamName) = (FsCodec.StreamName.toString s).StartsWith(prefix)

    let categoryFilter (categories: string[]) =
        let hasDesiredPrefix = categories |> Array.map startsWith
        fun (x: FsCodec.StreamName) -> hasDesiredPrefix |> Array.exists (fun f -> f x)

    let mapFilters categories streamFilter =
        match categories, streamFilter with
        | None, None ->                         fun _ -> true
        | Some categories, None ->              categoryFilter categories
        | None, Some (filter: Func<_, bool>) -> filter.Invoke
        | Some categories, Some filter ->
            let categoryFilter = categoryFilter categories
            fun x -> categoryFilter x && filter.Invoke x

namespace Propulsion.Feed

open FSharp.Control // taskSeq
open Propulsion.Internal
open System

[<NoComparison; NoEquality>]
type Page<'F> = { items: FsCodec.ITimelineEvent<'F>[]; checkpoint: Position; isTail: bool }

/// Drives reading and checkpointing for a set of change feeds (tranches) of a custom data source that can represent their
///   content as an append-only data source with a change feed wherein each <c>FsCodec.ITimelineEvent</c> has a monotonically increasing <c>Index</c>. <br/>
/// Processing concludes if <c>readTranches</c> and <c>readPage</c> throw, in which case the <c>Pump</c> loop terminates, propagating the exception.
type FeedSource
    (   log: Serilog.ILogger, statsInterval: TimeSpan,
        sourceId, tailSleepInterval: TimeSpan,
        checkpoints: IFeedCheckpointStore, sink: Propulsion.Sinks.SinkPipeline,
        ?renderPos, ?shutdownTimeout) =
    inherit Core.FeedSourceBase(log, statsInterval, sourceId, checkpoints, None, sink,
                                defaultArg renderPos string, defaultArg shutdownTimeout (TimeSpan.seconds 5))

    let crawl (readPage: Func<TrancheId,Position,CancellationToken,Task<Page<_>>>) trancheId =
        let streamName = FsCodec.StreamName.compose "Messages" [| SourceId.toString sourceId; TrancheId.toString trancheId |]
        fun (wasLast, pos) ct -> taskSeq {
            if wasLast then
                do! Task.delay tailSleepInterval ct
            let readTs = Stopwatch.timestamp ()
            let! page = readPage.Invoke(trancheId, pos, ct)
            let items' = page.items |> Array.map (fun x -> struct (streamName, x))
            yield struct (Stopwatch.elapsed readTs, ({ items = items'; checkpoint = page.checkpoint; isTail = page.isTail }: Batch<_>)) }

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
