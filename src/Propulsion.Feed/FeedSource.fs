namespace Propulsion.Feed.Internal

open FSharp.Control
open Propulsion
open Propulsion.Feed
open System
open System.Threading.Tasks

/// Drives reading and checkpointing for a set of feeds (tranches) of a custom source feed
type FeedSourceBase internal
    (   log : Serilog.ILogger, statsInterval : TimeSpan, sourceId,
        checkpoints : IFeedCheckpointStore, establishOrigin : (TrancheId -> Async<Position>) option,
        startIngester,
        renderPos : Position -> string,
        ?logCommitFailure) =

    let log = log.ForContext("source", sourceId)

    let pumpPartition crawl partitionId trancheId = async {
        let log = log.ForContext("tranche", trancheId)
        let ingester : Ingestion.Ingester<_> = startIngester (log, partitionId)
        let reader = FeedReader(log, sourceId, trancheId, statsInterval, crawl trancheId, ingester.Ingest, checkpoints.Commit, renderPos, ?logCommitFailure = logCommitFailure)
        try let! freq, pos = checkpoints.Start(sourceId, trancheId, ?establishOrigin = (match establishOrigin with None -> None | Some f -> Some (f trancheId)))
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

/// Drives reading and checkpointing from a source that contains data from multiple streams. While a TrancheId is always required,
/// it may have a default value of `"0"` if the underlying source representation does not involve autonomous shards/physical partitions etc
type TailingFeedSource
    (   log : Serilog.ILogger, statsInterval : TimeSpan,
        sourceId, tailSleepInterval : TimeSpan,
        crawl : TrancheId * Position -> AsyncSeq<TimeSpan * Batch<_>>,
        checkpoints : IFeedCheckpointStore, establishOrigin : (TrancheId -> Async<Position>) option,
        startIngester,
        renderPos,
        ?logReadFailure,
        ?readFailureSleepInterval : TimeSpan,
        ?logCommitFailure) =
    inherit FeedSourceBase(log, statsInterval, sourceId, checkpoints, establishOrigin, startIngester, renderPos, ?logCommitFailure = logCommitFailure)

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
        checkpoints : IFeedCheckpointStore,
        startIngester,
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
            checkpoints, establishOrigin, startIngester,
            renderPos = defaultArg renderPos string)

    member _.Pump() =
        let readTranches () = async { return [| TrancheId.parse "0" |] }
        base.Pump(readTranches)

    member x.Start() = base.Start(x.Pump())

namespace Propulsion.Feed

open FSharp.Control
open Propulsion
open Propulsion.Streams
open System

[<NoComparison; NoEquality>]
type Page<'e> = { items : FsCodec.ITimelineEvent<'e>[]; checkpoint : Position; isTail : bool }

/// Drives reading and checkpointing for a set of change feeds (tranches) of a custom data source that can represent their
///   content as an append-only data source with a change feed wherein each <c>FsCodec.ITimelineEvent</c> has a monotonically increasing <c>Index</c>. <br/>
/// Processing concludes if <c>readTranches</c> and <c>readPage</c> throw, in which case the <c>Pump</c> loop terminates, propagating the exception.
type FeedSource
    (   log : Serilog.ILogger, statsInterval : TimeSpan,
        sourceId, tailSleepInterval : TimeSpan,
        checkpoints : IFeedCheckpointStore,
        // Responsible for managing retries and back offs; yielding an exception will result in abend of the read loop
        readPage : TrancheId * Position -> Async<Page<byte[]>>,
        startIngester,
        ?renderPos) =
    inherit Internal.FeedSourceBase(log, statsInterval, sourceId, checkpoints, None, startIngester, defaultArg renderPos string)

    let crawl trancheId =
        let streamName = FsCodec.StreamName.compose "Messages" [SourceId.toString sourceId; TrancheId.toString trancheId]
        fun (wasLast, pos) -> asyncSeq {
            if wasLast then
                do! Async.Sleep tailSleepInterval
            let sw = System.Diagnostics.Stopwatch.StartNew()
            let! page = readPage (trancheId, pos)
            let items' = page.items |> Array.map (fun x -> { stream = streamName; event = x })
            yield sw.Elapsed, ({ items = items'; checkpoint = page.checkpoint; isTail = page.isTail } : Internal.Batch<_>)
        }

    /// Drives the continual loop of reading and checkpointing each tranche until a fault occurs. <br/>
    /// The <c>readTranches</c> and <c>readPage</c> functions are expected to manage their own resilience strategies (retries etc). <br/>
    /// Any exception from <c>readTranches</c> or <c>readPage</c> will be propagated in order to enable termination of the overall projector loop
    member _.Pump(readTranches : unit -> Async<TrancheId[]>) =
        base.Pump(readTranches, crawl)
