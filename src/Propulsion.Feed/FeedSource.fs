namespace Propulsion.Feed.Internal

open FSharp.Control
open Propulsion
open Propulsion.Feed
open Propulsion.Streams
open System

/// Drives reading and checkpointing for a set of feeds (tranches) of a custom source feed
type FeedSourceBase internal
    (   log : Serilog.ILogger, statsInterval : TimeSpan, sourceId,
        checkpoints : IFeedCheckpointStore, defaultCheckpointEventInterval : TimeSpan,
        sink : ProjectorPipeline<Ingestion.Ingester<seq<StreamEvent<byte[]>>, Submission.SubmissionBatch<int,StreamEvent<byte[]>>>>) =

    let log =
        log.ForContext("instanceId", let g = Guid.NewGuid() in g.ToString "N")
           .ForContext("source", sourceId)

    let pumpPartition crawl partitionId trancheId = async {
        let log = log.ForContext("tranche", trancheId)
        let ingester : Ingestion.Ingester<_, _> = sink.StartIngester(log, partitionId)
        let reader = FeedReader(log, sourceId, trancheId, statsInterval, crawl trancheId, ingester.Submit, checkpoints.Commit)
        try let! freq, pos = checkpoints.Start(sourceId, trancheId, defaultCheckpointEventInterval)
            log.Information("Reading {sourceId}/{trancheId} @ {pos} checkpointing every {checkpointFreq:n1}m", sourceId, trancheId, pos, freq.TotalMinutes)
            do! reader.Pump(pos)
        with e ->
            log.Warning(e, "Exception encountered while running reader, exiting loop")
            return! Async.Raise e
    }

    /// Propagates exceptions raised by <c>readTranches</c> or <c>crawl</c>,
    member internal _.Pump
        (   readTranches : unit -> Async<TrancheId[]>,
            /// Responsible for managing retries and back offs; yielding an exception will result in abend of the read loop
            crawl : TrancheId -> bool * Position -> AsyncSeq<Internal.Batch<byte[]>>) = async {
        try let! tranches = readTranches ()
            log.Information("Starting {tranches} tranche readers...", tranches.Length)
            let crawl trancheId (wasLast, pos) = asyncSeq {
                yield! crawl trancheId (wasLast, pos) }
            return! Async.Parallel(tranches |> Seq.mapi (pumpPartition crawl)) |> Async.Ignore<unit[]>
        with e ->
            log.Warning(e, "Exception encountered while running source, exiting loop")
            return! Async.Raise e }

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
        checkpoints : IFeedCheckpointStore, defaultCheckpointEventInterval : TimeSpan,
        /// Responsible for managing retries and back offs; yielding an exception will result in abend of the read loop
        readPage : TrancheId * Position -> Async<Page<byte[]>>,
        sink : ProjectorPipeline<Ingestion.Ingester<seq<StreamEvent<byte[]>>, Submission.SubmissionBatch<int,StreamEvent<byte[]>>>>) =
    inherit Internal.FeedSourceBase(log, statsInterval, sourceId, checkpoints, defaultCheckpointEventInterval, sink)

    let crawl trancheId =
        let streamName = FsCodec.StreamName.compose "Messages" [SourceId.toString sourceId; TrancheId.toString trancheId]
        fun (wasLast, pos) -> asyncSeq {
            if wasLast then
                do! Async.Sleep tailSleepInterval
            let! page = readPage (trancheId, pos)
            let items' = page.items |> Array.map (fun x -> { stream = streamName; event = x })
            let slice : Internal.Batch<_> = { items = items'; checkpoint = page.checkpoint; isTail = page.isTail }
            yield slice
        }

    /// Drives the continual loop of reading and checkpointing each tranche until a fault occurs. <br/>
    /// The <c>readTranches</c> and <c>readPage</c> functions are expected to manage their own resilience strategies (retries etc). <br/>
    /// Any exception from <c>readTranches</c> or <c>readPage</c> will be propagated in order to enable termination of the overall projector loop
    member _.Pump(?readTranches : unit -> Async<TrancheId[]>) =
        let readTranches = match readTranches with Some f -> f | None -> fun () -> async { return [| TrancheId.parse "0" |] }
        base.Pump(readTranches, crawl)
