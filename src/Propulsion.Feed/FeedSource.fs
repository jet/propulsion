namespace Propulsion.Feed

open FSharp.Control
open Propulsion
open Propulsion.Streams
open System

/// Drives reading and checkpointing for a set of feeds (tranches) of a custom source feed. <br/>
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

    /// Drives the processing activity.
    /// Propagates exceptions raised by <c>readTranches</c> or <c>crawl</c>,
    ///   in order to enable triggering termination of the overall projector loop
    member internal _.Pump
        (   readTranches : unit -> Async<TrancheId[]>,
            /// Responsible for managing retries and back offs; yielding an exception will result in abend of the read loop
            crawl : TrancheId -> bool * Position -> AsyncSeq<Page<byte[]>>) = async {
        try let! tranches = readTranches ()
            log.Information("Starting {tranches} tranche readers...", tranches.Length)
            let crawl trancheId (wasLast, pos) = asyncSeq {
                yield! crawl trancheId (wasLast, pos) }
            return! Async.Parallel(tranches |> Seq.mapi (pumpPartition crawl)) |> Async.Ignore<unit[]>
        with e ->
            log.Warning(e, "Exception encountered while running source, exiting loop")
            return! Async.Raise e }

/// Drives reading and checkpointing for a set of feeds (tranches) of a custom source feed. <br/>
/// The <c>readTranches</c> and <c>readPage</c> callbacks are expected to manage their own resilience strategies (retries etc). <br/>
/// Yielding an exception from either will result in the tearing down of the source pipeline,
///   which typically concludes in the termination of the entire processing pipeline.
type FeedSource
    (   log : Serilog.ILogger, statsInterval : TimeSpan,
        sourceId, tailSleepInterval : TimeSpan,
        checkpoints : IFeedCheckpointStore, defaultCheckpointEventInterval : TimeSpan,
        /// Responsible for managing retries and back offs; yielding an exception will result in abend of the read loop
        readPage : TrancheId * Position -> Async<Page<byte[]>>,
        sink : ProjectorPipeline<Ingestion.Ingester<seq<StreamEvent<byte[]>>, Submission.SubmissionBatch<int,StreamEvent<byte[]>>>>) =
    inherit FeedSourceBase(log, statsInterval, sourceId, checkpoints, defaultCheckpointEventInterval, sink)

    let crawl trancheId (wasLast, pos) = asyncSeq {
        if wasLast then
            do! Async.Sleep tailSleepInterval
        let! page = readPage (trancheId, pos)
        yield page
    }

    /// Drives the processing activity.
    /// Propagates exceptions raised by <c>readTranches</c> or <c>readPage</c>,
    ///   in order to let trigger termination of the overall projector loop
    member _.Pump(readTranches : unit -> Async<TrancheId[]>) =
        base.Pump(readTranches, crawl)
