namespace Propulsion.Feed

open Propulsion
open System

type StreamEvent = Propulsion.Streams.StreamEvent<byte[]>

type StartPos =
    | StartOrCheckpoint
    | TailOrCheckpoint
    | Position of Position

type FeedSource
    (   log : Serilog.ILogger, statsInterval : TimeSpan,
        sourceId, tailSleepInterval : TimeSpan,
        checkpoints : IFeedCheckpointStore, defaultCheckpointEventInterval : TimeSpan,
        /// Responsible for managing retries and back offs; yielding an exception will result in abend of the read loop
        readPage : TrancheId * Position -> Async<Page<_>>,
        sink : ProjectorPipeline<Ingestion.Ingester<seq<StreamEvent>, Submission.SubmissionBatch<int,StreamEvent>>>) =

    let log =
        log.ForContext("instanceId", let g = Guid.NewGuid() in g.ToString "N")
           .ForContext("source", sourceId)

    let pumpPartition partitionId trancheId = async {
        let log = log.ForContext("tranche", trancheId)
        let ingester : Ingestion.Ingester<_, _> =
            sink.StartIngester(log, partitionId)
        let reader =
            let submit : SubmitBatchHandler = SubmitBatchHandler(fun epoch commit events -> ingester.Submit(epoch, commit, events)  : Async<int * int>)
            let commit : CommitCheckpointHandler = CommitCheckpointHandler(fun source tranche pos -> checkpoints.Commit(source, tranche, pos))
            let readPage wasLast pos = async {
                if wasLast then
                    do! Async.Sleep tailSleepInterval
                return! readPage (trancheId, pos) }
            FeedReader(log, sourceId, trancheId, statsInterval, ReadPageHandler readPage, submit, commit)
        try let! freq, pos = checkpoints.Start(sourceId, trancheId, defaultCheckpointEventInterval)
            log.Information("Reading {sourceId}/{trancheId} @ {pos} checkpointing every {checkpointFreq:n1}m", sourceId, trancheId, pos, freq.TotalMinutes)
            do! reader.Pump(pos)
        with e ->
            log.Warning(e, "Exception encountered while running reader, exiting loop")
            return! Async.Raise e
    }

    member _.Pump(readTranches : unit -> Async<TrancheId[]>) = async {
        try let! tranches = readTranches ()
            log.Information("Starting {tranches} tranche readers...", tranches.Length)
            return! Async.Parallel(tranches |> Seq.mapi pumpPartition) |> Async.Ignore<unit[]>
        with e ->
            log.Warning(e, "Exception encountered while running source, exiting loop")
            return! Async.Raise e
    }
