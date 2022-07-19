module Propulsion.DynamoStore.DynamoStoreIndexReader

open Propulsion.Streams
#if false
type IndexReader
    (   log : ILogger, sourceId, trancheId, statsInterval : TimeSpan,
        // Walk all content in the source. Responsible for managing exceptions, retries and backoff.
        // Implementation is expected to inject an appropriate sleep based on the supplied `Position`
        // Processing loop will abort if an exception is yielded
        crawl :
            bool // lastWasTail : may be used to induce a suitable backoff when repeatedly reading from tail
            * Position // checkpointPosition
            -> AsyncSeq<TimeSpan * Batch<byte[]>>,
        // <summary>Feed a batch into the ingester. Internal checkpointing decides which Commit callback will be called
        // Throwing will tear down the processing loop, which is intended; we fail fast on poison messages
        // In the case where the number of batches reading has gotten ahead of processing exceeds the limit,
        //   <c>submitBatch</c> triggers the backoff of the reading ahead loop by sleeping prior to returning</summary>
        submitBatch :
            int64 // unique tag used to identify batch in internal logging
            * Async<unit> // commit callback. Internal checkpointing dictates when it will be called.
            * seq<Propulsion.Streams.StreamEvent<byte[]>>
            // Yields (current batches pending,max readAhead) for logging purposes
            -> Async<int*int>,
        // Periodically triggered, asynchronously, by the scheduler as processing of submitted batches progresses
        // Should make one attempt to persist a checkpoint
        // Throwing exceptions is acceptable; retrying and handling of exceptions is managed by the internal loop
        commitCheckpoint :
            SourceId
            * TrancheId// identifiers of source and tranche within that; a checkpoint is maintained per such pairing
            * Position // index representing next read position in stream
            // permitted to throw if it fails; failures are counted and/or retried with throttling
            -> Async<unit>,
        renderPos,
        ?logCommitFailure) =

    let log = log.ForContext("source", sourceId).ForContext("tranche", trancheId)
    let stats = Stats(log, statsInterval, sourceId, trancheId, renderPos)

    let commit position = async {
        try do! commitCheckpoint (sourceId, trancheId, position)
            stats.UpdateCommittedPosition(position)
            log.Debug("Committed checkpoint {position}", position)
        with e ->
            match logCommitFailure with None -> log.ForContext<FeedReader>().Debug(e, "Exception while committing checkpoint {position}", position) | Some l -> l e
            return! Async.Raise e }

    let submitPage (readLatency, batch : Batch<byte[]>) = async {
        stats.RecordBatch(readLatency, batch)
        match Array.length batch.items with
        | 0 -> log.Verbose("Page {latency:f0}ms Checkpoint {checkpoint} Empty", readLatency.TotalMilliseconds, batch.checkpoint)
        | c -> if log.IsEnabled(Serilog.Events.LogEventLevel.Debug) then
                   let streamsCount = batch.items |> Seq.distinctBy (fun x -> x.stream) |> Seq.length
                   log.Debug("Page {latency:f0}ms Checkpoint {checkpoint} {eventCount}e {streamCount}s",
                             readLatency.TotalMilliseconds, batch.checkpoint, c, streamsCount)
        let epoch, streamEvents : int64 * Propulsion.Streams.StreamEvent<_> seq = int64 batch.checkpoint, Seq.ofArray batch.items
        let ingestTimer = System.Diagnostics.Stopwatch.StartNew()
        let! cur, max = submitBatch (epoch, commit batch.checkpoint, streamEvents)
        stats.UpdateCurMax(ingestTimer.Elapsed, cur, max) }

    member _.Pump(initialPosition : Position) = async {
        log.Debug("Starting reading stream from position {initialPosition}", renderPos initialPosition)
        stats.UpdateCommittedPosition(initialPosition)
        // Commence reporting stats until such time as we quit pumping
        let! _ = Async.StartChild stats.Pump
        let! ct = Async.CancellationToken
        let mutable currentPos, lastWasTail = initialPosition, false
        while not ct.IsCancellationRequested do
            for readLatency, batch in crawl (lastWasTail, currentPos) do
                do! submitPage (readLatency, batch)
                currentPos <- batch.checkpoint
                lastWasTail <- batch.isTail }

type DynamoStoreIndexReader
    (   log : Serilog.ILogger, statsInterval,
        indexClient : DynamoStoreClient, batchSizeCutoff,
        // Separated log for DynamoStore calls in order to facilitate filtering and/or gathering metrics
        ?storeLog,
        ?sourceId) =

    let log = log.ForContext("source", sourceId)

    let pumpPartition crawl partitionId trancheId = async {
        let log = log.ForContext("tranche", trancheId)
        let ingester : Ingestion.Ingester<_, _> = sink.StartIngester(log, partitionId)
        let reader = FeedReader(log, sourceId, trancheId, statsInterval, crawl trancheId, ingester.Submit, checkpoints.Commit, renderPos, ?logCommitFailure = logCommitFailure)
        try let! freq, pos = checkpoints.Start(sourceId, trancheId, ?establishOrigin = (match establishOrigin with None -> None | Some f -> Some (f trancheId)))
            log.Information("Reading {source:l}/{tranche:l} From {pos} Checkpoint Event interval {checkpointFreq:n1}m",
                            sourceId, trancheId, renderPos pos, freq.TotalMinutes)
            return! reader.Pump(pos)
        with e ->
            log.Warning(e, "Exception encountered while running reader, exiting loop")
            return! Async.Raise e
    }
#endif
