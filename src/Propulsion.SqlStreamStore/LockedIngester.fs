namespace Propulsion.SqlStreamStore

module LockedIngester =

    open System
    open System.Collections.Concurrent

    open StreamReader

    open Serilog

    [<NoComparison;NoEquality>]
    type SliceResult =
        | Behind
        | Overlapped of InternalBatch * batchFirstPosition: int64
        | Ahead of InternalBatch

    module InternalBatch =

        let sliceAt (pos: Nullable<int64>) (batch: InternalBatch) =
            if pos.HasValue then
                let pos = pos.Value
                if pos < batch.firstPosition then
                    Ahead batch
                elif pos >= batch.lastPosition then
                    Behind
                else
                    let filtered =
                        batch.messages
                        |> Array.filter (fun x -> x.event.Index > pos)
                    Overlapped ({
                        firstPosition = pos + 1L
                        lastPosition = batch.lastPosition
                        messages = filtered
                        isEnd = batch.isEnd
                    }, batch.firstPosition)
            else
                Ahead batch

    type Stats(logger: ILogger, ?statsInterval: TimeSpan) =

        let statsInterval = defaultArg statsInterval (TimeSpan.FromSeconds(30.))

        let mutable workQueueLength = 0

        let mutable batchFirstPosition = 0L
        let mutable batchLastPosition = 0L
        let mutable batchCaughtUp = false

        let mutable batchesRead = 0
        let mutable batchesCommitted = 0

        let mutable lastReadPosition = 0L
        let mutable lastCommittedPosition = 0L

        let report () =
            logger.Information(
                "Batches Queued {queued} Read {batchesRead} Committed {batchesCommitted} | Batch {firstPosition}-{lastPosition} Caught up {caughtUp} | Position Read {lastReadPosition} Committed {lastCommittedPosition}",
                workQueueLength, batchesRead, batchesCommitted, batchFirstPosition, batchLastPosition, batchCaughtUp, lastReadPosition, lastCommittedPosition)

        member this.UpdateQueue (workQueue: BlockingCollection<_>) =
            workQueueLength <- workQueue.Count

        member this.UpdateBatch(batch: InternalBatch) =
            batchFirstPosition <- batch.firstPosition
            batchLastPosition <- batch.lastPosition
            batchCaughtUp <- batch.isEnd
            batchesRead <- batchesRead + 1

        member this.UpdateReadPosition(pos : Nullable<int64>) =
            lastReadPosition <- pos.GetValueOrDefault(-1L)

        member this.UpdateCommitedPosition(pos) =
            lastCommittedPosition <- pos
            batchesCommitted <- batchesCommitted + 1

        member this.Start =
            async {
                let! ct = Async.CancellationToken
                while not ct.IsCancellationRequested do
                    report ()
                    do! Async.Sleep statsInterval
            }

    [<RequireQualifiedAccess>]
    [<NoComparison;NoEquality>]
    type private Work =
        | Take
        | Batch of InternalBatch

    type SubmitBatchHandler =
        // ingester submit method: epoch * checkpoint * items -> write result
        int64 * Async<unit> * seq<Propulsion.Streams.StreamEvent<byte[]>> -> Async<int*int>

    type LockedIngester
        (
            logger: ILogger,
            ledger: ILedger,
            submit: SubmitBatchHandler,
            consumerGroup : string,
            streamId : string,
            ?boundedCapacity: int,
            ?statsInterval: TimeSpan,
            ?onCaughtUp : unit -> unit
        ) =

        let logger = logger.ForContext("component", "LockedIngester")

        let onCaughtUp = defaultArg onCaughtUp ignore

        let work =
            match boundedCapacity with
            | Some cap -> new BlockingCollection<InternalBatch>(ConcurrentQueue(), cap)
            | None     -> new BlockingCollection<InternalBatch>(ConcurrentQueue())

        let stats = Stats(logger, ?statsInterval = statsInterval)

        let commit (handle: IDisposable) batch =
            async {
                try
                    try
                        do! ledger.CommitPosition { Stream = streamId; ConsumerGroup = consumerGroup; Position = Nullable(batch.lastPosition) }
                        stats.UpdateCommitedPosition(batch.lastPosition)
                        logger.Debug("Committed position {position}", batch.lastPosition)

                        // Handle end of stream if reached.
                        if batch.isEnd then
                            do onCaughtUp ()
                    with
                    | exc ->
                        logger.Error(exc, "Exception while commiting position {position}", batch.lastPosition)
                        return! Async.Raise exc
                finally
                    // Release the lock after committing the position.
                    logger.Debug("Releasing the lock after commiting")
                    do handle.Dispose()
            }

        member this.SubmitBatch (batch: InternalBatch) =
            async {
                let! ct = Async.CancellationToken
                do work.Add(batch, ct)
                stats.UpdateQueue(work)
            }

        member private this.TakeBatch () =
            async {
                let! ct = Async.CancellationToken
                let batch = work.Take(ct)
                stats.UpdateQueue(work)
                return batch
            }

        member this.Start () =
            async {
                // Start reporting stats
                do! Async.StartChild stats.Start |> Async.Ignore

                let mutable workItem = Work.Take

                let! ct = Async.CancellationToken
                while not ct.IsCancellationRequested do
                    try
                        // Take work if haven't done it already or finished previous batch
                        let! batch =
                            async {
                                match workItem with
                                | Work.Batch b -> return b
                                | Work.Take ->
                                    let! b = this.TakeBatch()
                                    workItem <- Work.Batch b
                                    return b
                            }

                        stats.UpdateBatch(batch)

                        logger.Debug("Trying to obtain the lock")

                        // Try obtaining a distributed sql lock for the consumer group.
                        // The lock would be released as part of commit operation on the underlying ingester.
                        let! handle = ledger.Lock(consumerGroup)

                        if handle <> null then
                            logger.Debug("Obtained the lock")
                            let mutable submitted = false
                            try
                                // Obtained the lock, check batch against committed position.
                                let! position = ledger.GetPosition { Stream = streamId; ConsumerGroup = consumerGroup }

                                stats.UpdateReadPosition(position)

                                match InternalBatch.sliceAt position batch with
                                | Ahead batch ->
                                    logger.Debug(
                                        "Batch at position {firstPosition} through {lastPosition} is ahead of committed position {committedPosition}, processing.",
                                        batch.firstPosition, batch.lastPosition, position)
                                    do! submit (batch.firstPosition, commit handle batch, batch.messages) |> Async.Ignore
                                    submitted <- true
                                | Overlapped (batch, firstPosition) ->
                                    logger.Debug(
                                        "Batch at position {firstPosition} through {lastPosition} is overlapping committed position {committedPosition}, processing from committed position.",
                                        firstPosition, batch.lastPosition, position)
                                    do! submit (batch.firstPosition, commit handle batch, batch.messages) |> Async.Ignore
                                    submitted <- true
                                | Behind ->
                                    logger.Debug(
                                        "Batch at position {firstPosition} through {lastPosition} is behind committed position {committedPosition}, skipping.",
                                        batch.firstPosition, batch.lastPosition, position)
                            finally
                                if not submitted then
                                    // No processing to be done on this batch, release the lock.
                                    logger.Debug("Releasing the lock without submission")
                                    do handle.Dispose()

                            // Release the current batch to pick a new batch on the next iteration.
                            workItem <- Work.Take
                        else
                            // Failed obtaining the lock, retry with the same work item.
                            logger.Debug("Failed to obtain the lock")
                    with
                    | exc ->
                        logger.Error(exc, "Exception while running StreamReader loop")
                        return! Async.Raise exc
            }

        interface IDisposable with
            member this.Dispose() =
                work.CompleteAdding()
                work.Dispose()
