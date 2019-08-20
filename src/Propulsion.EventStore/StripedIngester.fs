namespace Propulsion.EventStore

open Propulsion.Internal
open Propulsion.Streams
open Serilog
open System
open System.Collections.Generic
open System.Collections.Concurrent
open System.Threading

type [<NoComparison; NoEquality>] Message =
    | Batch of seriesIndex: int * epoch: int64 * checkpoint: Async<unit> * items: StreamEvent<byte[]> seq
    | CloseSeries of seriesIndex: int

module StripedIngesterImpl =

    type Stats(log : ILogger, statsInterval) =
        let statsDue = intervalCheck statsInterval
        let mutable cycles, ingested = 0, 0
        let dumpStats activeSeries (readingAhead,ready) (currentBuffer,maxBuffer) =
            let mutable buffered = 0
            let count (xs : IDictionary<int,ResizeArray<_>>) = seq { for x in xs do buffered <- buffered + x.Value.Count; yield x.Key, x.Value.Count } |> Seq.sortBy fst |> Seq.toArray
            let ahead, ready = count readingAhead, count ready
            log.Information("Read {ingested} Cycles {cycles} Series {series} Holding {buffered} Reading {@reading} Ready {@ready} Active {currentBuffer}/{maxBuffer}",
                ingested, cycles, activeSeries, buffered, ahead, ready, currentBuffer, maxBuffer)
            ingested <- 0; cycles <- 0
        member __.Handle : InternalMessage -> unit = function
            | Batch _ -> ingested <- ingested + 1
            | ActivateSeries _ | CloseSeries _ -> ()
        member __.TryDump(activeSeries, readingAhead, ready, readMaxState) =
            cycles <- cycles + 1
            if statsDue () then
                dumpStats activeSeries (readingAhead,ready) readMaxState

    and [<NoComparison; NoEquality>] InternalMessage =
        | Batch of seriesIndex: int * epoch: int64 * checkpoint: Async<unit> * items: StreamEvent<byte[]> seq
        | CloseSeries of seriesIndex: int
        | ActivateSeries of seriesIndex: int

    let tryTake key (dict: Dictionary<_,_>) =
        match dict.TryGetValue key with
        | true, value ->
            dict.Remove key |> ignore
            Some value
        | false, _ -> None

open StripedIngesterImpl

/// Holds batches away from Core processing to limit in-flight processing
type StripedIngester
    (   log : ILogger, inner : Propulsion.Ingestion.Ingester<seq<StreamEvent<byte[]>>,Propulsion.Submission.SubmissionBatch<StreamEvent<byte[]>>>,
        maxInFlightBatches, initialSeriesIndex, statsInterval : TimeSpan, ?pumpInterval) =
    let cts = new CancellationTokenSource()
    let pumpInterval = defaultArg pumpInterval (TimeSpan.FromMilliseconds 5.)
    let work = ConcurrentQueue<InternalMessage>() // Queue as need ordering semantically
    let maxInFlightBatches = Sem maxInFlightBatches
    let stats = Stats(log, statsInterval)
    let pending = Queue<_>()
    let readingAhead, ready = Dictionary<int,ResizeArray<_>>(), Dictionary<int,ResizeArray<_>>()
    let mutable activeSeries = initialSeriesIndex

    let reserveAsInFlightBatch () = maxInFlightBatches.Await(cts.Token)
    let releaseInFlightBatchAllocation () = maxInFlightBatches.Release()
    
    let handle = function
        | Batch (seriesId, epoch, checkpoint, items) ->
            let isForActiveStripe = activeSeries = seriesId
            let batchInfo =
                let items = Array.ofSeq items
                let onCompleted =
                    if isForActiveStripe then
                        // If this read represents a batch that we will immediately submit for processing, we will defer the releasing of the batch in out buffer
                        // limit only when the batch's processing has concluded
                        releaseInFlightBatchAllocation
                    else
                        // if the batch pertains to a stripe other than the active one, we don't count that as a 'buffered item'
                        // (there will be indirect backpressure by virtue of the fact that the processing will not mark batches on 'active' series completed until
                        //   any ones we hold and forward through `readingAhead` are processed)
                        // - yield a null function as the onCompleted callback to be triggered when the batch's processing has concluded
                        id
                epoch,checkpoint,items,onCompleted
            if isForActiveStripe then
                pending.Enqueue batchInfo
            else
                match readingAhead.TryGetValue seriesId with
                | false, _ -> readingAhead.[seriesId] <- ResizeArray[|batchInfo|]
                | true,current -> current.Add(batchInfo)
                // As we'll be submitting `id` as the onCompleted callback, we now immediately release the allocation that gets `Await`ed in `Submit()`
                releaseInFlightBatchAllocation()
         | CloseSeries seriesIndex ->
            if activeSeries = seriesIndex then
                log.Information("Completed reading active series {activeSeries}; moving to next", activeSeries)
                work.Enqueue <| ActivateSeries (activeSeries + 1)
            else
                match readingAhead |> tryTake seriesIndex with
                | Some batchesRead ->
                    ready.[seriesIndex] <- batchesRead
                    log.Information("Completed reading {series}, marking {buffered} buffered items ready", seriesIndex, batchesRead.Count)
                | None ->
                    ready.[seriesIndex] <- ResizeArray()
                    log.Information("Completed reading {series}, leaving empty batch list", seriesIndex)
        | ActivateSeries newActiveSeries ->
            activeSeries <- newActiveSeries
            let buffered =
                match ready |> tryTake newActiveSeries with
                | Some completedChunkBatches ->
                    completedChunkBatches |> Seq.iter pending.Enqueue
                    work.Enqueue <| ActivateSeries (newActiveSeries + 1)
                    completedChunkBatches.Count
                | None ->
                    match readingAhead |> tryTake newActiveSeries with
                    | Some batchesReadToDate -> batchesReadToDate |> Seq.iter pending.Enqueue; batchesReadToDate.Count
                    | None -> 0
            log.Information("Moving to series {activeChunk}, releasing {buffered} buffered batches, {ready} others ready, {ahead} reading ahead",
                newActiveSeries, buffered, ready.Count, readingAhead.Count)

    member __.Pump = async {
        while not cts.IsCancellationRequested do
            let mutable itemLimit = 1024
            while itemLimit > 0 do
                match work.TryDequeue() with
                | true, x -> handle x; stats.Handle x; itemLimit <- itemLimit - 1
                | false, _ -> itemLimit <- 0
            while pending.Count <> 0 do
                let epoch,checkpoint,items,markCompleted = pending.Dequeue()
                let! _,_ = inner.Submit(epoch, checkpoint, items, markCompleted) in ()
            stats.TryDump(activeSeries,readingAhead,ready,maxInFlightBatches.State)
            do! Async.Sleep pumpInterval }

    /// Yields (used,maximum) of in-flight batches limit
    /// return can be delayed where we're over the limit until such time as the background processing ingests the batch
    member __.Submit(content : Message) = async {
        match content with
        | Message.Batch (seriesId, epoch, checkpoint, events) ->
            work.Enqueue <| Batch (seriesId, epoch, checkpoint, events)
            // each Await of the semaphore has an associated Release() in `handle`'s `Batch` case handling
            do! reserveAsInFlightBatch()
        | Message.CloseSeries seriesId ->
            work.Enqueue <| CloseSeries seriesId
        return maxInFlightBatches.State }

    /// As range assignments get revoked, a user is expected to `Stop `the active processing thread for the Ingester before releasing references to it
    member __.Stop() = cts.Cancel()