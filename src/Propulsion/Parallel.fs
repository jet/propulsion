namespace Propulsion.Parallel

open Propulsion
open Propulsion.Internal
open Serilog
open System
open System.Collections.Concurrent
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks

/// Deals with dispatch and result handling, triggering completion callbacks as batches reach completed state
module Scheduling =

    /// Single instance per system; coordinates the dispatching of work, subject to the maxDop concurrent processors constraint
    /// Semaphore is allocated on queueing, deallocated on completion of the processing
    type Dispatcher(maxDop) =
        // Using a Queue as a) the ordering is more correct, favoring more important work b) we are adding from many threads so no value in ConcurrentBag's thread-affinity
        let tryWrite, wait, apply =
            let c = Channel.unboundedSwSr<_> in let r, w = c.Reader, c.Writer
            w.TryWrite, Channel.awaitRead r, Channel.apply r
        let dop = Sem maxDop

        let wrap computation = async {
            try do! computation
            // Release the capacity on conclusion of the processing (exceptions should not pass to this level but the correctness here is critical)
            finally dop.Release() }

        /// Attempt to dispatch the supplied task - returns false if processing is presently running at full capacity
        member _.TryAdd computation =
            dop.TryTake() && tryWrite computation

        /// Loop that continuously drains the work queue
        member _.Pump(ct : CancellationToken) = task {
            while not ct.IsCancellationRequested do
                do! wait ct :> Task
                apply (wrap >> Async.Start) |> ignore }

    /// Batch of work as passed from the Submitter to the Scheduler comprising messages with their associated checkpointing/completion callback
    [<NoComparison; NoEquality>]
    type Batch<'S, 'M> = { source : 'S; messages: 'M []; onCompletion: unit -> unit }

    /// Thread-safe/lock-free batch-level processing state
    /// - referenced [indirectly, see `mkDispatcher`] among all task invocations for a given batch
    /// - scheduler loop continuously inspects oldest active instance per partition in order to infer attainment of terminal (completed or faulted) state
    [<NoComparison; NoEquality>]
    type WipBatch<'S, 'M> =
        {   mutable elapsedMs : int64 // accumulated processing time for stats
            mutable remaining : int // number of outstanding completions; 0 => batch is eligible for completion
            mutable faults : ConcurrentStack<exn> // exceptions, order is not relevant and use is infrequent hence ConcurrentStack
            batch: Batch<'S, 'M> }

        member private x.RecordOk(duration : TimeSpan) =
            // need to record stats first as remaining = 0 is used as completion gate
            Interlocked.Add(&x.elapsedMs, int64 duration.TotalMilliseconds + 1L) |> ignore
            Interlocked.Decrement(&x.remaining) |> ignore
        member private x.RecordExn(_duration, exn) =
            x.faults.Push exn

        /// Prepares an initial set of shared state for a batch of tasks, together with the Async<unit> computations that will feed their results into it
        static member Create(batch : Batch<'S, 'M>, handle) : WipBatch<'S, 'M> * seq<Async<unit>> =
            let x = { elapsedMs = 0L; remaining = batch.messages.Length; faults = ConcurrentStack(); batch = batch }
            x, seq {
                for item in batch.messages -> async {
                    let sw = Stopwatch.start ()
                    try match! handle item with
                        | Choice1Of2 () -> x.RecordOk sw.Elapsed
                        | Choice2Of2 exn -> x.RecordExn(sw.Elapsed, exn)
                    // This exception guard _should_ technically not be necessary per the interface contract, but cannot risk an orphaned batch
                    with exn -> x.RecordExn(sw.Elapsed, exn) } }

    /// Infers whether a WipBatch is in a terminal state
    let (|Busy|Completed|Faulted|) = function
        | { remaining = 0; elapsedMs = ms } -> Completed (TimeSpan.FromMilliseconds <| float ms)
        | { faults = f } when not f.IsEmpty -> Faulted (f.ToArray())
        | _ -> Busy

    /// Continuously coordinates the propagation of incoming requests and mapping that to completed batches
    /// - replenishing the Dispatcher
    /// - determining when WipBatches attain terminal state in order to triggering completion callbacks at the earliest possible opportunity
    /// - triggering abend of the processing should any dispatched tasks start to fault
    type PartitionedSchedulingEngine<'S, 'M when 'S : equality>(log : ILogger, handle, tryDispatch : Async<unit> -> bool, statsInterval, ?logExternalStats) =
        // Submitters dictate batch commencement order by supply batches in a fair order; should never be empty if there is work in the system
        let incoming = ConcurrentQueue<Batch<'S, 'M>>()
        // Prepared work items ready to feed to Dispatcher (only created on demand in order to ensure we maximize overall progress and fairness)
        let waiting = Queue<Async<unit>>(1024)
        // Index of batches that have yet to attain terminal state (can be >1 per partition)
        let active = Dictionary<'S(*partitionId*),Queue<WipBatch<'S, 'M>>>()
        (* accumulators for periodically emitted statistics info *)
        let mutable cycles, processingDuration = 0, TimeSpan.Zero
        let startedBatches, completedBatches = Submission.PartitionStats(), Submission.PartitionStats()
        let startedItems, completedItems = Submission.PartitionStats(), Submission.PartitionStats()

        let dumpStats () =
            let startedB, completedB = Array.ofSeq startedBatches.StatsDescending, Array.ofSeq completedBatches.StatsDescending
            let startedI, completedI = Array.ofSeq startedItems.StatsDescending, Array.ofSeq completedItems.StatsDescending
            let statsTotal (xs : struct (_ * int64) array) = xs |> Array.sumBy ValueTuple.snd
            let totalItemsCompleted = statsTotal completedI
            let latencyMs = match totalItemsCompleted with 0L -> null | cnt -> box (processingDuration.TotalMilliseconds / float cnt)
            log.Information("Scheduler {cycles} cycles Started {startedBatches}b {startedItems}i Completed {completedBatches}b {completedItems}i latency {completedLatency:f1}ms Ready {readyitems} Waiting {waitingBatches}b",
                cycles, statsTotal startedB, statsTotal startedI, statsTotal completedB, totalItemsCompleted, latencyMs, waiting.Count, incoming.Count)
            let active =
                seq { for KeyValue(pid,q) in active -> pid, q |> Seq.sumBy (fun x -> x.remaining) }
                |> Seq.filter (fun (_,snd) -> snd <> 0)
                |> Seq.sortBy (fun (_,snd) -> -snd)
            log.Information("Partitions Active items {@active} Started batches {@startedBatches} items {@startedItems} Completed batches {@completedBatches} items {@completedItems}",
                active, startedB, startedI, completedB, completedI)
            cycles <- 0; processingDuration <- TimeSpan.Zero; startedBatches.Clear(); completedBatches.Clear(); startedItems.Clear(); completedItems.Clear()
            logExternalStats |> Option.iter (fun f -> f log) // doing this in here allows stats intervals to be aligned with that of the scheduler engine

        let maybeLogStats : unit -> bool =
            let timer = IntervalTimer statsInterval
            fun () ->
                cycles <- cycles + 1
                if timer.IfExpiredRestart() then dumpStats (); true else false

        /// Inspects the oldest in-flight batch per partition to determine if it's reached a terminal state; if it has, remove and trigger completion callback
        let drainCompleted abend =
            let mutable more, worked = true, false
            while more do
                more <- false
                for queue in active.Values do
                    match queue.TryPeek() with
                    | false, _ // empty
                    | true, Busy -> () // still working
                    | true, Faulted exns -> // outer layers will react to this by tearing us down
                        abend (AggregateException(exns))
                    | true, Completed batchProcessingDuration -> // call completion function asap
                        let partitionId, markCompleted, itemCount =
                            let { batch = { source = p; onCompletion = f; messages = msgs } } = queue.Dequeue()
                            p, f, msgs.LongLength
                        completedBatches.Record partitionId
                        completedItems.Record(partitionId, itemCount)
                        processingDuration <- processingDuration.Add batchProcessingDuration
                        markCompleted ()
                        worked <- true
                        more <- true // vote for another iteration as the next one could already be complete too. Not looping inline/immediately to give others partitions equal credit
            worked

        /// Unpacks a new batch from the queue; each item goes through the `waiting` queue as the loop will continue to next iteration if dispatcher is full
        let tryPrepareNext () =
            match incoming.TryDequeue() with
            | false, _ -> false
            | true, ({ source = pid; messages = msgs} as batch) ->
                startedBatches.Record(pid)
                startedItems.Record(pid, msgs.LongLength)
                let wipBatch, runners = WipBatch.Create(batch, handle)
                runners |> Seq.iter waiting.Enqueue
                match active.TryGetValue pid with
                | false, _ -> let q = Queue(1024) in active[pid] <- q; q.Enqueue wipBatch
                | true, q -> q.Enqueue wipBatch
                true

        /// Tops up the current work in progress
        let reprovisionDispatcher () =
            let mutable more, worked = true, false
            while more do
                match waiting.TryPeek() with
                | false, _ -> // Crack open a new batch if we don't have anything ready
                    more <- tryPrepareNext ()
                | true, pending -> // Dispatch until we reach capacity if we do have something
                    if tryDispatch pending then
                        worked <- true
                        waiting.Dequeue() |> ignore
                    else // Stop when it's full up
                        more <- false
            worked

        /// Main pumping loop; `abend` is a callback triggered by a faulted task which the outer controller can use to shut down the processing
        member _.Pump(abend, ct : CancellationToken) = task {
            while not ct.IsCancellationRequested do
                let hadResults = drainCompleted abend
                let queuedWork = reprovisionDispatcher ()
                let loggedStats = maybeLogStats ()
                if not hadResults && not queuedWork && not loggedStats then
                    do! Task.Delay(1, ct) }

        /// Feeds a batch of work into the queue; the caller is expected to ensure submissions are timely to avoid starvation, but throttled to ensure fair ordering
        member _.Submit(batches : Batch<'S, 'M>) =
            incoming.Enqueue batches

type ParallelIngester<'Item> =

    static member Start(log, partitionId, maxRead, submit, statsInterval) =
        let submitBatch (items : 'Item seq, onCompletion) =
            let items = Array.ofSeq items
            let batch : Submission.Batch<_, 'Item> = { source = partitionId; onCompletion = onCompletion; messages = items }
            submit batch
            struct (items.Length, items.Length)
        Ingestion.Ingester<'Item seq>.Start(log, partitionId, maxRead, submitBatch, statsInterval)

type ParallelSink =

    static member Start
            (    log : ILogger, maxReadAhead, maxDop, handle,
                 statsInterval,
                 // Default 5
                 ?maxSubmissionsPerPartition, ?logExternalStats,
                 ?ingesterStatsInterval)
            : Sink<Ingestion.Ingester<'Item seq>> =

        let maxSubmissionsPerPartition = defaultArg maxSubmissionsPerPartition 5
        let ingesterStatsInterval = defaultArg ingesterStatsInterval statsInterval
        let dispatcher = Scheduling.Dispatcher maxDop
        let scheduler = Scheduling.PartitionedSchedulingEngine<_, 'Item>(log, handle, dispatcher.TryAdd, statsInterval, ?logExternalStats=logExternalStats)

        let mapBatch onCompletion (x : Submission.Batch<_, 'Item>) : Scheduling.Batch<_, 'Item> =
            let onCompletion () = x.onCompletion(); onCompletion()
            { source = x.source; onCompletion = onCompletion; messages = x.messages}

        let submitBatch (x : Scheduling.Batch<_, 'Item>) : int =
            scheduler.Submit x
            0

        let submitter = Submission.SubmissionEngine<_, _, _>(log, maxSubmissionsPerPartition, mapBatch, submitBatch, statsInterval)
        let startIngester (rangeLog, partitionId) = ParallelIngester<'Item>.Start(rangeLog, partitionId, maxReadAhead, submitter.Ingest, ingesterStatsInterval)
        Sink.Start(log, dispatcher.Pump, (fun abend ct -> scheduler.Pump(abend, ct)), submitter.Pump, startIngester)
