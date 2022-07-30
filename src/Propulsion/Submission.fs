namespace Propulsion

open Serilog
open System
open System.Collections.Concurrent
open System.Collections.Generic
open System.Diagnostics
open System.Threading

module Internal =

    let sortByVsndDescending (xs : seq<struct (_ * _)>) = xs |> Seq.sortByDescending (fun struct (_k, v) -> v)
    let statsDescending (xs : Dictionary<_, _>) = xs |> Seq.map (fun x -> struct (x.Key, x.Value)) |> sortByVsndDescending
    let statsTotal (xs : struct (_ * int64) array) = xs |> Array.sumBy (fun struct (_k, v) -> v)

    /// Gathers stats relating to how many items of a given partition have been observed
    type PartitionStats<'S when 'S : equality>() =
        let partitions = Dictionary<'S, int64>()

        member _.Record(partitionId, ?weight) =
            let weight = defaultArg weight 1L
            match partitions.TryGetValue partitionId with
            | true, catCount -> partitions[partitionId] <- catCount + weight
            | false, _ -> partitions[partitionId] <- weight

        member _.Clear() = partitions.Clear()
        member _.StatsDescending = statsDescending partitions

    /// Maintains a Stopwatch such that invoking will yield true at intervals defined by `period`
    let intervalCheck (period : TimeSpan) =
        let timer, max = Stopwatch.StartNew(), int64 period.TotalMilliseconds
        fun () ->
            let due = timer.ElapsedMilliseconds > max
            if due then timer.Restart()
            due
    let timeRemaining (period : TimeSpan) =
        let timer, max = Stopwatch.StartNew(), int64 period.TotalMilliseconds
        fun () ->
            match max - timer.ElapsedMilliseconds |> int with
            | rem when rem <= 0 -> timer.Restart(); true, max
            | rem -> false, rem
    let atTimedIntervals (period : TimeSpan) =
        let timer, max = Stopwatch.StartNew(), int64 period.TotalMilliseconds
        let remNow () = max - timer.ElapsedMilliseconds |> int
        fun f ->
            match remNow () with
            | rem when rem <= 0 -> f (); timer.Restart(); remNow ()
            | rem -> rem

    module Channel =

        open System.Threading.Channels

        let unboundedSwSr<'t> = Channel.CreateUnbounded<'t>(UnboundedChannelOptions(SingleWriter = true, SingleReader = true))
        let unboundedSr<'t> = Channel.CreateUnbounded<'t>(UnboundedChannelOptions(SingleReader = true))
        let write (c : Channel<_>) = c.Writer.TryWrite >> ignore
        let awaitRead (c : Channel<_>) () = let vt = c.Reader.WaitToReadAsync() in vt.AsTask()
        let apply (c : Channel<_>) f =
            let mutable worked, msg = false, Unchecked.defaultof<_>
            while c.Reader.TryRead(&msg) do
                worked <- true
                f msg
            worked

    module Task =

        open System.Threading.Tasks

        let start t = Task.Run(Action(fun () -> t |> ignore<Task>)) |> ignore
        let sleep (ts : TimeSpan) ct = Task.Delay(int ts.TotalMilliseconds, cancellationToken = ct)

    type Sem(max) =
        let inner = new SemaphoreSlim(max)
        member _.HasCapacity = inner.CurrentCount <> 0
        member _.State = max-inner.CurrentCount,max
        member _.Await(ct : CancellationToken) = inner.WaitAsync(ct) |> Async.AwaitTaskCorrect
        member _.Release() = inner.Release() |> ignore
        member _.TryTake() = inner.Wait 0

open Internal

/// Holds batches from the Ingestion pipe, feeding them continuously to the scheduler in an appropriate order
module Submission =

    /// Batch of work as passed from the Submitter to the Scheduler comprising messages with their associated checkpointing/completion callback
    [<NoComparison; NoEquality>]
    type SubmissionBatch<'S, 'M> = { source : 'S; onCompletion : unit -> unit; messages : 'M [] }

    /// Holds the queue for a given partition, together with a semaphore we use to ensure the number of in-flight batches per partition is constrained
    [<NoComparison>]
    type PartitionQueue<'B> = { submissions : Sem; queue : Queue<'B> } with
        member x.Append(batch) = x.queue.Enqueue batch
        static member Create(maxSubmits) = { submissions = Sem maxSubmits; queue = Queue(maxSubmits) }

    /// Holds the stream of incoming batches, grouping by partition
    /// Manages the submission of batches into the Scheduler in a fair manner
    type SubmissionEngine<'S, 'M, 'B when 'S : equality>
        (   log : ILogger, maxSubmitsPerPartition, mapBatch : (unit -> unit) -> SubmissionBatch<'S, 'M> -> 'B, submitBatch : 'B -> int, statsInterval, ?pumpInterval : TimeSpan,
            ?tryCompactQueue) =

        let pumpInterval = defaultArg pumpInterval (TimeSpan.FromMilliseconds 5.)
        let incoming = new BlockingCollection<SubmissionBatch<'S, 'M>[]>(ConcurrentQueue())
        let buffer = Dictionary<'S, PartitionQueue<'B>>()
        let mutable cycles, ingested, completed, compacted = 0, 0, 0, 0
        let submittedBatches,submittedMessages = PartitionStats(), PartitionStats()

        let dumpStats () =
            let waiting = seq { for x in buffer do if x.Value.queue.Count <> 0 then yield struct (x.Key, x.Value.queue.Count) } |> sortByVsndDescending
            log.Information("Submitter {cycles} cycles {ingested} accepted {compactions} compactions Holding {@waiting}", cycles, ingested, compacted, waiting)
            log.Information(" Submitted {@batches} Completed {completed} Messages {@messages}", submittedBatches.StatsDescending, completed, submittedMessages.StatsDescending)
            cycles <- 0; ingested <- 0; compacted <- 0; completed <- 0; submittedBatches.Clear(); submittedMessages.Clear()

        let maybeLogStats =
            let due = intervalCheck statsInterval
            fun () ->
                cycles <- cycles + 1
                if due () then dumpStats ()

        // Loop, submitting 0 or 1 item per partition per iteration to ensure
        // - each partition has a controlled maximum number of entrants in the scheduler queue
        // - a fair ordering of batch submissions
        let propagate () =
            let mutable more, worked = true, false
            while more do
                more <- false
                for KeyValue (pi, pq) in buffer do
                    if pq.queue.Count <> 0 then
                        if pq.submissions.TryTake() then
                            worked <- true
                            more <- true
                            let count = submitBatch <| pq.queue.Dequeue()
                            submittedBatches.Record(pi)
                            submittedMessages.Record(pi, int64 count)
            worked

        /// Take one timeslice worth of ingestion and add to relevant partition queues
        /// When ingested, we allow one propagation submission per partition
        let ingest (partitionBatches : SubmissionBatch<'S, 'M>[]) =
            for { source = pid } as batch in partitionBatches do
                let pq =
                    match buffer.TryGetValue pid with
                    | false, _ -> let t = PartitionQueue<_>.Create(maxSubmitsPerPartition) in buffer[pid] <- t; t
                    | true, pq -> pq
                let markCompleted () =
                    Interlocked.Increment(&completed) |> ignore
                    pq.submissions.Release()
                let mapped = mapBatch markCompleted batch
                pq.Append(mapped)
            propagate()

        /// We use timeslices where we're we've fully provisioned the scheduler to index any waiting Batches
        let compact f =
            let mutable worked = false
            for KeyValue(_, pq) in buffer do
                if f pq.queue then
                    worked <- true
            if worked then compacted <- compacted + 1; true
            else false

        /// Processing loop, continuously splitting `Submit`ted items into per-partition queues and ensuring enough items are provided to the Scheduler
        member _.Pump() = async {
            let! ct = Async.CancellationToken
            while not ct.IsCancellationRequested do
                let mutable items = Unchecked.defaultof<_>
                let mutable propagated = false
                if incoming.TryTake(&items, pumpInterval) then
                    propagated <- ingest items
                    while incoming.TryTake(&items) do
                        if ingest items then propagated <- true
                else propagated <- propagate ()
                match propagated, tryCompactQueue with
                | false, None -> Thread.Sleep 2
                | false, Some f when not (compact f) -> Thread.Sleep 2
                | _ -> ()

                maybeLogStats () }

        /// Supplies a set of Batches for holding and forwarding to scheduler at the right time
        member _.Ingest(items : SubmissionBatch<'S, 'M>[]) =
            Interlocked.Increment(&ingested) |> ignore
            incoming.Add items

        /// Supplies an incoming Batch for holding and forwarding to scheduler at the right time
        member x.Ingest(batch : SubmissionBatch<'S, 'M>) =
            x.Ingest [| batch |]
