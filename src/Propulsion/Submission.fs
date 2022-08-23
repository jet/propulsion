/// Holds batches from the Ingestion pipe, feeding them continuously to the scheduler in an appropriate order
module Propulsion.Submission

open Propulsion.Internal // Helpers
open Serilog
open System
open System.Collections.Generic
open System.Diagnostics
open System.Threading
open System.Threading.Tasks

[<AutoOpen>]
module Helpers =

    let statsTotal (xs : struct (_ * int64) array) = xs |> Array.sumBy ValueTuple.snd

    /// Gathers stats relating to how many items of a given partition have been observed
    type PartitionStats<'S when 'S : equality>() =
        let partitions = Dictionary<'S, int64>()

        member _.Record(partitionId, ?weight) =
            let weight = defaultArg weight 1L
            match partitions.TryGetValue partitionId with
            | true, catCount -> partitions[partitionId] <- catCount + weight
            | false, _ -> partitions[partitionId] <- weight

        member _.Clear() = partitions.Clear()
        member _.StatsDescending = Stats.statsDescending partitions

    let atTimedIntervals (period : TimeSpan) =
        let timer, max = Stopwatch.StartNew(), int64 period.TotalMilliseconds
        let remNow () = max - timer.ElapsedMilliseconds |> int
        fun f ->
            match remNow () with
            | rem when rem <= 0 -> f (); timer.Restart(); remNow ()
            | rem -> rem

/// Batch of work as passed from the Submitter to the Scheduler comprising messages with their associated checkpointing/completion callback
[<NoComparison; NoEquality>]
type Batch<'S, 'M> = { source : 'S; onCompletion : unit -> unit; messages : 'M [] }

/// Holds the queue for a given partition, together with a semaphore we use to ensure the number of in-flight batches per partition is constrained
[<NoComparison>]
type PartitionQueue<'B> = { submissions : Sem; queue : Queue<'B> } with
    member x.Append(batch) = x.queue.Enqueue batch
    static member Create(maxSubmits) = { submissions = Sem maxSubmits; queue = Queue(maxSubmits) }

/// Holds the stream of incoming batches, grouping by partition
/// Manages the submission of batches into the Scheduler in a fair manner
type SubmissionEngine<'S, 'M, 'B when 'S : equality>
    (   log : ILogger, maxSubmitsPerPartition, mapBatch : (unit -> unit) -> Batch<'S, 'M> -> 'B, submitBatch : 'B -> int, statsInterval,
        ?tryCompactQueue) =

    let awaitIncoming, applyIncoming, enqueueIncoming =
        let c = Channel.unboundedSr
        Channel.awaitRead c.Reader, Channel.apply c.Reader, Channel.write c.Writer
    let buffer = Dictionary<'S, PartitionQueue<'B>>()

    let mutable cycles, ingested, completed, compacted = 0, 0, 0, 0
    let submittedBatches,submittedMessages = PartitionStats(), PartitionStats()
    let statsInterval = timeRemaining statsInterval
    let dumpStats () =
        let waiting = seq { for x in buffer do if x.Value.queue.Count <> 0 then struct (x.Key, x.Value.queue.Count) } |> Seq.sortByDescending ValueTuple.snd
        log.Information("Submitter ingested {ingested} compacted {compacted} completed {completed} Events {items} Batches {batches} Holding {holding} Cycles {cycles}",
                        ingested, compacted, completed, submittedMessages.StatsDescending, submittedBatches.StatsDescending, waiting, cycles)
        cycles <- 0; ingested <- 0; compacted <- 0; completed <- 0; submittedBatches.Clear(); submittedMessages.Clear()
    let maybeDumpStats () =
        cycles <- cycles + 1
        let struct (due, remaining) = statsInterval ()
        if due then dumpStats ()
        int remaining

    // Loop, submitting 0 or 1 item per partition per iteration to ensure
    // - each partition has a controlled maximum number of entrants in the scheduler queue
    // - a fair ordering of batch submissions
    let tryPropagate (waiting : ResizeArray<Sem>) =
        waiting.Clear()
        let mutable worked = false
        for KeyValue (pi, pq) in buffer do
            if pq.queue.Count <> 0 then
                if pq.submissions.TryTake() then
                    worked <- true
                    let count = submitBatch <| pq.queue.Dequeue()
                    submittedBatches.Record(pi)
                    submittedMessages.Record(pi, int64 count)
                else waiting.Add(pq.submissions)
        worked

    let ingest (partitionBatches : Batch<'S, 'M>[]) =
        ingested <- ingested + 1
        for { source = pid } as batch in partitionBatches do
            let mutable pq = Unchecked.defaultof<_>
            if not (buffer.TryGetValue(pid, &pq)) then
                pq <- PartitionQueue<_>.Create(maxSubmitsPerPartition)
                buffer[pid] <- pq
            let markCompleted () =
                Interlocked.Increment(&completed) |> ignore
                pq.submissions.Release()
            let mapped = mapBatch markCompleted batch
            pq.Append(mapped)

    /// We use timeslices where we're we've fully provisioned the scheduler to index any waiting Batches
    let compact f =
        let mutable worked = false
        for KeyValue(_, pq) in buffer do
            if f pq.queue then
                worked <- true
        if worked then compacted <- compacted + 1
        worked
    let maybeCompact () =
        match tryCompactQueue with
        | Some f -> compact f
        | None -> false

    /// Processing loop, continuously splitting `Submit`ted items into per-partition queues and ensuring enough items are provided to the Scheduler
    member _.Pump(ct : CancellationToken) = task {
        // Semaphores for partitions that have reached their submit limit; if capacity becomes available, we want to wake to submit
        let waitingSubmissions = ResizeArray<Sem>()
        let submitCapacityAvailable : seq<Task> = seq { for w in waitingSubmissions -> w.AwaitButRelease() }
        while not ct.IsCancellationRequested do
            while applyIncoming ingest || tryPropagate waitingSubmissions || maybeCompact () do ()
            let nextStatsIntervalMs = maybeDumpStats ()
            do! Task.WhenAny[| awaitIncoming ct :> Task; yield! submitCapacityAvailable; Task.Delay(nextStatsIntervalMs) |] :> Task }

    /// Supplies a set of Batches for holding and forwarding to scheduler at the right time
    member _.Ingest(items : Batch<'S, 'M>[]) =
        enqueueIncoming items

    /// Supplies an incoming Batch for holding and forwarding to scheduler at the right time
    member x.Ingest(batch : Batch<'S, 'M>) =
        x.Ingest [| batch |]
