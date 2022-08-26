/// Holds batches from the Ingestion pipe, feeding them continuously to the scheduler in an appropriate order
module Propulsion.Submission

open Propulsion.Internal
open Serilog
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks

/// Batch of work as passed from the Submitter to the Scheduler comprising messages with their associated checkpointing/completion callback
[<NoComparison; NoEquality>]
type Batch<'S, 'M> = { source : 'S; onCompletion : unit -> unit; messages : 'M [] }

/// Holds the queue for a given partition, together with a semaphore we use to ensure the number of in-flight batches per partition is constrained
[<NoComparison; NoEquality>]
type private PartitionQueue<'B> = { submissions : Sem; queue : Queue<'B> } with
    member x.Append(batch) = x.queue.Enqueue batch
    static member Create(maxSubmits) = { submissions = Sem maxSubmits; queue = Queue(maxSubmits) }

type internal Stats<'S when 'S : equality>(log : ILogger, interval) =

    let mutable cycles, ingested, completed, compacted = 0, 0, 0, 0
    let submittedBatches, submittedMessages = PartitionStats<'S>(), PartitionStats<'S>()

    member val Interval = IntervalTimer interval

    member _.Dump(waiting : seq<struct ('S * int)>) =
        log.Information("Submitter ingested {ingested} compacted {compacted} completed {completed} Events {items} Batches {batches} Holding {holding} Cycles {cycles}",
                        ingested, compacted, completed, submittedMessages.StatsDescending, submittedBatches.StatsDescending, waiting, cycles)
        cycles <- 0; ingested <- 0; compacted <- 0; completed <- 0; submittedBatches.Clear(); submittedMessages.Clear()

    member _.RecordCompacted() =
        compacted <- compacted + 1
    member _.RecordBatchIngested() =
         ingested <- ingested + 1
    member _.RecordBatchCompleted() =
        Interlocked.Increment(&completed) |> ignore
    member _.RecordBatch(pi, count : int64) =
        submittedBatches.Record(pi)
        submittedMessages.Record(pi, count)
    member _.RecordCycle() =
        cycles <- cycles + 1

/// Gathers stats relating to how many items of a given partition have been observed
and PartitionStats<'S when 'S : equality>() =
    let partitions = Dictionary<'S, int64>()

    member _.Record(partitionId, ?weight) =
        let weight = defaultArg weight 1L
        let mutable catCount = Unchecked.defaultof<_>
        if partitions.TryGetValue(partitionId, &catCount) then partitions[partitionId] <- catCount + weight
        else partitions[partitionId] <- weight

    member _.Clear() = partitions.Clear()
    member _.StatsDescending = Stats.statsDescending partitions

/// Holds the stream of incoming batches, grouping by partition
/// Manages the submission of batches into the Scheduler in a fair manner
type SubmissionEngine<'S, 'M, 'B when 'S : equality>
    (   log : ILogger, maxSubmitsPerPartition, mapBatch : (unit -> unit) -> Batch<'S, 'M> -> 'B, submitBatch : 'B -> int, statsInterval,
        ?tryCompactQueue) =

    let awaitIncoming, applyIncoming, enqueueIncoming =
        let c = Channel.unboundedSr in let r, w = c.Reader, c.Writer
        Channel.awaitRead r, Channel.apply r, Channel.write w
    let buffer = Dictionary<'S, PartitionQueue<'B>>()
    let queueStats = seq { for x in buffer do if x.Value.queue.Count <> 0 then struct (x.Key, x.Value.queue.Count) } |> Seq.sortByDescending ValueTuple.snd
    let stats = Stats<'S>(log, statsInterval)

    // Loop, submitting 0 or 1 item per partition per iteration to ensure
    // - each partition has a controlled maximum number of entrants in the scheduler queue
    // - a fair ordering of batch submissions
    let tryPropagate (waiting : ResizeArray<Sem>) =
        waiting.Clear()
        let mutable worked = false
        for kv in buffer do
            let pi, pq = kv.Key, kv.Value
            if pq.queue.Count <> 0 then
                if pq.submissions.TryTake() then
                    worked <- true
                    let count = submitBatch <| pq.queue.Dequeue()
                    stats.RecordBatch(pi, count)
                else waiting.Add(pq.submissions)
        worked

    let ingest (partitionBatches : Batch<'S, 'M>[]) =
        stats.RecordBatchIngested()
        for { source = pid } as batch in partitionBatches do
            let mutable pq = Unchecked.defaultof<_>
            if not (buffer.TryGetValue(pid, &pq)) then
                pq <- PartitionQueue<_>.Create(maxSubmitsPerPartition)
                buffer[pid] <- pq
            let markCompleted () =
                stats.RecordBatchCompleted()
                pq.submissions.Release()
            let mapped = mapBatch markCompleted batch
            pq.Append(mapped)

    /// We use timeslices where we're we've fully provisioned the scheduler to index any waiting Batches
    let compact f =
        let mutable worked = false
        for kv in buffer do
            if f kv.Value.queue then
                worked <- true
        if worked then stats.RecordCompacted()
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
            stats.RecordCycle()
            if stats.Interval.IfExpiredRestart() then stats.Dump(queueStats)
            do! Task.WhenAny[| awaitIncoming ct :> Task; yield! submitCapacityAvailable; Task.Delay(stats.Interval.RemainingMs) |] :> Task }

    /// Supplies a set of Batches for holding and forwarding to scheduler at the right time
    member _.Ingest(items : Batch<'S, 'M>[]) =
        enqueueIncoming items

    /// Supplies an incoming Batch for holding and forwarding to scheduler at the right time
    member x.Ingest(batch : Batch<'S, 'M>) =
        x.Ingest [| batch |]
