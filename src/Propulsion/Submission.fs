/// Holds batches from the Ingestion pipe, feeding them continuously to the scheduler in an appropriate order
module Propulsion.Submission

open Propulsion.Internal
open Serilog
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks

/// Batch of work as passed from the Submitter to the Scheduler comprising messages with their associated checkpointing/completion callback
[<NoComparison; NoEquality>]
type Batch<'P, 'M> = { partitionId : 'P; onCompletion : unit -> unit; messages : 'M [] }

/// Holds the queue for a given partition, together with a semaphore we use to ensure the number of in-flight batches per partition is constrained
[<NoComparison; NoEquality>]
type private PartitionQueue<'B> = { queue : Queue<'B> } with
    member x.Append(batch) = x.queue.Enqueue batch
    static member Create() = { queue = Queue() }

type internal Stats<'S when 'S : equality>(log : ILogger, interval) =

    let mutable cycles, ingested, completed = 0, 0, 0
    let submittedBatches, submittedSpans = PartitionStats<'S>(), PartitionStats<'S>()

    member val Interval = IntervalTimer interval

    member _.Dump(waiting : seq<struct ('S * int)>) =
        log.Information("Submitter Ingested {ingested} Completed {completed} Submitted batches {batches} spans {streams} Holding {holding} Cycles {cycles}",
                        ingested, completed, submittedBatches.StatsDescending, submittedSpans.StatsDescending, waiting, cycles)
        cycles <- 0; ingested <- 0; completed <- 0; submittedBatches.Clear(); submittedSpans.Clear()

    member _.RecordBatchIngested() =
         ingested <- ingested + 1
    member _.RecordBatchCompleted() =
        Interlocked.Increment(&completed) |> ignore
    member _.RecordBatchSubmitted(pi, spanCount : int64) =
        submittedBatches.Record(pi, 1L)
        submittedSpans.Record(pi, spanCount)
    member _.RecordCycle() =
        cycles <- cycles + 1

/// Gathers stats relating to how many items of a given partition have been observed
and PartitionStats<'S when 'S : equality>() =
    let partitions = Dictionary<'S, int64>()

    member _.Record(partitionId, weight) =
        match partitions.TryGetValue partitionId with
        | true, catCount -> partitions[partitionId] <- catCount + weight
        | false, _ -> partitions[partitionId] <- weight

    member _.Clear() = partitions.Clear()
    member _.StatsDescending = Stats.statsDescending partitions

type private SubmitOutcome = Worked | TargetFull | SourceDrained

/// Holds the incoming batches, grouping by partition
/// Manages the submission of batches into the Scheduler on a round-robin basis
type SubmissionEngine<'P, 'M, 'S, 'B when 'P : equality>
    (   log : ILogger, statsInterval,
        mapBatch : (unit -> unit) -> Batch<'P, 'M> -> struct ('S * 'B),
        submitStreams : 'S -> unit, // We continually submit the spans of events as we receive them, in order to minimise handler invocations required
        waitToSubmitBatch : CancellationToken -> Task, // We continually fill the target to the degree possible. When full, we sleep until capacity is released on completion
        trySubmitBatch : 'B -> int voption) = // batches are submitted just in time in order to balance progress across partitions
    let stats = Stats<'P>(log, statsInterval)
    let buffer = Dictionary<'P, PartitionQueue<'B>>()
    let partitions = ResizeArray()
    let ingest (partitionBatches : Batch<'P, 'M>[]) =
        stats.RecordBatchIngested()
        for { partitionId = partitionId } as batch in partitionBatches do
            let mutable pq = Unchecked.defaultof<_>
            if not (buffer.TryGetValue(partitionId, &pq)) then
                pq <- PartitionQueue<_>.Create()
                buffer[partitionId] <- pq
                partitions.Add partitionId
            let markCompleted () =
                batch.onCompletion ()
                stats.RecordBatchCompleted()
            let struct (s : 'S, b) = mapBatch markCompleted batch
            submitStreams s
            pq.Append b
    let queueStats = seq { for x in buffer do if x.Value.queue.Count <> 0 then struct (x.Key, x.Value.queue.Count) } |> Seq.sortByDescending ValueTuple.snd

    let mutable partitionIndex = 0 // index into partitions, wraps to 0 when it reaches Count in order to provide round-robin submission attempts
    let tryFindSubmissionCandidate () =
        let index i = (partitionIndex + i) % partitions.Count
        seq { for i in 0..partitions.Count - 1 ->
                let partitionId = partitions[index i]
                struct (partitionId, buffer[partitionId].queue, index (i + 1)) }
        |> Seq.tryFind (fun struct (_pid, pq, _next) -> pq.Count <> 0)
    let tryPropagate () =
        match tryFindSubmissionCandidate () with
        | None -> SourceDrained
        | Some (partitionId, pq, nextIndex) ->
            match pq.Peek() |> trySubmitBatch with
            | ValueSome spanCount ->
               partitionIndex <- nextIndex
               pq.Dequeue() |> ignore
               stats.RecordBatchSubmitted(partitionId, spanCount)
               Worked
            | ValueNone -> TargetFull

    let enqueueIncoming, awaitIncoming, applyIncoming =
        let c = Channel.unboundedSr in let r, w = c.Reader, c.Writer
        Channel.write w, Channel.awaitRead r, Channel.apply r

    /// Processing loop, continuously splitting `Submit`ted items into per-partition queues and ensuring enough items are provided to the Scheduler
    member _.Pump(ct : CancellationToken) = task {
        while not ct.IsCancellationRequested do
            while applyIncoming ingest do ()
            let mutable awaitCapacity = false
            let shouldLoop = function
                | Worked -> true
                | TargetFull -> awaitCapacity <- true; false
                | SourceDrained -> false
            while tryPropagate () |> shouldLoop do ()
            stats.RecordCycle()
            if stats.Interval.IfDueRestart() then stats.Dump(queueStats)
            let startWaits ct = [| if awaitCapacity then waitToSubmitBatch ct
                                   awaitIncoming ct
                                   Task.Delay(stats.Interval.RemainingMs, ct) |]
            do! Task.runWithCancellation ct (fun ct -> Task.WhenAny(startWaits ct)) }

    /// Supplies a set of Batches for holding and forwarding to scheduler at the right time
    member _.Ingest(items : Batch<'P, 'M>[]) =
        enqueueIncoming items

    /// Supplies an incoming Batch for holding and forwarding to scheduler at the right time
    member x.Ingest(batch : Batch<'P, 'M>) =
        x.Ingest [| batch |]
