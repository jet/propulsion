module Propulsion.Ingestion

open Propulsion.Internal
open Serilog
open System
open System.Threading
open System.Threading.Tasks

/// Manages writing of progress
/// - Each write attempt is always of the newest token (each update is assumed to also count for all preceding ones)
/// - a failed commit will be retried on the next CommitIfDirty (unless a new checkpoint has been posted that supersedes it)
type ProgressWriter<'Res when 'Res : equality>() =
    let mutable committedEpoch = None
    let mutable validatedPos = None
    let result = Event<Choice<'Res, exn>>()

    member _.CommitIfDirty ct = task {
        match Volatile.Read &validatedPos with
        | Some (v, f) when Volatile.Read(&committedEpoch) <> Some v ->
            try do! Async.StartImmediateAsTask(f, cancellationToken = ct)
                Volatile.Write(&committedEpoch, Some v)
                result.Trigger(Choice1Of2 v)
            with e -> result.Trigger(Choice2Of2 e)
        | _ -> () }

    [<CLIEvent>] member _.Result = result.Publish

    member _.Post(version, f) =
        Volatile.Write(&validatedPos, Some (version, f))

    member _.CommittedEpoch = Volatile.Read(&committedEpoch)

[<NoComparison; NoEquality>]
type private InternalMessage =
    /// Confirmed completion of a batch
    | Validated of epoch : int64
    /// Result from updating of Progress to backing store - processed up to nominated `epoch` or threw `exn`
    | ProgressResult of Choice<int64, exn>
    /// Internal message for stats purposes
    | Added of streams : int * events : int

[<Struct; NoComparison; NoEquality>]
type Batch<'Items> = { epoch : int64; items : 'Items; onCompletion : unit -> unit; checkpoint : Async<unit>; isTail : bool }

type private Stats(log : ILogger, partitionId, statsInterval : TimeSpan) =
    let mutable validatedEpoch, committedEpoch : int64 option * int64 option = None, None
    let mutable commitFails, commits = 0, 0
    let mutable cycles, batchesPended, streamsPended, eventsPended = 0, 0, 0, 0
    member val Interval = IntervalTimer statsInterval

    member _.DumpStats(activeReads, maxReads) =
        log.Information("Ingester {partition} Ahead {activeReads}/{maxReads} @ {validated} Committed {committed} ok {commits} failed {fails} Ingested {batches} ({streams:n0}s {events:n0}e) Cycles {cycles}",
                        partitionId, activeReads, maxReads, Option.toNullable validatedEpoch, Option.toNullable committedEpoch, commits, commitFails, batchesPended, streamsPended, eventsPended, cycles)
        cycles <- 0; batchesPended <- 0; streamsPended <- 0; eventsPended <- 0
        if commitFails <> 0 && commits = 0 then log.Error("Ingester {partition} Commits failing: {failures} failures", partitionId, commitFails)
        commits <- 0; commitFails <- 0

    member _.Handle : InternalMessage -> unit = function
        | Validated epoch ->
            validatedEpoch <- Some epoch
        | ProgressResult (Choice1Of2 epoch) ->
            commits <- commits + 1
            committedEpoch <- Some epoch
        | ProgressResult (Choice2Of2 (_exn : exn)) ->
            commitFails <- commitFails + 1
        | Added (streams, events) ->
            batchesPended <- batchesPended + 1
            streamsPended <- streamsPended + streams
            eventsPended <- eventsPended + events

    member x.RecordCycle() =
        cycles <- cycles + 1

/// Buffers items read from a range, unpacking them out of band from the reading so that can overlap
/// On completion of the unpacking, they get submitted onward to the Submitter which will buffer them for us
type Ingester<'Items> private
    (   stats : Stats, maxReadAhead,
        // forwards a set of items and the completion callback, yielding streams count * event count
        submitBatch : 'Items * (unit -> unit) -> struct (int * int),
        cts : CancellationTokenSource,
        ?commitInterval) =

    let maxRead = Sem maxReadAhead
    let awaitIncoming, applyIncoming, enqueueIncoming =
        let c = Channel.unboundedSwSr<Batch<'Items>> in let r, w = c.Reader, c.Writer
        Channel.awaitRead r, Channel.apply r, Channel.write w
    let awaitMessage, applyMessages, enqueueMessage =
        let c = Channel.unboundedSr in let r, w = c.Reader, c.Writer
        Channel.awaitRead r, Channel.apply r, Channel.write w

    let commitInterval = defaultArg commitInterval (TimeSpan.FromSeconds 5.)
    let progressWriter = ProgressWriter<_>()

    let handleIncoming (batch : Batch<'Items>) =
        let markCompleted () =
            maxRead.Release()
            batch.onCompletion () // we guarantee this happens before checkpoint can be called
            enqueueMessage <| Validated batch.epoch
            progressWriter.Post(batch.epoch, batch.checkpoint)
        let struct (streamCount, itemCount) = submitBatch (batch.items, markCompleted)
        enqueueMessage <| Added (streamCount, itemCount)

    member _.FlushProgress ct =
        progressWriter.CommitIfDirty ct

    member private x.CheckpointPeriodically(ct : CancellationToken) = task {
        while not ct.IsCancellationRequested do
            do! x.FlushProgress ct
            do! Task.Delay(commitInterval, ct) }

    member private x.Pump(ct) = task {
        use _ = progressWriter.Result.Subscribe(ProgressResult >> enqueueMessage)
        Task.start (fun () -> x.CheckpointPeriodically ct)
        while not ct.IsCancellationRequested do
            while applyIncoming handleIncoming || applyMessages stats.Handle do ()
            stats.RecordCycle()
            if stats.Interval.IfDueRestart() then let struct (active, max) = maxRead.State in stats.DumpStats(active, max)
            do! Task.WhenAny(awaitIncoming ct, awaitMessage ct, Task.Delay(stats.Interval.RemainingMs)) :> Task }
            // arguably the impl should be submitting while unpacking but
            // - maintaining consistency between incoming order and submit order is required
            // - in general maxRead will be double maxSubmit so this will only be relevant in catchup situations

    /// Starts an independent Task that handles
    /// a) `unpack`ing of `incoming` items
    /// b) `submit`ting them onward (assuming there is capacity within the `readLimit`)
    static member Start<'Items>(log, partitionId, maxRead, submitBatch, statsInterval, ?commitInterval) =
        let cts = new CancellationTokenSource()
        let stats = Stats(log, partitionId, statsInterval)
        let instance = Ingester<'Items>(stats, maxRead, submitBatch, cts, ?commitInterval = commitInterval)
        let startPump () = task {
            try do! instance.Pump cts.Token
            finally log.Information("... ingester stopped") }
        Task.start startPump
        instance

    /// Submits a batch as read for unpacking and submission; will only return after the in-flight reads drops below the limit
    /// Returns (reads in flight, maximum reads in flight)
    member _.Ingest(batch : Batch<'Items>) = task {
        // It's been read... feed it into the queue for unpacking
        enqueueIncoming batch
        // ... but we might hold off on yielding if we're at capacity
        do! maxRead.Wait(cts.Token)
        return maxRead.State }

    /// As range assignments get revoked, a user is expected to `Stop` the active processing thread for the Ingester before releasing references to it
    member _.Stop() = cts.Cancel()
