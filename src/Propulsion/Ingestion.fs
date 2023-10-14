module Propulsion.Ingestion

open Propulsion.Internal
open Serilog
open System
open System.Threading

/// Manages writing of progress
/// - Each write attempt is always of the newest token (each update is assumed to also count for all preceding ones)
/// - a failed commit will be retried on the next CommitIfDirty (unless a new checkpoint has been posted that supersedes it)
type ProgressWriter<'Res when 'Res: equality>() =
    let mutable committedEpoch = None
    let mutable validatedPos = None
    let result = Event<Result<'Res, exn>>()

    [<CLIEvent>] member _.Result = result.Publish

    member _.IsDirty =
        match Volatile.Read &validatedPos with
        | Some (v, _) when Volatile.Read(&committedEpoch) <> Some v -> true
        | _ -> false

    member x.CommitIfDirty ct = task {
        match Volatile.Read &validatedPos with
        | Some (v, f) when Volatile.Read(&committedEpoch) <> Some v ->
            try do! f ct
                result.Trigger(Ok v)
                Volatile.Write(&committedEpoch, Some v)
            with e -> result.Trigger(Error e)
        | _ -> () }

    member _.Post(version, f) =
        Volatile.Write(&validatedPos, Some (version, f))

[<NoComparison; NoEquality>]
type private InternalMessage =
    /// Confirmed completion of a batch
    | Validated of epoch: int64
    /// Result from updating of Progress to backing store - processed up to nominated `epoch` or threw `exn`
    | ProgressResult of Result<int64, exn>
    /// Internal message for stats purposes
    | Added of epoch: int64 * streams: int * events: int

[<Struct; NoComparison; NoEquality>]
type Batch<'Items> = { epoch: int64; items: 'Items; onCompletion: unit -> unit; checkpoint: CancellationToken -> Task<unit>; isTail: bool }

type private Stats(log: ILogger, partitionId, statsInterval: TimeSpan) =
    let mutable readEpoch, validatedEpoch, committedEpoch = None, None, None
    let mutable commitFails, commits = 0, 0
    let mutable cycles, batchesPended, streamsPended, eventsPended = 0, 0, 0, 0
    member val Interval = IntervalTimer statsInterval

    member _.DumpStats(activeReads, maxReads) =
        log.Information("Ingester {partition} Ahead {activeReads}/{maxReads} Committed {committed} @ {validated}/{pos} ok {commits} failed {fails} Ingested {batches} ({streams:n0}s {events:n0}e) Cycles {cycles}",
                        partitionId, activeReads, maxReads, Option.toNullable committedEpoch, Option.toNullable validatedEpoch, Option.toNullable readEpoch, commits, commitFails, batchesPended, streamsPended, eventsPended, cycles)
        cycles <- 0; batchesPended <- 0; streamsPended <- 0; eventsPended <- 0
        if commitFails <> 0 && commits = 0 then log.Error("Ingester {partition} Commits failing: {failures} failures", partitionId, commitFails)
        commits <- 0; commitFails <- 0

    member _.Handle: InternalMessage -> unit = function
        | Validated epoch ->
            validatedEpoch <- Some epoch
        | ProgressResult (Ok epoch) ->
            commits <- commits + 1
            committedEpoch <- Some epoch
        | ProgressResult (Error (_exn: exn)) ->
            commitFails <- commitFails + 1
        | Added (epoch, streams, events) ->
            readEpoch <- Some epoch
            batchesPended <- batchesPended + 1
            streamsPended <- streamsPended + streams
            eventsPended <- eventsPended + events

    member x.RecordCycle() =
        cycles <- cycles + 1

/// Buffers items read from a range, unpacking them out of band from the reading so that can overlap
/// On completion of the unpacking, they get submitted onward to the Submitter which will buffer them for us
type Ingester<'Items> private
    (   stats: Stats, maxReadAhead,
        // forwards a set of items and the completion callback, yielding streams count * event count
        submitBatch: 'Items * (unit -> unit) -> struct (int * int),
        cts: CancellationTokenSource,
        ?commitInterval) =

    let maxRead = Sem maxReadAhead
    let awaitIncoming, applyIncoming, enqueueIncoming =
        let c = Channel.unboundedSr<Batch<'Items>> in let r, w = c.Reader, c.Writer
        Channel.awaitRead r, Channel.apply r, Channel.write w
    let awaitMessage, applyMessages, enqueueMessage =
        let c = Channel.unboundedSr in let r, w = c.Reader, c.Writer
        Channel.awaitRead r, Channel.apply r, Channel.write w

    let commitInterval = defaultArg commitInterval (TimeSpan.FromSeconds 5.)
    let progressWriter = ProgressWriter<_>()

    let handleIncoming (batch: Batch<'Items>) =
        let markCompleted () =
            enqueueMessage <| Validated batch.epoch
            // Need to report progress before the Release or batch.OnCompletion, in order for AwaitCheckpointed to be correct
            progressWriter.Post(batch.epoch, batch.checkpoint)
            batch.onCompletion ()
            maxRead.Release()
        let struct (streamCount, itemCount) = submitBatch (batch.items, markCompleted)
        enqueueMessage <| Added (batch.epoch, streamCount, itemCount)

    member _.FlushProgress ct =
        progressWriter.CommitIfDirty ct

    member private x.AwaitCheckpointed ct = task {
        if progressWriter.IsDirty then
            try do! x.FlushProgress ct
            with _ -> () // one attempt to do it proactively
            while progressWriter.IsDirty do
                do! Task.delay (commitInterval / 2.) ct }

    member private x.CheckpointPeriodically(ct: CancellationToken) = task {
        while not ct.IsCancellationRequested do
            do! x.FlushProgress ct
            do! Task.delay commitInterval ct }

    member private x.Pump(ct) = task {
        use _ = progressWriter.Result.Subscribe(ProgressResult >> enqueueMessage)
        Task.start (fun () -> x.CheckpointPeriodically ct)
        let mutable exiting = false
        while not exiting do
            exiting <- ct.IsCancellationRequested
            while applyIncoming handleIncoming || applyMessages stats.Handle do ()
            stats.RecordCycle()
            if exiting || stats.Interval.IfDueRestart() then let struct (active, max) = maxRead.State in stats.DumpStats(active, max)
            let startWaits ct = [| awaitIncoming ct :> Task
                                   awaitMessage ct
                                   Task.Delay(stats.Interval.RemainingMs, ct) |]
            if not exiting then do! Task.runWithCancellation ct (fun ct -> Task.WhenAny(startWaits ct)) }
    /// Starts an independent Task that handles
    /// a) `unpack`ing of `incoming` items
    /// b) `submit`ting them onward (assuming there is capacity within the `maxReadAhead`)
    static member Start<'Items>(log, partitionId, maxReadAhead, submitBatch, statsInterval, ?commitInterval) =
        let cts = new CancellationTokenSource()
        let stats = Stats(log, partitionId, statsInterval)
        let instance = Ingester<'Items>(stats, maxReadAhead, submitBatch, cts, ?commitInterval = commitInterval)
        let startPump () = task {
            try do! instance.Pump cts.Token
            finally log.Information("... ingester stopped") }
        Task.start startPump
        instance

    /// Submits a batch as read for unpacking and submission; will only return after the in-flight reads drops below the limit
    /// Returns (reads in flight, maximum reads in flight)
    member _.Ingest(batch: Batch<'Items>) = task {
        // It's been read... feed it into the queue for unpacking
        enqueueIncoming batch
        // ... but we might hold off on yielding if we're at capacity
        do! maxRead.Wait(cts.Token)
        return maxRead.State }

    /// As range assignments get revoked, a user is expected to `Stop` the active processing thread for the Ingester before releasing references to it
    member _.Stop() = cts.Cancel()

    member x.Wait(ct) = task {
        let! r = maxRead.WaitForCompleted ct
        do! x.AwaitCheckpointed ct
        return r }
