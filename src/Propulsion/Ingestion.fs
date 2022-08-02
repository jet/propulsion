module Propulsion.Ingestion

open Propulsion.Internal // Helpers
open Serilog
open System
open System.Threading
open System.Threading.Tasks

/// Manages writing of progress
/// - Each write attempt is always of the newest token (each update is assumed to also count for all preceding ones)
/// - retries until success or a new item is posted
type ProgressWriter<'Res when 'Res : equality>(?period) =
    let commitInterval = defaultArg period (TimeSpan.FromSeconds 5.)
    let mutable committedEpoch = None
    let mutable validatedPos = None
    let result = Event<Choice<'Res, exn>>()

    let commitIfDirty ct = task {
        match Volatile.Read &validatedPos with
        | Some (v,f) when Volatile.Read(&committedEpoch) <> Some v ->
            try do! Async.StartAsTask(f, cancellationToken = ct)
                Volatile.Write(&committedEpoch, Some v)
                result.Trigger (Choice1Of2 v)
            with e -> result.Trigger (Choice2Of2 e)
        | _ -> () }

    [<CLIEvent>] member _.Result = result.Publish

    member _.Post(version,f) =
        Volatile.Write(&validatedPos,Some (version, f))

    member _.CommittedEpoch = Volatile.Read(&committedEpoch)

    member _.Pump ct = task {
        while true do
            do! commitIfDirty ct
            do! Task.Delay(commitInterval, ct) }

[<NoComparison; NoEquality>]
type private InternalMessage =
    /// Confirmed completion of a batch
    | Validated of epoch : int64
    /// Result from updating of Progress to backing store - processed up to nominated `epoch` or threw `exn`
    | ProgressResult of Choice<int64, exn>
    /// Internal message for stats purposes
    | Added of streams : int * events : int

type private Stats(log : ILogger, partitionId, statsInterval : TimeSpan) =
    let mutable validatedEpoch, committedEpoch : int64 option * int64 option = None, None
    let mutable commitFails, commits = 0, 0
    let mutable cycles, batchesPended, streamsPended, eventsPended = 0, 0, 0, 0
    let statsInterval = timeRemaining statsInterval

    let dumpStats (activeReads, maxReads) =
        log.Information("Ingester {partitionId} Ahead {activeReads}/{maxReads} @ {validated} (committed: {committed}, {commits} commits) Ingested {batches} ({streams:n0}s {events:n0}e) Cycles {cycles}",
                        partitionId, activeReads, maxReads, Option.toNullable validatedEpoch, Option.toNullable committedEpoch, commits, batchesPended, streamsPended, eventsPended, cycles)
        cycles <- 0; batchesPended <- 0; streamsPended <- 0; eventsPended <- 0
        if commitFails <> 0 || commits <> 0 then
            if commits = 0 then log.Error("Ingester {partitionId} Commits failing: {failures} failures", partitionId, commitFails)
            else log.Warning("Ingester {partitionId} Commits {failures} failures, {commits} successes", partitionId, commitFails, commits)
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

    member _.TryDump(readState) =
        cycles <- cycles + 1
        let due, remaining = statsInterval ()
        if due then dumpStats readState
        remaining

/// Buffers items read from a range, unpacking them out of band from the reading so that can overlap
/// On completion of the unpacking, they get submitted onward to the Submitter which will buffer them for us
type Ingester<'Items, 'Batch> private
    (   stats : Stats, maxRead,
        makeBatch : (unit -> unit) -> 'Items -> 'Batch * (int * int),
        submit : 'Batch -> unit,
        cts : CancellationTokenSource) =

    let maxRead = Sem maxRead
    let awaitIncoming, applyIncoming, enqueueIncoming =
        let c = Channel.unboundedSwSr
        Channel.awaitRead c, Channel.apply c, Channel.write c
    let awaitMessage, applyMessages, enqueueMessage =
        let c = Channel.unboundedSr
        Channel.awaitRead c, Channel.apply c, Channel.write c

    let progressWriter = ProgressWriter<_>()

    let handleIncoming (epoch, checkpoint, items, outerMarkCompleted) =
        let markCompleted () =
            maxRead.Release()
            outerMarkCompleted |> Option.iter (fun f -> f ()) // we guarantee this happens before checkpoint can be called
            enqueueMessage <| Validated epoch
            progressWriter.Post(epoch, checkpoint)
        let batch, (streamCount, itemCount) = makeBatch markCompleted items
        submit batch
        enqueueMessage <| Added (streamCount,itemCount)

    member private _.Pump ct = task {
        use _ = progressWriter.Result.Subscribe(ProgressResult >> enqueueMessage)
        Task.start (progressWriter.Pump ct)
        while true do
            while applyIncoming handleIncoming || applyMessages stats.Handle do ()
            let nextStatsIntervalMs = stats.TryDump(maxRead.State)
            do! Task.WhenAny(awaitIncoming ct, awaitMessage ct, Task.Delay(int nextStatsIntervalMs)) :> Task }
            // arguably the impl should be submitting while unpacking but
            // - maintaining consistency between incoming order and submit order is required
            // - in general maxRead will be double maxSubmit so this will only be relevant in catchup situations

    /// Starts an independent Task that handles
    /// a) `unpack`ing of `incoming` items
    /// b) `submit`ting them onward (assuming there is capacity within the `readLimit`)
    static member Start<'Item>(log, partitionId, maxRead, makeBatch, submit, statsInterval) =
        let cts = new CancellationTokenSource()
        let stats = Stats(log, partitionId, statsInterval)
        let instance = Ingester<_, _>(stats, maxRead, makeBatch, submit, cts)
        Task.start (instance.Pump cts.Token)
        instance

    /// Submits a batch as read for unpacking and submission; will only return after the in-flight reads drops below the limit
    /// Returns (reads in flight, maximum reads in flight)
    /// markCompleted will (if supplied) be triggered when the supplied batch has completed processing
    ///   (but prior to the calling of the checkpoint method, which will take place asynchronously)
    member _.Submit(epoch, checkpoint, items, ?markCompleted) = async {
        // If we've read it, feed it into the queue for unpacking
        enqueueIncoming (epoch, checkpoint, items, markCompleted)
        // ... but we might hold off on yielding if we're at capacity
        do! maxRead.Await(cts.Token)
        return maxRead.State }

    /// As range assignments get revoked, a user is expected to `Stop` the active processing thread for the Ingester before releasing references to it
    member _.Stop() = cts.Cancel()
