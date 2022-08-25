namespace Propulsion.MemoryStore

open Propulsion
open Propulsion.Internal
open System
open System.Threading
open System.Threading.Tasks

/// Coordinates forwarding of a VolatileStore's Committed events to a supplied Sink
/// Supports awaiting the (asynchronous) handling by the Sink of all Committed events from a given point in time
type MemoryStoreSource<'F>(log, store : Equinox.MemoryStore.VolatileStore<'F>, streamFilter,
                           mapTimelineEvent : FsCodec.ITimelineEvent<'F> -> FsCodec.ITimelineEvent<Streams.Default.EventBody>,
                           sink : Propulsion.Streams.Default.Sink) =
    let ingester : Ingestion.Ingester<_> = sink.StartIngester(log, 0)
    let positions = TranchePositions()
    let monitor = lazy MemoryStoreMonitor(log, positions, sink)
    // epoch index of most recently prepared submission - conceptually events arrive concurrently though V4 impl makes them serial
    let mutable prepared = -1L

    let enqueueSubmission, awaitSubmissions, tryDequeueSubmission =
        let c = Channel.unboundedSr<Ingestion.Batch<Propulsion.Streams.StreamEvent<_> seq>> in let r, w = c.Reader, c.Writer
        Channel.write w, Channel.awaitRead r, r.TryRead

    let handleStoreCommitted (stream, events : FsCodec.ITimelineEvent<_> []) =
        let epoch = Interlocked.Increment &prepared
        positions.Prepared <- epoch
        MemoryStoreLogger.renderSubmit log (epoch, stream, events)
        // Completion notifications are guaranteed to be delivered deterministically, in order of submission
        let markCompleted () =
            MemoryStoreLogger.renderCompleted log (epoch, stream)
            positions.Completed <- epoch
        // We don't have anything Async to do, so we pass a null checkpointing function
        enqueueSubmission { epoch = epoch; checkpoint = async.Zero (); items = events |> Array.map (fun e -> stream, e); onCompletion = markCompleted }

    let storeCommitsSubscription =
        let mapBody (s, e) = s, e |> Array.map mapTimelineEvent
        store.Committed
        |> Observable.filter (fst >> streamFilter)
        |> Observable.subscribe (mapBody >> handleStoreCommitted)

    member private _.Pump(ct : CancellationToken) = task {
        while not ct.IsCancellationRequested do
            let mutable more = true
            while more do
                match tryDequeueSubmission () with
                | false, _ -> more <- false
                | true, batch -> do! ingester.Ingest(batch) |> Async.Ignore
            do! awaitSubmissions ct :> Task }

    member x.Start() =
        let ct, stop =
            let cts = new CancellationTokenSource()
            cts.Token, fun () -> log.Information "Source stopping..."; cts.Cancel()

        let setSuccess, awaitCompletion =
            let tcs = System.Threading.Tasks.TaskCompletionSource<unit>()
            (fun () -> tcs.TrySetResult () |> ignore),
            fun () -> task {
                try return! tcs.Task // aka base.AwaitShutdown()
                finally log.Information "... source stopped" }

        let supervise () = task {
            // external cancellation (via Stop()) should yield a success result
            use _ = ct.Register(setSuccess)
            Task.start(fun () -> x.Pump ct)
            do! awaitCompletion ()
            storeCommitsSubscription.Dispose() }
        new Pipeline(Task.run supervise, stop)

    member _.Monitor = monitor.Value

/// Intercepts receipt and completion of batches, recording the read and completion positions
and internal TranchePositions() =

    member val Prepared : int64 = -1L with get, set
    member val Completed : int64 = -1L with get, set

and MemoryStoreMonitor internal (log : Serilog.ILogger, positions : TranchePositions, sink : Propulsion.Streams.Default.Sink) =

    /// Deterministically waits until all <c>Submit</c>ed batches have been successfully processed via the Sink
    /// NOTE this relies on specific guarantees the MemoryStore's Committed event affords us
    /// 1. a Decider's Transact will not return until such time as the Committed events have been handled
    ///      (i.e., we have prepared the batch for submission)
    /// 2. At the point where the caller triggers AwaitCompletion, we can infer that all reactions have been processed
    ///      when checkpointing/completion has passed beyond our starting point
    member _.AwaitCompletion
        (   // sleep interval while awaiting completion. Default 1ms.
            ?delay,
            // interval at which to log status of the Await (to assist in analyzing stuck Sinks). Default 10s.
            ?logInterval,
            // Also wait for processing of batches that arrived subsequent to the start of the AwaitCompletion call
            ?ignoreSubsequent) = async {
        match positions.Prepared with
        | -1L -> log.Information "No events submitted; completing immediately"
        | epoch when epoch = positions.Completed -> log.Information("No processing pending. Completed Epoch {epoch}", positions.Completed)
        | startingEpoch ->
            let includeSubsequent = ignoreSubsequent <> Some true
            let delayMs =
                let delay = defaultArg delay (TimeSpan.FromMilliseconds 1.)
                int delay.TotalMilliseconds
            let logInterval = IntervalTimer(defaultArg logInterval (TimeSpan.FromSeconds 10.))
            let logStatus () =
                let completed = match positions.Completed with -1L -> Nullable() | x -> Nullable x
                if includeSubsequent then
                    log.Information("Awaiting Completion of all Batches. Starting Epoch {epoch} Current Epoch {current} Completed Epoch {completed}",
                                    startingEpoch, positions.Prepared, completed)
                else log.Information("Awaiting Completion of Starting Epoch {startingEpoch} Completed Epoch {completed}", startingEpoch, completed)
            let isComplete () =
                let currentCompleted = positions.Completed
                positions.Prepared = currentCompleted // All submitted work (including follow-on work), completed
                || (currentCompleted >= startingEpoch && not includeSubsequent) // At or beyond starting point
            while not (isComplete ()) && not sink.IsCompleted do
                if logInterval.IfDueRestart() then logStatus ()
                do! Async.Sleep delayMs // TODO this should really be driven by a condition variable / event flipped when `Volatile.Write completed` happens
            // If the sink Faulted, let the awaiter observe the associated Exception that triggered the shutdown
            if sink.IsCompleted && not sink.RanToCompletion then
                return! sink.AwaitShutdown() }

module TimelineEvent =

    let mapEncoded = FsCodec.Core.TimelineEvent.Map FsCodec.Deflate.EncodedToUtf8

/// Coordinates forwarding of a VolatileStore's Committed events to a supplied Sink
/// Supports awaiting the (asynchronous) handling by the Sink of all Committed events from a given point in time
type MemoryStoreSource(log, store : Equinox.MemoryStore.VolatileStore<struct (int * ReadOnlyMemory<byte>)>, filter, sink) =
    inherit MemoryStoreSource<struct (int * ReadOnlyMemory<byte>)>(log, store, filter, TimelineEvent.mapEncoded, sink)
