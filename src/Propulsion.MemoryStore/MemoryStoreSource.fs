namespace Propulsion.MemoryStore

open Propulsion
open Propulsion.Internal
open System
open System.Threading
open System.Threading.Tasks

module MemoryStoreLogger =

    let private propEvents name (xs : System.Collections.Generic.KeyValuePair<string,string> seq) (log : Serilog.ILogger) =
        let items = seq { for kv in xs do yield sprintf "{\"%s\": %s}" kv.Key kv.Value }
        log.ForContext(name, sprintf "[%s]" (String.concat ",\n\r" items))

    let private propEventJsonUtf8 name (events : Propulsion.Streams.StreamEvent<ReadOnlyMemory<byte>> array) (log : Serilog.ILogger) =
        log |> propEvents name (seq {
            for _s, e in events do
                let d = e.Data
                if not d.IsEmpty then System.Collections.Generic.KeyValuePair<_,_>(e.EventType, System.Text.Encoding.UTF8.GetString d.Span) })

    let renderSubmit (log : Serilog.ILogger) (epoch, stream, events : Propulsion.Streams.StreamEvent<'F> array) =
        if log.IsEnabled Serilog.Events.LogEventLevel.Verbose then
            let log =
                if (not << log.IsEnabled) Serilog.Events.LogEventLevel.Debug then log
                elif typedefof<'F> <> typeof<ReadOnlyMemory<byte>> then log
                else log |> propEventJsonUtf8 "Json" (unbox events)
            let types = seq { for _s, e in events -> e.EventType }
            log.ForContext("types", types).Debug("Submit #{epoch} {stream}x{count}", epoch, stream, events.Length)
        elif log.IsEnabled Serilog.Events.LogEventLevel.Debug then
            let types = seq { for _s, e in events -> e.EventType } |> Seq.truncate 5
            log.Debug("Submit #{epoch} {stream}x{count} {types}", epoch, stream, events.Length, types)
    let renderCompleted (log : Serilog.ILogger) (epoch, stream) =
        log.Verbose("Done!  #{epoch} {stream}", epoch, stream)

    let toStreamEvents stream (events : FsCodec.ITimelineEvent<'F> seq) : Propulsion.Streams.StreamEvent<'F> array =
        [| for x in events -> stream, x |]

    /// Wires specified <c>Observable</c> source (e.g. <c>VolatileStore.Committed</c>) to the Logger
    let subscribe log source =
        let mutable epoch = -1L
        let aux (stream, events) =
            let events = toStreamEvents stream events
            let epoch = Interlocked.Increment &epoch
            renderSubmit log (epoch, stream, events)
        if log.IsEnabled Serilog.Events.LogEventLevel.Debug then Observable.subscribe aux source
        else { new IDisposable with member _.Dispose() = () }

/// Coordinates forwarding of a VolatileStore's Committed events to a supplied Sink
/// Supports awaiting the (asynchronous) handling by the Sink of all Committed events from a given point in time
type MemoryStoreSource<'F>(log, store : Equinox.MemoryStore.VolatileStore<'F>, streamFilter,
                           mapTimelineEvent : FsCodec.ITimelineEvent<'F> -> FsCodec.ITimelineEvent<byte array>,
                           sink : Streams.Sink) =
    let ingester : Ingestion.Ingester<_> = sink.StartIngester(log, 0)

    // epoch index of most recently prepared and completed submissions
    let mutable prepared, completed = -1L, -1L

    let enqueueSubmission, awaitSubmissions, tryDequeueSubmission =
        let c = Channel.unboundedSr<Ingestion.Batch<Propulsion.Streams.StreamEvent<byte array> seq>>
        Channel.write c, Channel.awaitRead c, c.Reader.TryRead

    let handleCommitted (stream, events : FsCodec.ITimelineEvent<_> seq) =
        let epoch = Interlocked.Increment &prepared
        let events = MemoryStoreLogger.toStreamEvents stream events
        MemoryStoreLogger.renderSubmit log (epoch, stream, events)
        let markCompleted () =
            MemoryStoreLogger.renderCompleted log (epoch, stream)
            Volatile.Write(&completed, epoch)
        // We don't have anything Async to do, so we pass a null checkpointing function
        let checkpoint = async { () }
        enqueueSubmission { epoch = epoch; checkpoint = checkpoint; items = events; onCompletion = markCompleted }

    let storeCommitsSubscription =
        let mapBody (s, e) = s, e |> Array.map mapTimelineEvent
        store.Committed
        |> Observable.filter (fst >> streamFilter)
        |> Observable.subscribe (mapBody >> handleCommitted)

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
        new Pipeline(Task.Run<unit>(supervise), stop)

    /// Waits until all <c>Ingest</c>ed batches have been successfully processed via the Sink
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
        match Volatile.Read &prepared with
        | -1L -> log.Information "No events submitted; completing immediately"
        | epoch when epoch = Volatile.Read(&completed) -> log.Verbose("No processing pending. Completed Epoch {epoch}", completed)
        | startingEpoch ->
            let includeSubsequent = ignoreSubsequent <> Some true
            let delayMs =
                let delay = defaultArg delay TimeSpan.FromMilliseconds 1.
                int delay.TotalMilliseconds
            let maybeLog =
                let logInterval = defaultArg logInterval (TimeSpan.FromSeconds 10.)
                let logDue = intervalCheck logInterval
                fun () ->
                    if logDue () then
                        let completed = match Volatile.Read &completed with -1L -> Nullable() | x -> Nullable x
                        if includeSubsequent then
                            log.Information("Awaiting Completion of all Batches. Starting Epoch {epoch} Current Epoch {current} Completed Epoch {completed}",
                                            startingEpoch, Volatile.Read &prepared, completed)
                        else log.Information("Awaiting Completion of Starting Epoch {startingEpoch} Completed Epoch {completed}", startingEpoch, completed)
            let isComplete () =
                let currentCompleted = Volatile.Read &completed
                Volatile.Read &prepared = currentCompleted // All submitted work (including follow-on work), completed
                || (currentCompleted >= startingEpoch && not includeSubsequent) // At or beyond starting point
            while not (isComplete ()) && not sink.IsCompleted do
                maybeLog ()
                do! Async.Sleep delayMs
            // If the sink Faulted, let the awaiter observe the associated Exception that triggered the shutdown
            if sink.IsCompleted && not sink.RanToCompletion then
                return! sink.AwaitShutdown()
    }

module TimelineEvent =

    let mapEncoded =
        let mapBodyToBytes = (fun (x : ReadOnlyMemory<byte>) -> x.ToArray())
        FsCodec.Core.TimelineEvent.Map (FsCodec.Deflate.EncodedToUtf8 >> mapBodyToBytes) // TODO replace with FsCodec.Deflate.EncodedToByteArray

/// Coordinates forwarding of a VolatileStore's Committed events to a supplied Sink
/// Supports awaiting the (asynchronous) handling by the Sink of all Committed events from a given point in time
type MemoryStoreSource(log, store : Equinox.MemoryStore.VolatileStore<struct (int * ReadOnlyMemory<byte>)>, filter, sink) =
    inherit MemoryStoreSource<struct (int * ReadOnlyMemory<byte>)>(log, store, filter, TimelineEvent.mapEncoded, sink)
