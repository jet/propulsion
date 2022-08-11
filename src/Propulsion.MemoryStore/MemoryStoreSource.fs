namespace Propulsion.MemoryStore

open Equinox.MemoryStore
open Propulsion
open Propulsion.Internal
open System
open System.Threading
open System.Threading.Tasks

module MemoryStoreLogger =

    let private propEvents name (kvps : System.Collections.Generic.KeyValuePair<string,string> seq) (log : Serilog.ILogger) =
        let items = seq { for kv in kvps do yield sprintf "{\"%s\": %s}" kv.Key kv.Value }
        log.ForContext(name, sprintf "[%s]" (String.concat ",\n\r" items))

    let private propEventJsonUtf8 name (events : Propulsion.Streams.StreamEvent<ReadOnlyMemory<byte>> array) (log : Serilog.ILogger) =
        log |> propEvents name (seq {
            for { event = e } in events do
                let d = e.Data
                if not d.IsEmpty then System.Collections.Generic.KeyValuePair<_,_>(e.EventType, System.Text.Encoding.UTF8.GetString d.Span) })

    let renderSubmit (log : Serilog.ILogger) (epoch, stream, events : Propulsion.Streams.StreamEvent<'F> array) =
        if log.IsEnabled Serilog.Events.LogEventLevel.Verbose then
            let log =
                if (not << log.IsEnabled) Serilog.Events.LogEventLevel.Debug then log
                elif typedefof<'F> <> typeof<ReadOnlyMemory<byte>> then log
                else log |> propEventJsonUtf8 "Json" (unbox events)
            let types = seq { for x in events -> x.event.EventType }
            log.ForContext("types", types).Debug("Submit #{epoch} {stream}x{count}", epoch, stream, events.Length)
        elif log.IsEnabled Serilog.Events.LogEventLevel.Debug then
            let types = seq { for x in events -> x.event.EventType } |> Seq.truncate 5
            log.Debug("Submit #{epoch} {stream}x{count} {types}", epoch, stream, events.Length, types)
    let renderCompleted (log : Serilog.ILogger) (epoch, stream) =
        log.Verbose("Done!  #{epoch} {stream}", epoch, stream)

    let toStreamEvents stream (events : FsCodec.ITimelineEvent<'F> seq) =
        [| for x in events -> { stream = stream; event = x } : Propulsion.Streams.StreamEvent<'F> |]

    /// Wires specified <c>Observable</c> source (e.g. <c>VolatileStore.Committed</c>) to the Logger
    let subscribe log source =
        let mutable epoch = -1L
        let aux (stream, events) =
            let events = toStreamEvents stream events
            let epoch = Interlocked.Increment &epoch
            renderSubmit log (epoch, stream, events)
        if log.IsEnabled Serilog.Events.LogEventLevel.Debug then Observable.subscribe aux source
        else { new IDisposable with member _.Dispose() = () }

module TimelineEvent =

    let mapEncoded =
        let mapBodyToBytes = (fun (x : ReadOnlyMemory<byte>) -> x.ToArray())
        FsCodec.Core.TimelineEvent.Map (FsCodec.Deflate.EncodedToUtf8 >> mapBodyToBytes) // TODO replace with FsCodec.Deflate.EncodedToByteArray

type MemoryStoreSource<'F, 'B>(log, store : VolatileStore<'F>, filter,
                               mapTimelineEvent,
                               sink : ProjectorPipeline<Ingestion.Ingester<Propulsion.Streams.StreamEvent<byte[]> seq, 'B>>) =
    let ingester = sink.StartIngester(log, 0)

    let mutable epoch = -1L
    let mutable completed = None
    let mutable checkpointed = None

    let enqueueSubmission, awaitSubmissions, tryDequeueSubmission =
        let c = Channel.unboundedSr
        Channel.write c, Channel.awaitRead c, c.Reader.TryRead

    let handleCommitted (stream, events : FsCodec.ITimelineEvent<_> seq) =
        let epoch = Interlocked.Increment &epoch
        let events = MemoryStoreLogger.toStreamEvents stream events
        MemoryStoreLogger.renderSubmit log (epoch, stream, events)
        let markCompleted () =
            MemoryStoreLogger.renderCompleted log (epoch, stream)
            Volatile.Write(&completed, Some epoch)
        let checkpoint = async { checkpointed <- Some epoch }
        enqueueSubmission (epoch, checkpoint, events, markCompleted)

    let storeCommitsSubscription =
        let mapBody (s, e) = s, e |> Array.map mapTimelineEvent
        store.Committed
        |> Observable.filter (fst >> filter)
        |> Observable.subscribe (mapBody >> handleCommitted)

    member private _.Pump(ct : CancellationToken) = task {
        while not ct.IsCancellationRequested do
            let mutable more = true
            while more do
                match tryDequeueSubmission () with
                | false, _ -> more <- false
                | true, (epoch, checkpoint, events, markCompleted) -> do! ingester.Submit(epoch, checkpoint, events, markCompleted) |> Async.Ignore
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
(*
    /// Waits until all <c>Submit</c>ted batches have been fed into the <c>inner</c> Projector
    member _.AwaitWithStopOnCancellation
        (   // sleep time while awaiting completion
            ?delay,
            // interval at which to log progress of Projector loop
            ?logInterval) = async {
        if -1L = Volatile.Read(&epoch) then
            log.Warning("No events submitted; completing immediately")
        else
            let delay = defaultArg delay TimeSpan.FromMilliseconds 5.
            let maybeLog =
                let logInterval = defaultArg logInterval (TimeSpan.FromSeconds 10.)
                let logDue = Propulsion.Internal.intervalCheck logInterval
                fun () ->
                    if logDue () then
                        log.ForContext("checkpoint", checkpointed)
                            .Information("Waiting for epoch {epoch}. Current completed epoch {completed}", epoch, Option.toNullable completed)
            let delayMs = int delay.TotalMilliseconds
            while Some (Volatile.Read &epoch) <> Volatile.Read &completed do
                maybeLog()
                do! Async.Sleep delayMs
        // the ingestion pump can be stopped now...
        ingester.Stop()
        // as we've validated all submissions have had their processing completed, we can stop the inner projector too
        inner.Stop()
        // trigger termination of GetConsumingEnumerable()-driven pumping loop
        queue.CompleteAdding()
        return! inner.AwaitWithStopOnCancellation()
    }
*)

type MemoryStoreSource<'B>(log, store : VolatileStore<struct (int * ReadOnlyMemory<byte>)>, filter,
                           sink : ProjectorPipeline<Ingestion.Ingester<Propulsion.Streams.StreamEvent<byte[]> seq, 'B>>) =
    inherit MemoryStoreSource<struct (int * ReadOnlyMemory<byte>), 'B>(log, store, filter, TimelineEvent.mapEncoded, sink)
