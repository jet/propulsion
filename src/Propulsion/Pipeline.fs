namespace Propulsion

open Propulsion.Internal
open System

/// Represents a running Pipeline as triggered by a `Start` method , until `Stop()` is requested or the pipeline becomes Faulted for any reason
/// Conclusion of processing can be awaited by via `Await`/`Wait` or `AwaitWithStopOnCancellation` (or synchronously via IsCompleted)
type Pipeline(task: Task<unit>, triggerStop) =

    interface IDisposable with member x.Dispose() = triggerStop true

    /// Inspects current status of task representing the Pipeline's overall state
    member _.Status = task.Status

    /// Determines whether processing has completed, be that due to an intentional Stop(), due to a Fault, or successful completion (see also RanToCompletion)
    member _.IsCompleted = task.IsCompleted

    /// After Await/Wait (or IsCompleted returns true), can be used to infer whether exit was clean (via Stop) or due to a Pipeline Fault (which ca be observed via Await/Wait)
    member _.RanToCompletion = task.Status = System.Threading.Tasks.TaskStatus.RanToCompletion

    /// Request completion of processing and shutdown of the Pipeline
    member _.Stop() = triggerStop false

    /// Asynchronously waits until Stop()ped or the Pipeline Faults (in which case the underlying Exception is observed)
    member _.Wait(): Task<unit> = task

    /// Asynchronously waits until Stop()ped or the Pipeline Faults (in which case the underlying Exception is observed)
    member _.Await(): Async<unit> = task |> Async.ofTask

    /// Asynchronously awaits until this pipeline stops or is faulted.<br/>
    /// Reacts to cancellation by aborting the processing via <c>Stop()</c>; see <c>Await</c> if such semantics are not desired.
    member x.AwaitWithStopOnCancellation() = async {
        let! ct = Async.CancellationToken
        use _ = ct.Register(Action x.Stop)
        return! x.Await() }

 type SourcePipeline<'M, 'P>(task, flush: unit -> Task<'P>, triggerStop, monitor: Lazy<'M>) =
    inherit Pipeline(task, triggerStop)

    member _.Monitor = monitor.Value
    member _.FlushAsync(): Task<'P> = flush ()
    member x.Flush(): Async<'P> = x.FlushAsync() |> Async.ofTask

type SinkPipeline<'Ingester> internal (task: Task<unit>, triggerStop, startIngester) =
    inherit Pipeline(task, triggerStop)

    member _.StartIngester(rangeLog: Serilog.ILogger, partitionId: int): 'Ingester = startIngester (rangeLog, partitionId)

type [<AbstractClass; Sealed>] PipelineFactory private () =

    static member PrepareSource(log: Serilog.ILogger, pump: CancellationToken -> Task<unit>, markCompleted: unit -> unit) =
        let ct, stop =
            let cts = new System.Threading.CancellationTokenSource()
            cts.Token, fun disposing ->
                let level = if cts.IsCancellationRequested || disposing then LogEventLevel.Debug else LogEventLevel.Information
                log.Write(level, "Source stopping...")
                cts.Cancel()

        let inner, outcomeTask, markCompleted =
            let tcs = System.Threading.Tasks.TaskCompletionSource<unit>()
            let markCompleted () =
                markCompleted ()
                tcs.TrySetResult () |> ignore
            let recordExn (e: exn) = tcs.TrySetException e |> ignore
            let inner () = task {
                try do! pump ct
                    // If the source completes all reading cleanly, convey that fact
                    if not ct.IsCancellationRequested then log.Information "Source drained..."
                    markCompleted ()
                with e ->
                    let level = if ct.IsCancellationRequested then LogEventLevel.Debug else LogEventLevel.Warning
                    // first exception from a supervised task becomes the outcome if that happens
                    log.Write(level, e, "Exception encountered while running source, exiting loop")
                    recordExn e
                return! tcs.Task }
            inner, tcs.Task, markCompleted

        let machine () = task {
            // external cancellation should yield a success result (in the absence of failures from the supervised tasks)
            use _ = ct.Register markCompleted

            // Start the work on an independent task; if it fails, it'll flow via the TCS.TrySetException into outcomeTask's Result
            try return! inner ()
            finally log.Information "Source stopped" }
        machine, stop, outcomeTask

    static member PrepareSource2(log: Serilog.ILogger, startup: CancellationToken -> Task<unit>, shutdown: unit -> Task<unit>, markCompleted) =
        let ct, stop =
            let cts = new System.Threading.CancellationTokenSource()
            cts.Token, fun disposing ->
                let level = if cts.IsCancellationRequested || disposing then LogEventLevel.Debug else LogEventLevel.Information
                log.Write(level, "Source stopping...")
                cts.Cancel()

        let outcomeTask, markCompleted =
            let tcs = System.Threading.Tasks.TaskCompletionSource<unit>()
            let markCompleted () =
                markCompleted ()
                tcs.TrySetResult () |> ignore
            tcs.Task, markCompleted

        let machine () = task {
            // external cancellation should yield a success result
            use _ = ct.Register markCompleted
            try do! startup ct
                do! outcomeTask // Wait for external stop()
                do! shutdown ()
            finally log.Information "Source stopped" }
        machine, stop, outcomeTask

    static member PrepareSink(log: Serilog.ILogger, pumpScheduler, pumpSubmitter, ?pumpIngester, ?pumpDispatcher) =
        let cts = new System.Threading.CancellationTokenSource()
        let triggerStop disposing =
            let level = if disposing || cts.IsCancellationRequested then LogEventLevel.Debug else LogEventLevel.Information
            log.Write(level, "Sink stopping...")
            cts.Cancel()
        let ct = cts.Token

        let tcs = System.Threading.Tasks.TaskCompletionSource<unit>()
        let mutable abended = false
        // if scheduler encounters a faulted handler, we propagate that as the consumer's Result
        let abend (exn: exn) =
            if tcs.TrySetException(exn) then
                log.Warning(exn, "Cancelling processing due to faulted scheduler or health checks")
                abended <- true
            // Health check can flag need to abend multiple times; first one has to win
            elif not abended then log.Information(exn, "Failed setting abend exn")
            // NB cancel needs to be after TSE or the Register(TSE) will win
            cts.Cancel()

        let run (name: string) (f: CancellationToken -> Task<unit>) =
            let wrap () = task {
                try do! f ct
                    log.Information("... {name} stopped", name)
                with e ->
                    log.Fatal(e, "Abend from {name}", name)
                    triggerStop false }
            Task.run wrap
        let start name = run name >> ignore<Task>

        let supervise () = task {
            // external cancellation should yield a success result
            use _ = ct.Register(fun _ -> tcs.TrySetResult () |> ignore)

            pumpIngester |> Option.iter (start "ingester")
            pumpDispatcher |> Option.iter (start "dispatcher")
            // ... fault results from dispatched tasks result in the `machine` concluding with an exception
            let scheduler = run "scheduler" (fun ct -> pumpScheduler (abend, ct))
            start "submitter" pumpSubmitter

            // await for either handler-driven abend or external cancellation via Stop()
            try return! tcs.Task
            finally // Scheduler needs to print stats, and we don't want to report shutdown until that's complete
                let ts = Stopwatch.timestamp ()
                let finishedAsRequested = scheduler.Wait(TimeSpan.seconds 2)
                let ms = let t = Stopwatch.elapsed ts in int t.TotalMilliseconds
                let level = if finishedAsRequested && ms < 200 then LogEventLevel.Information else LogEventLevel.Warning
                log.Write(level, "Sink stopped {schedulerCleanupMs}ms", ms) }

        let task = Task.Run<unit>(supervise)
        task, triggerStop

    static member StartSink(log: Serilog.ILogger, pumpScheduler, pumpSubmitter, startIngester, ?pumpDispatcher) =
        let task, triggerStop = PipelineFactory.PrepareSink(log, pumpScheduler, pumpSubmitter, ?pumpIngester = None, ?pumpDispatcher = pumpDispatcher)
        new SinkPipeline<'Ingester>(task, triggerStop, startIngester)
