﻿namespace Propulsion

open Propulsion.Internal
open System
open System.Threading.Tasks

/// Represents a running Pipeline as triggered by a `Start` method , until `Stop()` is requested or the pipeline becomes Faulted for any reason
/// Conclusion of processing can be awaited by via `Await`/`Wait` or `AwaitWithStopOnCancellation` (or synchronously via IsCompleted)
type Pipeline(task: Task<unit>, triggerStop) =

    interface IDisposable with member x.Dispose() = triggerStop true

    /// Inspects current status of task representing the Pipeline's overall state
    member _.Status = task.Status

    /// Determines whether processing has completed, be that due to an intentional Stop(), due to a Fault, or successful completion (see also RanToCompletion)
    member _.IsCompleted = task.IsCompleted

    /// After Await/Wait (or IsCompleted returns true), can be used to infer whether exit was clean (via Stop) or due to a Pipeline Fault (which ca be observed via Await/Wait)
    member _.RanToCompletion = task.Status = TaskStatus.RanToCompletion

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

 type SourcePipeline<'M>(task, triggerStop, monitor: Lazy<'M>) =
    inherit Pipeline(task, triggerStop)

    member _.Monitor = monitor.Value

type Sink<'Ingester> private (task: Task<unit>, triggerStop, startIngester) =
    inherit Pipeline(task, triggerStop)

    member _.StartIngester(rangeLog: Serilog.ILogger, partitionId: int): 'Ingester = startIngester (rangeLog, partitionId)

    static member Start(log: Serilog.ILogger, pumpScheduler, pumpSubmitter, startIngester, ?pumpDispatcher) =
        let task, triggerStop = PipelineFactory.PrepareSink(log, pumpScheduler, pumpSubmitter, ?pumpIngester = None, ?pumpDispatcher = pumpDispatcher)
        new Sink<'Ingester>(task, triggerStop, startIngester)

and [<AbstractClass; Sealed>] PipelineFactory private () =

    static member PrepareSource(log: Serilog.ILogger, pump: CancellationToken -> Task<unit>) =
        let ct, triggerStop =
            let cts = new System.Threading.CancellationTokenSource()
            let triggerStop disposing =
                if not cts.IsCancellationRequested && not disposing then log.Information "Source stopping..."
                cts.Cancel()
            cts.Token, triggerStop

        let inner, markCompleted, outcomeTask =
            let tcs = System.Threading.Tasks.TaskCompletionSource<unit>()
            let markCompleted () = tcs.TrySetResult () |> ignore
            let recordExn (e: exn) = tcs.TrySetException e |> ignore
            // first exception from a supervised task becomes the outcome if that happens
            let inner () = task {
                try do! pump ct
                    // If the source completes all reading cleanly, declare completion
                    log.Information "Source drained..."
                    markCompleted ()
                with e ->
                    log.Warning(e, "Exception encountered while running source, exiting loop")
                    recordExn e }
            inner, markCompleted, tcs.Task

        let machine () = task {
            // external cancellation should yield a success result (in the absence of failures from the supervised tasks)
            use _ = ct.Register markCompleted

            // Start the work on an independent task; if it fails, it'll flow via the TCS.TrySetException into outcomeTask's Result
            Task.start inner

            try return! outcomeTask
            finally log.Information "... source completed" }
        machine, outcomeTask, triggerStop

    static member private PrepareSource2(log: Serilog.ILogger, start: unit -> Task<unit>, maybeStartChild, stop: unit -> Task<unit>) =
        let ct, triggerStop =
            let cts = new System.Threading.CancellationTokenSource()
            let triggerStop _disposing =
                let level = if cts.IsCancellationRequested then LogEventLevel.Debug else LogEventLevel.Information
                log.Write(level, "Source stopping...")
                cts.Cancel()
            cts.Token, triggerStop

        let markCompleted, outcomeTask =
            let tcs = System.Threading.Tasks.TaskCompletionSource<unit>()
            let markCompleted () = tcs.TrySetResult () |> ignore
            markCompleted, tcs.Task

        let inner () = task {
            do! start ()
            try match maybeStartChild with
                | None -> ()
                | Some child -> Task.start (fun () -> child ct)
                do! outcomeTask
            with _ -> // TODO: F# 7 supports do! in a finally, this catch-swallow-then is a very poor persons subtitute
                () // For now just ignore
                // NOTE None of our work should actually trigger exceptions (we don't call tcs.TrySetException)
                //      so this catch is really just for completeness
            do! stop () } // NB: Stops the CFP, hence critical to return leases etc

        let machine () = task {
            // external cancellation should yield a success result
            use _ = ct.Register markCompleted
            Task.start inner
            try return! outcomeTask
            finally log.Information "... source completed" }
        machine, triggerStop

    static member Start(log: Serilog.ILogger, start, maybeStartChild, stop) =
        let machine, triggerStop = PipelineFactory.PrepareSource2(log, start, maybeStartChild, stop)
        new Pipeline(Task.run machine, triggerStop)

    static member PrepareSink(log: Serilog.ILogger, pumpScheduler, pumpSubmitter, ?pumpIngester, ?pumpDispatcher) =
        let cts = new System.Threading.CancellationTokenSource()
        let triggerStop disposing =
            let level = if disposing || cts.IsCancellationRequested then LogEventLevel.Debug else LogEventLevel.Information
            log.Write(level, "Sink stopping...")
            cts.Cancel()
        let ct = cts.Token

        let tcs = TaskCompletionSource<unit>()
        // if scheduler encounters a faulted handler, we propagate that as the consumer's Result
        let abend (exns: AggregateException) =
            if tcs.TrySetException(exns) then log.Warning(exns, "Cancelling processing due to {count} faulted handlers", exns.InnerExceptions.Count)
            else log.Information("Failed setting {count} exceptions", exns.InnerExceptions.Count)
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
                let finishedAsRequested = scheduler.Wait(TimeSpan.FromSeconds 2)
                let ms = let t = Stopwatch.elapsed ts in int t.TotalMilliseconds
                let level = if finishedAsRequested && ms < 200 then LogEventLevel.Information else LogEventLevel.Warning
                log.Write(level, "... sink completed {schedulerCleanupMs}ms", ms) }

        let task = Task.Run<unit>(supervise)
        task, triggerStop
