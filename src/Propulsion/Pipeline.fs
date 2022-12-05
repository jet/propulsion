namespace Propulsion

open Propulsion.Internal
open Serilog
open System
open System.Threading
open System.Threading.Tasks

/// Represents a running Pipeline as triggered by a `Start` method , until `Stop()` is requested or the pipeline becomes Faulted for any reason
/// Conclusion of processing can be awaited by via `AwaitShutdown` or `AwaitWithStopOnCancellation` (or synchronously via IsCompleted)
type Pipeline(task : Task<unit>, triggerStop) =

    interface IDisposable with member x.Dispose() = triggerStop true

    /// Inspects current status of task representing the Pipeline's overall state
    member _.Status = task.Status

    /// Determines whether processing has completed, be that due to an intentional Stop(), or due to a Fault (see also RanToCompletion)
    member _.IsCompleted = Task.isCompleted task

    /// After AwaitShutdown (or IsCompleted returns true), can be used to infer whether exit was clean (via Stop) or due to a Pipeline Fault (which ca be observed via AwaitShutdown)
    member _.RanToCompletion = task.Status = TaskStatus.RanToCompletion

    /// Request completion of processing and shutdown of the Pipeline
    member _.Stop() = triggerStop false

    /// Asynchronously waits until Stop()ped or the Pipeline Faults (in which case the underlying Exception is observed)
    member _.AwaitShutdown() = Async.AwaitTaskCorrect task

    /// Asynchronously awaits until this pipeline stops or is faulted.<br/>
    /// Reacts to cancellation by aborting the processing via <c>Stop()</c>; see <c>AwaitShutdown</c> if such semantics are not desired.
    member x.AwaitWithStopOnCancellation() = async {
        let! ct = Async.CancellationToken
        use _ = ct.Register(fun () -> x.Stop())
        return! x.AwaitShutdown() }

    static member Prepare(log : ILogger, pumpScheduler, pumpSubmitter, ?pumpIngester, ?pumpDispatcher) =
        let cts = new CancellationTokenSource()
        let triggerStop disposing =
            let level = if disposing || cts.IsCancellationRequested then Events.LogEventLevel.Debug else Events.LogEventLevel.Information
            log.Write(level, "Sink stopping...")
            cts.Cancel()
        let ct = cts.Token

        let tcs = TaskCompletionSource<unit>()
        // if scheduler encounters a faulted handler, we propagate that as the consumer's Result
        let abend (exns : AggregateException) =
            if tcs.TrySetException(exns) then log.Warning(exns, "Cancelling processing due to {count} faulted handlers", exns.InnerExceptions.Count)
            else log.Information("Failed setting {count} exceptions", exns.InnerExceptions.Count)
            // NB cancel needs to be after TSE or the Register(TSE) will win
            cts.Cancel()

        let run (name : string) (f : CancellationToken -> Task<unit>) =
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
                let level = if finishedAsRequested && ms < 200 then Events.LogEventLevel.Information else Events.LogEventLevel.Warning
                log.Write(level, "... sink completed {schedulerCleanupMs}ms", ms) }

        let task = Task.Run<unit>(supervise)
        task, triggerStop

type SourcePipeline<'M>(task, triggerStop, monitor : Lazy<'M>) =
    inherit Pipeline(task, triggerStop)

    member _.Monitor = monitor.Value

type Sink<'Ingester> private (task : Task<unit>, triggerStop, startIngester) =
    inherit Pipeline(task, triggerStop)

    member _.StartIngester(rangeLog : ILogger, partitionId : int) : 'Ingester = startIngester (rangeLog, partitionId)

    static member Start(log : ILogger, pumpScheduler, pumpSubmitter, startIngester, ?pumpDispatcher) =
        let task, triggerStop = Pipeline.Prepare(log, pumpScheduler, pumpSubmitter, ?pumpIngester = None, ?pumpDispatcher = pumpDispatcher)
        new Sink<'Ingester>(task, triggerStop, startIngester)
