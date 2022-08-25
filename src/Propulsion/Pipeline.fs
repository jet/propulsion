namespace Propulsion

open Serilog
open System
open System.Threading
open System.Threading.Tasks

/// Represents a running Pipeline as triggered by a `Start` method , until `Stop()` is requested or the pipeline becomes Faulted for any reason
/// Conclusion of processing can be awaited by via `AwaitShutdown` or `AwaitWithStopOnCancellation` (or synchronously via IsCompleted)
type Pipeline (task : Task<unit>, triggerStop) =

    interface IDisposable with member x.Dispose() = x.Stop()

    /// Inspects current status of task representing the Pipeline's overall state
    member _.Status = task.Status

    /// Determines whether processing has completed, be that due to an intentional Stop(), or due to a Fault (see also RanToCompletion)
    member _.IsCompleted = let s = task.Status in s = TaskStatus.RanToCompletion || s = TaskStatus.Faulted

    /// After AwaitShutdown (or IsCompleted returns true), can be used to infer whether exit was clean (via Stop) or due to a Pipeline Fault (which ca be observed via AwaitShutdown)
    member _.RanToCompletion = task.Status = TaskStatus.RanToCompletion

    /// Request completion of processing and shutdown of the Pipeline
    member _.Stop() = triggerStop ()

    /// Asynchronously waits until Stop()ped or the Pipeline Faults (in which case the underlying Exception is observed)
    member _.AwaitShutdown() = Async.AwaitTaskCorrect task

    /// Asynchronously awaits until this pipeline stops or is faulted.<br/>
    /// Reacts to cancellation by aborting the processing via <c>Stop()</c>; see <c>AwaitShutdown</c> if such semantics are not desired.
    member x.AwaitWithStopOnCancellation() = async {
        let! ct = Async.CancellationToken
        use _ = ct.Register(fun () -> x.Stop())
        return! x.AwaitShutdown() }

type Sink<'Ingester> private (task : Task<unit>, triggerStop, startIngester) =
    inherit Pipeline(task, triggerStop)

    member _.StartIngester(rangeLog : ILogger, partitionId : int) : 'Ingester = startIngester (rangeLog, partitionId)

    static member Start(log : ILogger, pumpDispatcher, pumpScheduler, pumpSubmitter, startIngester) =
        let cts = new CancellationTokenSource()
        let triggerStop () =
            let level = if cts.IsCancellationRequested then Events.LogEventLevel.Debug else Events.LogEventLevel.Information
            log.Write(level, "Projector stopping...")
            cts.Cancel()
        let ct = cts.Token

        let tcs = TaskCompletionSource<unit>()
        // if scheduler encounters a faulted handler, we propagate that as the consumer's Result
        let abend (exns : AggregateException) =
            if tcs.TrySetException(exns) then log.Warning(exns, "Cancelling processing due to {count} faulted handlers", exns.InnerExceptions.Count)
            else log.Information("Failed setting {count} exceptions", exns.InnerExceptions.Count)
            // NB cancel needs to be after TSE or the Register(TSE) will win
            cts.Cancel()

        let start (name : string) (f : CancellationToken -> Task<unit>) =
            let wrap () = task {
                try do! f ct
                    log.Information("Exiting {name}", name)
                with e ->
                    log.Fatal(e, "Abend from {name}", name)
                    triggerStop () }
            Internal.Task.start wrap

        let supervise () = task {
            // external cancellation should yield a success result
            use _ = ct.Register(fun _ -> tcs.TrySetResult () |> ignore)

            start "dispatcher" pumpDispatcher
            // ... fault results from dispatched tasks result in the `machine` concluding with an exception
            start "scheduler" (pumpScheduler abend)
            start "submitter" pumpSubmitter

            // await for either handler-driven abend or external cancellation via Stop()
            try return! tcs.Task
            finally log.Information("... projector stopped") }

        let task = Task.Run<unit>(supervise)

        new Sink<'Ingester>(task, triggerStop, startIngester)
