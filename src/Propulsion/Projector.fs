namespace Propulsion

open Serilog
open System
open System.Threading
open System.Threading.Tasks

/// Runs triggered by a `Start` method , until `Stop()` is requested or `handle` yields a fault.
/// Conclusion of processing can be awaited by via `AwaitShutdown` or `AwaitWithStopOnCancellation`.
type Pipeline (task : Task<unit>, triggerStop) =

    interface IDisposable with member __.Dispose() = __.Stop()

    /// Inspects current status of processing task
    member _.Status = task.Status

    /// After AwaitShutdown, can be used to infer whether exit was clean
    member _.RanToCompletion = task.Status = TaskStatus.RanToCompletion

    /// Request cancellation of processing
    member _.Stop() = triggerStop ()

    /// Asynchronously awaits until consumer stops or a `handle` invocation yields a fault
    member _.AwaitShutdown() = Async.AwaitTaskCorrect task

    /// Asynchronously awaits until this pipeline stops or is faulted.<br/>
    /// Reacts to cancellation by Stopping the Consume loop via <c>Stop()</c>; see <c>AwaitShutdown</c> if such semantics are not desired.
    member x.AwaitWithStopOnCancellation() = async {
        let! ct = Async.CancellationToken
        use _ = ct.Register(fun () -> x.Stop())
        return! x.AwaitShutdown() }

type ProjectorPipeline<'Ingester> private (task : Task<unit>, triggerStop, startIngester) =
    inherit Pipeline(task, triggerStop)

    member _.StartIngester(rangeLog : ILogger, partitionId : int) : 'Ingester = startIngester (rangeLog, partitionId)

    static member Start(log : ILogger, pumpDispatcher, pumpScheduler, pumpSubmitter, startIngester) =
        let cts = new CancellationTokenSource()
        let ct = cts.Token
        let tcs = TaskCompletionSource<unit>()

        let start (name : string) (f : CancellationToken -> Task<unit>) =
            let wrap () = task {
                try do! f ct
                    log.Information("Exiting {name}", name)
                with e -> log.Fatal(e, "Abend from {name}", name) }
            Internal.Task.start wrap

        // if scheduler encounters a faulted handler, we propagate that as the consumer's Result
        let abend (exns : AggregateException) =
            if tcs.TrySetException(exns) then log.Warning(exns, "Cancelling processing due to {count} faulted handlers", exns.InnerExceptions.Count)
            else log.Information("Failed setting {count} exceptions", exns.InnerExceptions.Count)
            // NB cancel needs to be after TSE or the Register(TSE) will win
            cts.Cancel()

        let supervisor () = task {
            // external cancellation should yield a success result
            use _ = ct.Register(fun _ -> tcs.TrySetResult () |> ignore)
            start "dispatcher" pumpDispatcher
            // ... fault results from dispatched tasks result in the `machine` concluding with an exception
            start "scheduler" (pumpScheduler abend)
            start "submitter" pumpSubmitter

            // await for either handler-driven abend or external cancellation via Stop()
            return! tcs.Task }

        let task = Task.Run<unit>(supervisor)
        let triggerStop () =
            let level = if cts.IsCancellationRequested then Events.LogEventLevel.Debug else Events.LogEventLevel.Information
            log.Write(level, "Projector stopping...")
            cts.Cancel()

        new ProjectorPipeline<_>(task, triggerStop, startIngester)
