namespace Propulsion

open Serilog
open System
open System.Threading
open System.Threading.Tasks

/// Runs triggered by a `Start` method , until `Stop()` is requested or `handle` yields a fault.
/// Conclusion of processing can be awaited by via `AwaitCompletion()`.
type Pipeline (task : Task<unit>, triggerStop) =

    interface IDisposable with member __.Dispose() = __.Stop()

    /// Inspects current status of processing task
    member __.Status = task.Status
    /// After AwaitCompletion, can be used to infer whether exit was clean
    member __.RanToCompletion = task.Status = TaskStatus.RanToCompletion 

    /// Request cancellation of processing
    member __.Stop() = triggerStop ()

    /// Asynchronously awaits until consumer stops or a `handle` invocation yields a fault
    member __.AwaitCompletion() = Async.AwaitTaskCorrect task

type ProjectorPipeline<'Ingester> private (task : Task<unit>, triggerStop, startIngester) =
    inherit Pipeline(task, triggerStop)

    member __.StartIngester(rangeLog : ILogger, partitionId : int) : 'Ingester = startIngester (rangeLog, partitionId)

    static member Start(log : Serilog.ILogger, pumpDispatcher, pumpScheduler, pumpSubmitter, startIngester) =
        let cts = new CancellationTokenSource()
        let ct = cts.Token
        let tcs = new TaskCompletionSource<unit>()
        let start name f =
            let wrap (name : string) computation = async {
                try do! computation
                    log.Information("Exiting {name}", name)
                with e -> log.Fatal(e, "Abend from {name}", name) }
            Async.Start(wrap name f, ct)
        // if scheduler encounters a faulted handler, we propagate that as the consumer's Result
        let abend (exns : AggregateException) =
            if tcs.TrySetException(exns) then log.Warning(exns, "Cancelling processing due to {count} faulted handlers", exns.InnerExceptions.Count)
            else log.Information("Failed setting {count} exceptions", exns.InnerExceptions.Count)
            // NB cancel needs to be after TSE or the Register(TSE) will win
            cts.Cancel()
        let machine = async {
            // external cancellation should yield a success result
            use _ = ct.Register(fun _ -> tcs.TrySetResult () |> ignore)
            start "dispatcher" <| pumpDispatcher
            // ... fault results from dispatched tasks result in the `machine` concluding with an exception
            start "scheduler" <| pumpScheduler abend
            start "submitter" <| pumpSubmitter

            // await for either handler-driven abend or external cancellation via Stop()
            do! Async.AwaitTaskCorrect tcs.Task }
        let task = Async.StartAsTask machine
        let triggerStop () =
            log.Information("Stopping")
            cts.Cancel();  
        new ProjectorPipeline<_>(task, triggerStop, startIngester)