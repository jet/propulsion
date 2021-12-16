module Propulsion.CosmosStore.Infrastructure

open System
open System.Threading.Tasks

type Async with
    /// Asynchronously awaits the next keyboard interrupt event, throwing a TaskCancelledException
    static member AwaitKeyboardInterruptAsTaskCancelledException() : Async<unit> =
        Async.FromContinuations <| fun (_, ec, _) ->
            let mutable isDisposed = 0
            let rec callback (a : ConsoleCancelEventArgs) = Task.Run(fun () ->
                a.Cancel <- true // We're using this exception to drive a controlled shutdown so inhibit the standard behavior
                if System.Threading.Interlocked.Increment &isDisposed = 1 then d.Dispose()
                ec (TaskCanceledException("Execution cancelled; exiting..."))) |> ignore<Task>
            and d : IDisposable = Console.CancelKeyPress.Subscribe callback
            in ()
