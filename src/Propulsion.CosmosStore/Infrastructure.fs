module Propulsion.CosmosStore.Infrastructure

open Propulsion.Infrastructure // AwaitTaskCorrect
open System
open System.Threading.Tasks

type Async with

    /// Asynchronously awaits the next keyboard interrupt event, throwing a TaskCancelledException
    /// Honors cancellation so it can be used with Async.Parallel to have multiple pump loops couple their fates
    static member AwaitKeyboardInterruptAsTaskCancelledException() = async {
        let! ct = Async.CancellationToken
        let tcs = TaskCompletionSource()
        use _ = ct.Register(fun () ->
            tcs.TrySetCanceled() |> ignore)
        use _ = Console.CancelKeyPress.Subscribe(fun (a : ConsoleCancelEventArgs) ->
            a.Cancel <- true // We're using this exception to drive a controlled shutdown so inhibit the standard behavior
            tcs.TrySetException(TaskCanceledException "Execution cancelled via Ctrl-C/Break; exiting...") |> ignore)
        return! Async.AwaitTaskCorrect tcs.Task }
