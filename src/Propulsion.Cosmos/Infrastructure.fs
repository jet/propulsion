module Propulsion.Cosmos.Infrastructure

open System
open System.Threading
open System.Threading.Tasks

#nowarn "21" // re AwaitKeyboardInterrupt
#nowarn "40" // re AwaitKeyboardInterrupt

type Async with
    /// Asynchronously awaits the next keyboard interrupt event
    static member AwaitKeyboardInterrupt () : Async<unit> =
        Async.FromContinuations(fun (sc,_,_) ->
            let isDisposed = ref 0
            let rec callback _ = Task.Run(fun () -> if Interlocked.Increment isDisposed = 1 then d.Dispose() ; sc ()) |> ignore
            and d : IDisposable = Console.CancelKeyPress.Subscribe callback
            in ())
