[<AutoOpen>]
module Propulsion.Tool.Infrastructure.Prelude

open System
open System.Threading

#nowarn "21" // re AwaitKeyboardInterrupt
#nowarn "40" // re AwaitKeyboardInterrupt
type Async with
    /// <summary>
    ///     Raises an exception using Async's continuation mechanism directly.
    /// </summary>
    /// <param name="exn">Exception to be raised.</param>
    static member Raise (exn : #exn) = Async.FromContinuations(fun (_,ec,_) -> ec exn)

    /// Asynchronously awaits the next keyboard interrupt event
    static member AwaitKeyboardInterrupt () : Async<unit> = 
        Async.FromContinuations(fun (sc,_,_) ->
            let isDisposed = ref 0
            let rec callback _ = Tasks.Task.Run(fun () -> if Interlocked.Increment isDisposed = 1 then d.Dispose() ; sc ()) |> ignore
            and d : IDisposable = System.Console.CancelKeyPress.Subscribe callback
            in ())