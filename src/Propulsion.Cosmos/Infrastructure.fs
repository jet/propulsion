namespace Propulsion.Cosmos

open System
open System.Threading
open System.Threading.Tasks

#nowarn "21" // re AwaitKeyboardInterrupt
#nowarn "40" // re AwaitKeyboardInterrupt

[<AutoOpen>]
module private AsyncHelpers =
    type Async with
        /// Asynchronously awaits the next keyboard interrupt event
        static member AwaitKeyboardInterrupt () : Async<unit> = 
            Async.FromContinuations(fun (sc,_,_) ->
                let isDisposed = ref 0
                let rec callback _ = Task.Run(fun () -> if Interlocked.Increment isDisposed = 1 then d.Dispose() ; sc ()) |> ignore
                and d : IDisposable = Console.CancelKeyPress.Subscribe callback
                in ())
        static member Sleep(t : TimeSpan) : Async<unit> = Async.Sleep(int t.TotalMilliseconds)
        /// Re-raise an exception so that the current stacktrace is preserved
        static member Raise(e : #exn) : Async<'T> = Async.FromContinuations (fun (_,ec,_) -> ec e)
        static member AwaitTaskCorrect (task : Task<'T>) : Async<'T> =
            Async.FromContinuations <| fun (k,ek,_) ->
                task.ContinueWith (fun (t:Task<'T>) ->
                    if t.IsFaulted then
                        let e = t.Exception
                        if e.InnerExceptions.Count = 1 then ek e.InnerExceptions.[0]
                        else ek e
                    elif t.IsCanceled then ek (TaskCanceledException("Task wrapped with Async has been cancelled."))
                    elif t.IsCompleted then k t.Result
                    else ek(Exception "invalid Task state!"))
                |> ignore
        static member AwaitTaskCorrect (task : Task) : Async<unit> =
            Async.FromContinuations <| fun (k,ek,_) ->
                task.ContinueWith (fun (t:Task) ->
                    if t.IsFaulted then
                        let e = t.Exception
                        if e.InnerExceptions.Count = 1 then ek e.InnerExceptions.[0]
                        else ek e
                    elif t.IsCanceled then ek (TaskCanceledException("Task wrapped with Async has been cancelled."))
                    elif t.IsCompleted then k ()
                    else ek(Exception "invalid Task state!"))
                |> ignore

[<AutoOpen>] // TODO remove shim after Equinox 2.0.0-rc7
module EquinoxCosmosHelpers =
    type Equinox.Cosmos.Connector with
         /// Yields a DocumentClient configured per the specified strategy
        member __.CreateClient
            (   /// Name should be sufficient to uniquely identify this connection within a single app instance's logs
                name, discovery : Equinox.Cosmos.Discovery,
                /// <c>true</c> to inhibit logging of client name
                ?skipLog) : Microsoft.Azure.Documents.Client.DocumentClient =
            let (Equinox.Cosmos.Discovery.UriAndKey (u,k)) = discovery
            new Microsoft.Azure.Documents.Client.DocumentClient(serviceEndpoint=u, authKeyOrResourceToken=k, connectionPolicy=__.ClientOptions)