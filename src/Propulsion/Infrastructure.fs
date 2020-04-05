#if CORE
namespace Propulsion
#else
#if EVENTSTORE
namespace Propulsion.EventStore
#else
#if SQLSTREAMSTORE
namespace Propulsion.SqlStreamStore
#endif
#endif
#endif

open System
open System.Threading.Tasks

#if NET461
module Array =
    let takeWhile p = Seq.takeWhile p >> Array.ofSeq
    let head = Seq.head

module Option = 
    let toNullable option = match option with None -> System.Nullable() | Some v -> System.Nullable(v)
#endif

[<AutoOpen>]
module private AsyncHelpers =
    type Async with
        static member Sleep(t : TimeSpan) : Async<unit> = Async.Sleep(int t.TotalMilliseconds)

        static member AwaitTaskCorrect (task : Task<'T>) : Async<'T> =
            Async.FromContinuations <| fun (k, ek, _) ->
                task.ContinueWith (fun (t : Task<'T>) ->
                    if t.IsFaulted then
                        let e = t.Exception
                        if e.InnerExceptions.Count = 1 then ek e.InnerExceptions.[0]
                        else ek e
                    elif t.IsCanceled then ek (TaskCanceledException("Task wrapped with Async has been cancelled."))
                    elif t.IsCompleted then k t.Result
                    else ek(Exception "invalid Task state!"))
                |> ignore

        static member AwaitTaskCorrect (task : Task) : Async<unit> =
            Async.FromContinuations <| fun (k, ek, _) ->
                task.ContinueWith (fun (t : Task) ->
                    if t.IsFaulted then
                        let e = t.Exception
                        if e.InnerExceptions.Count = 1 then ek e.InnerExceptions.[0]
                        else ek e
                    elif t.IsCanceled then ek (TaskCanceledException("Task wrapped with Async has been cancelled."))
                    elif t.IsCompleted then k ()
                    else ek(Exception "invalid Task state!"))
                |> ignore
