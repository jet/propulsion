﻿namespace Propulsion.EventStore

open System
open System.Threading.Tasks

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
