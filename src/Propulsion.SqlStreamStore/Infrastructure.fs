namespace Propulsion.SqlStreamStore

[<AutoOpen>]
module private AsyncHelpers =

    open System
    open System.Threading.Tasks

    type Async with
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

//#if NET461
module private Option =

    let toNullable option =
        match option with
        | None -> System.Nullable()
        | Some v -> System.Nullable(v)

module private Seq =

    let tryHead (source : seq<_>) =
        if source = null then
            None
        else
            use e = source.GetEnumerator()
            if (e.MoveNext()) then Some e.Current
            else None
//#endif
