[<AutoOpen>]
module internal Propulsion.Infrastructure

open System.Threading.Tasks

// http://www.fssnip.net/7Rc/title/AsyncAwaitTaskCorrect
// pending that being officially packaged somewhere or integrated into FSharp.Core https://github.com/fsharp/fslang-suggestions/issues/840
type Async with

    /// <summary>
    ///     Gets the result of given task so that in the event of exception
    ///     the actual user exception is raised as opposed to being wrapped
    ///     in a System.AggregateException.
    /// </summary>
    /// <param name="task">Task to be awaited.</param>
    [<System.Diagnostics.DebuggerStepThrough>]
    static member AwaitTaskCorrect(task : Task<'T>) : Async<'T> =
        Async.FromContinuations(fun (sc, ec, _cc) ->
            task.ContinueWith(fun (t : Task<'T>) ->
                if t.IsFaulted then
                    let e = t.Exception
                    if e.InnerExceptions.Count = 1 then ec e.InnerExceptions[0]
                    else ec e
                elif t.IsCanceled then ec (TaskCanceledException())
                else sc t.Result)
            |> ignore)

    /// <summary>
    ///     Gets the result of given task so that in the event of exception
    ///     the actual user exception is raised as opposed to being wrapped
    ///     in a System.AggregateException.
    /// </summary>
    /// <param name="task">Task to be awaited.</param>
    [<System.Diagnostics.DebuggerStepThrough>]
    static member AwaitTaskCorrect(task : Task) : Async<unit> =
        Async.FromContinuations(fun (sc, ec, _cc) ->
            task.ContinueWith(fun (task : Task) ->
                if task.IsFaulted then
                    let e = task.Exception
                    if e.InnerExceptions.Count = 1 then ec e.InnerExceptions[0]
                    else ec e
                elif task.IsCanceled then
                    ec (TaskCanceledException())
                else
                    sc ())
            |> ignore)
