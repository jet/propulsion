module Propulsion.Internal

open System
open System.Diagnostics
open System.Threading
open System.Threading.Tasks

/// Maintains a Stopwatch such that invoking will yield true at intervals defined by `period`
let intervalCheck (period : TimeSpan) =
    let timer, max = Stopwatch.StartNew(), int64 period.TotalMilliseconds
    fun () ->
        let due = timer.ElapsedMilliseconds > max
        if due then timer.Restart()
        due
let timeRemaining (period : TimeSpan) =
    let timer, max = Stopwatch.StartNew(), int64 period.TotalMilliseconds
    fun () ->
        match max - timer.ElapsedMilliseconds |> int with
        | rem when rem <= 0 -> timer.Restart(); true, max
        | rem -> false, rem

module Channel =

    open System.Threading.Channels

    let unboundedSr<'t> = Channel.CreateUnbounded<'t>(UnboundedChannelOptions(SingleReader = true))
    let unboundedSw<'t> = Channel.CreateUnbounded<'t>(UnboundedChannelOptions(SingleWriter = true))
    let unboundedSwSr<'t> = Channel.CreateUnbounded<'t>(UnboundedChannelOptions(SingleWriter = true, SingleReader = true))
    let write (c : Channel<_>) = c.Writer.TryWrite >> ignore
    let awaitRead (c : Channel<_>) ct = let vt = c.Reader.WaitToReadAsync(ct) in vt.AsTask()
    let apply (c : Channel<_>) f =
        let mutable worked, msg = false, Unchecked.defaultof<_>
        while c.Reader.TryRead(&msg) do
            worked <- true
            f msg
        worked

module Task =

    let start create = Task.Run<unit>(Func<Task<unit>> create) |> ignore<Task>

type Sem(max) =
    let inner = new SemaphoreSlim(max)
    member _.HasCapacity = inner.CurrentCount <> 0
    member _.State = max-inner.CurrentCount,max
    member _.Await(ct : CancellationToken) = inner.WaitAsync(ct) |> Async.AwaitTaskCorrect
    member x.AwaitButRelease() = inner.WaitAsync().ContinueWith(fun _t -> x.Release())
    member _.Release() = inner.Release() |> ignore
    member _.TryTake() = inner.Wait 0

/// Helpers for use in Propulsion.Tool
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

