module Propulsion.Internal

open System

/// Manages a time cycle defined by `period`. Can be explicitly Trigger()ed prematurely
type IntervalTimer(period : TimeSpan) =

    let timer, periodMs = System.Diagnostics.Stopwatch.StartNew(), int64 period.TotalMilliseconds
    let mutable force = false

    member val Period = period
    member _.RemainingMs = periodMs - timer.ElapsedMilliseconds |> int |> max 0

    member _.Trigger() = force <- true

    // NOTE asking the question is destructive - the timer is reset as a side effect
    member _.IfDueRestart() =
        let due = force || timer.ElapsedMilliseconds > periodMs
        if due then timer.Restart(); force <- false
        due

let inline mb x = float x / 1024. / 1024.

type System.Diagnostics.Stopwatch with member x.ElapsedSeconds = float x.ElapsedMilliseconds / 1000.

module Channel =

    open System.Threading.Channels

    let unboundedSr<'t> = Channel.CreateUnbounded<'t>(UnboundedChannelOptions(SingleReader = true))
    let unboundedSw<'t> = Channel.CreateUnbounded<'t>(UnboundedChannelOptions(SingleWriter = true))
    let unboundedSwSr<'t> = Channel.CreateUnbounded<'t>(UnboundedChannelOptions(SingleWriter = true, SingleReader = true))
    let write (w : ChannelWriter<_>) = w.TryWrite >> ignore
    let inline awaitRead (r : ChannelReader<_>) ct = let vt = r.WaitToReadAsync(ct) in vt.AsTask()
    let inline tryRead (r : ChannelReader<_>) () =
        let mutable msg = Unchecked.defaultof<_>
        if r.TryRead(&msg) then ValueSome msg else ValueNone
    let inline apply (r : ChannelReader<_>) f =
        let mutable worked = false
        let mutable msg = Unchecked.defaultof<_>
        while r.TryRead(&msg) do
            worked <- true
            f msg
        worked

open System.Threading
open System.Threading.Tasks

module Task =

    let inline start create = Task.Run<unit>(Func<Task<unit>> create) |> ignore<Task>

type Sem(max) =
    let inner = new SemaphoreSlim(max)
    member _.HasCapacity = inner.CurrentCount <> 0
    member _.State = struct(max - inner.CurrentCount, max)
    member _.Await(ct : CancellationToken) = inner.WaitAsync(ct) |> Async.AwaitTaskCorrect
    member x.AwaitButRelease() = // see https://stackoverflow.com/questions/31621644/task-whenany-and-semaphoreslim-class/73197290?noredirect=1#comment129334330_73197290
        inner.WaitAsync().ContinueWith((fun _ -> x.Release()), CancellationToken.None, TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default)
    member _.Release() = inner.Release() |> ignore
    member _.TryTake() = inner.Wait 0

/// Helpers for use in Propulsion.Tool
type Async with

    /// Asynchronously awaits the next keyboard interrupt event, throwing a TaskCanceledException
    /// Honors cancellation so it can be used with Async.Parallel to have multiple pump loops couple their fates
    static member AwaitKeyboardInterruptAsTaskCanceledException() = async {
        let! ct = Async.CancellationToken
        let tcs = TaskCompletionSource()
        use _ = ct.Register(fun () ->
            tcs.TrySetCanceled() |> ignore)
        use _ = Console.CancelKeyPress.Subscribe(fun (a : ConsoleCancelEventArgs) ->
            a.Cancel <- true // We're using this exception to drive a controlled shutdown so inhibit the standard behavior
            tcs.TrySetException(TaskCanceledException "Execution cancelled via Ctrl-C/Break; exiting...") |> ignore)
        return! Async.AwaitTaskCorrect tcs.Task }

module Stats =

    open System.Collections.Generic

    let toValueTuple (x : KeyValuePair<_, _>) = struct (x.Key, x.Value)
    let statsDescending (xs : Dictionary<_, _>) = xs |> Seq.map toValueTuple |> Seq.sortByDescending ValueTuple.snd

    /// Gathers stats relating to how many items of a given category have been observed
    type CatStats() =
        let cats = Dictionary<string, int64>()

        member _.Ingest(cat, ?weight) =
            let weight = defaultArg weight 1L
            match cats.TryGetValue cat with
            | true, catCount -> cats[cat] <- catCount + weight
            | false, _ -> cats[cat] <- weight

        member _.Count = cats.Count
        member _.Any = (not << Seq.isEmpty) cats
        member _.Clear() = cats.Clear()
        member _.StatsDescending = statsDescending cats

    type private Data =
        {   min    : TimeSpan
            p50    : TimeSpan
            p95    : TimeSpan
            p99    : TimeSpan
            max    : TimeSpan
            avg    : TimeSpan
            stddev : TimeSpan option }

    open MathNet.Numerics.Statistics
    let private dumpStats (kind : string) (xs : TimeSpan seq) (log : Serilog.ILogger) =
        let sortedLatencies = xs |> Seq.map (fun r -> r.TotalSeconds) |> Seq.sort |> Seq.toArray

        let pc p = SortedArrayStatistics.Percentile(sortedLatencies, p) |> TimeSpan.FromSeconds
        let l = {
            avg = ArrayStatistics.Mean sortedLatencies |> TimeSpan.FromSeconds
            stddev =
                let stdDev = ArrayStatistics.StandardDeviation sortedLatencies
                // stddev of singletons is NaN
                if Double.IsNaN stdDev then None else TimeSpan.FromSeconds stdDev |> Some

            min = SortedArrayStatistics.Minimum sortedLatencies |> TimeSpan.FromSeconds
            max = SortedArrayStatistics.Maximum sortedLatencies |> TimeSpan.FromSeconds
            p50 = pc 50
            p95 = pc 95
            p99 = pc 99 }
        let inline sec (t : TimeSpan) = t.TotalSeconds
        let stdDev = match l.stddev with None -> Double.NaN | Some d -> sec d
        log.Information(" {kind} {count} : max={max:n3}s p99={p99:n3}s p95={p95:n3}s p50={p50:n3}s min={min:n3}s avg={avg:n3}s stddev={stddev:n3}s",
            kind, sortedLatencies.Length, sec l.max, sec l.p99, sec l.p95, sec l.p50, sec l.min, sec l.avg, stdDev)

    /// Operations on an instance are safe cross-thread
    type ConcurrentLatencyStats(kind) =
        let buffer = System.Collections.Concurrent.ConcurrentStack<TimeSpan>()
        member _.Record value = buffer.Push value
        member _.Dump(log : Serilog.ILogger) =
            if not buffer.IsEmpty then
                dumpStats kind buffer log
                buffer.Clear() // yes, there is a race

    /// Should only be used on one thread
    type LatencyStats(kind) =
        let buffer = ResizeArray<TimeSpan>()
        member _.Record value = buffer.Add value
        member _.Dump(log : Serilog.ILogger) =
            if buffer.Count <> 0 then
                dumpStats kind buffer log
                buffer.Clear()

