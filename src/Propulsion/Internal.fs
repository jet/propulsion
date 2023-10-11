module Propulsion.Internal

open System

module TimeSpan =

    let seconds (x: float) = TimeSpan.FromSeconds x
    let minutes (x: float) = TimeSpan.FromMinutes x
    let hours (x: float) = TimeSpan.FromHours x
    let ms (x: float) = TimeSpan.FromMilliseconds x
    let toMs (ts: TimeSpan): int = ts.TotalMilliseconds |> int

module Stopwatch =

    let inline start () = System.Diagnostics.Stopwatch.StartNew()
    let inline timestamp () = System.Diagnostics.Stopwatch.GetTimestamp()
    let inline ticksToSeconds ticks = double ticks / double System.Diagnostics.Stopwatch.Frequency
    let inline ticksToTimeSpan ticks = ticksToSeconds ticks |> TimeSpan.FromSeconds

    let inline elapsedTicks (ts: int64) = timestamp () - ts
    let inline elapsedSeconds (ts: int64) = elapsedTicks ts |> ticksToSeconds
    let inline elapsed (ts: int64) = elapsedTicks ts |> ticksToTimeSpan // equivalent to .NET 7 System.Diagnostics.Stopwatch.GetElapsedTime()

type System.Diagnostics.Stopwatch with

    member x.ElapsedSeconds = float x.ElapsedMilliseconds / 1000.
    member x.ElapsedMinutes = x.ElapsedSeconds / 60.

/// Manages a time cycle defined by `period`. Can be explicitly Trigger()ed prematurely
type IntervalTimer(period: TimeSpan) =

    let sw = Stopwatch.start ()
    let mutable force = false
    let periodT = period.TotalSeconds * double System.Diagnostics.Stopwatch.Frequency |> int64
    let periodMs = int64 period.TotalMilliseconds

    member _.Trigger() =
        force <- true
    member _.Restart() =
        force <- false
        sw.Restart()

    member val Period = period
    member _.IsDue = sw.ElapsedTicks > periodT || force
    member _.IsTriggered = force
    member _.RemainingMs = match periodMs - sw.ElapsedMilliseconds with t when t <= 0L -> 0 | t -> int t

    // NOTE asking the question is destructive - the timer is reset as a side effect
    member x.IfDueRestart() =
        if x.IsDue then x.Restart(); true
        else false

    /// Awaits reaction to a Trigger() invocation
    member x.SleepUntilTriggerCleared(?timeout, ?sleepMs) =
        if not x.IsTriggered then () else

        // The processing loops run on 1s timers, so we busy-wait until they wake
        let timeout = IntervalTimer(defaultArg timeout (TimeSpan.FromSeconds 2))
        while x.IsTriggered && not timeout.IsDue do
            System.Threading.Thread.Sleep(defaultArg sleepMs 1)

module Exception =

    let rec inner (e: exn) =
        match e with
        | :? AggregateException as ae when ae.InnerExceptions.Count = 1 -> inner ae.InnerException
        | e -> e
    let (|Inner|) = inner
    let [<return: Struct>] (|Log|_|) log (e: exn) = log e; ValueNone

type CancellationToken = System.Threading.CancellationToken
type Task<'T> = System.Threading.Tasks.Task<'T>
open System.Threading.Tasks

module Channel =

    open System.Threading.Channels

    let unboundedSr<'t> = Channel.CreateUnbounded<'t>(UnboundedChannelOptions(SingleReader = true))
    let unboundedSw<'t> = Channel.CreateUnbounded<'t>(UnboundedChannelOptions(SingleWriter = true))
    let unboundedSwSr<'t> = Channel.CreateUnbounded<'t>(UnboundedChannelOptions(SingleWriter = true, SingleReader = true))
    let boundedSw<'t> c = Channel.CreateBounded<'t>(BoundedChannelOptions(c, SingleWriter = true))
    let waitToWrite (w: ChannelWriter<_>) ct = let vt = w.WaitToWriteAsync(ct) in vt.AsTask() :> Task
    let tryWrite (w: ChannelWriter<_>) = w.TryWrite
    let write (w: ChannelWriter<_>) = w.TryWrite >> ignore
    let inline awaitRead (r: ChannelReader<_>) ct = let vt = r.WaitToReadAsync(ct) in vt.AsTask()
    let inline tryRead (r: ChannelReader<_>) () =
        let mutable msg = Unchecked.defaultof<_>
        if r.TryRead(&msg) then ValueSome msg else ValueNone
    let inline apply (r: ChannelReader<_>) f =
        let mutable worked = false
        let mutable msg = Unchecked.defaultof<_>
        while r.TryRead(&msg) do
            worked <- true
            f msg
        worked
    let inline readAll (r: ChannelReader<_>) () = seq {
        let mutable msg = Unchecked.defaultof<_>
        while r.TryRead(&msg) do
            yield msg }

module Async =

    let ofUnitTask (t: Task) = Async.AwaitTaskCorrect t
    let ofTask (t: Task<'t>) = Async.AwaitTaskCorrect t

    let inline call (start: CancellationToken -> Task<'T>): Async<'T> = async {
        let! ct = Async.CancellationToken
        return! start ct |> ofTask }
    let inline callUnit (start: CancellationToken -> Task): Async<unit> = async {
        let! ct = Async.CancellationToken
        return! start ct |> ofUnitTask }

    let inline startImmediateAsTask (computation: Async<'T>) ct: Task<'T> = Async.StartImmediateAsTask(computation, ct)
    let inline executeAsTask ct (computation: Async<'T>): Task<'T> = startImmediateAsTask computation ct
    let parallelLimit maxDop xs: Async<'t []> = Async.Parallel(xs, maxDegreeOfParallelism = maxDop)

module Task =

    let inline run create = Task.Run<unit>(Func<Task<unit>> create)
    let inline start create = run create |> ignore<Task>
    let inline delay (ts: TimeSpan) ct = Task.Delay(ts, ct)
    let inline Catch (t: Task<'t>) = task { try let! r = t in return Ok r with e -> return Error e }
    let private parallel_ maxDop ct (xs: seq<CancellationToken -> Task<'t>>): Task<'t []> =
        Async.Parallel(xs |> Seq.map Async.call, ?maxDegreeOfParallelism = match maxDop with 0 -> None | x -> Some x) |> Async.executeAsTask ct
    /// Runs an inner task with a dedicated Linked Token Source. Cancels via the ct upon completion, before Disposing the LCTS
    let inline runWithCancellation (ct: CancellationToken) ([<InlineIfLambda>]f: CancellationToken -> Task) = task {
        use cts = System.Threading.CancellationTokenSource.CreateLinkedTokenSource(ct) // https://stackoverflow.com/questions/6960520/when-to-dispose-cancellationtokensource
        try do! f cts.Token
        finally cts.Cancel() }
    let parallelLimit maxDop ct xs: Task<'t []> =
        parallel_ maxDop ct xs
    let sequential ct xs: Task<'t []> =
        parallel_ 1 ct xs
    let parallelUnlimited ct xs: Task<'t []> =
        parallel_ 0 ct xs
    let inline ignore<'T> (a: Task<'T>): Task<unit> = task {
        let! _ = a
        return () }

type Sem(max) =
    let inner = new System.Threading.SemaphoreSlim(max)
    member _.HasCapacity = inner.CurrentCount <> 0
    member _.State = struct(max - inner.CurrentCount, max)
    member _.TryTake() = inner.Wait 0
    member _.Release() = inner.Release() |> ignore
    member _.Wait(ct: CancellationToken) = inner.WaitAsync(ct)
    member x.WaitButRelease(ct: CancellationToken) = // see https://stackoverflow.com/questions/31621644/task-whenany-and-semaphoreslim-class/73197290?noredirect=1#comment129334330_73197290
        if x.TryTake() then x.Release(); Task.CompletedTask
        else let tco = TaskContinuationOptions.OnlyOnRanToCompletion ||| TaskContinuationOptions.ExecuteSynchronously
             x.Wait(ct).ContinueWith((fun _ -> x.Release()), ct, tco, TaskScheduler.Default)
    /// Manage a controlled shutdown by accumulating reservations of the full capacity.
    member x.WaitForCompleted(ct: CancellationToken) = task {
        for _ in 1..max do do! x.Wait(ct)
        return struct (0, max) }

/// Helper for use in Propulsion.Tool and/or equivalent apps; needs to be (informally) exposed
type Async with

    /// Asynchronously awaits the next keyboard interrupt event, throwing a TaskCanceledException
    /// Honors cancellation so it can be used with Async.Parallel to have multiple pump loops couple their fates
    static member AwaitKeyboardInterruptAsTaskCanceledException() = async {
        let! ct = Async.CancellationToken
        let tcs = TaskCompletionSource()
        use _ = ct.Register(fun () ->
            tcs.TrySetCanceled() |> ignore)
        use _ = Console.CancelKeyPress.Subscribe(fun (a: ConsoleCancelEventArgs) ->
            a.Cancel <- true // We're using this exception to drive a controlled shutdown so inhibit the standard behavior
            tcs.TrySetException(TaskCanceledException "Execution cancelled via Ctrl-C/Break; exiting...") |> ignore)
        return! tcs.Task |> Async.ofUnitTask }

type OAttribute = System.Runtime.InteropServices.OptionalAttribute
type DAttribute = System.Runtime.InteropServices.DefaultParameterValueAttribute

module ValueTuple =

    let inline fst struct (f, _s) = f
    let inline snd struct (_f, s) = s
    let inline ofKvp (x: System.Collections.Generic.KeyValuePair<_, _>) = struct (x.Key, x.Value)

module ValueOption =

    let inline ofOption x = match x with Some x -> ValueSome x | None -> ValueNone
    let inline toOption x = match x with ValueSome x -> Some x | ValueNone -> None
    let inline map f x = match x with ValueSome x -> ValueSome (f x) | ValueNone -> ValueNone

module Seq =

    let tryPickV f (xs: _ seq) =
        use e = xs.GetEnumerator()
        let mutable res = ValueNone
        while (ValueOption.isNone res && e.MoveNext()) do
            res <- f e.Current
        res
    let inline chooseV f xs = seq { for x in xs do match f x with ValueSome v -> yield v | ValueNone -> () }

module Array =

    let inline any xs = (not << Array.isEmpty) xs
    let inline chooseV f xs = [| for item in xs do match f item with ValueSome v -> yield v | ValueNone -> () |]

module Stats =

    open System.Collections.Generic

    let statsDescending (xs: Dictionary<_, _>) = xs |> Seq.map ValueTuple.ofKvp |> Seq.sortByDescending ValueTuple.snd

    /// Gathers stats relating to how many items of a given category have been observed
    type CatStats() =
        let cats = Dictionary<string, int64>()

        member _.Ingest(cat, ?weight) =
            let weight = defaultArg weight 1L
            match cats.TryGetValue cat with
            | true, catCount -> cats[cat] <- catCount + weight
            | false, _ -> cats[cat] <- weight

        member _.Count = cats.Count
        member _.Any = cats.Count <> 0
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
    let private dumpStats (log: Serilog.ILogger) (label: string) (xs: TimeSpan seq) =
        let sortedLatencies = xs |> Seq.map (fun ts -> ts.TotalSeconds) |> Seq.sort |> Seq.toArray

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
        let stdDev = match l.stddev with None -> Double.NaN | Some d -> d.TotalSeconds
        log.Information(" {kind} {count,4} : max={max:n3}s p99={p99:n3}s p95={p95:n3}s p50={p50:n3}s min={min:n3}s avg={avg:n3}s stddev={stddev:n3}s",
            label, sortedLatencies.Length, l.max.TotalSeconds, l.p99.TotalSeconds, l.p95.TotalSeconds, l.p50.TotalSeconds, l.min.TotalSeconds, l.avg.TotalSeconds, stdDev)

    /// Operations on an instance are safe cross-thread
    type ConcurrentLatencyStats(label) =
        let buffer = System.Collections.Concurrent.ConcurrentStack<TimeSpan>()
        member _.Record value = buffer.Push value
        member _.Dump(log: Serilog.ILogger) =
            if not buffer.IsEmpty then
                dumpStats log label buffer
                buffer.Clear() // yes, there is a race

    /// Not thread-safe, i.e. suitable for use in a Stats handler only
    type LatencyStats(label) =
        let buffer = ResizeArray<TimeSpan>()
        member _.Record value = buffer.Add value
        member _.Dump(log: Serilog.ILogger) =
            if buffer.Count <> 0 then
                dumpStats log label buffer
                buffer.Clear()

    /// Not thread-safe, i.e. suitable for use in a Stats handler only
    type LatencyStatsSet() =
        let buckets = Dictionary<string, ResizeArray<TimeSpan>>()
        let emit log names =
            let maxGroupLen = names |> Seq.map String.length |> Seq.max // NOTE caller must guarantee >1 item
            fun (label: string) -> dumpStats log (label.PadRight maxGroupLen)
        member _.Record(bucket, value: TimeSpan) =
            match buckets.TryGetValue bucket with
            | false, _ -> let n = ResizeArray() in n.Add value; buckets.Add(bucket, n)
            | true, buf -> buf.Add value
        member _.Dump(log: Serilog.ILogger, ?labelSortOrder) =
            if buckets.Count <> 0 then
                let emit = emit log buckets.Keys
                for name in Seq.sortBy (defaultArg labelSortOrder id) buckets.Keys do
                    emit name buckets[name]
        member _.DumpGrouped(bucketGroup, log: Serilog.ILogger, ?totalLabel) =
            if buckets.Count <> 0 then
                let clusters = buckets |> Seq.groupBy (fun kv -> bucketGroup kv.Key) |> Seq.sortBy fst |> Seq.toArray
                let emit = emit log (clusters |> Seq.map fst)
                totalLabel |> Option.iter (fun l -> emit l (buckets |> Seq.collect (fun kv -> kv.Value)))
                for name, items in clusters do
                    emit name (items |> Seq.collect (fun kv -> kv.Value))
        member _.Clear() = buckets.Clear()

type LogEventLevel = Serilog.Events.LogEventLevel

module Log =

    let inline miB x = float x / 1024. / 1024.

    /// Attach a property to the captured event record to hold the metric information
    // Sidestep Log.ForContext converting to a string; see https://github.com/serilog/serilog/issues/1124
    let withScalarProperty (key: string) (value: 'T) (log: Serilog.ILogger) =
        let enrich (e: Serilog.Events.LogEvent) =
            e.AddPropertyIfAbsent(Serilog.Events.LogEventProperty(key, Serilog.Events.ScalarValue(value)))
        log.ForContext({ new Serilog.Core.ILogEventEnricher with member _.Enrich(evt,_) = enrich evt })
    let [<return: Struct>] (|ScalarValue|_|): Serilog.Events.LogEventPropertyValue -> obj voption = function
        | :? Serilog.Events.ScalarValue as x -> ValueSome x.Value
        | _ -> ValueNone
