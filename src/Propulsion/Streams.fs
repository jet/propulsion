namespace Propulsion.Streams

open Propulsion
open Serilog
open System
open System.Collections.Concurrent
open System.Collections.Generic
open System.Threading

module Log =

    type BufferMetric = { cats : int; streams : int; events : int; bytes : int64 }
    [<RequireQualifiedAccess; NoEquality; NoComparison>]
    type Metric =
        /// Summary of data held in an Ingester buffer, not yet submitted to the Scheduler
        | BufferReport of BufferMetric
        /// Buffer Report, classified based on the current State from the perspective of the scheduling algorithms
        | SchedulerStateReport of
            /// All ingested data has been processed
            synced : int *
            /// Work is currently been carried for streams in this state
            busy : BufferMetric *
            /// Buffered data is ready to be processed, but not yet scheduled
            ready : BufferMetric *
            /// Some buffered data is available, but data from start of stream has not yet been loaded (only relevant for striped readers)
            buffering : BufferMetric *
            /// Buffered data is held, but Sink has reported it as unprocessable (i.e., categorized as poison messages)
            malformed : BufferMetric
        /// Time spent on the various aspects of the scheduler loop; used to analyze performance issue
        | SchedulerCpu of
            merge : TimeSpan * // mt: Time spent merging input events and streams prior to ingestion
            ingest : TimeSpan * // it: Time spent ingesting streams from the ingester into the Scheduler's buffers
            dispatch : TimeSpan * // ft: Time spent preparing requests (filling) for the Dispatcher
            results : TimeSpan * // dt: Time spent handling results (draining) from the Dispatcher's response queue
            stats : TimeSpan // st: Time spent emitting statistics per period (default period is 1m)
        /// Scheduler attempt latency reports
        | HandlerResult of
            /// "exception" or "ok"
            kind : string *
            /// handler duration in s
            latency : float
        | StreamsBusy of
            /// "Failing" or "stuck"
            kind : string *
            count : int *
            /// age in S
            oldest : float *
            /// age in S
            newest : float

    /// Attach a property to the captured event record to hold the metric information
    // Sidestep Log.ForContext converting to a string; see https://github.com/serilog/serilog/issues/1124
    let [<Literal>] PropertyTag = "propulsionEvent"
    let [<Literal>] GroupTag = "group"
    let internal metric (value : Metric) (log : ILogger) =
        let enrich (e : Serilog.Events.LogEvent) =
            e.AddPropertyIfAbsent(Serilog.Events.LogEventProperty(PropertyTag, Serilog.Events.ScalarValue(value)))
        log.ForContext({ new Serilog.Core.ILogEventEnricher with member _.Enrich(evt,_) = enrich evt })
    let internal (|SerilogScalar|_|) : Serilog.Events.LogEventPropertyValue -> obj option = function
        | :? Serilog.Events.ScalarValue as x -> Some x.Value
        | _ -> None
    let tryGetScalar<'t> key (logEvent : Serilog.Events.LogEvent) : 't option =
        match logEvent.Properties.TryGetValue key with
        | true, SerilogScalar (:? 't as e) -> Some e
        | _ -> None
    let (|MetricEvent|_|) logEvent =
        let metric = tryGetScalar<Metric> PropertyTag logEvent
        match metric with
        | Some m -> Some (m, tryGetScalar<string> GroupTag logEvent)
        | None -> None

/// A Single Event from an Ordered stream
[<NoComparison; NoEquality>]
type StreamEvent<'Format> = { stream : FsCodec.StreamName; event : FsCodec.ITimelineEvent<'Format> }

/// Span of events from an Ordered Stream
[<NoComparison; NoEquality>]
type StreamSpan<'Format> =
    { index : int64; events : FsCodec.ITimelineEvent<'Format>[] }
    member x.Version = x.events[x.events.Length - 1].Index + 1L

module Internal =

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
        member _.StatsDescending = Internal.statsDescending cats

    type private Data =
        {   min    : TimeSpan
            p50    : TimeSpan
            p95    : TimeSpan
            p99    : TimeSpan
            max    : TimeSpan
            avg    : TimeSpan
            stddev : TimeSpan option }

    open MathNet.Numerics.Statistics
    let private dumpStats (kind : string) (xs : TimeSpan seq) (log : ILogger) =
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
        let buffer = ConcurrentStack<TimeSpan>()
        member _.Record value = buffer.Push value
        member _.Dump(log : ILogger) =
            if not buffer.IsEmpty then
                dumpStats kind buffer log
                buffer.Clear() // yes, there is a race

    /// Should only be used on one thread
    type LatencyStats(kind) =
        let buffer = ResizeArray<TimeSpan>()
        member _.Record value = buffer.Add value
        member _.Dump(log : ILogger) =
            if buffer.Count <> 0 then
                dumpStats kind buffer log
                buffer.Clear()

open Internal

[<AutoOpen>]
module private Impl =

    let (|NNA|) xs = if obj.ReferenceEquals(null, xs) then Array.empty else xs
    let inline arrayBytes (x : _ []) = if obj.ReferenceEquals(null, x) then 0 else x.Length
    let inline stringBytes (x : string) = match x with null -> 0 | x -> x.Length * sizeof<char>
    let inline eventSize (x : FsCodec.IEventData<byte[]>) = arrayBytes x.Data + arrayBytes x.Meta + stringBytes x.EventType + 16
    let inline mb x = float x / 1024. / 1024.
    let inline accStopwatch (f : unit -> 't) at =
        let sw = System.Diagnostics.Stopwatch.StartNew()
        let r = f ()
        at sw.Elapsed
        r

module StreamName =

    /// Despite conventions, there's no guarantee that an arbitrary Kafka `key`, EventStore `StreamId` etc.
    /// will necessarily adhere to the `{category}-{aggregateId}` form
    /// This helper ensures there's always a dash (even if that means having a defaultStringId)
    let parseWithDefaultCategory defaultCategory (rawStreamName : string) : FsCodec.StreamName =
        if rawStreamName.IndexOf '-' = -1 then String.Concat(defaultCategory, "-", rawStreamName)
        else rawStreamName
        |> FSharp.UMX.UMX.tag

    /// Guard against inputs that don't adhere to "{category}-{aggregateId}" by prefixing with `-`  })
    let internalParseSafe rawStreamName : FsCodec.StreamName = parseWithDefaultCategory "" rawStreamName

    /// Because we coerce all stream names to be well-formed, we can split it too
    /// where there's no category, we use the aggregateId instead
    let categorize : FsCodec.StreamName -> string = function
        | FsCodec.StreamName.CategoryAndId ("", aggregateId) -> aggregateId
        | FsCodec.StreamName.CategoryAndId (category, _) -> category

#nowarn "52" // see tmp.Sort

module Progress =

    type [<NoComparison; NoEquality>] internal BatchState = { markCompleted : unit -> unit; streamToRequiredIndex : Dictionary<FsCodec.StreamName, int64> }

    type ProgressState<'Pos>() =
        let pending = Queue<_>()
        let trim () =
            while pending.Count <> 0 && pending.Peek().streamToRequiredIndex.Count = 0 do
                let batch = pending.Dequeue()
                batch.markCompleted()

        member _.AppendBatch(markCompleted, reqs : Dictionary<FsCodec.StreamName, int64>) =
            pending.Enqueue { markCompleted = markCompleted; streamToRequiredIndex = reqs }
            trim ()

        member _.MarkStreamProgress(stream, index) =
            for x in pending do
                match x.streamToRequiredIndex.TryGetValue stream with
                | true, requiredIndex when requiredIndex <= index -> x.streamToRequiredIndex.Remove stream |> ignore
                | _, _ -> ()
            trim ()

        member _.InScheduledOrder getStreamWeight =
            trim ()
            let streams = HashSet()
            let tmp = ResizeArray(16384)
            let mutable batch = 0
            for x in pending do
                batch <- batch + 1
                for s in x.streamToRequiredIndex.Keys do
                    if streams.Add s then
                        tmp.Add((s, (batch, -getStreamWeight s)))
            let c = Comparer<_>.Default
            tmp.Sort(fun (_, _a) (_, _b) -> c.Compare(_a, _b))
            tmp |> Seq.map (fun (s, _) -> s)

module Buffering =

    module StreamSpan =
        let (|End|) (x : StreamSpan<_>) = x.index + if x.events = null then 0L else x.events.LongLength

        let dropBeforeIndex min : StreamSpan<_> -> StreamSpan<_> = function
            | x when x.index >= min -> x // don't adjust if min not within
            | End n when n < min -> { index = min; events = [||] } // throw away if before min
            | x -> { index = min; events = x.events |> Array.skip (min - x.index |> int) }  // slice

        let merge min (xs : StreamSpan<_> seq) =
            let xs =
                seq { for x in xs -> { x with events = (|NNA|) x.events } }
                |> Seq.map (dropBeforeIndex min)
                |> Seq.filter (fun x -> x.events.Length <> 0)
                |> Seq.sortBy (fun x -> x.index)
            let buffer = ResizeArray()
            let mutable curr = None
            for x in xs do
                match curr, x with
                // Not overlapping, no data buffered -> buffer
                | None, _ ->
                    curr <- Some x
                // Gap
                | Some (End nextIndex as c), x when x.index > nextIndex ->
                    buffer.Add c
                    curr <- Some x
                // Overlapping, join
                | Some (End nextIndex as c), x  ->
                    curr <- Some { c with events = Array.append c.events (dropBeforeIndex nextIndex x).events }
            curr |> Option.iter buffer.Add
            if buffer.Count = 0 then null else buffer.ToArray()

        let inline estimateBytesAsJsonUtf8 (x : FsCodec.IEventData<_>) = eventSize x + 80

        let stats (x : StreamSpan<_>) =
            x.events.Length, x.events |> Seq.sumBy estimateBytesAsJsonUtf8

        let slice (maxEvents, maxBytes) streamSpan =
            let mutable count, bytes = 0, 0
            let mutable countBudget, bytesBudget = maxEvents, maxBytes
            let withinLimits y =
                countBudget <- countBudget - 1
                let eventBytes = estimateBytesAsJsonUtf8 y
                bytesBudget <- bytesBudget - eventBytes
                // always send at least one event in order to surface the problem and have the stream marked malformed
                let res = count = 0 || (countBudget >= 0 && bytesBudget >= 0)
                if res then count <- count + 1; bytes <- bytes + eventBytes
                res
            let trimmed = { streamSpan with events = streamSpan.events |> Array.takeWhile withinLimits }
            stats trimmed, trimmed

(*  // ORIGINAL StreamState memory representation:

    Type layout for 'StreamState`1'
    Size: 24 bytes. Paddings: 7 bytes (%29 of empty space)
    |========================================|
    | Object Header (8 bytes)                |
    |----------------------------------------|
    | Method Table Ptr (8 bytes)             |
    |========================================|
    |   0-7: FSharpOption`1 write@ (8 bytes) |
    |----------------------------------------|
    |  8-15: StreamSpan`1[] queue@ (8 bytes) |
    |----------------------------------------|
    |    16: Boolean isMalformed@ (1 byte)   |
    |----------------------------------------|
    | 17-23: padding (7 bytes)               |
    |========================================|

    // CURRENT layout:

    Type layout for 'StreamState`1'
    Size: 16 bytes. Paddings: 0 bytes (%0 of empty space)
    |========================================|
    |   0-7: StreamSpan`1[] queue@ (8 bytes) |
    |----------------------------------------|
    |  8-15: Int64 write@ (8 bytes)          |
    |========================================|
    *)

    // NOTE: Optimized Representation as we can have a lot of these
    // 1. -2 sentinel value for write position signifying `None` (no write position yet established)
    // 2. -3 sentinel value for malformed data
    [<NoComparison; NoEquality; Struct>]
    type StreamState<'Format> = private { write : int64; queue : StreamSpan<'Format>[] } with
        static member Create(write, queue, ?malformed) =
            let effWrite =
                match write with
                | _ when malformed = Some true -> -3L
                | None -> -2L
                | Some w -> w
            { write = effWrite; queue = queue }
        member x.IsEmpty = obj.ReferenceEquals(null, x.queue)
        member x.IsPurgeable = x.IsEmpty && not x.IsMalformed
        member x.IsMalformed = not x.IsEmpty && -3L = x.write
        member x.HasValid = not x.IsEmpty && not x.IsMalformed
        member x.Write = match x.write with -2L -> None | x -> Some x
        member x.Queue : StreamSpan<'Format>[] = x.queue
        member x.IsReady =
            if not x.HasValid then false else

            match x.Write, Array.head x.queue with
            | Some w, { index = i } -> i = w
            | None, _ -> true

    module StreamState =
        let eventsSize (x : StreamState<byte[]>) =
            if x.IsEmpty then 0L else

            let mutable acc = 0
            for x in x.queue do
                for x in x.events do
                    acc <- acc + eventSize x
            int64 acc

        let inline private optionCombine f (r1 : 'a option) (r2 : 'a option) =
            match r1, r2 with
            | Some x, Some y -> f x y |> Some
            | None, None -> None
            | None, x | x, None -> x

        let combine (s1 : StreamState<_>) (s2 : StreamState<_>) : StreamState<'Format> =
            let writePos = optionCombine max s1.Write s2.Write
            let items = let NNA q1, NNA q2 = s1.queue, s2.queue in Seq.append q1 q2
            StreamState<'Format>.Create(writePos, StreamSpan.merge (defaultArg writePos 0L) items, s1.IsMalformed || s2.IsMalformed)

    type Streams<'Format>() =
        let states = Dictionary<FsCodec.StreamName, StreamState<'Format>>()
        let merge stream (state : StreamState<_>) =
            match states.TryGetValue stream with
            | false, _ ->
                states.Add(stream, state)
            | true, current ->
                let updated = StreamState.combine current state
                states[stream] <- updated

        member _.Merge(item : StreamEvent<'Format>) =
            merge item.stream (StreamState<'Format>.Create(None, [| { index = item.event.Index; events = [| item.event |] } |]))

        member private _.States = states

        member _.Items = states :> seq<KeyValuePair<FsCodec.StreamName, StreamState<'Format>>>

        member _.Merge(other : Streams<'Format>) =
            for x in other.States do
                merge x.Key x.Value

        member _.Dump(log : ILogger, estimateSize, categorize) =
            let mutable waiting, waitingE, waitingB = 0, 0, 0L
            let waitingCats, waitingStreams = CatStats(), CatStats()
            for KeyValue (stream, state) in states do
                let sz = estimateSize state
                waitingCats.Ingest(categorize stream)
                if sz <> 0L then
                    let sn, wp = FsCodec.StreamName.toString stream, defaultArg state.Write 0L
                    waitingStreams.Ingest(sprintf "%s@%dx%d" sn wp state.queue[0].events.Length, (sz + 512L) / 1024L)
                waiting <- waiting + 1
                waitingE <- waitingE + (state.queue |> Array.sumBy (fun x -> x.events.Length))
                waitingB <- waitingB + sz
            let m = Log.Metric.BufferReport { cats = waitingCats.Count; streams = waiting; events = waitingE; bytes = waitingB }
            (log |> Log.metric m).Information(" Streams Waiting {busy:n0}/{busyMb:n1}MB ", waiting, mb waitingB)
            if waitingCats.Any then log.Information(" Waiting Categories, events {@readyCats}", Seq.truncate 5 waitingCats.StatsDescending)
            if waitingCats.Any then log.Information(" Waiting Streams, KB {@readyStreams}", Seq.truncate 5 waitingStreams.StatsDescending)

type EventMetrics = int * int

module Scheduling =

    open Buffering

    type [<NoComparison; NoEquality>] DispatchItem<'Format> = { stream : FsCodec.StreamName; writePos : int64 option; span : StreamSpan<'Format> }

    type StreamStates<'Format>() =
        let states = Dictionary<FsCodec.StreamName, StreamState<'Format>>()

        let tryGetItem stream =
            match states.TryGetValue stream with
            | true, v -> Some v
            | _ -> None
        let update stream (state : StreamState<_>) =
            match tryGetItem stream with
            | None ->
                states.Add(stream, state)
                stream, state
            | Some current ->
                let updated = StreamState.combine current state
                states[stream] <- updated
                stream, updated
        let updateWritePos stream isMalformed pos span = update stream (StreamState<'Format>.Create(pos, span, isMalformed))
        let markCompleted stream index = updateWritePos stream false (Some index) null |> ignore
        let merge (buffer : Streams<'Format>) =
            for x in buffer.Items do
                update x.Key x.Value |> ignore
        let purge () =
            for x in states do
                if x.Value.IsPurgeable then
                    states.Remove x.Key |> ignore

        let busy = HashSet<FsCodec.StreamName>()
        let pending trySlipstreamed (requestedOrder : FsCodec.StreamName seq) : seq<DispatchItem<'Format>> = seq {
            let proposed = HashSet()
            for s in requestedOrder do
                match tryGetItem s with
                | Some state when state.HasValid && not (busy.Contains s) ->
                    proposed.Add s |> ignore
                    yield { writePos = state.Write; stream = s; span = Array.head state.Queue }
                | _ -> ()
            if trySlipstreamed then
                // [lazily] slipstream in further events that are not yet referenced by in-scope batches
                for KeyValue(s, v) in states do
                    if v.HasValid && not (busy.Contains s) && proposed.Add s then
                        yield { writePos = v.Write; stream = s; span = Array.head v.Queue } }

        let markBusy stream = busy.Add stream |> ignore
        let markNotBusy stream = busy.Remove stream |> ignore

        member _.InternalMerge buffer = merge buffer
        member x.Purge() = purge ()
        member _.InternalUpdate stream pos queue = update stream (StreamState<'Format>.Create(Some pos,queue))

        member _.Add(stream, index, event, ?isMalformed) =
            updateWritePos stream (defaultArg isMalformed false) None [| { index = index; events = [| event |] } |]

        member _.Add(stream, span: StreamSpan<_>, isMalformed) =
            updateWritePos stream isMalformed None [| span |]

        member _.SetMalformed(stream, isMalformed) =
            updateWritePos stream isMalformed None [| { index = 0L; events = null } |]

        member _.TryGetItem(stream) = tryGetItem stream

        member _.WritePositionIsAlreadyBeyond(stream, required) =
            match tryGetItem stream with
            | Some streamState -> streamState.Write |> Option.exists (fun cw -> cw >= required)
            | _ -> false

        member _.MarkBusy stream =
            markBusy stream

        member _.MarkCompleted(stream, index) =
            markNotBusy stream
            markCompleted stream index

        member _.MarkFailed stream =
            markNotBusy stream

        member _.Pending(trySlipstreamed, byQueuedPriority : FsCodec.StreamName seq) : DispatchItem<'Format> seq =
            pending trySlipstreamed byQueuedPriority

        member _.Dump(log : ILogger, estimateSize) =
            let mutable (busyCount, busyE, busyB), (ready, readyE, readyB), synced = (0, 0, 0L), (0, 0, 0L), 0
            let mutable (unprefixed, unprefixedE, unprefixedB), (malformed, malformedE, malformedB) = (0, 0, 0L), (0, 0, 0L)
            let busyCats, readyCats, readyStreams = CatStats(), CatStats(), CatStats()
            let unprefixedCats, unprefixedStreams, malformedCats, malformedStreams = CatStats(), CatStats(), CatStats(), CatStats()
            let kb sz = (sz + 512L) / 1024L
            for KeyValue (stream, state) in states do
                match estimateSize state with
                | 0L ->
                    synced <- synced + 1
                | sz when busy.Contains stream ->
                    busyCats.Ingest(StreamName.categorize stream)
                    busyCount <- busyCount + 1
                    busyB <- busyB + sz
                    busyE <- busyE + (state.Queue |> Array.sumBy (fun x -> x.events.Length))
                | sz when state.IsMalformed ->
                    malformedCats.Ingest(StreamName.categorize stream)
                    malformedStreams.Ingest(FsCodec.StreamName.toString stream, mb sz |> int64)
                    malformed <- malformed + 1
                    malformedB <- malformedB + sz
                    malformedE <- malformedE + (state.Queue |> Array.sumBy (fun x -> x.events.Length))
                | sz when not state.IsReady ->
                    unprefixedCats.Ingest(StreamName.categorize stream)
                    unprefixedStreams.Ingest(FsCodec.StreamName.toString stream, mb sz |> int64)
                    unprefixed <- unprefixed + 1
                    unprefixedB <- unprefixedB + sz
                    unprefixedE <- unprefixedE + (state.Queue |> Array.sumBy (fun x -> x.events.Length))
                | sz ->
                    readyCats.Ingest(StreamName.categorize stream)
                    readyStreams.Ingest(sprintf "%s@%dx%d" (FsCodec.StreamName.toString stream) (defaultArg state.Write 0L) state.Queue[0].events.Length, kb sz)
                    ready <- ready + 1
                    readyB <- readyB + sz
                    readyE <- readyE + (state.Queue |> Array.sumBy (fun x -> x.events.Length))
            let busyStats : Log.BufferMetric = { cats = busyCats.Count; streams = busyCount; events = busyE; bytes = busyB }
            let readyStats : Log.BufferMetric = { cats = readyCats.Count; streams = readyStreams.Count; events = readyE; bytes = readyB }
            let bufferingStats : Log.BufferMetric = { cats = unprefixedCats.Count; streams = unprefixedStreams.Count; events = unprefixedE; bytes = unprefixedB }
            let malformedStats : Log.BufferMetric = { cats = malformedCats.Count; streams = malformedStreams.Count; events = malformedE; bytes = malformedB }
            let m = Log.Metric.SchedulerStateReport (synced, busyStats, readyStats, bufferingStats, malformedStats)
            (log |> Log.metric m).Information("Streams Synced {synced:n0} Active {busy:n0}/{busyMb:n1}MB Ready {ready:n0}/{readyMb:n1}MB Waiting {waiting}/{waitingMb:n1}MB Malformed {malformed}/{malformedMb:n1}MB",
                synced, busyCount, mb busyB, ready, mb readyB, unprefixed, mb unprefixedB, malformed, mb malformedB)
            if busyCats.Any then log.Information(" Active Categories, events {@busyCats}", Seq.truncate 5 busyCats.StatsDescending)
            if readyCats.Any then log.Information(" Ready Categories, events {@readyCats}", Seq.truncate 5 readyCats.StatsDescending)
            if readyCats.Any then log.Information(" Ready Streams, KB {@readyStreams}", Seq.truncate 5 readyStreams.StatsDescending)
            if unprefixedStreams.Any then log.Information(" Waiting Streams, KB {@missingStreams}", Seq.truncate 3 unprefixedStreams.StatsDescending)
            if malformedStreams.Any then log.Information(" Malformed Streams, MB {@malformedStreams}", malformedStreams.StatsDescending)

    /// Messages used internally by projector, including synthetic ones for the purposes of the `Stats` listeners
    [<NoComparison; NoEquality>]
    type InternalMessage<'R> =
        /// Stats per submitted batch for stats listeners to aggregate
        | Added of streams : int * skip : int * events : int
        /// Result of processing on stream - result (with basic stats) or the `exn` encountered
        | Result of duration : TimeSpan * stream : FsCodec.StreamName * progressed : bool * result : 'R

    type BufferState = Idle | Busy | Full | Slipstreaming

    /// Manages state used to generate metrics (and summary logs) regarding streams currently being processed by a Handler
    module Busy =
        let private ticksPerSecond = double System.Diagnostics.Stopwatch.Frequency
        let timeSpanFromStopwatchTicks = function
            | ticks when ticks > 0L -> TimeSpan.FromSeconds(double ticks / ticksPerSecond)
            | _ -> TimeSpan.Zero
        type private StreamState = { ts : int64; mutable count : int }
        let private walkAges (state : Dictionary<_, _>) =
            let now = System.Diagnostics.Stopwatch.GetTimestamp()
            if state.Count = 0 then Seq.empty else
            seq { for x in state.Values -> struct (now - x.ts, x.count) }
        let private renderState agesAndCounts =
            let mutable oldest, newest, streams, attempts = Int64.MinValue, Int64.MaxValue, 0, 0
            for struct (diff, count) in agesAndCounts do
                oldest <- max oldest diff
                newest <- min newest diff
                streams <- streams + 1
                attempts <- attempts + count
            if streams = 0 then oldest <- 0L; newest <- 0L
            (streams, attempts), (timeSpanFromStopwatchTicks oldest, timeSpanFromStopwatchTicks newest)
        /// Manages the list of currently dispatched Handlers
        /// NOTE we are guaranteed we'll hear about a Start before a Finish (or another Start) per stream by the design of the Dispatcher
        type private Active() =
            let state = Dictionary<FsCodec.StreamName, StreamState>()
            member _.HandleStarted(sn, ts) = state.Add(sn, { ts = ts; count = 1 })
            member _.TakeFinished(sn) =
                let res = state[sn]
                state.Remove sn |> ignore
                res.ts
            member _.State = walkAges state |> renderState
        /// Represents state of streams where the handler did not make progress on the last execution either intentionally or due to an exception
        type private Repeating() =
            let state = Dictionary<FsCodec.StreamName, StreamState>()
            member _.HandleResult(sn, isStuck, startTs) =
                if not isStuck then state.Remove sn |> ignore
                else match state.TryGetValue sn with
                     | true, v -> v.count <- v.count + 1
                     | false, _ -> state.Add(sn, { ts = startTs; count = 1 })
            member _.State = walkAges state |> renderState
        /// Collates all state and reactions to manage the list of busy streams based on callbacks/notifications from the Dispatcher
        type Monitor() =
            let active, failing, stuck = Active(), Repeating(), Repeating()
            let emit (log : ILogger) state (streams, attempts) (oldest : TimeSpan, newest : TimeSpan) =
                log.Information(" {state} {streams} for {newest:n1}-{oldest:n1}s, {attempts} attempts",
                                state, streams, newest.TotalSeconds, oldest.TotalSeconds, attempts)
            member _.HandleStarted(sn, ts) =
                active.HandleStarted(sn, ts)
            member _.HandleResult(sn, succeeded, progressed) =
                let startTs = active.TakeFinished(sn)
                failing.HandleResult(sn, not succeeded, startTs)
                stuck.HandleResult(sn, succeeded && not progressed, startTs)
            member _.DumpState(log : ILogger) =
                let inline dump state (streams, attempts) ages =
                    if streams <> 0 then
                        emit log state (streams, attempts) ages
                active.State ||> dump "active"
                failing.State ||> dump "failing"
                stuck.State ||> dump "stalled"
            member _.EmitMetrics(log : ILogger) =
                let inline report state (streams, attempts) (oldest : TimeSpan, newest : TimeSpan) =
                    let m = Log.Metric.StreamsBusy (state, streams, oldest.TotalSeconds, newest.TotalSeconds)
                    emit (log |> Log.metric m) state (streams, attempts) (oldest, newest)
                active.State ||> report "active"
                failing.State ||> report "failing"
                stuck.State ||> report "stalled"

    /// Gathers stats pertaining to the core projection/ingestion activity
    [<AbstractClass>]
    type Stats<'R, 'E>(log : ILogger, statsInterval : TimeSpan, stateInterval : TimeSpan) =
        let mutable cycles, fullCycles = 0, 0
        let states, oks, exns, mon = CatStats(), LatencyStats("ok"), LatencyStats("exceptions"), Busy.Monitor()
        let mutable batchesPended, streamsPended, eventsSkipped, eventsPended = 0, 0, 0, 0
        let statsDue, stateDue, stucksDue = intervalCheck statsInterval, intervalCheck stateInterval, intervalCheck (TimeSpan.FromSeconds 1.)
        let metricsLog = log.ForContext("isMetric", true)
        let mutable dt, ft, it, st, mt = TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero

        let dumpStats (dispatchActive, dispatchMax) batchesWaiting =
            log.Information("Scheduler {cycles} cycles ({fullCycles} full) {@states} Running {busy}/{processors}",
                cycles, fullCycles, states.StatsDescending, dispatchActive, dispatchMax)
            cycles <- 0; fullCycles <- 0; states.Clear()
            oks.Dump log; exns.Dump log
            log.Information(" Batches Holding {batchesWaiting} Started {batches} ({streams:n0}s {events:n0}-{skipped:n0}e)",
                batchesWaiting, batchesPended, streamsPended, eventsSkipped + eventsPended, eventsSkipped)
            batchesPended <- 0; streamsPended <- 0; eventsSkipped <- 0; eventsPended <- 0
            let m = Log.Metric.SchedulerCpu (mt, it, ft, dt, st)
            (log |> Log.metric m).Information(" Cpu Streams {mt:n1}s Batches {it:n1}s Dispatch {ft:n1}s Results {dt:n1}s Stats {st:n1}s",
                mt.TotalSeconds, it.TotalSeconds, ft.TotalSeconds, dt.TotalSeconds, st.TotalSeconds)
            dt <- TimeSpan.Zero; ft <- TimeSpan.Zero; it <- TimeSpan.Zero; st <- TimeSpan.Zero; mt <- TimeSpan.Zero

        abstract member Handle : InternalMessage<Choice<'R, 'E>> -> unit
        default x.Handle msg = msg |> function
            | Added (streams, skipped, events) ->
                batchesPended <- batchesPended + 1
                streamsPended <- streamsPended + streams
                eventsPended <- eventsPended + events
                eventsSkipped <- eventsSkipped + skipped
            | Result (duration, stream, progressed, Choice1Of2 _) ->
                oks.Record duration
                x.HandleResult(stream, duration, true, progressed)
            | Result (duration, stream, progressed, Choice2Of2 _) ->
                exns.Record duration
                x.HandleResult(stream, duration, false, progressed)

        abstract HandleResult : FsCodec.StreamName * TimeSpan * progressed : bool * succeeded : bool -> unit
        default _.HandleResult(stream, duration, succeeded, progressed) =
            mon.HandleResult(stream, succeeded, progressed)
            if metricsLog.IsEnabled Serilog.Events.LogEventLevel.Information then
                let outcomeKind = if succeeded then "ok" else "exceptions"
                let m = Log.Metric.HandlerResult (outcomeKind, duration.TotalSeconds)
                (metricsLog |> Log.metric m).Information("Outcome {kind} in {ms:n0}ms, progressed: {progressed}", outcomeKind, duration.TotalMilliseconds, progressed)
                if stucksDue () then
                    mon.EmitMetrics metricsLog

        abstract MarkStarted : stream : FsCodec.StreamName * stopwatchTicks : int64 -> unit
        default _.MarkStarted(stream, stopwatchTicks) =
            mon.HandleStarted(stream, stopwatchTicks)

        member x.DumpStats((used, max), batchesWaiting) =
            cycles <- cycles + 1
            if statsDue () then
                dumpStats (used, max) batchesWaiting
                mon.DumpState log
                x.DumpStats()

        member _.TryDumpState(state, dump, (_dt, _ft, _mt, _it, _st)) =
            dt <- dt + _dt
            ft <- ft + _ft
            mt <- mt + _mt
            it <- it + _it
            st <- st + _st
            fullCycles <- fullCycles + 1
            states.Ingest(string state)

            let due = stateDue ()
            if due then
                dump log
            due

        /// Allows an ingester or projector to wire in custom stats (typically based on data gathered in a `Handle` override)
        abstract DumpStats : unit -> unit
        default _.DumpStats () = ()

    /// Coordinates the dispatching of work and emission of results, subject to the maxDop concurrent processors constraint
    type private DopDispatcher<'R>(maxDop) =
        // Using a Queue as a) the ordering is more correct, favoring more important work b) we are adding from many threads so no value in ConcurrentBag's thread-affinity
        let work = new BlockingCollection<_>(ConcurrentQueue<_>())
        let result = Event<'R>()
        let dop = Sem maxDop
        let dispatch computation = async {
            let! res = computation
            result.Trigger res
            dop.Release() }

        [<CLIEvent>] member _.Result = result.Publish
        member _.HasCapacity = dop.HasCapacity
        member _.State = dop.State

        member _.TryAdd(item) =
            if dop.TryTake() then
                work.Add(item)
                true
            else false

        member _.Pump () = async {
            let! ct = Async.CancellationToken
            for item in work.GetConsumingEnumerable ct do
                Async.Start(dispatch item) }

    /// Kicks off enough work to fill the inner Dispatcher up to capacity
    type ItemDispatcher<'R>(maxDop) =
        let inner = DopDispatcher<TimeSpan * FsCodec.StreamName * bool * 'R>(maxDop)

        // On each iteration, we try to fill the in-flight queue, taking the oldest and/or heaviest streams first
        let tryFillDispatcher (pending, markStarted) project markBusy =
            let mutable hasCapacity, dispatched = inner.HasCapacity, false
            if hasCapacity then
                let potential : seq<DispatchItem<byte[]>> = pending ()
                let xs = potential.GetEnumerator()
                let ts = System.Diagnostics.Stopwatch.GetTimestamp()
                while xs.MoveNext() && hasCapacity do
                    let item = xs.Current
                    let succeeded = inner.TryAdd(project ts item)
                    if succeeded then
                        markBusy item.stream
                        markStarted (item.stream, ts)
                    hasCapacity <- succeeded
                    dispatched <- dispatched || succeeded // if we added any request, we'll skip sleeping
            hasCapacity, dispatched

        member _.Pump() = inner.Pump()
        [<CLIEvent>] member _.Result = inner.Result
        member _.State = inner.State
        member _.TryReplenish (pending, markStarted) project markStreamBusy =
            tryFillDispatcher (pending, markStarted) project markStreamBusy

    /// Defines interface between Scheduler (which owns the pending work) and the Dispatcher which periodically selects work to commence based on a policy
    type IDispatcher<'P, 'R, 'E> =
        abstract member TryReplenish : pending : (unit -> seq<DispatchItem<byte[]>>) -> markStreamBusy : (FsCodec.StreamName -> unit) -> bool * bool
        [<CLIEvent>] abstract member Result : IEvent<TimeSpan * FsCodec.StreamName * bool * Choice<'P, 'E>>
        abstract member InterpretProgress : StreamStates<byte[]> * FsCodec.StreamName * Choice<'P, 'E> -> int64 option * Choice<'R, 'E>
        abstract member RecordResultStats : InternalMessage<Choice<'R, 'E>> -> unit
        abstract member DumpStats : int -> unit
        abstract member TryDumpState : BufferState * StreamStates<byte[]> * (TimeSpan * TimeSpan * TimeSpan * TimeSpan * TimeSpan) -> bool

    /// Implementation of IDispatcher that feeds items to an item dispatcher that maximizes concurrent requests (within a limit)
    type MultiDispatcher<'P, 'R, 'E>
        (   inner : ItemDispatcher<Choice<'P, 'E>>,
            project : int64 -> DispatchItem<byte[]> -> Async<TimeSpan * FsCodec.StreamName * bool * Choice<'P, 'E>>,
            interpretProgress : StreamStates<byte[]> -> FsCodec.StreamName -> Choice<'P, 'E> -> int64 option * Choice<'R, 'E>,
            stats : Stats<'R, 'E>,
            dumpStreams) =
        static member Create(inner, project, interpretProgress, stats, dumpStreams) =
            let project sw (item : DispatchItem<byte[]>) : Async<TimeSpan * FsCodec.StreamName * bool * Choice<'P, 'E>> = async {
                let! progressed, res = project (item.stream, item.span)
                let now = System.Diagnostics.Stopwatch.GetTimestamp()
                return Busy.timeSpanFromStopwatchTicks (now - sw), item.stream, progressed, res }
            MultiDispatcher(inner, project, interpretProgress, stats, dumpStreams)
        static member Create(inner, handle, interpret, toIndex, stats, dumpStreams) =
            let project item = async {
                let met, (stream, span) = interpret item
                try let! spanResult, outcome = handle (stream, span)
                    let index' = toIndex (stream, span) spanResult
                    return index' > span.index, Choice1Of2 (index', met, outcome)
                with e -> return false, Choice2Of2 (met, e) }
            let interpretProgress (_streams : StreamStates<_>) _stream = function
                | Choice1Of2 (index, met, outcome) -> Some index, Choice1Of2 (met, outcome)
                | Choice2Of2 (stats, exn) -> None, Choice2Of2 (stats, exn)
            MultiDispatcher<_, _, _>.Create(inner, project, interpretProgress, stats, dumpStreams)

        interface IDispatcher<'P, 'R, 'E> with
            override _.TryReplenish pending markStreamBusy =
                inner.TryReplenish (pending, stats.MarkStarted) project markStreamBusy
            [<CLIEvent>] override _.Result = inner.Result
            override _.InterpretProgress(streams : StreamStates<_>, stream : FsCodec.StreamName, res : Choice<'P, 'E>) =
                interpretProgress streams stream res
            override _.RecordResultStats msg = stats.Handle msg
            override _.DumpStats pendingCount = stats.DumpStats(inner.State, pendingCount)
            override _.TryDumpState(dispatcherState, streams, (dt, ft, mt, it, st)) =
                stats.TryDumpState(dispatcherState, dumpStreams streams, (dt, ft, mt, it, st))

    /// Implementation of IDispatcher that allows a supplied handler select work and declare completion based on arbitrarily defined criteria
    type BatchedDispatcher
        (   select : DispatchItem<byte[]> seq -> DispatchItem<byte[]>[],
            handle : DispatchItem<byte[]>[] -> Async<(TimeSpan * FsCodec.StreamName * bool * Choice<int64 * (EventMetrics * unit), EventMetrics * exn>)[]>,
            stats : Stats<_, _>,
            dumpStreams) =
        let dop = DopDispatcher 1
        let result = Event<TimeSpan * FsCodec.StreamName * bool * Choice<int64 * (EventMetrics * unit), EventMetrics * exn>>()

        // On each iteration, we offer the ordered work queue to the selector
        // we propagate the selected streams to the handler
        let trySelect pending markBusy =
            let mutable hasCapacity, dispatched = dop.HasCapacity, false
            if hasCapacity then
                let potential : seq<DispatchItem<byte[]>> = pending ()
                let streams : DispatchItem<byte[]>[] = select potential
                let succeeded = (not << Array.isEmpty) streams
                if succeeded then
                    let res = dop.TryAdd(handle streams)
                    if not res then failwith "Checked we can add, what gives?"
                    for x in streams do
                        markBusy x.stream
                    dispatched <- true // if we added any request, we'll skip sleeping
                    hasCapacity <- false
            hasCapacity, dispatched

        member _.Pump() = async {
            use _ = dop.Result.Subscribe(Array.iter result.Trigger)
            return! dop.Pump() }

        interface IDispatcher<int64 * (EventMetrics * unit), EventMetrics * unit, EventMetrics * exn> with
            override _.TryReplenish pending markStreamBusy = trySelect pending markStreamBusy
            [<CLIEvent>] override _.Result = result.Publish
            override _.InterpretProgress(_streams : StreamStates<_>, _stream : FsCodec.StreamName, res : Choice<_, _>) =
                match res with
                | Choice1Of2 (pos', (stats, outcome)) -> Some pos', Choice1Of2 (stats, outcome)
                | Choice2Of2 (stats, exn) -> None, Choice2Of2 (stats, exn)
            override _.RecordResultStats msg = stats.Handle msg
            override _.DumpStats pendingCount = stats.DumpStats(dop.State, pendingCount)
            override _.TryDumpState(dispatcherState, streams, (dt, ft, mt, it, st)) =
                stats.TryDumpState(dispatcherState, dumpStreams streams, (dt, ft, mt, it, st))

    [<NoComparison; NoEquality>]
    type StreamsBatch<'Format> private (onCompletion, buffer, reqs) =
        let mutable buffer = Some buffer
        static member Create(onCompletion, items : StreamEvent<'Format> seq) =
            let buffer, reqs = Streams<'Format>(), Dictionary<FsCodec.StreamName, int64>()
            let mutable itemCount = 0
            for item in items do
                itemCount <- itemCount + 1
                buffer.Merge(item)
                match reqs.TryGetValue(item.stream), item.event.Index + 1L with
                | (false, _), required -> reqs[item.stream] <- required
                | (true, actual), required when actual < required -> reqs[item.stream] <- required
                | (true, _), _ -> () // replayed same or earlier item
            let batch = StreamsBatch(onCompletion, buffer, reqs)
            batch, (batch.RemainingStreamsCount, itemCount)

        member _.OnCompletion = onCompletion
        member _.Reqs = reqs :> seq<KeyValuePair<FsCodec.StreamName, int64>>
        member _.RemainingStreamsCount = reqs.Count
        member _.TryTakeStreams() = let t = buffer in buffer <- None; t
        member _.TryMerge(other : StreamsBatch<_>) =
            match buffer, other.TryTakeStreams() with
            | Some x, Some y -> x.Merge(y); true
            | Some _, None -> false
            | None, x -> buffer <- x; false

    /// Consolidates ingested events into streams; coordinates dispatching of these to projector/ingester in the order implied by the submission order
    /// a) does not itself perform any reading activities
    /// b) triggers synchronous callbacks as batches complete; writing of progress is managed asynchronously by the TrancheEngine(s)
    /// c) submits work to the supplied Dispatcher (which it triggers pumping of)
    /// d) periodically reports state (with hooks for ingestion engines to report same)
    type StreamSchedulingEngine<'P, 'R, 'E>
        (   dispatcher : IDispatcher<'P, 'R, 'E>,
            // Tune number of batches to ingest at a time. Default 5.
            ?maxBatches,
            // Tune the max number of check/dispatch cycles. Default 16.
            ?maxCycles,
            // Tune the sleep time when there are no items to schedule or responses to process. Default 1ms.
            ?idleDelay,
            // Frequency of jettisoning Write Position state of inactive streams (held by the scheduler for deduplication purposes) to limit memory consumption
            // NOTE: Purging can impair performance, increase write costs or result in duplicate event emissions due to redundant inputs not being deduplicated
            ?purgeInterval,
            // Opt-in to allowing items to be processed independent of batch sequencing - requires upstream/projection function to be able to identify gaps. Default false.
            ?enableSlipstreaming) =
        let idleDelay = defaultArg idleDelay (TimeSpan.FromMilliseconds 1.)
        let sleepIntervalMs = int idleDelay.TotalMilliseconds
        let maxCycles, maxBatches, slipstreamingEnabled = defaultArg maxCycles 16, defaultArg maxBatches 5, defaultArg enableSlipstreaming false
        let work = ConcurrentStack<InternalMessage<Choice<'P, 'E>>>() // dont need them ordered so Queue is unwarranted; usage is cross-thread so Bag is not better
        let pending = ConcurrentQueue<StreamsBatch<byte[]>>() // Queue as need ordering
        let streams = StreamStates<byte[]>()
        let progressState = Progress.ProgressState()
        let purgeDue = purgeInterval |> Option.map intervalCheck

        let weight stream =
            match streams.TryGetItem stream with
            | Some state when not state.IsEmpty ->
                let firstSpan = Array.head state.Queue
                let mutable acc = 0
                for x in firstSpan.events do acc <- acc + eventSize x
                int64 acc
            | _ -> 0L

        // ingest information to be gleaned from processing the results into `streams`
        let workLocalBuffer = Array.zeroCreate 1024
        let tryDrainResults feedStats =
            let mutable worked, more = false, true
            while more do
                let c = work.TryPopRange(workLocalBuffer)
                if c = 0 then more <- false
                else worked <- true
                for i in 0..c-1 do
                    let x = workLocalBuffer[i]
                    let res' : InternalMessage<Choice<'R, 'E>> =
                        match x with
                        | Added (streams, skipped, events) ->
                            // Only processed in Stats (and actually never enters this queue)
                            Added (streams, skipped, events)
                        | Result (duration, stream : FsCodec.StreamName, progressed : bool, res : Choice<'P, 'E>) ->
                            match dispatcher.InterpretProgress(streams, stream, res) with
                            | None, Choice1Of2 (r : 'R) ->
                                streams.MarkFailed(stream)
                                Result (duration, stream, progressed, Choice1Of2 r)
                            | Some index, Choice1Of2 (r : 'R) ->
                                progressState.MarkStreamProgress(stream, index)
                                streams.MarkCompleted(stream, index)
                                Result (duration, stream, progressed, Choice1Of2 r)
                            | _, Choice2Of2 exn ->
                                streams.MarkFailed(stream)
                                Result (duration, stream, progressed, Choice2Of2 exn)
                    feedStats res'
            worked

        // Take an incoming batch of events, correlating it against our known stream state to yield a set of remaining work
        let ingestPendingBatch feedStats (markCompleted, items : seq<KeyValuePair<FsCodec.StreamName, int64>>) =
            let reqs = Dictionary()
            let mutable count, skipCount = 0, 0
            for item in items do
                if streams.WritePositionIsAlreadyBeyond(item.Key, item.Value) then
                    skipCount <- skipCount + 1
                else
                    count <- count + 1
                    reqs[item.Key] <- item.Value
            progressState.AppendBatch(markCompleted, reqs)
            feedStats <| Added (reqs.Count, skipCount, count)

        member _.Pump _abend = async {
            use _ = dispatcher.Result.Subscribe(Result >> work.Push)
            let! ct = Async.CancellationToken
            while not ct.IsCancellationRequested do
                let mutable idle, dispatcherState, remaining = true, Idle, maxCycles
                let mutable dt, ft, mt, it, st = TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero
                while remaining <> 0 do
                    remaining <- remaining - 1
                    // 1. propagate write write outcomes to buffer (can mark batches completed etc)
                    let processedResults = (fun () -> tryDrainResults dispatcher.RecordResultStats) |> accStopwatch <| fun x -> dt <- dt + x
                    // 2. top up provisioning of writers queue
                    // On each iteration, we try to fill the in-flight queue, taking the oldest and/or heaviest streams first
                    let isSlipStreaming = dispatcherState = Slipstreaming
                    let potential () : seq<DispatchItem<_>> = streams.Pending(isSlipStreaming, progressState.InScheduledOrder weight)
                    let hasCapacity, dispatched = (fun () -> dispatcher.TryReplenish potential streams.MarkBusy) |> accStopwatch <| fun x -> ft <- ft + x
                    idle <- idle && not processedResults && not dispatched
                    match dispatcherState with
                    | Idle when not hasCapacity ->
                        // If we've achieved full state, spin around the loop to dump stats and ingest reader data
                        dispatcherState <- Full
                        remaining <- 0
                    | Idle when remaining = 0 ->
                        dispatcherState <- Busy
                    | Idle -> // need to bring more work into the pool as we can't fill the work queue from what we have
                        // If we're going to fill the write queue with random work, we should bring all read events into the state first
                        // If we're going to bring in lots of batches, that's more efficient when the stream-wise merges are carried out first
                        let mutable more, batchesTaken = true, 0
                        while more do
                            match pending.TryDequeue() with
                            | true, batch ->
                                match batch.TryTakeStreams() with None -> () | Some s -> (fun () -> streams.InternalMerge(s)) |> accStopwatch <| fun t -> mt <- mt + t
                                (fun () -> ingestPendingBatch dispatcher.RecordResultStats (batch.OnCompletion, batch.Reqs)) |> accStopwatch <| fun t -> it <- it + t
                                batchesTaken <- batchesTaken + 1
                                more <- batchesTaken < maxBatches
                            | false, _ when batchesTaken <> 0  ->
                                more <- false
                            | false, _ when (*batchesTaken = 0 &&*) slipstreamingEnabled ->
                                dispatcherState <- Slipstreaming
                                more <- false
                            | false, _  ->
                                remaining <- 0
                                more <- false
                    | Slipstreaming -> // only do one round of slipstreaming
                        remaining <- 0
                    | Busy | Full -> failwith "Not handled here"
                    // This loop can take a long time; attempt logging of stats per iteration
                    (fun () -> dispatcher.DumpStats pending.Count) |> accStopwatch <| fun t -> st <- st + t
                // 3. Record completion state once per full iteration; dumping streams is expensive so needs to be done infrequently
                if dispatcher.TryDumpState(dispatcherState, streams, (dt, ft, mt, it, st)) then
                    // After we've dumped the state, it may also be due for pruning
                    match purgeDue with Some dueNow when dueNow () -> streams.Purge() | _ -> ()
                elif idle then
                    // 4. Do a minimal sleep so we don't run completely hot when empty (unless we did something non-trivial)
                    Thread.Sleep sleepIntervalMs } // Not Async.Sleep so we don't give up the thread

        member _.Submit(x : StreamsBatch<_>) =
            pending.Enqueue(x)

    type StreamSchedulingEngine =

        static member Create<'Metrics, 'Progress, 'Outcome>
            (   itemDispatcher : ItemDispatcher<Choice<int64 * 'Metrics * 'Outcome, 'Metrics * exn>>,
                stats : Stats<'Metrics * 'Outcome, 'Metrics * exn>,
                prepare : FsCodec.StreamName * StreamSpan<_> -> 'Metrics * (FsCodec.StreamName * StreamSpan<_>),
                handle : FsCodec.StreamName * StreamSpan<_> -> Async<'Progress * 'Outcome>,
                toIndex : FsCodec.StreamName * StreamSpan<_> -> 'Progress -> int64,
                dumpStreams, ?maxBatches, ?idleDelay, ?purgeInterval, ?enableSlipstreaming)
            : StreamSchedulingEngine<int64 * 'Metrics * 'Outcome, 'Metrics * 'Outcome, 'Metrics * exn> =

            let dispatcher = MultiDispatcher<_, _, _>.Create(itemDispatcher, handle, prepare, toIndex, stats, dumpStreams)
            StreamSchedulingEngine<_, _, _>(dispatcher, ?maxBatches=maxBatches, ?idleDelay=idleDelay, ?purgeInterval=purgeInterval, ?enableSlipstreaming=enableSlipstreaming)

        static member Create(dispatcher, ?maxBatches, ?idleDelay, ?purgeInterval, ?enableSlipstreaming)
            : StreamSchedulingEngine<int64 * ('Metrics * unit), 'Stats * unit, 'Stats * exn> =
            StreamSchedulingEngine<_, _, _>(dispatcher, ?maxBatches=maxBatches, ?idleDelay=idleDelay, ?purgeInterval=purgeInterval, ?enableSlipstreaming=enableSlipstreaming)

[<AbstractClass>]
type Stats<'Outcome>(log : ILogger, statsInterval, statesInterval) =
    inherit Scheduling.Stats<EventMetrics * 'Outcome, EventMetrics * exn>(log, statsInterval, statesInterval)
    let okStreams, failStreams, badCats = HashSet(), HashSet(), CatStats()
    let mutable resultOk, resultExnOther, okEvents, okBytes, exnEvents, exnBytes = 0, 0, 0, 0L, 0, 0L

    override _.DumpStats() =
        log.Information("Projected {mb:n0}MB {completed:n0}r {streams:n0}s {events:n0}e ({ok:n0} ok)", mb okBytes, resultOk, okStreams.Count, okEvents, resultOk)
        okStreams.Clear(); resultOk <- 0; okEvents <- 0; okBytes <- 0L
        if resultExnOther <> 0 then
            log.Warning("Exceptions {mb:n0}MB {fails:n0}r {streams:n0}s {events:n0}e", mb exnBytes, resultExnOther, failStreams.Count, exnEvents)
            resultExnOther <- 0; failStreams.Clear(); exnBytes <- 0L; exnEvents <- 0
            log.Warning(" Affected cats {@badCats}", badCats.StatsDescending)
            badCats.Clear()

    override this.Handle message =
        let inline addStream x (set : HashSet<_>) = set.Add x |> ignore
        let inline addBadStream x (set : HashSet<_>) = badCats.Ingest(StreamName.categorize x); addStream x set
        base.Handle message
        match message with
        | Scheduling.Added _ -> () // Processed by standard logging already; we have nothing to add
        | Scheduling.Result (_duration, stream, _progressed, Choice1Of2 ((es, bs), res)) ->
            addStream stream okStreams
            okEvents <- okEvents + es
            okBytes <- okBytes + int64 bs
            resultOk <- resultOk + 1
            this.HandleOk res
        | Scheduling.Result (duration, stream, _progressed, Choice2Of2 ((es, bs), exn)) ->
            addBadStream stream failStreams
            exnEvents <- exnEvents + es
            exnBytes <- exnBytes + int64 bs
            resultExnOther <- resultExnOther + 1
            this.HandleExn(log.ForContext("stream", stream).ForContext("events", es).ForContext("duration", duration), exn)
    abstract member HandleOk : outcome : 'Outcome -> unit
    abstract member HandleExn : log : ILogger * exn : exn -> unit

module Projector =

    type StreamsIngester =

        static member Start(log, partitionId, maxRead, submit, ?statsInterval, ?sleepInterval) =
            let makeBatch onCompletion (items : StreamEvent<_> seq) =
                let items = Array.ofSeq items
                let streams = HashSet(seq { for x in items -> x.stream })
                let batch : Submission.SubmissionBatch<_, _> = { source = partitionId; onCompletion = onCompletion; messages = items }
                batch, (streams.Count, items.Length)
            Ingestion.Ingester<StreamEvent<_> seq, Submission.SubmissionBatch<_, StreamEvent<_>>>.Start(log, partitionId, maxRead, makeBatch, submit, ?statsInterval=statsInterval, ?sleepInterval=sleepInterval)

    type StreamsSubmitter =

        static member Create
            (   log : ILogger, maxSubmissionsPerPartition, mapBatch, submitStreamsBatch, statsInterval,
                ?pumpInterval, ?disableCompaction) =
            let submitBatch (x : Scheduling.StreamsBatch<_>) : int =
                submitStreamsBatch x
                x.RemainingStreamsCount
            let tryCompactQueueImpl (queue : Queue<Scheduling.StreamsBatch<_>>) =
                let mutable acc, worked = None, false
                for x in queue do
                    match acc with
                    | None -> acc <- Some x
                    | Some a -> if a.TryMerge x then worked <- true
                worked
            let tryCompactQueue = if defaultArg disableCompaction false then None else Some tryCompactQueueImpl
            Submission.SubmissionEngine<_, _, _>(log, maxSubmissionsPerPartition, mapBatch, submitBatch, statsInterval, ?tryCompactQueue=tryCompactQueue, ?pumpInterval=pumpInterval)

    type StreamsProjectorPipeline =

        static member Start
            (   log : ILogger, pumpDispatcher, pumpScheduler, maxReadAhead, submitStreamsBatch, statsInterval,
                // Limits number of batches passed to the scheduler.
                // Holding items back makes scheduler processing more efficient as less state needs to be traversed.
                // Holding items back is also key to the submitter's compaction mechanism working best.
                // Defaults to holding back 20% of maxReadAhead per partition
                ?maxSubmissionsPerPartition,
                ?ingesterStatsInterval, ?pumpInterval) =
            let mapBatch onCompletion (x : Submission.SubmissionBatch<_, StreamEvent<_>>) : Scheduling.StreamsBatch<_> =
                let onCompletion () = x.onCompletion(); onCompletion()
                Scheduling.StreamsBatch.Create(onCompletion, x.messages) |> fst
            let maxSubmissionsPerPartition = defaultArg maxSubmissionsPerPartition (maxReadAhead - maxReadAhead/5) // NOTE needs to handle overflow if maxReadAhead is Int32.MaxValue
            let submitter = StreamsSubmitter.Create(log, maxSubmissionsPerPartition, mapBatch, submitStreamsBatch, statsInterval, ?pumpInterval=pumpInterval)
            let startIngester (rangeLog, projectionId) = StreamsIngester.Start(rangeLog, projectionId, maxReadAhead, submitter.Ingest, ?statsInterval=ingesterStatsInterval)
            ProjectorPipeline.Start(log, pumpDispatcher, pumpScheduler, submitter.Pump(), startIngester)

/// Represents progress attained during the processing of the supplied <c>StreamSpan</c> for a given <c>StreamName</c>.
/// This will be reflected in adjustments to the Write Position for the stream in question.
/// Incoming <c>StreamEvents</c> with <c>Index</c>es prior to the Write Position implied by the result are proactively
/// dropped from incoming buffers, yielding increased throughput due to reduction of redundant processing.
type SpanResult =
   /// Indicates no events where processed.
   /// Handler should be supplied the same events (plus any that arrived in the interim) in the next scheduling cycle.
   | NoneProcessed
   /// Indicates all events supplied in the <c>StreamSpan</c> have been processed.
   /// Write Position should move beyond the last event in the supplied StreamSpan.
   | AllProcessed
   /// Indicates only a subset of the presented events have been processed;
   /// Write Position should move <c>count</c> items of the <c>StreamSpan</c> forward.
   | PartiallyProcessed of count : int
   /// Apply an externally observed Version determined by the handler during processing.
   /// If the Version of the stream is running ahead or behind the current input StreamSpan, this enables one to have
   /// events that have already been handled be dropped from the scheduler's buffers and/or as they arrive.
   | OverrideWritePosition of version : int64

module SpanResult =

    let toIndex (_sn, span : StreamSpan<byte[]>) = function
        | NoneProcessed -> span.index
        | AllProcessed -> span.index + span.events.LongLength
        | PartiallyProcessed count -> span.index + int64 count
        | OverrideWritePosition index -> index

type StreamsProjector =

    /// Custom projection mechanism that divides work into a <code>prepare</code> phase that selects the prefix of the queued StreamSpan to handle,
    /// and a <code>handle</code> function that yields a Write Position representing the next event that's to be handled on this Stream
    static member StartEx<'Progress, 'Outcome>
        (   log : ILogger, maxReadAhead, maxConcurrentStreams,
            prepare, handle, toIndex,
            stats, statsInterval,
            ?maxSubmissionsPerPartition, ?pumpInterval,
            // Tune the sleep time when there are no items to schedule or responses to process. Default 1ms.
            ?idleDelay,
            // Frequency of jettisoning Write Position state of inactive streams (held by the scheduler for deduplication purposes) to limit memory consumption
            // NOTE: Purging can impair performance, increase write costs or result in duplicate event emissions due to redundant inputs not being deduplicated
            ?purgeInterval)
        : ProjectorPipeline<_> =
        let dispatcher = Scheduling.ItemDispatcher<_>(maxConcurrentStreams)
        let streamScheduler =
            Scheduling.StreamSchedulingEngine.Create<_, 'Progress, 'Outcome>
                (   dispatcher, stats,
                    prepare, handle, toIndex,
                    (fun s l -> s.Dump(l, Buffering.StreamState.eventsSize)),
                    ?idleDelay=idleDelay, ?purgeInterval=purgeInterval)
        Projector.StreamsProjectorPipeline.Start(
                log, dispatcher.Pump(), streamScheduler.Pump, maxReadAhead, streamScheduler.Submit, statsInterval,
                ?maxSubmissionsPerPartition=maxSubmissionsPerPartition, ?pumpInterval=pumpInterval)

    /// Project StreamSpans using a <code>handle</code> function that yields a Write Position representing the next event that's to be handled on this Stream
    static member Start<'Outcome>
        (   log : ILogger, maxReadAhead, maxConcurrentStreams,
            handle : FsCodec.StreamName * StreamSpan<_> -> Async<SpanResult * 'Outcome>,
            stats, statsInterval,
            // Limits number of batches passed to the scheduler.
            // Holding items back makes scheduler processing more efficient as less state needs to be traversed.
            // Holding items back is also key to the compaction mechanism working best.
            // Defaults to holding back 20% of maxReadAhead per partition
            ?maxSubmissionsPerPartition,
            ?pumpInterval,
            // Tune the sleep time when there are no items to schedule or responses to process. Default 1ms.
            ?idleDelay,
            // Frequency of jettisoning Write Position state of inactive streams (held by the scheduler for deduplication purposes) to limit memory consumption
            // NOTE: Purging can impair performance, increase write costs or result in duplicate event emissions due to redundant inputs not being deduplicated
            ?purgeInterval)
        : ProjectorPipeline<_> =
        let prepare (streamName, span) =
            let stats = Buffering.StreamSpan.stats span
            stats, (streamName, span)
        StreamsProjector.StartEx<SpanResult, 'Outcome>(
            log, maxReadAhead, maxConcurrentStreams, prepare, handle, SpanResult.toIndex, stats, statsInterval,
            ?maxSubmissionsPerPartition=maxSubmissionsPerPartition, ?pumpInterval=pumpInterval, ?idleDelay=idleDelay, ?purgeInterval=purgeInterval)

    /// Project StreamSpans using a <code>handle</code> function that guarantees to always handles all events in the <code>span</code>
    static member Start<'Outcome>
        (   log : ILogger, maxReadAhead, maxConcurrentStreams,
            handle : FsCodec.StreamName * StreamSpan<_> -> Async<'Outcome>,
            stats, statsInterval,
            // Limits number of batches passed to the scheduler.
            // Holding items back makes scheduler processing more efficient as less state needs to be traversed.
            // Holding items back is also key to the compaction mechanism working best.
            // Defaults to holding back 20% of maxReadAhead per partition
            ?maxSubmissionsPerPartition, ?pumpInterval,
            // Tune the sleep time when there are no items to schedule or responses to process. Default 1ms.
            ?idleDelay,
            // Frequency of jettisoning Write Position state of inactive streams (held by the scheduler for deduplication purposes) to limit memory consumption
            // NOTE: Purging can impair performance, increase write costs or result in duplicate event emissions due to redundant inputs not being deduplicated
            ?purgeInterval)
        : ProjectorPipeline<_> =
        let handle (streamName, span : StreamSpan<_>) = async {
            let! res = handle (streamName, span)
            return SpanResult.AllProcessed, res }
        StreamsProjector.Start<'Outcome>(
            log, maxReadAhead, maxConcurrentStreams, handle, stats, statsInterval, ?maxSubmissionsPerPartition=maxSubmissionsPerPartition,
            ?pumpInterval=pumpInterval, ?idleDelay=idleDelay, ?purgeInterval=purgeInterval)

module Sync =

    [<AbstractClass>]
    type Stats<'Outcome>(log : ILogger, statsInterval, stateInterval) =
        inherit Scheduling.Stats<(EventMetrics * TimeSpan) * 'Outcome, EventMetrics * exn>(log, statsInterval, stateInterval)
        let okStreams, failStreams = HashSet(), HashSet()
        let prepareStats = LatencyStats("prepare")
        let mutable okEvents, okBytes, exnEvents, exnBytes = 0, 0L, 0, 0L

        override _.DumpStats() =
            if okStreams.Count <> 0 && failStreams.Count <> 0 then
                log.Information("Completed {okMb:n0}MB {okStreams:n0}s {okEvents:n0}e Exceptions {exnMb:n0}MB {exnStreams:n0}s {exnEvents:n0}e",
                    mb okBytes, okStreams.Count, okEvents, mb exnBytes, failStreams.Count, exnEvents)
            okStreams.Clear(); okEvents <- 0; okBytes <- 0L
            prepareStats.Dump log

        override this.Handle message =
            let inline adds x (set : HashSet<_>) = set.Add x |> ignore
            base.Handle message
            match message with
            | Scheduling.InternalMessage.Added _ -> () // Processed by standard logging already; we have nothing to add
            | Scheduling.InternalMessage.Result (_duration, stream, _progressed, Choice1Of2 (((es, bs), prepareElapsed), outcome)) ->
                adds stream okStreams
                okEvents <- okEvents + es
                okBytes <- okBytes + int64 bs
                prepareStats.Record prepareElapsed
                this.HandleOk outcome
            | Scheduling.InternalMessage.Result (_duration, stream, _progressed, Choice2Of2 ((es, bs), exn)) ->
                adds stream failStreams
                exnEvents <- exnEvents + es
                exnBytes <- exnBytes + int64 bs
                this.HandleExn(log.ForContext("stream", stream).ForContext("events", es), exn)
        abstract member HandleOk : outcome : 'Outcome -> unit
        abstract member HandleExn : log : ILogger * exn : exn -> unit

    type StreamsSync =

        static member Start
            (   log : ILogger, maxReadAhead, maxConcurrentStreams,
                handle : FsCodec.StreamName * StreamSpan<_> -> Async<SpanResult * 'Outcome>,
                stats : Stats<'Outcome>, statsInterval,
                // Default 1 ms
                ?idleDelay,
                // Default 1 MiB
                ?maxBytes,
                // Default 16384
                ?maxEvents,
                // Max scheduling readahead. Default 128.
                ?maxBatches, ?pumpInterval,
                // Max inner cycles per loop. Default 128.
                ?maxCycles,
                // Hook to wire in external stats
                ?dumpExternalStats,
                // Frequency of jettisoning Write Position state of inactive streams (held by the scheduler for deduplication purposes) to limit memory consumption
                // NOTE: Purging can impair performance, increase write costs or result in duplicate event emissions due to redundant inputs not being deduplicated
                ?purgeInterval)
            : ProjectorPipeline<_> =

            let maxBatches, maxEvents, maxBytes = defaultArg maxBatches 128, defaultArg maxEvents 16384, (defaultArg maxBytes (1024 * 1024 - (*fudge*)4096))

            let attemptWrite (stream, span) = async {
                let met, span' = Buffering.StreamSpan.slice (maxEvents, maxBytes) span
                let sw = System.Diagnostics.Stopwatch.StartNew()
                try let req = (stream, span')
                    let! res, outcome = handle req
                    let prepareElapsed = sw.Elapsed
                    let index' = SpanResult.toIndex req res
                    return index' > span.index, Choice1Of2 (index', (met, prepareElapsed), outcome)
                with e -> return false, Choice2Of2 (met, e) }

            let interpretWriteResultProgress _streams (stream : FsCodec.StreamName) = function
                | Choice1Of2 (i', stats, outcome) ->
                    Some i', Choice1Of2 (stats, outcome)
                | Choice2Of2 (eventCount, bytesCount as stats, exn : exn) ->
                    log.Warning(exn, "Handling {events:n0}e {bytes:n0}b for {stream} failed, retrying", eventCount, bytesCount, stream)
                    None, Choice2Of2 (stats, exn)

            let itemDispatcher = Scheduling.ItemDispatcher<_>(maxConcurrentStreams)
            let dumpStreams (s : Scheduling.StreamStates<_>) l =
                s.Dump(l, Buffering.StreamState.eventsSize)
                match dumpExternalStats with Some f -> f l | None -> ()

            let dispatcher = Scheduling.MultiDispatcher<_, _, _>.Create(itemDispatcher, attemptWrite, interpretWriteResultProgress, stats, dumpStreams)
            let streamScheduler =
                Scheduling.StreamSchedulingEngine<int64 * (EventMetrics * TimeSpan) * 'Outcome, (EventMetrics * TimeSpan) * 'Outcome, EventMetrics * exn>
                    (   dispatcher, maxBatches=maxBatches, maxCycles=defaultArg maxCycles 128, ?idleDelay=idleDelay, ?purgeInterval=purgeInterval)

            Projector.StreamsProjectorPipeline.Start(
                log, itemDispatcher.Pump(), streamScheduler.Pump, maxReadAhead, streamScheduler.Submit, statsInterval, maxSubmissionsPerPartition=maxBatches, ?pumpInterval=pumpInterval)
