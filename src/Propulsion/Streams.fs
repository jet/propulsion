namespace Propulsion.Streams

open MathNet.Numerics.Statistics
open Propulsion
open Serilog
open System
open System.Collections.Concurrent
open System.Collections.Generic
open System.Diagnostics
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
        | AttemptLatencies of kind : string * latenciesS : float[]

    /// Attach a property to the captured event record to hold the metric information
    // Sidestep Log.ForContext converting to a string; see https://github.com/serilog/serilog/issues/1124
    let [<Literal>] PropertyTag = "propulsionEvent"
    let internal metric (value : Metric) (log : ILogger) =
        let enrich (e : Serilog.Events.LogEvent) =
            e.AddPropertyIfAbsent(Serilog.Events.LogEventProperty(PropertyTag, Serilog.Events.ScalarValue(value)))
        log.ForContext({ new Serilog.Core.ILogEventEnricher with member __.Enrich(evt,_) = enrich evt })
    let internal (|SerilogScalar|_|) : Serilog.Events.LogEventPropertyValue -> obj option = function
        | (:? Serilog.Events.ScalarValue as x) -> Some x.Value
        | _ -> None
    let (|MetricEvent|_|) (logEvent : Serilog.Events.LogEvent) : Metric option =
        match logEvent.Properties.TryGetValue PropertyTag with
        | true, SerilogScalar (:? Metric as e) -> Some e
        | _ -> None

/// A Single Event from an Ordered stream
[<NoComparison; NoEquality>]
type StreamEvent<'Format> = { stream : FsCodec.StreamName; event : FsCodec.ITimelineEvent<'Format> }

/// Span of events from an Ordered Stream
[<NoComparison; NoEquality>]
type StreamSpan<'Format> = { index : int64; events : FsCodec.ITimelineEvent<'Format>[] }

module Internal =

    /// Gathers stats relating to how many items of a given category have been observed
    type CatStats() =
        let cats = Dictionary<string, int64>()

        member __.Ingest(cat, ?weight) =
            let weight = defaultArg weight 1L
            match cats.TryGetValue cat with
            | true, catCount -> cats.[cat] <- catCount + weight
            | false, _ -> cats.[cat] <- weight

        member __.Count = cats.Count
        member __.Any = (not << Seq.isEmpty) cats
        member __.Clear() = cats.Clear()
        member __.StatsDescending = cats |> Seq.sortBy (fun x -> -x.Value) |> Seq.map (|KeyValue|)

    type private Data =
        {   min    : TimeSpan
            p50    : TimeSpan
            p95    : TimeSpan
            p99    : TimeSpan
            max    : TimeSpan
            avg    : TimeSpan
            stddev : TimeSpan option }

    let private dumpStats (kind : string) (xs : TimeSpan seq) (log : ILogger) =
        let sortedLatencies = xs |> Seq.map (fun r -> r.TotalSeconds) |> Seq.sort |> Seq.toArray

        let pc p = SortedArrayStatistics.Percentile(sortedLatencies, p) |> TimeSpan.FromSeconds
        let l = {
            avg = ArrayStatistics.Mean sortedLatencies |> TimeSpan.FromSeconds
            stddev =
                let stdDev = ArrayStatistics.StandardDeviation sortedLatencies
                // stdev of singletons is NaN
                if Double.IsNaN stdDev then None else TimeSpan.FromSeconds stdDev |> Some

            min = SortedArrayStatistics.Minimum sortedLatencies |> TimeSpan.FromSeconds
            max = SortedArrayStatistics.Maximum sortedLatencies |> TimeSpan.FromSeconds
            p50 = pc 50
            p95 = pc 95
            p99 = pc 99 }
        let inline sec (t : TimeSpan) = t.TotalSeconds
        let stdDev = match l.stddev with None -> Double.NaN | Some d -> sec d
        let m = Log.Metric.AttemptLatencies (kind, sortedLatencies)
        (log |> Log.metric m).Information(" {kind} {count} : max={max:n3}s p99={p99:n3}s p95={p95:n3}s p50={p50:n3}s min={min:n3}s avg={avg:n3}s stddev={stddev:n3}s",
            kind, sortedLatencies.Length, sec l.max, sec l.p99, sec l.p95, sec l.p50, sec l.min, sec l.avg, stdDev)

    /// Operations on an instance are safe cross-thread
    type ConcurrentLatencyStats(kind) =
        let buffer = ConcurrentStack<TimeSpan>()
        member __.Record value = buffer.Push value
        member __.Dump(log : ILogger) =
            if not buffer.IsEmpty then
                dumpStats kind buffer log
                buffer.Clear() // yes, there is a race

    /// Should only be used on one thread
    type LatencyStats(kind) =
        let buffer = ResizeArray<TimeSpan>()
        member __.Record value = buffer.Add value
        member __.Dump(log : ILogger) =
            if buffer.Count <> 0 then
                dumpStats kind buffer log
                buffer.Clear()

open FsCodec
open Internal

[<AutoOpen>]
module private Impl =

    let (|NNA|) xs = if obj.ReferenceEquals(null, xs) then Array.empty else xs
    let inline arrayBytes (x : _ []) = if obj.ReferenceEquals(null, x) then 0 else x.Length
    let inline stringBytes (x : string) = match x with null -> 0 | x -> x.Length * sizeof<char>
    let inline eventSize (x : FsCodec.IEventData<byte[]>) = arrayBytes x.Data + arrayBytes x.Meta + stringBytes x.EventType + 16
    let inline mb x = float x / 1024. / 1024.
    let inline accStopwatch (f : unit -> 't) at =
        let sw = Stopwatch.StartNew()
        let r = f ()
        at sw.Elapsed
        r

module StreamName =

    /// Despite conventions, there's no guarantee that an arbitrary Kafka `key`, EventStore `StreamId` etc.
    /// will necessarily adhere to the `{category}-{aggregateId}` form
    /// This helper ensures there's always a dash (even if that means having a defaultStringId)
    let parseWithDefaultCategory defaultCategory (rawStreamName : string) : StreamName =
        if rawStreamName.IndexOf '-' = -1 then String.Concat(defaultCategory, "-", rawStreamName)
        else rawStreamName
        |> FSharp.UMX.UMX.tag

    /// Guard against inputs that don't adhere to "{category}-{aggregateId}" by prefixing with `-`  })
    let internalParseSafe rawStreamName : StreamName = parseWithDefaultCategory "" rawStreamName

    /// Because we coerce all stream names to be well-formed, we can split it too
    /// where there's no category, we use the aggregateId instead
    let categorize : StreamName -> string = function
        | FsCodec.StreamName.CategoryAndId ("", aggregateId) -> aggregateId
        | FsCodec.StreamName.CategoryAndId (category, _) -> category

#nowarn "52" // see tmp.Sort

module Progress =

    type [<NoComparison; NoEquality>] internal BatchState = { markCompleted : unit -> unit; streamToRequiredIndex : Dictionary<StreamName, int64> }

    type ProgressState<'Pos>() =
        let pending = Queue<_>()
        let trim () =
            while pending.Count <> 0 && pending.Peek().streamToRequiredIndex.Count = 0 do
                let batch = pending.Dequeue()
                batch.markCompleted()

        member __.AppendBatch(markCompleted, reqs : Dictionary<StreamName, int64>) =
            pending.Enqueue { markCompleted = markCompleted; streamToRequiredIndex = reqs }
            trim ()

        member __.MarkStreamProgress(stream, index) =
            for x in pending do
                match x.streamToRequiredIndex.TryGetValue stream with
                | true, requiredIndex when requiredIndex <= index -> x.streamToRequiredIndex.Remove stream |> ignore
                | _, _ -> ()
            trim ()

        member __.InScheduledOrder getStreamWeight =
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
#if NET461
            | x -> { index = min; events = x.events |> Seq.skip (min - x.index |> int) |> Seq.toArray }
#else
            | x -> { index = min; events = x.events |> Array.skip (min - x.index |> int) }  // slice
#endif

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
        member __.IsEmpty = obj.ReferenceEquals(null, __.queue)
        member __.IsPurgeable = __.IsEmpty && not __.IsMalformed
        member __.IsMalformed = not __.IsEmpty && -3L = __.write
        member __.HasValid = not __.IsEmpty && not __.IsMalformed
        member __.Write = match __.write with -2L -> None | x -> Some x
        member __.Queue : StreamSpan<'Format>[] = __.queue
        member __.IsReady =
            if not __.HasValid then false else

            match __.Write, Array.head __.queue with
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
            let items = let (NNA q1, NNA q2) = s1.queue, s2.queue in Seq.append q1 q2
            StreamState<'Format>.Create(writePos, StreamSpan.merge (defaultArg writePos 0L) items, s1.IsMalformed || s2.IsMalformed)

    type Streams<'Format>() =
        let states = Dictionary<FsCodec.StreamName, StreamState<'Format>>()
        let merge stream (state : StreamState<_>) =
            match states.TryGetValue stream with
            | false, _ ->
                states.Add(stream, state)
            | true, current ->
                let updated = StreamState.combine current state
                states.[stream] <- updated

        member __.Merge(item : StreamEvent<'Format>) =
            merge item.stream (StreamState<'Format>.Create(None, [| { index = item.event.Index; events = [| item.event |] } |]))

        member private __.States = states

        member __.Items = states :> seq<KeyValuePair<FsCodec.StreamName, StreamState<'Format>>>

        member __.Merge(other : Streams<'Format>) =
            for x in other.States do
                merge x.Key x.Value

        member __.Dump(log : ILogger, estimateSize, categorize) =
            let mutable waiting, waitingE, waitingB = 0, 0, 0L
            let waitingCats, waitingStreams = CatStats(), CatStats()
            for KeyValue (stream, state) in states do
                let sz = estimateSize state
                waitingCats.Ingest(categorize stream)
                if sz <> 0L then waitingStreams.Ingest(sprintf "%s@%dx%d" (StreamName.toString stream) (defaultArg state.Write 0L) state.queue.[0].events.Length, (sz + 512L) / 1024L)
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

    type [<NoComparison; NoEquality>] DispatchItem<'Format> = { stream : StreamName; writePos : int64 option; span : StreamSpan<'Format> }

    type StreamStates<'Format>() =
        let states = Dictionary<StreamName, StreamState<'Format>>()

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
                states.[stream] <- updated
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

        let busy = HashSet<StreamName>()
        let pending trySlipstreamed (requestedOrder : StreamName seq) : seq<DispatchItem<'Format>> = seq {
            let proposed = HashSet()
            for s in requestedOrder do
                match tryGetItem s with
                | Some state when state.HasValid && not (busy.Contains s) ->
                    proposed.Add s |> ignore
                    yield { writePos = state.Write; stream = s; span = Array.head state.Queue }
                | _ -> ()
            if trySlipstreamed then
                // [lazily] Slipstream in further events that are not yet referenced by in-scope batches
                for KeyValue(s, v) in states do
                    if v.HasValid && not (busy.Contains s) && proposed.Add s then
                        yield { writePos = v.Write; stream = s; span = Array.head v.Queue } }

        let markBusy stream = busy.Add stream |> ignore
        let markNotBusy stream = busy.Remove stream |> ignore

        member __.InternalMerge buffer = merge buffer
        member __.Purge() = purge ()
        member __.InternalUpdate stream pos queue = update stream (StreamState<'Format>.Create(Some pos,queue))

        member __.Add(stream, index, event, ?isMalformed) =
            updateWritePos stream (defaultArg isMalformed false) None [| { index = index; events = [| event |] } |]

        member __.Add(stream, span: StreamSpan<_>, isMalformed) =
            updateWritePos stream isMalformed None [| span |]

        member __.SetMalformed(stream, isMalformed) =
            updateWritePos stream isMalformed None [| { index = 0L; events = null } |]

        member __.TryGetItem(stream) = tryGetItem stream

        member __.WritePositionIsAlreadyBeyond(stream, required) =
            match tryGetItem stream with
            | Some streamState -> streamState.Write |> Option.exists (fun cw -> cw >= required)
            | _ -> false

        member __.WritePositionIsAlreadyBeyond(stream, required) =
            match tryGetItem stream with
            | Some streamState -> streamState.Write |> Option.exists (fun cw -> cw >= required)
            | _ -> false

        member __.MarkBusy stream =
            markBusy stream

        member __.MarkCompleted(stream, index) =
            markNotBusy stream
            markCompleted stream index

        member __.MarkFailed stream =
            markNotBusy stream

        member __.Pending(trySlipstreamed, byQueuedPriority : StreamName seq) : DispatchItem<'Format> seq =
            pending trySlipstreamed byQueuedPriority

        member __.Dump(log : ILogger, estimateSize) =
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
                    malformedStreams.Ingest(StreamName.toString stream, mb sz |> int64)
                    malformed <- malformed + 1
                    malformedB <- malformedB + sz
                    malformedE <- malformedE + (state.Queue |> Array.sumBy (fun x -> x.events.Length))
                | sz when not state.IsReady ->
                    unprefixedCats.Ingest(StreamName.categorize stream)
                    unprefixedStreams.Ingest(StreamName.toString stream, mb sz |> int64)
                    unprefixed <- unprefixed + 1
                    unprefixedB <- unprefixedB + sz
                    unprefixedE <- unprefixedE + (state.Queue |> Array.sumBy (fun x -> x.events.Length))
                | sz ->
                    readyCats.Ingest(StreamName.categorize stream)
                    readyStreams.Ingest(sprintf "%s@%dx%d" (StreamName.toString stream) (defaultArg state.Write 0L) state.Queue.[0].events.Length, kb sz)
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
        | Result of duration : TimeSpan * (StreamName * 'R)

    type BufferState = Idle | Busy | Full | Slipstreaming

    /// Gathers stats pertaining to the core projection/ingestion activity
    [<AbstractClass>]
    type Stats<'R, 'E>(log : ILogger, statsInterval : TimeSpan, stateInterval : TimeSpan) =
        let cycles, fullCycles, states, oks, exns = ref 0, ref 0, CatStats(), LatencyStats("ok"), LatencyStats("exceptions")
        let batchesPended, streamsPended, eventsSkipped, eventsPended = ref 0, ref 0, ref 0, ref 0
        let statsDue, stateDue = intervalCheck statsInterval, intervalCheck stateInterval
        let mutable dt, ft, it, st, mt = TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero

        let dumpStats (dispatchActive, dispatchMax) batchesWaiting =
            log.Information("Scheduler {cycles} cycles ({fullCycles} full) {@states} Running {busy}/{processors}",
                !cycles, !fullCycles, states.StatsDescending, dispatchActive, dispatchMax)
            cycles := 0; fullCycles := 0; states.Clear()
            oks.Dump log; exns.Dump log
            log.Information(" Batches Holding {batchesWaiting} Started {batches} ({streams:n0}s {events:n0}-{skipped:n0}e)",
                batchesWaiting, !batchesPended, !streamsPended, !eventsSkipped + !eventsPended, !eventsSkipped)
            batchesPended := 0; streamsPended := 0; eventsSkipped := 0; eventsPended := 0
            let m = Log.Metric.SchedulerCpu (mt, it, ft, dt, st)
            (log |> Log.metric m).Information(" Cpu Streams {mt:n1}s Batches {it:n1}s Dispatch {ft:n1}s Results {dt:n1}s Stats {st:n1}s",
                mt.TotalSeconds, it.TotalSeconds, ft.TotalSeconds, dt.TotalSeconds, st.TotalSeconds)
            dt <- TimeSpan.Zero; ft <- TimeSpan.Zero; it <- TimeSpan.Zero; st <- TimeSpan.Zero; mt <- TimeSpan.Zero

        abstract member Handle : InternalMessage<Choice<'R, 'E>> -> unit
        default __.Handle msg = msg |> function
            | Added (streams, skipped, events) ->
                incr batchesPended
                streamsPended := !streamsPended + streams
                eventsPended := !eventsPended + events
                eventsSkipped := !eventsSkipped + skipped
            | Result (duration, (_stream, Choice1Of2 _)) ->
                oks.Record duration
            | Result (duration, (_stream, Choice2Of2 _)) ->
                exns.Record duration

        member __.DumpStats((used, max), batchesWaiting) =
            incr cycles
            if statsDue () then
                dumpStats (used, max) batchesWaiting
                __.DumpStats()

        member __.TryDumpState(state, dump, (_dt, _ft, _mt, _it, _st)) =
            dt <- dt + _dt
            ft <- ft + _ft
            mt <- mt + _mt
            it <- it + _it
            st <- st + _st
            incr fullCycles
            states.Ingest(string state)

            let due = stateDue ()
            if due then
                dump log
            due

        /// Allows an ingester or projector to wire in custom stats (typically based on data gathered in a `Handle` override)
        abstract DumpStats : unit -> unit
        default __.DumpStats () = ()

    /// Coordinates the dispatching of work and emission of results, subject to the maxDop concurrent processors constraint
    type private DopDispatcher<'R>(maxDop) =
        // Using a Queue as a) the ordering is more correct, favoring more important work b) we are adding from many threads so no value in ConcurrentBag's thread-affinity
        let work = new BlockingCollection<_>(ConcurrentQueue<_>())
        let result = Event<TimeSpan * 'R>()
        let dop = Sem maxDop
        let dispatch work = async {
            let sw = Stopwatch.StartNew()
            let! res = work
            result.Trigger(sw.Elapsed, res)
            dop.Release() }

        [<CLIEvent>] member __.Result = result.Publish
        member __.HasCapacity = dop.HasCapacity
        member __.State = dop.State

        member __.TryAdd(item) =
            if dop.TryTake() then
                work.Add(item)
                true
            else false

        member __.Pump () = async {
            let! ct = Async.CancellationToken
            for item in work.GetConsumingEnumerable ct do
                Async.Start(dispatch item) }

    /// Kicks off enough work to fill the inner Dispatcher up to capacity
    type ItemDispatcher<'R>(maxDop) =
        let inner = DopDispatcher<StreamName * 'R>(maxDop)

        // On each iteration, we try to fill the in-flight queue, taking the oldest and/or heaviest streams first
        let tryFillDispatcher pending project markBusy =
            let mutable hasCapacity, dispatched = inner.HasCapacity, false
            if hasCapacity then
                let potential : seq<DispatchItem<byte[]>> = pending ()
                let xs = potential.GetEnumerator()
                while xs.MoveNext() && hasCapacity do
                    let item = xs.Current
                    let succeeded = inner.TryAdd(async { let! r = project item in return item.stream, r })
                    if succeeded then
                        markBusy item.stream
                    hasCapacity <- succeeded
                    dispatched <- dispatched || succeeded // if we added any request, we'll skip sleeping
            hasCapacity, dispatched

        member __.Pump() = inner.Pump()
        [<CLIEvent>] member __.Result = inner.Result
        member __.State = inner.State
        member __.TryReplenish pending project markStreamBusy =
            tryFillDispatcher pending project markStreamBusy

    /// Defines interface between Scheduler (which owns the pending work) and the Dispatcher which periodically selects work to commence based on a policy
    type IDispatcher<'P, 'R, 'E> =
        abstract member TryReplenish : pending : (unit -> seq<DispatchItem<byte[]>>) -> markStreamBusy : (StreamName -> unit) -> bool * bool
        [<CLIEvent>] abstract member Result : IEvent<TimeSpan * (StreamName * Choice<'P, 'E>)>
        abstract member InterpretProgress : StreamStates<byte[]> * StreamName * Choice<'P, 'E> -> int64 option * Choice<'R, 'E>
        abstract member RecordResultStats : InternalMessage<Choice<'R, 'E>> -> unit
        abstract member DumpStats : int -> unit
        abstract member TryDumpState : BufferState * StreamStates<byte[]> * (TimeSpan * TimeSpan * TimeSpan * TimeSpan * TimeSpan) -> bool

    /// Implementation of IDispatcher which feeds items to an item dispatcher that maximizes concurrent requests (within a limit)
    type MultiDispatcher<'P, 'R, 'E>
        (   inner : ItemDispatcher<Choice<'P, 'E>>,
            project : DispatchItem<byte[]> -> Async<Choice<'P, 'E>>,
            interpretProgress : StreamStates<byte[]> -> StreamName -> Choice<'P, 'E> -> int64 option * Choice<'R, 'E>,
            stats : Stats<'R, 'E>,
            dumpStreams) =
        interface IDispatcher<'P, 'R, 'E> with
            override __.TryReplenish pending markStreamBusy = inner.TryReplenish pending project markStreamBusy
            [<CLIEvent>] override __.Result = inner.Result
            override __.InterpretProgress(streams : StreamStates<_>, stream : StreamName, res : Choice<'P, 'E>) =
                interpretProgress streams stream res
            override __.RecordResultStats msg = stats.Handle msg
            override __.DumpStats pendingCount = stats.DumpStats(inner.State, pendingCount)
            override __.TryDumpState(dispatcherState, streams, (dt, ft, mt, it, st)) =
                stats.TryDumpState(dispatcherState, dumpStreams streams, (dt, ft, mt, it, st))

    /// Implementation of IDispatcher that allows a supplied handler select work and declare completion based on arbitrarily defined criteria
    type BatchedDispatcher
        (   select : DispatchItem<byte[]> seq -> DispatchItem<byte[]>[],
            handle : DispatchItem<byte[]>[] -> Async<(StreamName * Choice<int64 * (EventMetrics * unit), EventMetrics * exn>)[]>,
            stats : Stats<_, _>,
            dumpStreams) =
        let dop = DopDispatcher 1
        let result = Event<TimeSpan * (StreamName * Choice<int64 * (EventMetrics * unit), EventMetrics * exn>)>()

        let dispatchSubResults (res : TimeSpan, itemResults : (StreamName * Choice<int64 * (EventMetrics * unit), EventMetrics * exn>)[]) =
            let tot = res.TotalMilliseconds
            let avg = TimeSpan.FromMilliseconds(tot / float itemResults.Length)
            for res in itemResults do result.Trigger(avg, res)

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

        member __.Pump() = async {
            use _ = dop.Result.Subscribe dispatchSubResults
            return! dop.Pump() }

        interface IDispatcher<int64 * (EventMetrics * unit), EventMetrics * unit, EventMetrics * exn> with
            override __.TryReplenish pending markStreamBusy = trySelect pending markStreamBusy
            [<CLIEvent>] override __.Result = result.Publish
            override __.InterpretProgress(_streams : StreamStates<_>, _stream : StreamName, res : Choice<_, _>) =
                match res with
                | Choice1Of2 (pos', (stats, outcome)) -> Some pos', Choice1Of2 (stats, outcome)
                | Choice2Of2 (stats, exn) -> None, Choice2Of2 (stats, exn)
            override __.RecordResultStats msg = stats.Handle msg
            override __.DumpStats pendingCount = stats.DumpStats(dop.State, pendingCount)
            override __.TryDumpState(dispatcherState, streams, (dt, ft, mt, it, st)) =
                stats.TryDumpState(dispatcherState, dumpStreams streams, (dt, ft, mt, it, st))

    [<NoComparison; NoEquality>]
    type StreamsBatch<'Format> private (onCompletion, buffer, reqs) =
        let mutable buffer = Some buffer
        static member Create(onCompletion, items : StreamEvent<'Format> seq) =
            let buffer, reqs = Buffering.Streams<'Format>(), Dictionary<StreamName, int64>()
            let mutable itemCount = 0
            for item in items do
                itemCount <- itemCount + 1
                buffer.Merge(item)
                match reqs.TryGetValue(item.stream), item.event.Index + 1L with
                | (false, _), required -> reqs.[item.stream] <- required
                | (true, actual), required when actual < required -> reqs.[item.stream] <- required
                | (true, _), _ -> () // replayed same or earlier item
            let batch = StreamsBatch(onCompletion, buffer, reqs)
            batch, (batch.RemainingStreamsCount, itemCount)

        member __.OnCompletion = onCompletion
        member __.Reqs = reqs :> seq<KeyValuePair<StreamName, int64>>
        member __.RemainingStreamsCount = reqs.Count
        member __.TryTakeStreams() = let t = buffer in buffer <- None; t
        member __.TryMerge(other : StreamsBatch<_>) =
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
            /// Tune number of batches to ingest at a time. Default 5.
            ?maxBatches,
            /// Tune the max number of check/dispatch cycles. Default 16.
            ?maxCycles,
            /// Tune the sleep time when there are no items to schedule or responses to process. Default 1ms.
            ?idleDelay,
            /// Frequency of jettisoning Write Position state of inactive streams (held by the scheduler for deduplication purposes) to limit memory consumption
            /// NOTE: Purging can impair performance, increase write costs or result in duplicate event emissions due to redundant inputs not being deduplicated
            ?purgeInterval,
            /// Opt-in to allowing items to be processed independent of batch sequencing - requires upstream/projection function to be able to identify gaps. Default false.
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
                    let x = workLocalBuffer.[i]
                    let res' : InternalMessage<Choice<'R, 'E>> =
                        match x with
                        | Added (streams, skipped, events) ->
                            // Only processed in Stats (and actually never enters this queue)
                            Added (streams, skipped, events)
                        | Result (duration, (stream : StreamName, (res : Choice<'P, 'E>))) ->
                            match dispatcher.InterpretProgress(streams, stream, res) with
                            | None, Choice1Of2 (r : 'R) ->
                                streams.MarkFailed(stream)
                                Result (duration, (stream, Choice1Of2 r))
                            | Some index, Choice1Of2 (r : 'R) ->
                                progressState.MarkStreamProgress(stream, index)
                                streams.MarkCompleted(stream, index)
                                Result (duration, (stream, Choice1Of2 r))
                            | _, Choice2Of2 exn ->
                                streams.MarkFailed(stream)
                                Result (duration, (stream, Choice2Of2 exn))
                    feedStats res'
            worked

        // Take an incoming batch of events, correlating it against our known stream state to yield a set of remaining work
        let ingestPendingBatch feedStats (markCompleted, items : seq<KeyValuePair<StreamName, int64>>) =
            let reqs = Dictionary()
            let mutable count, skipCount = 0, 0
            for item in items do
                if streams.WritePositionIsAlreadyBeyond(item.Key, item.Value) then
                    skipCount <- skipCount + 1
                else
                    count <- count + 1
                    reqs.[item.Key] <- item.Value
            progressState.AppendBatch(markCompleted, reqs)
            feedStats <| Added (reqs.Count, skipCount, count)

        member __.Pump _abend = async {
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
                        // If we're going to bring in lots of batches, that's more efficient when the streamwise merges are carried out first
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

        member __.Submit(x : StreamsBatch<_>) =
            pending.Enqueue(x)

    type StreamSchedulingEngine =

        static member Create<'Metrics, 'Req, 'Progress, 'Outcome>
            (   itemDispatcher : ItemDispatcher<Choice<int64 * 'Metrics * 'Outcome, 'Metrics * exn>>,
                stats : Stats<'Metrics * 'Outcome, 'Metrics * exn>,
                prepare : StreamName*StreamSpan<byte[]> -> 'Metrics * 'Req, handle : 'Req -> Async<'Progress * 'Outcome>, toIndex : 'Req -> 'Progress -> int64,
                dumpStreams, ?maxBatches, ?idleDelay, ?purgeInterval, ?enableSlipstreaming)
            : StreamSchedulingEngine<int64 * 'Metrics * 'Outcome, 'Metrics * 'Outcome, 'Metrics * exn> =

            let project (item : DispatchItem<byte[]>) : Async<Choice<int64 * 'Metrics * 'Outcome, 'Metrics * exn>> = async {
                let stats, req = prepare (item.stream, item.span)
                try let! progress, outcome = handle req
                    return Choice1Of2 (toIndex req progress, stats, outcome)
                with e -> return Choice2Of2 (stats, e) }

            let interpretProgress (_streams : StreamStates<_>) _stream : Choice<int64 * 'Metrics * 'Outcome, 'Metrics * exn> -> int64 option * Choice<'Metrics * 'Outcome, 'Metrics * exn> = function
                | Choice1Of2 (index, stats, outcome) -> Some index, Choice1Of2 (stats, outcome)
                | Choice2Of2 (stats, exn) -> None, Choice2Of2 (stats, exn)

            let dispatcher = MultiDispatcher<int64 * 'Metrics * 'Outcome, 'Metrics * 'Outcome, 'Metrics * exn>(itemDispatcher, project, interpretProgress, stats, dumpStreams)
            StreamSchedulingEngine<_, _, _>(dispatcher, ?maxBatches=maxBatches, ?idleDelay=idleDelay, ?purgeInterval=purgeInterval, ?enableSlipstreaming=enableSlipstreaming)

        static member Create(dispatcher, ?maxBatches, ?idleDelay, ?purgeInterval, ?enableSlipstreaming)
            : StreamSchedulingEngine<int64 * ('Metrics * unit), 'Stats * unit, 'Stats * exn> =
            StreamSchedulingEngine<_, _, _>(dispatcher, ?maxBatches=maxBatches, ?idleDelay=idleDelay, ?purgeInterval=purgeInterval, ?enableSlipstreaming=enableSlipstreaming)

module Projector =

    [<AbstractClass>]
    type Stats<'Outcome>(log : ILogger, statsInterval, statesInterval) =
        inherit Scheduling.Stats<EventMetrics * 'Outcome, EventMetrics * exn>(log, statsInterval, statesInterval)
        let okStreams, failStreams, badCats, resultOk, resultExnOther = HashSet(), HashSet(), CatStats(), ref 0, ref 0
        let mutable okEvents, okBytes, exnEvents, exnBytes = 0, 0L, 0, 0L

        override __.DumpStats() =
            log.Information("Projected {mb:n0}MB {completed:n0}r {streams:n0}s {events:n0}e ({ok:n0} ok)",
                mb okBytes, !resultOk, okStreams.Count, okEvents, !resultOk)
            okStreams.Clear(); resultOk := 0; okEvents <- 0; okBytes <- 0L
            if !resultExnOther <> 0 then
                log.Warning("Exceptions {mb:n0}MB {fails:n0}r {streams:n0}s {events:n0}e",
                    mb exnBytes, !resultExnOther, failStreams.Count, exnEvents)
                resultExnOther := 0; failStreams.Clear(); exnBytes <- 0L; exnEvents <- 0
                log.Warning("Affected cats {@badCats}", badCats.StatsDescending)
                badCats.Clear()

        override __.Handle message =
            let inline adds x (set : HashSet<_>) = set.Add x |> ignore
            let inline bads x (set : HashSet<_>) = badCats.Ingest(StreamName.categorize x); adds x set
            base.Handle message
            match message with
            | Scheduling.Added _ -> () // Processed by standard logging already; we have nothing to add
            | Scheduling.Result (_duration, (stream, Choice1Of2 ((es, bs), res))) ->
                adds stream okStreams
                okEvents <- okEvents + es
                okBytes <- okBytes + int64 bs
                incr resultOk
                __.HandleOk res
            | Scheduling.Result (_duration, (stream, Choice2Of2 ((es, bs), exn))) ->
                bads stream failStreams
                exnEvents <- exnEvents + es
                exnBytes <- exnBytes + int64 bs
                incr resultExnOther
                __.HandleExn(log.ForContext("stream", stream).ForContext("events", es), exn)
        abstract member HandleOk : outcome : 'Outcome -> unit
        abstract member HandleExn : log : ILogger * exn : exn -> unit

    type StreamsIngester =
        static member Start(log, partitionId, maxRead, submit, ?statsInterval, ?sleepInterval) =
            let makeBatch onCompletion (items : StreamEvent<_> seq) =
                let items = Array.ofSeq items
                let streams = HashSet(seq { for x in items -> x.stream })
                let batch : Submission.SubmissionBatch<_, _> = { source = partitionId; onCompletion = onCompletion; messages = items }
                batch, (streams.Count, items.Length)
            Ingestion.Ingester<StreamEvent<_> seq, Submission.SubmissionBatch<_, StreamEvent<_>>>.Start(log, maxRead, makeBatch, submit, ?statsInterval=statsInterval, ?sleepInterval=sleepInterval)

    type StreamsSubmitter =
        static member Create
            (   log : Serilog.ILogger, mapBatch, submitStreamsBatch, statsInterval,
                ?maxSubmissionsPerPartition, ?pumpInterval, ?disableCompaction) =
            let maxSubmissionsPerPartition = defaultArg maxSubmissionsPerPartition 5
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
            (   log : Serilog.ILogger, pumpDispatcher, pumpScheduler, maxReadAhead, submitStreamsBatch, statsInterval,
                ?ingesterStatsInterval, ?maxSubmissionsPerPartition, ?pumpInterval) =
            let mapBatch onCompletion (x : Submission.SubmissionBatch<_, StreamEvent<_>>) : Scheduling.StreamsBatch<_> =
                let onCompletion () = x.onCompletion(); onCompletion()
                Scheduling.StreamsBatch.Create(onCompletion, x.messages) |> fst
            let submitter = StreamsSubmitter.Create(log, mapBatch, submitStreamsBatch, statsInterval, ?maxSubmissionsPerPartition=maxSubmissionsPerPartition, ?pumpInterval=pumpInterval)
            let startIngester (rangeLog, projectionId) = StreamsIngester.Start(rangeLog, projectionId, maxReadAhead, submitter.Ingest, ?statsInterval=ingesterStatsInterval)
            ProjectorPipeline.Start(log, pumpDispatcher, pumpScheduler, submitter.Pump(), startIngester)

/// Represents progress attained during the processing of the supplied <c>StreamSpan</c> for a given <c>StreamName</c>.
/// This will be reflected in adjustments to the Write Position for the stream.
/// Incoming <c>StreamEvents</c> with <c>Index</c>es prior to the active Write Position are proactively dropped from incoming buffers.
type SpanResult =
   /// Indicates all events supplied in the <c>StreamSpan</c> have been processed; Write Position should move one beyond that of the last event.
   | AllProcessed
   /// Indicates only a subset of the presented events have been processed; Write Position should move one beyond the <c>Index</c> of Item <c>count</c> of the <c>StreamSpan</c>.
   | PartiallyProcessed of count : int
   /// Apply an externally derived Write Position determined by the handler during processing (e.g., if downstream is running ahead of current inputs)
   | OverrideWritePosition of index : int64

module SpanResult =

    let toIndex (_sn, span : StreamSpan<byte[]>) = function
        | AllProcessed -> span.index + span.events.LongLength
        | PartiallyProcessed count -> span.index + int64 count
        | OverrideWritePosition index -> index

type StreamsProjector =

    /// Custom projection mechanism that divides work into a <code>prepare</code> phase that selects the prefix of the queued StreamSpan to handle,
    /// and a <code>handle</code> function that yields a Write Position representing the next event that's to be handled on this Stream
    static member StartEx<'Progress, 'Outcome>
        (   log : ILogger, maxReadAhead, maxConcurrentStreams,
            prepare, handle, toIndex,
            stats, statsInterval, ?pumpInterval,
            /// Tune the sleep time when there are no items to schedule or responses to process. Default 1ms.
            ?idleDelay,
            /// Frequency of jettisoning Write Position state of inactive streams (held by the scheduler for deduplication purposes) to limit memory consumption
            /// NOTE: Purging can impair performance, increase write costs or result in duplicate event emissions due to redundant inputs not being deduplicated
            ?purgeInterval)
        : ProjectorPipeline<_> =
        let dispatcher = Scheduling.ItemDispatcher<_>(maxConcurrentStreams)
        let streamScheduler =
            Scheduling.StreamSchedulingEngine.Create<_, StreamName * StreamSpan<byte[]>, 'Progress, 'Outcome>
                (   dispatcher, stats,
                    prepare, handle, toIndex,
                    (fun s l -> s.Dump(l, Buffering.StreamState.eventsSize)),
                    ?idleDelay=idleDelay, ?purgeInterval=purgeInterval)
        Projector.StreamsProjectorPipeline.Start(log, dispatcher.Pump(), streamScheduler.Pump, maxReadAhead, streamScheduler.Submit, statsInterval, ?pumpInterval=pumpInterval)

    /// Project StreamSpans using a <code>handle</code> function that yields a Write Position representing the next event that's to be handled on this Stream
    static member Start<'Outcome>
        (   log : ILogger, maxReadAhead, maxConcurrentStreams,
            handle : StreamName * StreamSpan<_> -> Async<SpanResult * 'Outcome>,
            stats, statsInterval, ?pumpInterval,
            /// Tune the sleep time when there are no items to schedule or responses to process. Default 1ms.
            ?idleDelay,
            /// Frequency of jettisoning Write Position state of inactive streams (held by the scheduler for deduplication purposes) to limit memory consumption
            /// NOTE: Purging can impair performance, increase write costs or result in duplicate event emissions due to redundant inputs not being deduplicated
            ?purgeInterval)
        : ProjectorPipeline<_> =
        let prepare (streamName, span) =
            let stats = Buffering.StreamSpan.stats span
            stats, (streamName, span)
        StreamsProjector.StartEx<SpanResult, 'Outcome>
            (   log, maxReadAhead, maxConcurrentStreams, prepare, handle, SpanResult.toIndex, stats, statsInterval,
                ?pumpInterval=pumpInterval, ?idleDelay=idleDelay, ?purgeInterval=purgeInterval)

    /// Project StreamSpans using a <code>handle</code> function that guarantees to always handles all events in the <code>span</code>
    static member Start<'Outcome>
        (   log : ILogger, maxReadAhead, maxConcurrentStreams,
            handle : StreamName * StreamSpan<_> -> Async<'Outcome>,
            stats, statsInterval, ?pumpInterval,
            /// Tune the sleep time when there are no items to schedule or responses to process. Default 1ms.
            ?idleDelay,
            /// Frequency of jettisoning Write Position state of inactive streams (held by the scheduler for deduplication purposes) to limit memory consumption
            /// NOTE: Purging can impair performance, increase write costs or result in duplicate event emissions due to redundant inputs not being deduplicated
            ?purgeInterval)
        : ProjectorPipeline<_> =
        let handle (streamName, span : StreamSpan<_>) = async {
            let! res = handle (streamName, span)
            return SpanResult.AllProcessed, res }
        StreamsProjector.Start<'Outcome>
            (   log, maxReadAhead, maxConcurrentStreams, handle, stats, statsInterval,
                ?pumpInterval=pumpInterval, ?idleDelay=idleDelay, ?purgeInterval=purgeInterval)

module Sync =

    [<AbstractClass>]
    type Stats<'Outcome>(log : ILogger, statsInterval, stateInterval) =
        inherit Scheduling.Stats<(EventMetrics * TimeSpan) * 'Outcome, EventMetrics * exn>(log, statsInterval, stateInterval)
        let okStreams, failStreams = HashSet(), HashSet()
        let prepareStats = Internal.LatencyStats("prepare")
        let mutable okEvents, okBytes, exnEvents, exnBytes = 0, 0L, 0, 0L

        override __.DumpStats() =
            if okStreams.Count <> 0 && failStreams.Count <> 0 then
                log.Information("Completed {okMb:n0}MB {okStreams:n0}s {okEvents:n0}e Exceptions {exnMb:n0}MB {exnStreams:n0}s {exnEvents:n0}e",
                    mb okBytes, okStreams.Count, okEvents, mb exnBytes, failStreams.Count, exnEvents)
            okStreams.Clear(); okEvents <- 0; okBytes <- 0L
            prepareStats.Dump log

        override __.Handle message =
            let inline adds x (set : HashSet<_>) = set.Add x |> ignore
            base.Handle message
            match message with
            | Scheduling.InternalMessage.Added _ -> () // Processed by standard logging already; we have nothing to add
            | Scheduling.InternalMessage.Result (_duration, (stream, Choice1Of2 (((es, bs), prepareElapsed), outcome))) ->
                adds stream okStreams
                okEvents <- okEvents + es
                okBytes <- okBytes + int64 bs
                prepareStats.Record prepareElapsed
                __.HandleOk outcome
            | Scheduling.InternalMessage.Result (_duration, (stream, Choice2Of2 ((es, bs), exn))) ->
                adds stream failStreams
                exnEvents <- exnEvents + es
                exnBytes <- exnBytes + int64 bs
                __.HandleExn(log.ForContext("stream", stream).ForContext("events", es), exn)
        abstract member HandleOk : outcome : 'Outcome -> unit
        abstract member HandleExn : log : ILogger * exn : exn -> unit

    type StreamsSync =

        static member Start
            (   log : ILogger, maxReadAhead, maxConcurrentStreams,
                handle : StreamName * StreamSpan<_> -> Async<SpanResult * 'Outcome>,
                stats : Stats<'Outcome>, statsInterval,
                /// Default 1 ms
                ?idleDelay,
                /// Default 1 MiB
                ?maxBytes,
                /// Default 16384
                ?maxEvents,
                /// Max scheduling readahead. Default 128.
                ?maxBatches, ?pumpInterval,
                /// Max inner cycles per loop. Default 128.
                ?maxCycles,
                /// Hook to wire in external stats
                ?dumpExternalStats,
                /// Frequency of jettisoning Write Position state of inactive streams (held by the scheduler for deduplication purposes) to limit memory consumption
                /// NOTE: Purging can impair performance, increase write costs or result in duplicate event emissions due to redundant inputs not being deduplicated
                ?purgeInterval)
            : ProjectorPipeline<_> =

            let maxBatches, maxEvents, maxBytes = defaultArg maxBatches 128, defaultArg maxEvents 16384, (defaultArg maxBytes (1024 * 1024 - (*fudge*)4096))

            let attemptWrite (item : Scheduling.DispatchItem<_>) = async {
                let (eventCount, bytesCount), span = Buffering.StreamSpan.slice (maxEvents, maxBytes) item.span
                let sw = System.Diagnostics.Stopwatch.StartNew()
                try let req = (item.stream, span)
                    let! (res, outcome) = handle req
                    let prepareElapsed = sw.Elapsed
                    return Choice1Of2 (SpanResult.toIndex req res, ((eventCount, bytesCount), prepareElapsed), outcome)
                with e -> return Choice2Of2 ((eventCount, bytesCount), e) }

            let interpretWriteResultProgress _streams (stream : StreamName) = function
                | Choice1Of2 (i', stats, outcome) ->
                    Some i', Choice1Of2 (stats, outcome)
                | Choice2Of2 (((eventCount, bytesCount) as stats), exn : exn) ->
                    log.Warning(exn, "Handling {events:n0}e {bytes:n0}b for {stream} failed, retrying", eventCount, bytesCount, stream)
                    None, Choice2Of2 (stats, exn)

            let itemDispatcher = Scheduling.ItemDispatcher<_>(maxConcurrentStreams)
            let dumpStreams (s : Scheduling.StreamStates<_>) l =
                s.Dump(l, Buffering.StreamState.eventsSize)
                match dumpExternalStats with Some f -> f l | None -> ()

            let dispatcher = Scheduling.MultiDispatcher<_, _, _>(itemDispatcher, attemptWrite, interpretWriteResultProgress, stats, dumpStreams)
            let streamScheduler =
                Scheduling.StreamSchedulingEngine<int64 * (EventMetrics * TimeSpan) * 'Outcome, (EventMetrics * TimeSpan) * 'Outcome, EventMetrics * exn>
                    (   dispatcher, maxBatches=maxBatches, maxCycles=defaultArg maxCycles 128, ?idleDelay=idleDelay, ?purgeInterval=purgeInterval)

            Projector.StreamsProjectorPipeline.Start(
                log, itemDispatcher.Pump(), streamScheduler.Pump, maxReadAhead, streamScheduler.Submit, statsInterval, maxSubmissionsPerPartition=maxBatches, ?pumpInterval=pumpInterval)
