namespace Propulsion.Streams

open Propulsion
open Propulsion.Internal
open Serilog
open System
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks

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

    let [<Literal>] PropertyTag = "propulsionEvent"
    let [<Literal>] GroupTag = "group"
    /// Attach a property to the captured event record to hold the metric information
    // Sidestep Log.ForContext converting to a string; see https://github.com/serilog/serilog/issues/1124
    let internal withMetric (value : Metric) = Internal.Log.withScalarProperty PropertyTag value
    let tryGetScalar<'t> key (logEvent : Serilog.Events.LogEvent) : 't voption =
        let mutable p = Unchecked.defaultof<_>
        logEvent.Properties.TryGetValue(key, &p) |> ignore
        match p with Log.ScalarValue (:? 't as e) -> ValueSome e | _ -> ValueNone
    let [<return: Struct>] (|MetricEvent|_|) logEvent =
        match tryGetScalar<Metric> PropertyTag logEvent with
        | ValueSome m -> ValueSome (m, tryGetScalar<string> GroupTag logEvent)
        | ValueNone -> ValueNone

module StreamName =

    /// Despite conventions, there's no guarantee that an arbitrary Kafka `key`, EventStore `StreamId` etc.
    /// will necessarily adhere to the `{category}-{streamId}` form
    /// This helper ensures there's always a dash (even if that means having a defaultStringId)
    let parseWithDefaultCategory defaultCategory (rawStreamName : string) : FsCodec.StreamName =
        if rawStreamName.IndexOf '-' = -1 then String.Concat(defaultCategory, "-", rawStreamName)
        else rawStreamName
        |> FSharp.UMX.UMX.tag

    /// Guard against inputs that don't adhere to "{category}-{streamId}" by prefixing with `-`  })
    let internalParseSafe rawStreamName : FsCodec.StreamName = parseWithDefaultCategory "" rawStreamName

    /// Because we coerce all stream names to be well-formed, we can split it too
    /// where there's no category, we use the full streamId instead
    let categorize : FsCodec.StreamName -> string = function
        | FsCodec.StreamName.Category "" as sid -> (FsCodec.StreamName.toString sid).Substring(1)
        | FsCodec.StreamName.Category cat -> cat

/// A contiguous set of Events from a Ordered stream, as held internally within this module
type StreamSpan<'Format> = FsCodec.ITimelineEvent<'Format> array

module StreamSpan =

    type Metrics = (struct (int * int))
    let metrics eventSize (xs : FsCodec.ITimelineEvent<'F> array) : Metrics =
        struct (xs.Length, xs |> Seq.sumBy eventSize)
    let slice<'F> eventSize (maxEvents, maxBytes) (span : FsCodec.ITimelineEvent<'F> array) : struct (Metrics * FsCodec.ITimelineEvent<'F> array) =
        let mutable count, bytes = 0, 0
        let mutable countBudget, bytesBudget = maxEvents, maxBytes
        let withinLimits y =
            countBudget <- countBudget - 1
            let eventBytes = eventSize y
            bytesBudget <- bytesBudget - eventBytes
            // always send at least one event in order to surface the problem and have the stream marked malformed
            let res = count = 0 || (countBudget >= 0 && bytesBudget >= 0)
            if res then count <- count + 1; bytes <- bytes + eventBytes
            res
        let trimmed = span |> Array.takeWhile withinLimits
        metrics eventSize trimmed, trimmed

    let inline idx (span : StreamSpan<'F>) = span[0].Index
    let inline ver (span : StreamSpan<'F>) = idx span + span.LongLength
    let dropBeforeIndex min : FsCodec.ITimelineEvent<_> array -> FsCodec.ITimelineEvent<_> array = function
        | xs when xs.Length = 0 -> null
        | xs when idx xs >= min -> xs // don't adjust if min not within
        | v when ver v <= min -> null // throw away if before min
        | xs -> xs |> Array.skip (min - idx xs |> int) // slice

    let merge min (spans : FsCodec.ITimelineEvent<_> array array) =
        let candidates = [|
            for span in spans do
                if span <> null then
                    match dropBeforeIndex min span with
                    | null -> ()
                    | trimmed when trimmed.Length = 0 -> invalidOp "Cant add empty"
                    | trimmed -> trimmed |]
        if candidates.Length = 0 then null
        elif candidates.Length = 1 then candidates
        else
            candidates |> Array.sortInPlaceBy idx

            // no data buffered -> buffer first item
            let mutable curr = candidates[0]
            let mutable buffer = null
            for i in 1 .. candidates.Length - 1 do
                let x = candidates[i]
                let index = idx x
                let currNext = ver curr
                if index > currNext then // Gap
                    if buffer = null then buffer <- ResizeArray(candidates.Length)
                    buffer.Add curr
                    curr <- x
                // Overlapping, join
                elif index + x.LongLength > currNext then
                    curr <- Array.append curr (dropBeforeIndex currNext x)
            if buffer = null then Array.singleton curr
            else buffer.Add curr; buffer.ToArray()

/// A Single Event from an Ordered stream being supplied for ingestion into the internal data structures
type StreamEvent<'Format> = (struct (FsCodec.StreamName * FsCodec.ITimelineEvent<'Format>))

module Buffer =

    /// NOTE: Optimized Representation as this is the dominant data structure in terms of memory usage - takes it from 24b to a cache-friendlier 16b
    let [<Literal>] WritePosUnknown = -2L // sentinel value for write position signifying `None` (no write position yet established)
    let [<Literal>] WritePosMalformed = -3L // sentinel value for malformed data
    [<NoComparison; NoEquality; Struct>]
    type StreamState<'Format> = private { write : int64; queue : FsCodec.ITimelineEvent<'Format> array array } with
        static member Create(write, queue, malformed) =
            if malformed then { write = WritePosMalformed; queue = queue }
            else StreamState<'Format>.Create(write, queue)
        static member Create(write, queue) = { write = (match write with ValueSome x -> x | ValueNone -> WritePosUnknown); queue = queue }
        member x.IsEmpty = obj.ReferenceEquals(null, x.queue)
        member x.EventsSumBy(f) = if x.IsEmpty then 0L else x.queue |> Seq.map (Seq.sumBy f) |> Seq.sum |> int64
        member x.EventsCount = if x.IsEmpty then 0 else x.queue |> Seq.sumBy Array.length

        member x.HeadSpan = x.queue[0]
        member x.IsMalformed = not x.IsEmpty && WritePosMalformed = x.write
        member x.HasGap = match x.write with WritePosUnknown -> false | w -> w <> x.HeadSpan[0].Index
        member x.IsReady = not x.IsEmpty && not x.IsMalformed

        member x.WritePos = match x.write with WritePosUnknown | WritePosMalformed -> ValueNone | w -> ValueSome w
        member x.CanPurge = x.IsEmpty

    module StreamState =

        let combine (s1 : StreamState<_>) (s2 : StreamState<_>) : StreamState<'Format> =
            let writePos = max s1.WritePos s2.WritePos
            let malformed = s1.IsMalformed || s2.IsMalformed
            let any1 = not (isNull s1.queue)
            let any2 = not (isNull s2.queue)
            if any1 || any2 then
                let items = if any1 && any2 then Array.append s1.queue s2.queue elif any1 then s1.queue else s2.queue
                StreamState<'Format>.Create(writePos, StreamSpan.merge (defaultValueArg writePos 0L) items, malformed)
            else StreamState<'Format>.Create(writePos, null, malformed)

    type Streams<'Format>() =
        let states = Dictionary<FsCodec.StreamName, StreamState<'Format>>()
        let merge stream (state : StreamState<_>) =
            let mutable current = Unchecked.defaultof<_>
            if states.TryGetValue(stream, &current) then states[stream] <- StreamState.combine current state
            else states.Add(stream, state)

        member _.Merge(stream, event : FsCodec.ITimelineEvent<'Format>) =
            merge stream (StreamState<'Format>.Create(ValueNone, [| [| event |] |]))

        member _.States = states :> seq<KeyValuePair<FsCodec.StreamName, StreamState<'Format>>>
        member _.Merge(other : Streams<'Format>) = for x in other.States do merge x.Key x.Value

        member _.Dump(log : ILogger, estimateSize, categorize) =
            let mutable waiting, waitingE, waitingB = 0, 0, 0L
            let waitingCats, waitingStreams = Stats.CatStats(), Stats.CatStats()
            for KeyValue (stream, state) in states do
                if not state.IsEmpty then
                    let sz = estimateSize state
                    waitingCats.Ingest(categorize stream)
                    if sz <> 0L then
                        let sn, wp = FsCodec.StreamName.toString stream, defaultValueArg state.WritePos 0L
                        waitingStreams.Ingest(sprintf "%s@%dx%d" sn wp state.queue[0].Length, (sz + 512L) / 1024L)
                    waiting <- waiting + 1
                    waitingE <- waitingE + (state.queue |> Array.sumBy (fun x -> x.Length))
                    waitingB <- waitingB + sz
            let m = Log.Metric.BufferReport { cats = waitingCats.Count; streams = waiting; events = waitingE; bytes = waitingB }
            (log |> Log.withMetric m).Information(" Streams Waiting {busy:n0}/{busyMb:n1}MB", waiting, Log.miB waitingB)
            if waitingCats.Any then log.Information(" Waiting Categories, events {@readyCats}", Seq.truncate 5 waitingCats.StatsDescending)
            if waitingCats.Any then log.Information(" Waiting Streams, KB {@readyStreams}", Seq.truncate 5 waitingStreams.StatsDescending)

    [<NoComparison; NoEquality>]
    type Batch private (onCompletion, reqs : Dictionary<FsCodec.StreamName, int64>) =
        static member Create(onCompletion, streamEvents : StreamEvent<'Format> seq) =
            let streams, reqs = Streams<'Format>(), Dictionary<FsCodec.StreamName, int64>()
            for struct (stream, event) in streamEvents do
                streams.Merge(stream, event)
                match reqs.TryGetValue(stream), event.Index + 1L with
                | (false, _), required -> reqs[stream] <- required
                | (true, actual), required when actual < required -> reqs[stream] <- required
                | (true, _), _ -> () // replayed same or earlier item
            struct (streams, Batch(onCompletion, reqs))

        member val OnCompletion = onCompletion
        member val StreamsCount = reqs.Count
        member val Reqs = reqs :> seq<KeyValuePair<FsCodec.StreamName, int64>>

module Scheduling =

    open Buffer

    type StreamStates<'Format>() =
        let states = Dictionary<FsCodec.StreamName, StreamState<'Format>>()

        let tryGetItem stream =
            let mutable x = Unchecked.defaultof<_>
            if states.TryGetValue(stream, &x) then ValueSome x else ValueNone
        let merge stream (state : StreamState<_>) =
            match tryGetItem stream with
            | ValueSome current ->
                let updated = StreamState.combine current state
                states[stream] <- updated
                updated
            | ValueNone ->
                states.Add(stream, state)
                state
        let markCompleted stream index =
            merge stream (StreamState<'Format>.Create(ValueSome index, queue = null, malformed = false)) |> ignore
        let updateWritePos stream isMalformed pos span =
            merge stream (StreamState<'Format>.Create(pos, span, isMalformed))
        let purge () =
            let mutable purged = 0
            for x in states do
                let streamState = x.Value
                if streamState.CanPurge then
                    states.Remove x.Key |> ignore // Safe to do while iterating on netcore >=3.0
                    purged <- purged + 1
            states.Count, purged

        let busy = HashSet<FsCodec.StreamName>()
        let markBusy stream = busy.Add stream |> ignore
        let markNotBusy stream = busy.Remove stream |> ignore

        member _.ChooseDispatchable(s : FsCodec.StreamName, allowGaps) : StreamState<'Format> voption =
            match tryGetItem s with
            | ValueSome ss when ss.IsReady && (allowGaps || not ss.HasGap) && not (busy.Contains s) -> ValueSome ss
            | _ -> ValueNone

        member _.WritePositionIsAlreadyBeyond(stream, required) =
            match tryGetItem stream with
            // Example scenario: if a write reported we reached version 2, and we are ingesting an event that requires 2, then we drop it
            | ValueSome ss -> match ss.WritePos with ValueSome cw -> cw >= required | _ -> false
            | ValueNone -> false // If the entry has been purged, or we've yet to visit a stream, we can't drop them
        member _.Merge(streams : Streams<'Format>) =
            for kv in streams.States do
                merge kv.Key kv.Value |> ignore
        member _.RecordWriteProgress(stream, pos, queue) =
            merge stream (StreamState<'Format>.Create(ValueSome pos, queue))
        member _.SetMalformed(stream, isMalformed) =
            updateWritePos stream isMalformed ValueNone null
        member _.Purge() =
            purge ()

        member _.HeadSpanSizeBy(f : _ -> int) stream =
            match tryGetItem stream with
            | ValueSome state when not state.IsEmpty -> state.HeadSpan |> Array.sumBy f |> int64
            | _ -> 0L

        member _.MarkBusy stream =
            markBusy stream

        member _.RecordProgress(stream, index) =
            markNotBusy stream
            markCompleted stream index

        member _.RecordNoProgress stream =
            markNotBusy stream

        member _.Dump(log : ILogger, totalPurged : int, eventSize) =
            let mutable (busyCount, busyE, busyB), (ready, readyE, readyB), synced = (0, 0, 0L), (0, 0, 0L), 0
            let mutable (gaps, gapsE, gapsB), (malformed, malformedE, malformedB) = (0, 0, 0L), (0, 0, 0L)
            let busyCats, readyCats, readyStreams = Stats.CatStats(), Stats.CatStats(), Stats.CatStats()
            let gapCats, gapStreams, malformedCats, malformedStreams = Stats.CatStats(), Stats.CatStats(), Stats.CatStats(), Stats.CatStats()
            let kb sz = (sz + 512L) / 1024L
            for KeyValue (stream, state) in states do
                if state.IsEmpty then synced <- synced + 1 else

                let sz = state.EventsSumBy(eventSize)
                if busy.Contains stream then
                    busyCats.Ingest(StreamName.categorize stream)
                    busyCount <- busyCount + 1
                    busyB <- busyB + sz
                    busyE <- busyE + state.EventsCount
                else
                    let cat, label = StreamName.categorize stream, sprintf "%s@%dx%d" (FsCodec.StreamName.toString stream) state.HeadSpan[0].Index state.HeadSpan.Length
                    if state.IsMalformed then
                        malformedCats.Ingest(cat)
                        malformedStreams.Ingest(label, Log.miB sz |> int64)
                        malformed <- malformed + 1
                        malformedB <- malformedB + sz
                        malformedE <- malformedE + state.EventsCount
                    elif state.HasGap then
                        gapCats.Ingest(cat)
                        gapStreams.Ingest(label, kb sz)
                        gaps <- gaps + 1
                        gapsB <- gapsB + sz
                        gapsE <- gapsE + state.EventsCount
                    else
                        readyCats.Ingest(cat)
                        readyStreams.Ingest(label, kb sz)
                        ready <- ready + 1
                        readyB <- readyB + sz
                        readyE <- readyE + state.EventsCount
            let busyStats : Log.BufferMetric = { cats = busyCats.Count; streams = busyCount; events = busyE; bytes = busyB }
            let readyStats : Log.BufferMetric = { cats = readyCats.Count; streams = readyStreams.Count; events = readyE; bytes = readyB }
            let bufferingStats : Log.BufferMetric = { cats = gapCats.Count; streams = gapStreams.Count; events = gapsE; bytes = gapsB }
            let malformedStats : Log.BufferMetric = { cats = malformedCats.Count; streams = malformedStreams.Count; events = malformedE; bytes = malformedB }
            let m = Log.Metric.SchedulerStateReport (synced, busyStats, readyStats, bufferingStats, malformedStats)
            (log |> Log.withMetric m).Information("🏞Streams Synced {synced:n0} Purged {purged:n0} Active {busy:n0}/{busyMb:n1}MB Ready {ready:n0}/{readyMb:n1}MB Waiting {waiting}/{waitingMb:n1}MB Malformed {malformed}/{malformedMb:n1}MB",
                                                  synced, totalPurged, busyCount, Log.miB busyB, ready, Log.miB readyB, gaps, Log.miB gapsB, malformed, Log.miB malformedB)
            if busyCats.Any then log.Information(" Active Categories, events {@busyCats}", Seq.truncate 5 busyCats.StatsDescending)
            if readyCats.Any then log.Information(" Ready Categories, events {@readyCats}", Seq.truncate 5 readyCats.StatsDescending)
            if readyCats.Any then log.Information(" Ready Streams, KB {@readyStreams}", Seq.truncate 5 readyStreams.StatsDescending)
            if gapStreams.Any then log.Information(" Waiting Streams, KB {@waitingStreams}", Seq.truncate 5 gapStreams.StatsDescending)
            if malformedStreams.Any then log.Information(" Malformed Streams, MB {@malformedStreams}", malformedStreams.StatsDescending)
            gapStreams.Any

    [<Struct; NoComparison; NoEquality>]
    type InternalResult<'R> = { duration : TimeSpan; stream : FsCodec.StreamName; progressed : bool; result : 'R }

    type [<Struct; NoEquality; NoComparison>] BufferState = Idle | Active | Full

    module Stats =

        /// Manages state used to generate metrics (and summary logs) regarding streams currently being processed by a Handler
        module Busy =

            type private StreamState = { ts : int64; mutable count : int }
            let private walkAges (state : Dictionary<_, _>) =
                if state.Count = 0 then Seq.empty else
                let currentTs = Stopwatch.timestamp ()
                seq { for x in state.Values -> struct (currentTs - x.ts, x.count) }
            let private renderState agesAndCounts =
                let mutable oldest, newest, streams, attempts = Int64.MinValue, Int64.MaxValue, 0, 0
                for struct (diff, count) in agesAndCounts do
                    oldest <- max oldest diff
                    newest <- min newest diff
                    streams <- streams + 1
                    attempts <- attempts + count
                if streams = 0 then oldest <- 0L; newest <- 0L
                struct (streams, attempts), struct (Stopwatch.ticksToTimeSpan oldest, Stopwatch.ticksToTimeSpan newest)
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
                    else let mutable v = Unchecked.defaultof<_>
                         if state.TryGetValue(sn, &v) then v.count <- v.count + 1
                         else state.Add(sn, { ts = startTs; count = 1 })
                member _.State = walkAges state |> renderState
            /// Collates all state and reactions to manage the list of busy streams based on callbacks/notifications from the Dispatcher
            type Monitor() =
                let active, failing, stuck = Active(), Repeating(), Repeating()
                let emit (log : ILogger) state struct (streams, attempts) struct (oldest : TimeSpan, newest : TimeSpan) =
                    log.Information(" {state} {streams} for {newest:n1}-{oldest:n1}s, {attempts} attempts",
                                    state, streams, newest.TotalSeconds, oldest.TotalSeconds, attempts)
                member _.HandleStarted(sn, ts) =
                    active.HandleStarted(sn, ts)
                member _.HandleResult(sn, succeeded, progressed) =
                    let startTs = active.TakeFinished(sn)
                    failing.HandleResult(sn, not succeeded, startTs)
                    stuck.HandleResult(sn, succeeded && not progressed, startTs)
                member _.DumpState(log : ILogger) =
                    let dump state struct (streams, attempts) ages =
                        if streams <> 0 then
                            emit log state (streams, attempts) ages
                    active.State ||> dump "active"
                    failing.State ||> dump "failing"
                    stuck.State ||> dump "stalled"
                member _.EmitMetrics(log : ILogger) =
                    let report state struct (streams, attempts) struct (oldest : TimeSpan, newest : TimeSpan) =
                        let m = Log.Metric.StreamsBusy (state, streams, oldest.TotalSeconds, newest.TotalSeconds)
                        emit (log |> Log.withMetric m) state (streams, attempts) (oldest, newest)
                    active.State ||> report "active"
                    failing.State ||> report "failing"
                    stuck.State ||> report "stalled"

        type [<NoComparison; NoEquality>] Timers() =
            let mutable results, dispatch, merge, ingest, stats, sleep = 0L, 0L, 0L, 0L, 0L, 0L
            let sw = Stopwatch.start()
            member _.RecordResults ts = results <- results + Stopwatch.elapsedTicks ts
            member _.RecordDispatch ts = dispatch <- dispatch + Stopwatch.elapsedTicks ts
            // If we did not dispatch, we attempt ingestion of streams as a standalone task, but need to add to dispatch time to compensate for calcs below
            member x.RecordDispatchNone ts = x.RecordDispatch ts
            member _.RecordMerge ts = merge <- merge + Stopwatch.elapsedTicks ts
            member _.RecordIngest ts = ingest <- ingest + Stopwatch.elapsedTicks ts
            member _.RecordStats ts = stats <- stats + Stopwatch.elapsedTicks ts
            member _.RecordSleep ts = sleep <- sleep + Stopwatch.elapsedTicks ts
            member _.Dump(log : ILogger) =
                let tot = Stopwatch.ticksToTimeSpan (results + dispatch - merge - ingest + stats + sleep) |> max TimeSpan.Zero
                let it, mt = Stopwatch.ticksToTimeSpan ingest, Stopwatch.ticksToTimeSpan merge
                let dt = Stopwatch.ticksToTimeSpan dispatch - it - mt |> max TimeSpan.Zero
                let rt, st, zt = Stopwatch.ticksToTimeSpan results, Stopwatch.ticksToTimeSpan stats, Stopwatch.ticksToTimeSpan sleep
                let log = log |> Log.withMetric (Log.Metric.SchedulerCpu (mt, it, dt, rt, st))
                log.Information(" Cpu Dispatch {dispatch:n1}s streams {streams:n1}s batches {batches:n1}s Results {results:n1}s Stats {stats:n1}s Sleep {sleep:n1}s Total {total:n1}s Interval {int:n1}s",
                                dt.TotalSeconds, mt.TotalSeconds, it.TotalSeconds, rt.TotalSeconds, st.TotalSeconds, zt.TotalSeconds, tot.TotalSeconds, sw.ElapsedSeconds)
                results <- 0L; dispatch <- 0L; merge <- 0L; ingest <- 0L; stats <- 0L; sleep <- 0L
                sw.Restart()

        type StateStats() =
            let mutable idle, active, full = 0, 0, 0
            member _.Clear() = idle <- 0; active <- 0; full <- 0
            member _.Ingest state =
                match state with
                | Idle -> idle <- idle + 1
                | Active -> active <- active + 1
                | Full -> full <- full + 1
            member _.StatsDescending =
                let t = Stats.CatStats()
                if idle > 0   then t.Ingest(nameof Idle, idle)
                if active > 0 then t.Ingest(nameof Active, active)
                if full > 0   then t.Ingest(nameof Full, full)
                t.StatsDescending

    /// Gathers stats pertaining to the core projection/ingestion activity
    [<AbstractClass>]
    type Stats<'R, 'E>(log : ILogger, statsInterval : TimeSpan, stateInterval : TimeSpan) =
        let metricsLog = log.ForContext("isMetric", true)
        let monitor, monitorInterval = Stats.Busy.Monitor(), IntervalTimer(TimeSpan.FromSeconds 1.)
        let stateStats, oks, exns = Stats.StateStats(), Stats.LatencyStats("ok"), Stats.LatencyStats("exceptions")
        let mutable cycles, batchesCompleted, batchesStarted, streamsStarted, eventsStarted, streamsWrittenAhead, eventsWrittenAhead = 0, 0, 0, 0, 0, 0, 0

        member val Log = log
        member val StatsInterval = IntervalTimer statsInterval
        member val StateInterval = IntervalTimer stateInterval
        member val Timers = Stats.Timers()

        member x.DumpStats(struct (dispatchActive, dispatchMax), struct (batchesWaiting, batchesRunning)) =
            log.Information("Scheduler {cycles} cycles {@states} Running {busy}/{processors}",
                cycles, stateStats.StatsDescending, dispatchActive, dispatchMax)
            cycles <- 0; stateStats.Clear()
            oks.Dump log; exns.Dump log
            let batchesCompleted = Interlocked.Exchange(&batchesCompleted, 0)
            log.Information(" Batches waiting {waiting} started {started} {streams:n0}s {events:n0}e skipped {streamsSkipped:n0}s {eventsSkipped:n0}e completed {completed} Running {active}",
                            batchesWaiting, batchesStarted, streamsStarted, eventsStarted, streamsWrittenAhead, eventsWrittenAhead, batchesCompleted, batchesRunning)
            batchesStarted <- 0; streamsStarted <- 0; eventsStarted <- 0; streamsWrittenAhead <- 0; eventsWrittenAhead <- 0; (*batchesCompleted <- 0*)
            x.Timers.Dump log
            monitor.DumpState x.Log
            x.DumpStats()

        member _.RecordIngested(streams, events, skippedStreams, skippedEvents) =
            batchesStarted <- batchesStarted + 1
            streamsStarted <- streamsStarted + streams
            eventsStarted <- eventsStarted + events
            streamsWrittenAhead <- streamsWrittenAhead + skippedStreams
            eventsWrittenAhead <- eventsWrittenAhead + skippedEvents

        member _.RecordBatchCompletion() =
            Interlocked.Increment(&batchesCompleted) |> ignore

        member x.RecordStats() =
            cycles <- cycles + 1
            x.StatsInterval.IsDue

        member x.RecordState(state) =
            stateStats.Ingest(state)
            x.StateInterval.IfDueRestart()

        /// Allows an ingester or projector to wire in custom stats (typically based on data gathered in a `Handle` override)
        abstract DumpStats : unit -> unit
        default _.DumpStats () = ()

        /// Allows serialization of the emission of statistics where multiple Schedulers are active (via an externally managed lock object)
        abstract Serialize : (unit -> unit) -> unit
        default _.Serialize(f) = f ()

        abstract member Handle : InternalResult<Choice<'R, 'E>> -> unit
        default x.Handle msg = msg |> function
            | { duration = duration; stream = stream; progressed = progressed; result = Choice1Of2 _ } ->
                oks.Record duration
                x.HandleResult(stream, duration, true, progressed)
            | { duration = duration; stream = stream; progressed = progressed; result = Choice2Of2 _ } ->
                exns.Record duration
                x.HandleResult(stream, duration, false, progressed)

        abstract HandleResult : FsCodec.StreamName * TimeSpan * progressed : bool * succeeded : bool -> unit
        default x.HandleResult(stream, duration, succeeded, progressed) =
            monitor.HandleResult(stream, succeeded, progressed)
            if metricsLog.IsEnabled LogEventLevel.Information then
                let outcomeKind = if succeeded then "ok" else "exceptions"
                let m = Log.Metric.HandlerResult (outcomeKind, duration.TotalSeconds)
                (metricsLog |> Log.withMetric m).Information("Outcome {kind} in {ms:n0}ms, progressed: {progressed}",
                                                             outcomeKind, duration.TotalMilliseconds, progressed)
                if monitorInterval.IfDueRestart() then monitor.EmitMetrics metricsLog

        member _.HandleStarted(stream, stopwatchTicks) =
            monitor.HandleStarted(stream, stopwatchTicks)

    module Progress =

        type [<Struct; NoComparison; NoEquality>] BatchState = { markCompleted : unit -> unit; streamToRequiredIndex : Dictionary<FsCodec.StreamName, int64> }

        type ProgressState<'Pos>() =
            let pending = Queue<BatchState>()

            let trim () =
                while pending.Count <> 0 && pending.Peek().streamToRequiredIndex.Count = 0 do
                    let batch = pending.Dequeue()
                    batch.markCompleted ()
            member _.RunningCount = pending.Count
            member _.EnumPending() : seq<BatchState> =
                trim ()
                pending
            member _.AppendBatch(markCompleted, reqs : Dictionary<FsCodec.StreamName, int64>) =
                let fresh = { markCompleted = markCompleted; streamToRequiredIndex = reqs }
                pending.Enqueue fresh
                trim ()
                if pending.Count = 0 then ValueNone // If already complete, avoid triggering stream ingestion or a dispatch cycle
                else ValueSome fresh

            member _.MarkStreamProgress(stream, index) =
                for x in pending do
                    // example : when we reach position 1 on the stream (having handled event 0), and the required position was 1, we remove the requirement
                    let mutable requiredIndex = Unchecked.defaultof<_>
                    if x.streamToRequiredIndex.TryGetValue(stream, &requiredIndex) && requiredIndex <= index then
                        x.streamToRequiredIndex.Remove stream |> ignore

            member _.Dump(log : ILogger, force) =
                if (force || log.IsEnabled LogEventLevel.Debug) && pending.Count <> 0 then
                    let h = pending.Peek()
                    log.Write((if force then LogEventLevel.Information else LogEventLevel.Debug), "Active Batch {streams}", h.streamToRequiredIndex)

        // We potentially traverse the pending streams thousands of times per second so we reuse buffers for better L2 caching properties
        // NOTE internal reuse of `sortBuffer` and `streamsBuffer` means it's critical to never have >1 of these in flight
        type StreamsPrioritizer(getStreamWeight) =

            let streamsSuggested = HashSet()
            let collectUniqueStreams (xs : IEnumerator<BatchState>) = seq {
                while xs.MoveNext() do
                    let x = xs.Current
                    for s in x.streamToRequiredIndex.Keys do
                        if streamsSuggested.Add s then
                            yield s }

            let sortBuffer = ResizeArray()
            // Within the head batch, expedite work on longest streams, where requested
            let prioritizeHead (getStreamWeight : FsCodec.StreamName -> int64) (head : BatchState) : seq<FsCodec.StreamName> =
                // sortBuffer is reused per invocation, but the result is lazy so we can only clear on entry
                sortBuffer.Clear()
                let weight s = -getStreamWeight s |> int
                for s in head.streamToRequiredIndex.Keys do
                    if streamsSuggested.Add s then
                        let w = weight s
                        sortBuffer.Add(struct (s, w))
                if sortBuffer.Count > 0 then
                    let c = Comparer<_>.Default
                    sortBuffer.Sort(fun struct (_, _aw) struct (_, _bw) -> c.Compare(_aw, _bw))
                sortBuffer |> Seq.map ValueTuple.fst

            member _.CollectStreams(batches : BatchState seq) : seq<FsCodec.StreamName> =
                // streamsSuggested is reused per invocation (and also in prioritizeHead), so we need to clear it first
                streamsSuggested.Clear()
                // NOTE: NOT `use` as that would terminate the enumeration too early
                let xs = batches.GetEnumerator()
                match getStreamWeight with
                | None -> collectUniqueStreams xs
                | Some f ->
                    if xs.MoveNext() then Seq.append (prioritizeHead f xs.Current) (collectUniqueStreams xs)
                    else Seq.empty

    /// Defines interface between Scheduler (which owns the pending work) and the Dispatcher which periodically selects work to commence based on a policy
    type IDispatcher<'P, 'R, 'E, 'F> =
        abstract member Pump : CancellationToken -> Task<unit>
        abstract member State : struct (int * int)
        abstract member HasCapacity : bool with get
        abstract member AwaitCapacity : unit -> Task<unit>
        abstract member TryReplenish : pending : seq<Item<'F>> * handleStarted : (FsCodec.StreamName * int64 -> unit) -> struct (bool * bool)
        abstract member InterpretProgress : StreamStates<'F> * FsCodec.StreamName * Choice<'P, 'E> -> struct (int64 voption * Choice<'R, 'E>)
        [<CLIEvent>] abstract member Result : IEvent<struct (TimeSpan * FsCodec.StreamName * bool * Choice<'P, 'E>)>
    and [<Struct; NoComparison; NoEquality>]
        Item<'Format> = { stream : FsCodec.StreamName; writePos : int64 voption; span : FsCodec.ITimelineEvent<'Format> array }

    /// Consolidates ingested events into streams; coordinates dispatching of these to projector/ingester in the order implied by the submission order
    /// a) does not itself perform any reading activities
    /// b) triggers synchronous callbacks as batches complete; writing of progress is managed asynchronously by the TrancheEngine(s)
    /// c) submits work to the supplied Dispatcher (which it triggers pumping of)
    /// d) periodically reports state (with hooks for ingestion engines to report same)
    type Engine<'P, 'R, 'E, 'F>
        (   dispatcher : IDispatcher<'P, 'R, 'E, 'F>, stats : Stats<'R,'E>, dumpState,
            // Number of batches to buffer locally; rest held by submitter to enable fair ordering of submission as partitions are added/removed
            maxIngest,
            // Tune the max number of check/dispatch cycles. Default 2.
            // Frequency of jettisoning Write Position state of inactive streams (held by the scheduler for deduplication purposes) to limit memory consumption
            // NOTE: Purging can impair performance, increase write costs or result in duplicate event emissions due to redundant inputs not being deduplicated
            ?purgeInterval,
            // The default behavior is to wait for dispatch capacity
            // Enables having the scheduler service results immediately (quicker feedback on batch completions), at the cost of increased CPU usage (esp given in normal conditions, the `idleDelay` is not onerous and incoming batches or dispatch capacity becoming available typically are proxies)
            ?wakeForResults,
            // Tune the sleep time when there are no items to schedule or items to dispatch. Default 1s.
            ?idleDelay,
            // Prioritize processing of the largest Payloads within the Head batch when scheduling work
            // Can yield significant throughput improvement when ingesting large batches in the face of rate limiting
            ?prioritizeStreamsBy,
            // Where ingesters are potentially delivering events out of order, wait for first event until write position is known
            // Cannot be combined with purging in the current implementation
            ?requireCompleteStreams) =
        let writeResult, awaitResults, tryApplyResults =
            let r, w = let c = Channel.unboundedSr in c.Reader, c.Writer
            Channel.write w, Channel.awaitRead r, Channel.apply r
        let enqueueStreams, applyStreams =
            let r, w = let c = Channel.unboundedSr<Streams<_>> in c.Reader, c.Writer
            Channel.write w, Channel.apply r
        let waitToSubmit, tryWritePending, waitingCount, awaitPending, tryPending =
            let r, w = let c = Channel.boundedSw<Batch>(maxIngest) in c.Reader, c.Writer // Actually SingleReader too, but Count throws if you use that
            Channel.waitToWrite w, Channel.tryWrite w, (fun () -> r.Count), Channel.awaitRead r, Channel.tryRead r
        let streams = StreamStates<'F>()
        let batches = Progress.ProgressState()
        let batchesWaitingAndRunning () = struct (waitingCount (), batches.RunningCount)
        // Enumerates the active batches; when the caller pulls beyond that, more get ingested on the fly
        let enumBatches ingestStreams ingestBatches = seq {
            yield! batches.EnumPending()
            // We'll get here as soon as the dispatching process has exhausted the currently queued items
            match ingestBatches () with
            | [||] -> () // Nothing more available
            | freshlyAddedBatches ->
                // we've just enqueued fresh batches
                // hence we need to ingest events potentially added since first call to guarantee we have all the events on which the batches depend
                ingestStreams ()
                yield! freshlyAddedBatches }
        let priority = Progress.StreamsPrioritizer(prioritizeStreamsBy |> Option.map streams.HeadSpanSizeBy)
        let chooseDispatchable =
            let requireCompleteStreams = defaultArg requireCompleteStreams false
            if requireCompleteStreams && Option.isSome purgeInterval then invalidArg (nameof requireCompleteStreams) "Cannot be combined with a purgeInterval"
            fun stream ->
                streams.ChooseDispatchable(stream, not requireCompleteStreams)
                |> ValueOption.map (fun ss -> { stream = stream; writePos = ss.WritePos; span = ss.HeadSpan })
        let tryDispatch ingestStreams ingestBatches =
            let candidateItems : seq<Item<_>> = enumBatches ingestStreams ingestBatches |> priority.CollectStreams |> Seq.chooseV chooseDispatchable
            let handleStarted (stream, ts) = stats.HandleStarted(stream, ts); streams.MarkBusy(stream)
            dispatcher.TryReplenish(candidateItems, handleStarted)

        // Ingest information to be gleaned from processing the results into `streams` (i.e. remove stream requirements as they are completed)
        let handleResult { duration = duration; stream = stream; progressed = p; result = (res : Choice<'P, 'E>) } =
            match dispatcher.InterpretProgress(streams, stream, res) with
            | ValueSome index, Choice1Of2 (r : 'R) ->
                batches.MarkStreamProgress(stream, index)
                streams.RecordProgress(stream, index)
                stats.Handle { duration = duration; stream = stream; progressed = p; result = Choice1Of2 r }
            | ValueNone, Choice1Of2 (r : 'R) ->
                streams.RecordNoProgress(stream)
                stats.Handle { duration = duration; stream = stream; progressed = p; result = Choice1Of2 r }
            | _, Choice2Of2 exn ->
                streams.RecordNoProgress(stream)
                stats.Handle { duration = duration; stream = stream; progressed = p; result = Choice2Of2 exn }
        let tryHandleResults () = tryApplyResults handleResult

        // Take an incoming batch of events, correlating it against our known stream state to yield a set of remaining work
        let ingest (batch : Batch) =
            let reqs = Dictionary()
            let mutable events, eventsSkipped = 0, 0
            for item in batch.Reqs do
                if streams.WritePositionIsAlreadyBeyond(item.Key, item.Value) then
                    eventsSkipped <- eventsSkipped + 1
                else
                    events <- events + 1
                    reqs[item.Key] <- item.Value
            stats.RecordIngested(reqs.Count, events, batch.StreamsCount - reqs.Count, eventsSkipped)
            let onCompletion () =
                batch.OnCompletion ()
                stats.RecordBatchCompletion()
            batches.AppendBatch(onCompletion, reqs)
        let ingestBatch () = [| match tryPending () |> ValueOption.bind ingest with ValueSome b -> b | ValueNone -> () |]

        let mutable totalPurged = 0
        let purge () =
            let remaining, purged = streams.Purge()
            totalPurged <- totalPurged + purged
            let l = if purged = 0 then LogEventLevel.Debug else LogEventLevel.Information
            Log.Write(l, "PURGED Remaining {buffered:n0} Purged now {count:n0} Purged total {total:n0}", remaining, purged, totalPurged)
        let purgeDue : unit -> bool =
            match purgeInterval with
            | Some ts -> IntervalTimer(ts).IfDueRestart
            | None -> fun () -> false

        let recordStats force =
            if stats.RecordStats() || force then
                stats.Serialize(fun () -> stats.DumpStats(dispatcher.State, batchesWaitingAndRunning ()))
                stats.StatsInterval.Restart() // manual restart only after we've serviced the call so observers can await completion
        let sleepIntervalMs = match idleDelay with Some ts -> TimeSpan.toMs ts | None -> 1000
        let wakeForResults = defaultArg wakeForResults false

        member _.Pump(abend, ct : CancellationToken) = task {
            use _ = dispatcher.Result.Subscribe(fun struct (t, s, pr, r) -> writeResult { duration = t; stream = s; progressed = pr; result = r })
            Task.start (fun () -> task { try do! dispatcher.Pump ct
                                         with e -> abend (AggregateException e) })
            let inline ts () = Stopwatch.timestamp ()
            let t = stats.Timers
            let processResults () = let ts = ts () in let r = tryHandleResults () in t.RecordResults ts; r
            let ingestStreams () = let ts, r = ts (), applyStreams streams.Merge in t.RecordMerge ts; r
            let ingestBatches () = let ts, b = ts (), ingestBatch () in t.RecordIngest ts; b
            let ingestStreamsOnly () = let ts = ts () in let r = ingestStreams () in t.RecordDispatchNone ts; r

            let recordState force dispatcherState =
                if stats.RecordState(dispatcherState) || force then
                    let log = stats.Log
                    let dumpStreamStates (eventSize : FsCodec.ITimelineEvent<'F> -> int) =
                        let hasGaps = streams.Dump(log, totalPurged, eventSize)
                        batches.Dump(log, hasGaps)
                    dumpState dumpStreamStates log
                    true
                else false

            let reportStats force = let ts = ts () in recordStats force; t.RecordStats ts
            while not ct.IsCancellationRequested do
                // 1. propagate write write outcomes to buffer (can mark batches completed etc)
                let processedResults = processResults ()
                // 2. top up provisioning of writers queue
                // On each iteration, we try to fill the in-flight queue, taking the oldest and/or heaviest streams first
                // Where there is insufficient work in the queue, we trigger ingestion of batches as needed
                let struct (dispatched, hasCapacity) =
                    if not dispatcher.HasCapacity then struct ((*dispatched*)false, (*hasCapacity*)false)
                    else let ts = ts () in let r = tryDispatch (ingestStreams >> ignore) ingestBatches in t.RecordDispatch ts; r
                let idle = not processedResults && not dispatched && not (ingestStreamsOnly ())
                reportStats false
                // 3. Do a minimal sleep so we don't run completely hot when empty (unless we did something non-trivial)
                if idle then
                    let waitForIncomingBatches = hasCapacity
                    let waitForDispatcherCapacity = not hasCapacity && not wakeForResults
                    let sleepTs = Stopwatch.timestamp ()
                    let wakeConditions : Task array = [|
                        if wakeForResults then awaitResults ct
                        elif waitForDispatcherCapacity then dispatcher.AwaitCapacity()
                        if waitForIncomingBatches then awaitPending ct
                        Task.Delay(int sleepIntervalMs) |]
                    do! Task.WhenAny(wakeConditions) :> Task
                    t.RecordSleep sleepTs
                // 4. Record completion state once per iteration; dumping streams is expensive so needs to be done infrequently
                let dispatcherState = if not hasCapacity then Full elif idle then Idle else Active
                if recordState ct.IsCancellationRequested dispatcherState && purgeDue () then
                    purge ()
            // Flush
            batches.EnumPending() |> ignore
            processResults () |> ignore
            reportStats true }

        member internal _.SubmitStreams(x : Streams<_>) =
            enqueueStreams x
        member internal _.WaitToSubmit() =
            waitToSubmit ()
        member internal _.TrySubmit(x : Batch) =
            tryWritePending x

module Dispatcher =

    /// Coordinates the dispatching of work and emission of results, subject to the maxDop concurrent processors constraint
    type internal DopDispatcher<'R>(maxDop : int) =
        let tryWrite, wait, apply =
            let c = Channel.unboundedSwSr<CancellationToken -> Task<'R>> in let r, w = c.Reader, c.Writer
            w.TryWrite, Channel.awaitRead r, Channel.apply r >> ignore
        let result = Event<'R>()
        let dop = Sem maxDop

        // NOTE this obviously depends on the passed computation never throwing, or we'd leak dop
        let runHandler struct (computation : CancellationToken -> Task<'R>, ct) = task {
            let! res = computation ct
            dop.Release()
            result.Trigger res }

        member _.State = dop.State
        member _.HasCapacity = dop.HasCapacity
        member _.AwaitButRelease() = dop.AwaitButRelease()
        member _.TryAdd(item) = dop.TryTake() && tryWrite item
        [<CLIEvent>] member _.Result = result.Publish

        member _.Pump(ct : CancellationToken) = task {
            while not ct.IsCancellationRequested do
                try do! wait ct :> Task
                with :? OperationCanceledException -> ()
                let run (f : CancellationToken -> Task<'R>) = Task.start (fun () -> runHandler (f, ct))
                apply run }

    /// Kicks off enough work to fill the inner Dispatcher up to capacity
    type internal ItemDispatcher<'R, 'F>(maxDop) =
        let inner = DopDispatcher<struct (TimeSpan * FsCodec.StreamName * bool * 'R)>(maxDop)

        // On each iteration, we try to fill the in-flight queue, taking the oldest and/or heaviest streams first
        let tryFillDispatcher (potential : seq<Scheduling.Item<'F>>) markStarted project =
            let xs = potential.GetEnumerator()
            let startTs = Stopwatch.timestamp ()
            let mutable hasCapacity, dispatched = true, false
            while xs.MoveNext() && hasCapacity do
                let item = xs.Current
                let succeeded = inner.TryAdd(project struct (startTs, item))
                if succeeded then markStarted (item.stream, startTs)
                hasCapacity <- succeeded
                dispatched <- dispatched || succeeded // if we added any request, we'll skip sleeping
            struct (dispatched, hasCapacity)

        member _.Pump ct = inner.Pump ct
        member _.State = inner.State
        member _.HasCapacity = inner.HasCapacity
        member _.AwaitCapacity() = inner.AwaitButRelease()
        member _.TryReplenish(pending, markStarted, project) = tryFillDispatcher pending markStarted project
        [<CLIEvent>] member _.Result = inner.Result

    /// Implementation of IDispatcher that feeds items to an item dispatcher that maximizes concurrent requests (within a limit)
    type Concurrent<'P, 'R, 'E, 'F> internal
        (   inner : ItemDispatcher<Choice<'P, 'E>, 'F>,
            project : struct (int64 * Scheduling.Item<'F>) -> CancellationToken -> Task<struct (TimeSpan * FsCodec.StreamName * bool * Choice<'P, 'E>)>,
            interpretProgress : Scheduling.StreamStates<'F> -> FsCodec.StreamName -> Choice<'P, 'E> -> struct (int64 voption * Choice<'R, 'E>)) =
        static member Create
            (   maxDop,
                project : struct (FsCodec.StreamName * StreamSpan<'F>) -> CancellationToken -> Task<struct (bool * Choice<'P, 'E>)>,
                interpretProgress) =
            let project struct (startTs, item : Scheduling.Item<'F>) (ct : CancellationToken) = task {
                let! struct (progressed, res) = project (item.stream, item.span) ct
                return struct (Stopwatch.elapsed startTs, item.stream, progressed, res) }
            Concurrent<_, _, _, _>(ItemDispatcher(maxDop), project, interpretProgress)
        static member Create(maxDop, prepare, handle, toIndex) =
            let project item ct = task {
                let struct (met, (struct (_sn, span : StreamSpan<'F>) as ss)) = prepare item
                try let! struct (spanResult, outcome) = Async.StartImmediateAsTask(handle ss, cancellationToken = ct)
                    let index' = toIndex span spanResult
                    return struct (index' > span[0].Index, Choice1Of2 struct (index', met, outcome))
                with e -> return struct (false, Choice2Of2 struct (met, e)) }
            let interpretProgress (_streams : Scheduling.StreamStates<'F>) _stream = function
                | Choice1Of2 struct (index, met, outcome) -> struct (ValueSome index, Choice1Of2 struct (met, outcome))
                | Choice2Of2 struct (stats, exn) -> ValueNone, Choice2Of2 struct (stats, exn)
            Concurrent<_, _, _, 'F>.Create(maxDop, project, interpretProgress)
        interface Scheduling.IDispatcher<'P, 'R, 'E, 'F> with
            override _.Pump ct = inner.Pump ct
            override _.State = inner.State
            override _.HasCapacity = inner.HasCapacity
            override _.AwaitCapacity() = inner.AwaitCapacity()
            override _.TryReplenish(pending, handleStarted) = inner.TryReplenish(pending, handleStarted, project)
            override _.InterpretProgress(streams, stream, res) = interpretProgress streams stream res
            [<CLIEvent>] override _.Result = inner.Result

    /// Implementation of IDispatcher that allows a supplied handler select work and declare completion based on arbitrarily defined criteria
    type Batched<'F>
        (   select : Scheduling.Item<'F> seq -> Scheduling.Item<'F> array,
            handle : Scheduling.Item<'F> array -> CancellationToken ->
                     Task<struct (TimeSpan * FsCodec.StreamName * bool * Choice<struct (int64 * struct (StreamSpan.Metrics * unit)), struct (StreamSpan.Metrics * exn)>) array>) =
        let inner = DopDispatcher 1
        let result = Event<struct (TimeSpan * FsCodec.StreamName * bool * Choice<struct (int64 * struct (StreamSpan.Metrics * unit)), struct (StreamSpan.Metrics * exn)>)>()

        // On each iteration, we offer the ordered work queue to the selector
        // we propagate the selected streams to the handler
        let trySelect (potential : seq<Scheduling.Item<'F>>) markStarted =
            let mutable hasCapacity, dispatched = true, false
            let streams : Scheduling.Item<'F>[] = select potential
            if Array.any streams then
                let res = inner.TryAdd(handle streams)
                if not res then failwith "Checked we can add, what gives?"
                let startTs = Stopwatch.timestamp ()
                for x in streams do markStarted (x.stream, startTs)
                dispatched <- true // if we added any request, we'll skip sleeping
                hasCapacity <- false
            struct (dispatched, hasCapacity)

        interface Scheduling.IDispatcher<struct (int64 * struct (StreamSpan.Metrics * unit)), struct (StreamSpan.Metrics * unit), struct (StreamSpan.Metrics * exn), 'F> with
            override _.Pump ct = task {
                use _ = inner.Result.Subscribe(Array.iter result.Trigger)
                return! inner.Pump ct }
            override _.State = inner.State
            override _.HasCapacity = inner.HasCapacity
            override _.AwaitCapacity() = inner.AwaitButRelease()
            override _.TryReplenish(pending, handleStarted) = trySelect pending handleStarted
            override _.InterpretProgress(_streams : Scheduling.StreamStates<_>, _stream : FsCodec.StreamName, res : Choice<_, _>) =
                match res with
                | Choice1Of2 (pos', (stats, outcome)) -> ValueSome pos', Choice1Of2 (stats, outcome)
                | Choice2Of2 (stats, exn) -> ValueNone, Choice2Of2 (stats, exn)
            [<CLIEvent>] override _.Result = result.Publish

[<AbstractClass>]
type Stats<'Outcome>(log : ILogger, statsInterval, statesInterval) =
    inherit Scheduling.Stats<struct (StreamSpan.Metrics * 'Outcome), struct (StreamSpan.Metrics * exn)>(log, statsInterval, statesInterval)
    let okStreams, failStreams, badCats = HashSet(), HashSet(), Stats.CatStats()
    let mutable resultOk, resultExnOther, okEvents, okBytes, exnEvents, exnBytes = 0, 0, 0, 0L, 0, 0L

    override _.DumpStats() =
        log.Information("Projected {mb:n0}MB {completed:n0}r {streams:n0}s {events:n0}e ({ok:n0} ok)",
                        Log.miB okBytes, resultOk, okStreams.Count, okEvents, resultOk)
        okStreams.Clear(); resultOk <- 0; okEvents <- 0; okBytes <- 0L
        if resultExnOther <> 0 then
            log.Warning("Exceptions {mb:n0}MB {fails:n0}r {streams:n0}s {events:n0}e",
                        Log.miB exnBytes, resultExnOther, failStreams.Count, exnEvents)
            resultExnOther <- 0; failStreams.Clear(); exnBytes <- 0L; exnEvents <- 0
            log.Warning(" Affected cats {@badCats}", badCats.StatsDescending)
            badCats.Clear()

    override this.Handle res =
        let inline addStream sn (set : HashSet<_>) = set.Add sn |> ignore
        let inline addBadStream sn (set : HashSet<_>) = badCats.Ingest(StreamName.categorize sn); addStream sn set
        base.Handle res
        match res with
        | { stream = stream; result = Choice1Of2 ((es, bs), res) } ->
            addStream stream okStreams
            okEvents <- okEvents + es
            okBytes <- okBytes + int64 bs
            resultOk <- resultOk + 1
            this.HandleOk res
        | { duration = duration; stream = stream; result = Choice2Of2 ((es, bs), exn) } ->
            addBadStream stream failStreams
            exnEvents <- exnEvents + es
            exnBytes <- exnBytes + int64 bs
            resultExnOther <- resultExnOther + 1
            this.HandleExn(log.ForContext("stream", stream).ForContext("events", es).ForContext("duration", duration), exn)
    abstract member HandleOk : outcome : 'Outcome -> unit
    abstract member HandleExn : log : ILogger * exn : exn -> unit

module Projector =

    type StreamsIngester =

        static member Start(log, partitionId, maxRead, submit, statsInterval) =
            let submitBatch (items : StreamEvent<'F> seq, onCompletion) =
                let items = Array.ofSeq items
                let streams = items |> Seq.map ValueTuple.fst |> HashSet
                let batch : Submission.Batch<_, _> = { partitionId = partitionId; onCompletion = onCompletion; messages = items }
                submit batch
                struct (streams.Count, items.Length)
            Ingestion.Ingester<StreamEvent<'F> seq>.Start(log, partitionId, maxRead, submitBatch, statsInterval)

    type StreamsSubmitter =

        static member Create(log : ILogger, mapBatch, streamsScheduler : Scheduling.Engine<_, _, _, _>, statsInterval) =
            let trySubmitBatch (x : Buffer.Batch) : int voption =
                if streamsScheduler.TrySubmit x then ValueSome x.StreamsCount
                else ValueNone
            Submission.SubmissionEngine<_, _, _, _>(log, statsInterval, mapBatch, streamsScheduler.SubmitStreams, streamsScheduler.WaitToSubmit, trySubmitBatch)

    type Pipeline =

        static member Start(log : ILogger, pumpScheduler, maxReadAhead, streamsScheduler, ingesterStatsInterval) =
            let mapBatch onCompletion (x : Submission.Batch<_, StreamEvent<'F>>) : struct (Buffer.Streams<'F> * Buffer.Batch) =
                Buffer.Batch.Create(onCompletion, x.messages)
            let submitter = StreamsSubmitter.Create(log, mapBatch, streamsScheduler, ingesterStatsInterval)
            let startIngester (rangeLog, partitionId : int) = StreamsIngester.Start(rangeLog, partitionId, maxReadAhead, submitter.Ingest, ingesterStatsInterval)
            Sink.Start(log, pumpScheduler, submitter.Pump, startIngester)

/// Represents progress attained during the processing of the supplied <c>StreamSpan</c> for a given <c>StreamName</c>.
/// This will be reflected in adjustments to the Write Position for the stream in question.
/// Incoming <c>StreamEvent</c>s with <c>Index</c>es prior to the Write Position implied by the result are proactively
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

    let toIndex<'F> (span : StreamSpan<'F>) = function
        | NoneProcessed -> span[0].Index
        | AllProcessed -> span[0].Index + span.LongLength
        | PartiallyProcessed count -> span[0].Index + int64 count
        | OverrideWritePosition index -> index

type StreamsSink =

    /// Custom projection mechanism that divides work into a <code>prepare</code> phase that selects the prefix of the queued StreamSpan to handle,
    /// and a <code>handle</code> function that yields a Write Position representing the next event that's to be handled on this Stream
    static member StartEx<'Progress, 'Outcome, 'F>
        (   log : ILogger, maxReadAhead, maxConcurrentStreams,
            prepare : struct (FsCodec.StreamName * _) -> _,
            handle : struct (FsCodec.StreamName * FsCodec.ITimelineEvent<'F> array) -> Async<struct (SpanResult * 'Outcome)>,
            toIndex,
            stats, statsInterval, eventSize,
            // Configure max number of batches to ingest into scheduler; Default: Same as maxReadAhead
            ?maxIngest,
            // Frequency of jettisoning Write Position state of inactive streams (held by the scheduler for deduplication purposes) to limit memory consumption
            // NOTE: Purging can impair performance, increase write costs or result in duplicate event emissions due to redundant inputs not being deduplicated
            ?purgeInterval,
            // Request optimal throughput by waking based on handler outcomes even if there is unused dispatch capacity
            ?wakeForResults,
            // Tune the sleep time when there are no items to schedule or responses to process. Default 1s.
            ?idleDelay,
            ?ingesterStatsInterval, ?requireCompleteStreams)
        : Sink<Ingestion.Ingester<StreamEvent<'F> seq>> =
        let dispatcher : Scheduling.IDispatcher<_, _, _, _> = Dispatcher.Concurrent<_, _, _, 'F>.Create(maxConcurrentStreams, prepare, handle, toIndex)
        let dumpStreams logStreamStates _log = logStreamStates eventSize
        let scheduler = Scheduling.Engine(dispatcher, stats, dumpStreams,
                                          defaultArg maxIngest maxReadAhead, ?purgeInterval = purgeInterval, ?wakeForResults = wakeForResults, ?idleDelay = idleDelay,
                                          ?requireCompleteStreams = requireCompleteStreams)
        Projector.Pipeline.Start(log, scheduler.Pump, maxReadAhead, scheduler, ingesterStatsInterval = defaultArg ingesterStatsInterval statsInterval)

    /// Project StreamSpans using a <code>handle</code> function that yields a Write Position representing the next event that's to be handled on this Stream
    static member Start<'Outcome, 'F>
        (   log : ILogger, maxReadAhead, maxConcurrentStreams,
            handle : struct (FsCodec.StreamName * FsCodec.ITimelineEvent<'F> array) -> Async<struct (SpanResult * 'Outcome)>,
            stats, statsInterval, eventSize,
            // Configure max number of batches to ingest into scheduler; Default: Same as maxReadAhead
            ?maxIngest,
            // Frequency of jettisoning Write Position state of inactive streams (held by the scheduler for deduplication purposes) to limit memory consumption
            // NOTE: Purging can impair performance, increase write costs or result in duplicate event emissions due to redundant inputs not being deduplicated
            ?purgeInterval,
            // Request optimal throughput by waking based on handler outcomes even if there is unused dispatch capacity
            ?wakeForResults,
            // Tune the sleep time when there are no items to schedule or responses to process. Default 1s.
            ?idleDelay,
            ?ingesterStatsInterval,
            ?requireCompleteStreams)
        : Sink<Ingestion.Ingester<StreamEvent<'F> seq>> =
        let prepare struct (streamName, span) =
            let metrics = StreamSpan.metrics eventSize span
            struct (metrics, struct (streamName, span))
        StreamsSink.StartEx<SpanResult, 'Outcome, 'F>(
            log, maxReadAhead, maxConcurrentStreams, prepare, handle, SpanResult.toIndex, stats, statsInterval, eventSize,
            ?maxIngest = maxIngest, ?purgeInterval = purgeInterval, ?wakeForResults = wakeForResults, ?idleDelay = idleDelay,
            ?ingesterStatsInterval = ingesterStatsInterval, ?requireCompleteStreams = requireCompleteStreams)

    /// Project StreamSpans using a <code>handle</code> function that guarantees to always handles all events in the <code>span</code>
    static member Start<'Outcome, 'F>
        (   log : ILogger, maxReadAhead, maxConcurrentStreams,
            handle : struct (FsCodec.StreamName * FsCodec.ITimelineEvent<'F> array) -> Async<'Outcome>,
            stats, statsInterval, eventSize,
            // Configure max number of batches to ingest into scheduler; Default: Same as maxReadAhead
            ?maxIngest,
            // Frequency of jettisoning Write Position state of inactive streams (held by the scheduler for deduplication purposes) to limit memory consumption
            // NOTE: Purging can impair performance, increase write costs or result in duplicate event emissions due to redundant inputs not being deduplicated
            ?purgeInterval,
            // Request optimal throughput by waking based on handler outcomes even if there is unused dispatch capacity
            ?wakeForResults,
            // Tune the sleep time when there are no items to schedule or responses to process. Default 1s.
            ?idleDelay,
            ?ingesterStatsInterval)
        : Sink<Ingestion.Ingester<StreamEvent<'F> seq>> =
        let handle struct (streamName, span : FsCodec.ITimelineEvent<'F> array) = async {
            let! res = handle (streamName, span)
            return struct (SpanResult.AllProcessed, res) }
        StreamsSink.Start<'Outcome, 'F>(
            log, maxReadAhead, maxConcurrentStreams, handle, stats, statsInterval, eventSize,
            ?maxIngest = maxIngest, ?purgeInterval = purgeInterval, ?wakeForResults = wakeForResults, ?idleDelay = idleDelay,
            ?ingesterStatsInterval = ingesterStatsInterval)

module Sync =

    [<AbstractClass>]
    type Stats<'Outcome>(log : ILogger, statsInterval, stateInterval) =
        inherit Scheduling.Stats<struct (struct (StreamSpan.Metrics * TimeSpan) * 'Outcome), struct (StreamSpan.Metrics * exn)>(log, statsInterval, stateInterval)
        let okStreams, failStreams = HashSet(), HashSet()
        let prepareStats = Stats.LatencyStats("prepare")
        let mutable okEvents, okBytes, exnEvents, exnBytes = 0, 0L, 0, 0L

        override _.DumpStats() =
            if okStreams.Count <> 0 && failStreams.Count <> 0 then
                log.Information("Completed {okMb:n0}MB {okStreams:n0}s {okEvents:n0}e Exceptions {exnMb:n0}MB {exnStreams:n0}s {exnEvents:n0}e",
                                Log.miB okBytes, okStreams.Count, okEvents, Log.miB exnBytes, failStreams.Count, exnEvents)
            okStreams.Clear(); okEvents <- 0; okBytes <- 0L
            prepareStats.Dump log

        override this.Handle message =
            let inline adds x (set : HashSet<_>) = set.Add x |> ignore
            base.Handle message
            match message with
            | { stream = stream; result = Choice1Of2 (((es, bs), prepareElapsed), outcome) } ->
                adds stream okStreams
                okEvents <- okEvents + es
                okBytes <- okBytes + int64 bs
                prepareStats.Record prepareElapsed
                this.HandleOk outcome
            | { stream = stream; result = Choice2Of2 ((es, bs), exn) } ->
                adds stream failStreams
                exnEvents <- exnEvents + es
                exnBytes <- exnBytes + int64 bs
                this.HandleExn(log.ForContext("stream", stream).ForContext("events", es), exn)
        abstract member HandleOk : outcome : 'Outcome -> unit
        abstract member HandleExn : log : ILogger * exn : exn -> unit

    type StreamsSync =

        static member Start
            (   log : ILogger, maxReadAhead, maxConcurrentStreams,
                handle : struct (FsCodec.StreamName * FsCodec.ITimelineEvent<'F> array) -> Async<struct (SpanResult * 'Outcome)>,
                stats : Stats<'Outcome>, statsInterval, sliceSize, eventSize,
                // Default 1 ms
                ?idleDelay,
                // Default 1 MiB
                ?maxBytes,
                // Default 16384
                ?maxEvents,
                // Hook to wire in external stats
                ?dumpExternalStats,
                // Frequency of jettisoning Write Position state of inactive streams (held by the scheduler for deduplication purposes) to limit memory consumption
                // NOTE: Purging can impair performance, increase write costs or result in duplicate event emissions due to redundant inputs not being deduplicated
                ?purgeInterval)
            : Sink<Ingestion.Ingester<StreamEvent<'F> seq>> =

            let maxEvents, maxBytes = defaultArg maxEvents 16384, (defaultArg maxBytes (1024 * 1024 - (*fudge*)4096))

            let attemptWrite struct (stream, span : FsCodec.ITimelineEvent<'F> array) ct = task {
                let struct (met, span') = StreamSpan.slice<'F> sliceSize (maxEvents, maxBytes) span
                let prepareTs = Stopwatch.timestamp ()
                try let req = struct (stream, span')
                    let! res, outcome = Async.StartImmediateAsTask(handle req, cancellationToken = ct)
                    let index' = SpanResult.toIndex span' res
                    return struct (index' > span[0].Index, Choice1Of2 struct (index', struct (met, Stopwatch.elapsed prepareTs), outcome))
                with e -> return struct (false, Choice2Of2 struct (met, e)) }

            let interpretWriteResultProgress _streams (stream : FsCodec.StreamName) = function
                | Choice1Of2 struct (i', stats, outcome) ->
                    struct (ValueSome i', Choice1Of2 struct (stats, outcome))
                | Choice2Of2 struct (struct (eventCount, bytesCount) as stats, exn : exn) ->
                    log.Warning(exn, "Handling {events:n0}e {bytes:n0}b for {stream} failed, retrying", eventCount, bytesCount, stream)
                    ValueNone, Choice2Of2 struct (stats, exn)

            let dispatcher : Scheduling.IDispatcher<_, _, _, _> = Dispatcher.Concurrent<_, _, _, _>.Create(maxConcurrentStreams, attemptWrite, interpretWriteResultProgress)
            let dumpStreams logStreamStates log =
                logStreamStates eventSize
                match dumpExternalStats with Some f -> f log | None -> ()
            let scheduler =
                Scheduling.Engine<struct (int64 * struct (StreamSpan.Metrics * TimeSpan) * 'Outcome), struct (struct (StreamSpan.Metrics * TimeSpan) * 'Outcome), struct (StreamSpan.Metrics * exn), 'F>
                    (dispatcher, stats, dumpStreams, maxIngest = maxReadAhead, ?idleDelay = idleDelay, ?purgeInterval = purgeInterval)

            Projector.Pipeline.Start(log, scheduler.Pump, maxReadAhead, scheduler, statsInterval)

module Default =

    /// Canonical Data/Meta type supplied by the majority of Sources
    type EventBody = ReadOnlyMemory<byte>
    /// A contiguous set of Events from a Ordered stream, using the Canonical Data/Meta type
    type StreamSpan = StreamSpan<EventBody>
    /// A Single Event from an Ordered stream, using the Canonical Data/Meta type
    type StreamEvent = StreamEvent<EventBody>
    let eventSize (x : FsCodec.ITimelineEvent<EventBody>) = x.Size
    let jsonSize (x : FsCodec.ITimelineEvent<EventBody>) = eventSize x + 80

    type Sink = Sink<Ingestion.Ingester<StreamEvent seq>>

    type Config =

        static member Start<'Outcome>
            (   log, maxReadAhead, maxConcurrentStreams,
                handle : struct (FsCodec.StreamName * StreamSpan) -> Async<struct (SpanResult * 'Outcome)>,
                stats, statsInterval,
                // Configure max number of batches to ingest into scheduler; Default: Same as maxReadAhead
                ?maxIngest, ?purgeInterval,
                ?wakeForResults, ?idleDelay, ?ingesterStatsInterval, ?requireCompleteStreams)
            : Sink =
            StreamsSink.Start<'Outcome, EventBody>(
                log, maxReadAhead, maxConcurrentStreams, handle, stats, statsInterval, eventSize,
                ?maxIngest = maxIngest, ?purgeInterval = purgeInterval,
                ?wakeForResults = wakeForResults, ?idleDelay = idleDelay, ?ingesterStatsInterval = ingesterStatsInterval,
                ?requireCompleteStreams = requireCompleteStreams)
