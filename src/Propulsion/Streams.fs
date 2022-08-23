namespace Propulsion.Streams

open Propulsion
open Propulsion.Internal // Helpers
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

    let (|Ver|) (span : StreamSpan<'F>) = span[0].Index + span.LongLength
    let dropBeforeIndex min : FsCodec.ITimelineEvent<_> array -> FsCodec.ITimelineEvent<_> array = function
        | xs when xs.Length = 0 -> null
        | xs when xs[0].Index >= min -> xs // don't adjust if min not within
        | xs when (|Ver|) xs <= min -> null // throw away if before min
        | xs -> xs |> Array.skip (min - xs[0].Index |> int) // slice

    let merge min (spans : FsCodec.ITimelineEvent<_> array seq) =
        let candidates = ResizeArray()
        for span in spans do
            let trimmed = if span = null then null else dropBeforeIndex min span
            if trimmed <> null then
                if trimmed.Length = 0 then invalidOp "Cant add empty"
                candidates.Add trimmed
        let mutable buffer = null
        let mutable curr = ValueNone
        for x in candidates |> Seq.sortBy (fun x -> x[0].Index) do
            match curr with
            // Not overlapping, no data buffered -> buffer
            | ValueNone ->
                curr <- ValueSome x
            // Gap
            | ValueSome (Ver nextIndex as c) when x[0].Index > nextIndex ->
                if buffer = null then buffer <- ResizeArray()
                buffer.Add c
                curr <- ValueSome x
            // Overlapping, join
            | ValueSome (Ver nextIndex as c) when (|Ver|) x > nextIndex ->
                curr <- ValueSome (Array.append c (dropBeforeIndex nextIndex x))
            | _ -> () // drop
        match curr, buffer with
        | ValueSome x, null -> Array.singleton x
        | ValueSome x, b -> b.Add x; b.ToArray()
        | ValueNone, null -> null
        | ValueNone, b -> b.ToArray()

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
        member x.HasValid = not x.IsEmpty && not x.IsMalformed

        member x.WritePos = match x.write with WritePosUnknown -> ValueNone | x -> ValueSome x
        member x.HasGap = match x.write with w when w = WritePosUnknown -> false | w -> w <> x.HeadSpan[0].Index
        member x.CanPurge = x.IsEmpty && not x.IsMalformed

    module StreamState =

        let combine (s1 : StreamState<_>) (s2 : StreamState<_>) : StreamState<'Format> =
            let writePos = max s1.WritePos s2.WritePos
            let malformed = s1.IsMalformed || s2.IsMalformed
            let any1 = not (isNull s1.queue)
            let any2 = not (isNull s2.queue)
            if any1 || any2 then
                let items = if any1 && any2 then Seq.append s1.queue s2.queue elif any1 then s1.queue else s2.queue
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
            (log |> Log.metric m).Information(" Streams Waiting {busy:n0}/{busyMb:n1}MB", waiting, mb waitingB)
            if waitingCats.Any then log.Information(" Waiting Categories, events {@readyCats}", Seq.truncate 5 waitingCats.StatsDescending)
            if waitingCats.Any then log.Information(" Waiting Streams, KB {@readyStreams}", Seq.truncate 5 waitingStreams.StatsDescending)

    [<NoComparison; NoEquality>]
    type Batch<'Format> private (onCompletion, buffer, reqs) =
        let mutable buffer = ValueSome buffer
        static member Create(onCompletion, streamEvents : StreamEvent<'Format> seq) =
            let buffer, reqs = Streams<'Format>(), Dictionary<FsCodec.StreamName, int64>()
            let mutable itemCount = 0
            for struct (stream, event) in streamEvents do
                itemCount <- itemCount + 1
                buffer.Merge(stream, event)
                match reqs.TryGetValue(stream), event.Index + 1L with
                | (false, _), required -> reqs[stream] <- required
                | (true, actual), required when actual < required -> reqs[stream] <- required
                | (true, _), _ -> () // replayed same or earlier item
            let batch = Batch(onCompletion, buffer, reqs)
            struct (batch, struct (batch.RemainingStreamsCount, itemCount))

        member _.OnCompletion = onCompletion
        member _.Reqs = reqs :> seq<KeyValuePair<FsCodec.StreamName, int64>>
        member _.RemainingStreamsCount = reqs.Count
        member _.TryTakeStreams() = let t = buffer in buffer <- ValueNone; t
        member _.TryMerge(other : Batch<_>) =
            match buffer, other.TryTakeStreams() with
            | ValueSome x, ValueSome y -> x.Merge(y); true
            | ValueSome _, ValueNone -> false
            | ValueNone, x -> buffer <- x; false

/// A separate Async pump manages dispatching of scheduled work via the ThreadPool , managing the concurrency limits
module Dispatch =

    [<Struct; NoComparison; NoEquality>]
    type Item<'Format> = { stream : FsCodec.StreamName; writePos : int64 voption; span : FsCodec.ITimelineEvent<'Format> array }

    /// Coordinates the dispatching of work and emission of results, subject to the maxDop concurrent processors constraint
    type internal DopDispatcher<'R>(maxDop : int) =
        let tryWrite, wait, apply =
            let c = Channel.unboundedSwSr<CancellationToken -> Task<'R>> in let r, w = c.Reader, c.Writer
            w.TryWrite, Channel.awaitRead r, Channel.apply r >> ignore
        let result = Event<'R>()
        let dop = Sem maxDop

        // NOTE this obviously depends on the passed computation never throwing, or we'd leak dop
        let wrap ct (computation : CancellationToken -> Task<'R>) () = task {
            let! res = computation ct
            dop.Release()
            result.Trigger res }

        [<CLIEvent>] member _.Result = result.Publish
        member _.HasCapacity = dop.HasCapacity
        member _.AwaitButRelease() = dop.AwaitButRelease()
        member _.State = dop.State

        member _.TryAdd(item) =
            dop.TryTake() && tryWrite item

        member _.Pump(ct : CancellationToken) = task {
            while not ct.IsCancellationRequested do
                try do! wait ct :> Task
                with :? OperationCanceledException -> ()
                let run (f : CancellationToken -> Task<'R>) = Task.start (wrap ct f)
                apply run }

    /// Kicks off enough work to fill the inner Dispatcher up to capacity
    type ItemDispatcher<'R, 'F>(maxDop) =
        let inner = DopDispatcher<struct (TimeSpan * FsCodec.StreamName * bool * 'R)>(maxDop)

        // On each iteration, we try to fill the in-flight queue, taking the oldest and/or heaviest streams first
        let tryFillDispatcher (potential : seq<Item<'F>>) markStarted project markBusy =
            let xs = potential.GetEnumerator()
            let ts = System.Diagnostics.Stopwatch.GetTimestamp()
            let mutable hasCapacity, dispatched = true, false
            while xs.MoveNext() && hasCapacity do
                let item = xs.Current
                let succeeded = inner.TryAdd(project ts item)
                if succeeded then
                    markBusy item.stream
                    markStarted (item.stream, ts)
                hasCapacity <- succeeded
                dispatched <- dispatched || succeeded // if we added any request, we'll skip sleeping
            struct (dispatched, hasCapacity)

        member _.Pump ct = inner.Pump ct
        [<CLIEvent>] member _.Result = inner.Result
        member _.State = inner.State
        member _.TryReplenish(pending, markStarted, project, markStreamBusy) =
            tryFillDispatcher pending markStarted project markStreamBusy
        member _.HasCapacity = inner.HasCapacity
        member _.AwaitCapacity() = inner.AwaitButRelease()

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
                let v = x.Value
                if v.CanPurge then
                    states.Remove x.Key |> ignore // Safe to do while iterating on netcore >=3.0
                    purged <- purged + 1
            states.Count, purged

        let busy = HashSet<FsCodec.StreamName>()
        let pending trySlipstreamed (requestedOrder : FsCodec.StreamName seq) : seq<Dispatch.Item<'Format>> = seq {
            let proposed = HashSet()
            for s in requestedOrder do
                match tryGetItem s with
                | ValueSome ss when ss.HasValid && not (busy.Contains s) ->
                    proposed.Add s |> ignore // should always be true
                    yield { writePos = ss.WritePos; stream = s; span = ss.HeadSpan }
                | _ -> ()
            if trySlipstreamed then
                // [lazily] slipstream in further events that are not yet referenced by in-scope batches
                for KeyValue(s, ss) in states do
                    if ss.HasValid && not (busy.Contains s) && proposed.Add s then
                        yield { writePos = ss.WritePos; stream = s; span = ss.HeadSpan } }
        let markBusy stream = busy.Add stream |> ignore
        let markNotBusy stream = busy.Remove stream |> ignore

        member _.WritePositionIsAlreadyBeyond(stream, required) =
            match tryGetItem stream with
            | ValueSome ss -> match ss.WritePos with ValueSome cw -> cw >= required | _ -> false
            | _ -> false
        member _.Merge(streams : Streams<'Format>) =
            for kv in streams.States do
                merge kv.Key kv.Value |> ignore
        member _.RecordWriteProgress(stream, pos, queue) =
            merge stream (StreamState<'Format>.Create(ValueSome pos, queue))
        member _.SetMalformed(stream, isMalformed) =
            updateWritePos stream isMalformed ValueNone null
        member _.Purge() =
            purge ()

        member _.HeadSpanSizeBy(stream, f : _ -> int) =
            match tryGetItem stream with
            | ValueSome state when not state.IsEmpty -> state.HeadSpan |> Array.sumBy f |> int64
            | _ -> 0L

        member _.MarkBusy stream =
            markBusy stream

        member _.MarkCompleted(stream, index) =
            markNotBusy stream
            markCompleted stream index

        member _.MarkFailed stream =
            markNotBusy stream

        member _.Pending(trySlipstreamed, byQueuedPriority : FsCodec.StreamName seq) : Dispatch.Item<'Format> seq =
            pending trySlipstreamed byQueuedPriority

        member _.Dump(log : ILogger, totalPurged : int, eventSize) =
            let mutable (busyCount, busyE, busyB), (ready, readyE, readyB), synced = (0, 0, 0L), (0, 0, 0L), 0
            let mutable (gaps, gapsE, gapsB), (malformed, malformedE, malformedB) = (0, 0, 0L), (0, 0, 0L)
            let busyCats, readyCats, readyStreams = Stats.CatStats(), Stats.CatStats(), Stats.CatStats()
            let gapCats, gapStreams, malformedCats, malformedStreams = Stats.CatStats(), Stats.CatStats(), Stats.CatStats(), Stats.CatStats()
            let kb sz = (sz + 512L) / 1024L
            for KeyValue (stream, state) in states do
                match state.EventsSumBy(eventSize) with
                | 0L ->
                    synced <- synced + 1
                | sz when busy.Contains stream ->
                    busyCats.Ingest(StreamName.categorize stream)
                    busyCount <- busyCount + 1
                    busyB <- busyB + sz
                    busyE <- busyE + state.EventsCount
                | sz when state.IsMalformed ->
                    malformedCats.Ingest(StreamName.categorize stream)
                    malformedStreams.Ingest(FsCodec.StreamName.toString stream, mb sz |> int64)
                    malformed <- malformed + 1
                    malformedB <- malformedB + sz
                    malformedE <- malformedE + state.EventsCount
                | sz when state.HasGap ->
                    gapCats.Ingest(StreamName.categorize stream)
                    gapStreams.Ingest(FsCodec.StreamName.toString stream, mb sz |> int64)
                    gaps <- gaps + 1
                    gapsB <- gapsB + sz
                    gapsE <- gapsE + state.EventsCount
                | sz ->
                    readyCats.Ingest(StreamName.categorize stream)
                    readyStreams.Ingest(sprintf "%s@%dx%d" (FsCodec.StreamName.toString stream) (defaultValueArg state.WritePos 0L) state.HeadSpan.Length, kb sz)
                    ready <- ready + 1
                    readyB <- readyB + sz
                    readyE <- readyE + state.EventsCount
            let busyStats : Log.BufferMetric = { cats = busyCats.Count; streams = busyCount; events = busyE; bytes = busyB }
            let readyStats : Log.BufferMetric = { cats = readyCats.Count; streams = readyStreams.Count; events = readyE; bytes = readyB }
            let bufferingStats : Log.BufferMetric = { cats = gapCats.Count; streams = gapStreams.Count; events = gapsE; bytes = gapsB }
            let malformedStats : Log.BufferMetric = { cats = malformedCats.Count; streams = malformedStreams.Count; events = malformedE; bytes = malformedB }
            let m = Log.Metric.SchedulerStateReport (synced, busyStats, readyStats, bufferingStats, malformedStats)
            (log |> Log.metric m).Information("Streams Synced {synced:n0} Purged {purged:n0} Active {busy:n0}/{busyMb:n1}MB Ready {ready:n0}/{readyMb:n1}MB Waiting {waiting}/{waitingMb:n1}MB Malformed {malformed}/{malformedMb:n1}MB",
                synced, totalPurged, busyCount, mb busyB, ready, mb readyB, gaps, mb gapsB, malformed, mb malformedB)
            if busyCats.Any then log.Information(" Active Categories, events {@busyCats}", Seq.truncate 5 busyCats.StatsDescending)
            if readyCats.Any then log.Information(" Ready Categories, events {@readyCats}", Seq.truncate 5 readyCats.StatsDescending)
            if readyCats.Any then log.Information(" Ready Streams, KB {@readyStreams}", Seq.truncate 5 readyStreams.StatsDescending)
            if gapStreams.Any then log.Information(" Buffering Streams, KB {@missingStreams}", Seq.truncate 3 gapStreams.StatsDescending)
            if malformedStreams.Any then log.Information(" Malformed Streams, MB {@malformedStreams}", malformedStreams.StatsDescending)

    /// Messages used internally by projector, including synthetic ones for the purposes of the `Stats` listeners
    [<NoComparison; NoEquality>]
    type InternalMessage<'R> =
        /// Stats per submitted batch for stats listeners to aggregate
        | Added of streams : int * skip : int * events : int
        /// Result of processing on stream - result (with basic stats) or the `exn` encountered
        | Result of duration : TimeSpan * stream : FsCodec.StreamName * progressed : bool * result : 'R

    type [<Struct>] BufferState = Idle | Active | Full | Slipstreaming

    module Stats =

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

        type StateStats() =
            let mutable idle, active, full, slip = 0, 0, 0, 0
            member _.Clear() = idle <- 0; active <- 0; full <- 0; slip <- 0
            member _.Ingest state =
                match state with
                | Idle -> idle <- idle + 1
                | Active -> active <- active + 1
                | Full -> full <- full + 1
                | Slipstreaming -> slip <- slip + 1
            member _.StatsDescending =
                let t = Stats.CatStats()
                if idle > 0   then t.Ingest(nameof Idle, idle)
                if active > 0 then t.Ingest(nameof Active, active)
                if full > 0   then t.Ingest(nameof Full, full)
                if slip > 0   then t.Ingest(nameof Slipstreaming, slip)
                t.StatsDescending

    /// Gathers stats pertaining to the core projection/ingestion activity
    [<AbstractClass>]
    type Stats<'R, 'E>(log : ILogger, statsInterval : TimeSpan, stateInterval : TimeSpan) =
        let mutable cycles, fullCycles = 0, 0
        let stateStats, oks, exns, mon = Stats.StateStats(), Stats.LatencyStats("ok"), Stats.LatencyStats("exceptions"), Stats.Busy.Monitor()
        let mutable batchesPended, streamsPended, eventsSkipped, eventsPended = 0, 0, 0, 0
        let statsDue, stateDue, stucksDue = intervalCheck statsInterval, intervalCheck stateInterval, intervalCheck (TimeSpan.FromSeconds 1.)
        let metricsLog = log.ForContext("isMetric", true)
        let mutable dt, ft, mt, it, st, zt = TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero
        let dumpStats (dispatchActive, dispatchMax) batchesWaiting =
            log.Information("Scheduler {cycles} cycles ({fullCycles} full) {@states} Running {busy}/{processors}",
                cycles, fullCycles, stateStats.StatsDescending, dispatchActive, dispatchMax)
            cycles <- 0; fullCycles <- 0; stateStats.Clear()
            oks.Dump log; exns.Dump log
            log.Information(" Batches Holding {batchesWaiting} Started {batches} ({streams:n0}s {events:n0}-{skipped:n0}e)",
                batchesWaiting, batchesPended, streamsPended, eventsSkipped + eventsPended, eventsSkipped)
            batchesPended <- 0; streamsPended <- 0; eventsSkipped <- 0; eventsPended <- 0
            let m = Log.Metric.SchedulerCpu (mt, it, ft, dt, st)
            (log |> Log.metric m).Information(" Cpu Streams {mt:n1}s Batches {it:n1}s Dispatch {ft:n1}s Results {dt:n1}s Stats {st:n1}s Sleep {zt:n1}s",
                mt.TotalSeconds, it.TotalSeconds, ft.TotalSeconds, dt.TotalSeconds, st.TotalSeconds, zt.TotalSeconds)
            dt <- TimeSpan.Zero; ft <- TimeSpan.Zero; mt <- TimeSpan.Zero; it <- TimeSpan.Zero; st <- TimeSpan.Zero; zt <- TimeSpan.Zero

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

        member x.DumpStats(struct (used, max), batchesWaiting) =
            cycles <- cycles + 1
            if statsDue () then
                dumpStats (used, max) batchesWaiting
                mon.DumpState log
                x.DumpStats()

        member _.TryDumpState(state, _dt, _ft, _mt, _it, _st, _zt) =
            dt <- dt + _dt
            ft <- ft + _ft
            mt <- mt + _mt
            it <- it + _it
            st <- st + _st
            zt <- zt + _zt
            fullCycles <- fullCycles + 1
            stateStats.Ingest(state)

            if stateDue () then ValueSome log else ValueNone

        /// Allows an ingester or projector to wire in custom stats (typically based on data gathered in a `Handle` override)
        abstract DumpStats : unit -> unit
        default _.DumpStats () = ()

    module Dispatcher =

        /// Defines interface between Scheduler (which owns the pending work) and the Dispatcher which periodically selects work to commence based on a policy
        type IDispatcher<'P, 'R, 'E, 'F> =
            abstract member HasCapacity : bool with get
            abstract member AwaitCapacity : unit -> Task<unit>
            abstract member TryReplenish : pending : seq<Dispatch.Item<'F>> * markStreamBusy : (FsCodec.StreamName -> unit) -> struct (bool * bool)
            [<CLIEvent>] abstract member Result : IEvent<struct (TimeSpan * FsCodec.StreamName * bool * Choice<'P, 'E>)>
            abstract member InterpretProgress : StreamStates<'F> * FsCodec.StreamName * Choice<'P, 'E> -> struct (int64 voption * Choice<'R, 'E>)
            abstract member RecordResultStats : InternalMessage<Choice<'R, 'E>> -> unit
            abstract member DumpStats : int -> unit
            abstract member TryDumpState : BufferState * StreamStates<'F> * int * TimeSpan * TimeSpan * TimeSpan * TimeSpan * TimeSpan * TimeSpan -> bool

        /// Implementation of IDispatcher that feeds items to an item dispatcher that maximizes concurrent requests (within a limit)
        type MultiDispatcher<'P, 'R, 'E, 'F>
            (   inner : Dispatch.ItemDispatcher<Choice<'P, 'E>, 'F>,
                project : int64 -> Dispatch.Item<'F> -> CancellationToken -> Task<struct (TimeSpan * FsCodec.StreamName * bool * Choice<'P, 'E>)>,
                interpretProgress : StreamStates<'F> -> FsCodec.StreamName -> Choice<'P, 'E> -> struct (int64 voption * Choice<'R, 'E>),
                stats : Stats<'R, 'E>,
                dumpState : ((FsCodec.ITimelineEvent<'F> -> int) -> unit) -> ILogger -> unit) =
            static member Create(inner,
                    project : struct (FsCodec.StreamName * StreamSpan<'F>) -> CancellationToken -> Task<struct (bool * Choice<'P, 'E>)>,
                    interpretProgress, stats, dumpStreams) =
                let project sw (item : Dispatch.Item<'F>) (ct : CancellationToken) = task {
                    let! progressed, res = project (item.stream, item.span) ct
                    let now = System.Diagnostics.Stopwatch.GetTimestamp()
                    return struct (Stats.Busy.timeSpanFromStopwatchTicks (now - sw), item.stream, progressed, res) }
                MultiDispatcher<_, _, _, _>(inner, project, interpretProgress, stats, dumpStreams)
            static member Create(inner, handle, interpret, toIndex, stats, dumpStreams) =
                let project item ct = task {
                    let struct (met, (struct (_sn, span : StreamSpan<'F>) as ss)) = interpret item
                    try let! struct (spanResult, outcome) = handle ss |> fun f -> Async.StartAsTask(f, cancellationToken = ct)
                        let index' = toIndex span spanResult
                        return struct (index' > span[0].Index, Choice1Of2 struct (index', met, outcome))
                    with e -> return struct (false, Choice2Of2 struct (met, e)) }
                let interpretProgress (_streams : StreamStates<'F>) _stream = function
                    | Choice1Of2 struct (index, met, outcome) -> struct (ValueSome index, Choice1Of2 struct (met, outcome))
                    | Choice2Of2 struct (stats, exn) -> ValueNone, Choice2Of2 struct (stats, exn)
                MultiDispatcher<_, _, _, 'F>.Create(inner, project, interpretProgress, stats, dumpStreams)
            interface IDispatcher<'P, 'R, 'E, 'F> with
                override _.HasCapacity = inner.HasCapacity
                override _.AwaitCapacity() = inner.AwaitCapacity()
                override _.TryReplenish(pending, markStreamBusy) =
                    inner.TryReplenish(pending, stats.MarkStarted, project, markStreamBusy)
                [<CLIEvent>] override _.Result = inner.Result
                override _.InterpretProgress(streams, stream : FsCodec.StreamName, res : Choice<'P, 'E>) =
                    interpretProgress streams stream res
                override _.RecordResultStats msg = stats.Handle msg
                override _.DumpStats pendingCount = stats.DumpStats(inner.State, pendingCount)
                override _.TryDumpState(dispatcherState, streams, totalPurged, dt, ft, mt, it, st, zt) =
                    match stats.TryDumpState(dispatcherState, dt, ft, mt, it, st, zt) with
                    | ValueNone -> false
                    | ValueSome log ->
                        let dumpStreamStates (eventSize : FsCodec.ITimelineEvent<'F> -> int) = streams.Dump(log, totalPurged, eventSize)
                        dumpState dumpStreamStates log
                        true

        /// Implementation of IDispatcher that allows a supplied handler select work and declare completion based on arbitrarily defined criteria
        type BatchedDispatcher<'F>
            (   select : Dispatch.Item<'F> seq -> Dispatch.Item<'F> array,
                handle : Dispatch.Item<'F> array -> CancellationToken ->
                         Task<struct (TimeSpan * FsCodec.StreamName * bool * Choice<struct (int64 * struct (StreamSpan.Metrics * unit)), struct (StreamSpan.Metrics * exn)>) array>,
                stats : Stats<_, _>,
                dumpState) =
            let dop = Dispatch.DopDispatcher 1
            let result = Event<struct (TimeSpan * FsCodec.StreamName * bool * Choice<struct (int64 * struct (StreamSpan.Metrics * unit)), struct (StreamSpan.Metrics * exn)>)>()

            // On each iteration, we offer the ordered work queue to the selector
            // we propagate the selected streams to the handler
            let trySelect (potential : seq<Dispatch.Item<'F>>) markBusy =
                let mutable hasCapacity, dispatched = true, false
                let streams : Dispatch.Item<'F>[] = select potential
                let succeeded = (not << Array.isEmpty) streams
                if succeeded then
                    let res = dop.TryAdd(handle streams)
                    if not res then failwith "Checked we can add, what gives?"
                    for x in streams do
                        markBusy x.stream
                    dispatched <- true // if we added any request, we'll skip sleeping
                    hasCapacity <- false
                struct (dispatched, hasCapacity)

            member _.Pump ct = task {
                use _ = dop.Result.Subscribe(Array.iter result.Trigger)
                return! dop.Pump ct }

            interface IDispatcher<struct (int64 * struct (StreamSpan.Metrics * unit)), struct (StreamSpan.Metrics * unit), struct (StreamSpan.Metrics * exn), 'F> with
                override _.HasCapacity = dop.HasCapacity
                override _.AwaitCapacity() = dop.AwaitButRelease()
                override _.TryReplenish(pending, markStreamBusy) = trySelect pending markStreamBusy
                [<CLIEvent>] override _.Result = result.Publish
                override _.InterpretProgress(_streams : StreamStates<_>, _stream : FsCodec.StreamName, res : Choice<_, _>) =
                    match res with
                    | Choice1Of2 (pos', (stats, outcome)) -> ValueSome pos', Choice1Of2 (stats, outcome)
                    | Choice2Of2 (stats, exn) -> ValueNone, Choice2Of2 (stats, exn)
                override _.RecordResultStats msg = stats.Handle msg
                override _.DumpStats pendingCount = stats.DumpStats(dop.State, pendingCount)
                override _.TryDumpState(dispatcherState, streams, totalPurged, dt, ft, mt, it, st, zt) =
                    match stats.TryDumpState(dispatcherState, dt, ft, mt, it, st, zt) with
                    | ValueNone -> false
                    | ValueSome log ->
                        let dumpStreamStates (eventSize : FsCodec.ITimelineEvent<'F> -> int) = streams.Dump(log, totalPurged, eventSize)
                        dumpState dumpStreamStates log
                        true

    module Progress =

        type [<Struct; NoComparison; NoEquality>] internal BatchState = { markCompleted : unit -> unit; streamToRequiredIndex : Dictionary<FsCodec.StreamName, int64> }

        type ProgressState<'Pos>() =
            let pending = Queue<BatchState>()
            let trim () =
                while pending.Count <> 0 && pending.Peek().streamToRequiredIndex.Count = 0 do
                    let batch = pending.Dequeue()
                    batch.markCompleted()

            // We potentially traverse the pending streams thousands of times per second
            // so we reuse `InScheduledOrder`'s temps: sortBuffer and streamsBuffer for better L2 caching properties
            let sortBuffer = ResizeArray()
            let streamsBuffer = HashSet()

            member _.AppendBatch(markCompleted, reqs : Dictionary<FsCodec.StreamName, int64>) =
                pending.Enqueue { markCompleted = markCompleted; streamToRequiredIndex = reqs }
                trim ()

            member _.MarkStreamProgress(stream, index) =
                let mutable requiredIndex = Unchecked.defaultof<_>
                for x in pending do
                    if x.streamToRequiredIndex.TryGetValue(stream, &requiredIndex) && requiredIndex <= index then
                        x.streamToRequiredIndex.Remove stream |> ignore
                trim ()

            member _.IsEmpty = pending.Count = 0
            // NOTE internal reuse of `sortBuffer` and `streamsBuffer` means it's critical to never have >1 of these in flight
            member _.InScheduledOrder(getStreamWeight : (FsCodec.StreamName -> int64) option) : seq<FsCodec.StreamName> =
                trim ()
                // sortBuffer is used once per invocation, but the result is lazy so we can only clear it on entry
                sortBuffer.Clear()
                let mutable batch = 0
                let weight = match getStreamWeight with None -> (fun _s -> 0) | Some f -> (fun s -> -f s |> int)
                for x in pending do
                    for s in x.streamToRequiredIndex.Keys do
                        if streamsBuffer.Add s then
                            // Within the head batch, expedite work on longest streams, where requested
                            let w = if batch = 0 then weight s else 0 // For anything other than the head batch, submitted order is just fine
                            sortBuffer.Add(struct (s, batch, w))
                    if batch = 0 && sortBuffer.Count > 0 then
                        let c = Comparer<_>.Default
                        sortBuffer.Sort(fun struct (_, _ab, _aw) struct (_, _bb, _bw) -> c.Compare(struct(_ab, _aw), struct(_bb, _bw)))
                    batch <- batch + 1
                // We reuse this buffer next time around, but clear it now as it has no further use
                streamsBuffer.Clear()
                sortBuffer |> Seq.map (fun struct(s, _, _) -> s)

    /// Consolidates ingested events into streams; coordinates dispatching of these to projector/ingester in the order implied by the submission order
    /// a) does not itself perform any reading activities
    /// b) triggers synchronous callbacks as batches complete; writing of progress is managed asynchronously by the TrancheEngine(s)
    /// c) submits work to the supplied Dispatcher (which it triggers pumping of)
    /// d) periodically reports state (with hooks for ingestion engines to report same)
    type StreamSchedulingEngine<'P, 'R, 'E, 'F>
        (   dispatcher : Dispatcher.IDispatcher<'P, 'R, 'E, 'F>,
            // Tune number of batches to ingest at a time. Default 1.
            ?maxBatches,
            // Tune the max number of check/dispatch cycles. Default 2.
            // Frequency of jettisoning Write Position state of inactive streams (held by the scheduler for deduplication purposes) to limit memory consumption
            // NOTE: Purging can impair performance, increase write costs or result in duplicate event emissions due to redundant inputs not being deduplicated
            ?purgeInterval,
            // The default behavior is to wait for dispatch capacity
            // Enables having the scheduler service results immediately (quicker feedback on batch completions), at the cost of increased CPU usage (esp given in normal conditions, the `idleDelay` is not onerous and incoming batches or dispatch capacity becoming available typically are proxies)
            ?wakeForResults,
            // Tune the sleep time when there are no items to schedule or items to dispatch. Default 1s.
            ?idleDelay,
            // Tune the number of times to attempt handling results before sleeping. Default 3.
            ?maxCycles,
            // Prioritize processing of the largest Payloads within the Tip batch when scheduling work
            // Can yield significant throughput improvement when ingesting large batches in the face of rate limiting
            ?prioritizeStreamsBy,
            // Opt-in to allowing items to be processed independent of batch sequencing - requires upstream/projection function to be able to identify gaps. Default false.
            ?enableSlipstreaming) =
        let purgeDue = purgeInterval |> Option.map intervalCheck
        let sleepIntervalMs =
            let idleDelay = defaultArg idleDelay (TimeSpan.FromSeconds 1.)
            int idleDelay.TotalMilliseconds
        let wakeForResults = defaultArg wakeForResults false
        let maxCycles, maxBatches, slipstreamingEnabled = defaultArg maxCycles 3, defaultArg maxBatches 1, defaultArg enableSlipstreaming false
        let writeResult, awaitResults, tryApplyResults =
            let r, w = let c = Channel.unboundedSr in c.Reader, c.Writer
            Channel.write w >> ignore, Channel.awaitRead r, Channel.apply r
        let writePending, pendingCount, awaitPending, tryReadPending =
            let r, w = let c = Channel.unboundedSw<Batch<_>> in c.Reader, c.Writer // Actually SingleReader too, but Count throws if you use that
            Channel.write w >> ignore, (fun () -> r.Count), Channel.awaitRead r, Channel.tryRead r
        let streams = StreamStates<'F>()
        let weight f stream = streams.HeadSpanSizeBy(stream, f)
        let maybePrioritizeLargePayloads = match prioritizeStreamsBy with Some f -> Some (weight f) | None -> None
        let progressState = Progress.ProgressState()
        let mutable totalPurged = 0

        let tryDispatch isSlipStreaming =
            let hasCapacity = dispatcher.HasCapacity
            if not hasCapacity || (progressState.IsEmpty && not isSlipStreaming) then struct (false, hasCapacity) else

            let pending : seq<Dispatch.Item<_>> = streams.Pending(isSlipStreaming, progressState.InScheduledOrder maybePrioritizeLargePayloads)
            dispatcher.TryReplenish(pending, streams.MarkBusy)

        // ingest information to be gleaned from processing the results into `streams`
        let mapResult : InternalMessage<_> -> InternalMessage<Choice<'R, 'E>> = function
            | Added (streams, skipped, events) ->
                // Only processed in Stats (and actually never enters this queue)
                Added (streams, skipped, events)
            | Result (duration, stream : FsCodec.StreamName, progressed : bool, res : Choice<'P, 'E>) ->
                match dispatcher.InterpretProgress(streams, stream, res) with
                | ValueNone, Choice1Of2 (r : 'R) ->
                    streams.MarkFailed(stream)
                    Result (duration, stream, progressed, Choice1Of2 r)
                | ValueSome index, Choice1Of2 (r : 'R) ->
                    progressState.MarkStreamProgress(stream, index)
                    streams.MarkCompleted(stream, index)
                    Result (duration, stream, progressed, Choice1Of2 r)
                | _, Choice2Of2 exn ->
                    streams.MarkFailed(stream)
                    Result (duration, stream, progressed, Choice2Of2 exn)
        let mapRecord = mapResult >> dispatcher.RecordResultStats
        let tryHandleResults () = tryApplyResults mapRecord

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
        let ingestBatch (batch : Batch<_>) = ingestPendingBatch dispatcher.RecordResultStats (batch.OnCompletion, batch.Reqs)

        let maybePurge () =
            // After we've dumped the state, it may also be due for pruning
            match purgeDue with
            | Some dueNow when dueNow () ->
                let remaining, purged = streams.Purge()
                totalPurged <- totalPurged + purged
                let l = if purged = 0 then Events.LogEventLevel.Debug else Events.LogEventLevel.Information
                Log.Write(l, "PURGED Remaining {buffered:n0} Purged now {count:n0} Purged total {total:n0}", remaining, purged, totalPurged)
            | _ -> ()

        member _.Pump _abend (ct : CancellationToken) = task {
            use _ = dispatcher.Result.Subscribe(fun struct (t, s, pr, r) -> writeResult (Result (t, s, pr, r)))
            let mutable zt = TimeSpan.Zero
            let inline startSw () = System.Diagnostics.Stopwatch.StartNew()
            while not ct.IsCancellationRequested do
                let mutable mt = TimeSpan.Zero
                let mergeStreams batchStreams = let sw = startSw () in streams.Merge batchStreams; mt <- mt + sw.Elapsed

                let mutable it = TimeSpan.Zero
                let ingestBatch batch = let sw = startSw () in ingestBatch batch; it <- it + sw.Elapsed

                let mutable dispatcherState = Idle
                let mutable remaining = maxCycles
                let mutable waitForCapacity, waitForPending = Unchecked.defaultof<_>
                let rec tryIngest batchesRemaining =
                    match tryReadPending () with
                    | ValueSome batch ->
                        // Accommodate where stream-wise merges have been performed preemptively in the ingester
                        batch.TryTakeStreams() |> ValueOption.iter mergeStreams
                        ingestBatch batch
                        match batchesRemaining - 1 with
                        | 0 -> false // no need to wait, we filled the capacity
                        | r -> tryIngest r
                    | ValueNone ->
                        if batchesRemaining <> maxBatches then false // already added some items, so we don't need to wait for pending
                        elif slipstreamingEnabled then dispatcherState <- Slipstreaming; false
                        else remaining <- 0; true // definitely need to wait as there were no items
                let mutable dt, ft, st = Unchecked.defaultof<_>
                let mutable idle = true
                while remaining <> 0 do
                    remaining <- remaining - 1
                    // 1. propagate write write outcomes to buffer (can mark batches completed etc)
                    let processedResults = let sw = startSw () in let r = tryHandleResults () in dt <- dt + sw.Elapsed; r
                    // 2. top up provisioning of writers queue
                    // On each iteration, we try to fill the in-flight queue, taking the oldest and/or heaviest streams first
                    let struct (dispatched, hasCapacity) = let sw = startSw () in let r = tryDispatch (dispatcherState = Slipstreaming) in ft <- ft + sw.Elapsed; r
                    idle <- idle && not processedResults && not dispatched
                    match dispatcherState with
                    | Idle when not hasCapacity ->
                        // If we've achieved full state, spin around the loop to dump stats and ingest reader data
                        dispatcherState <- Full
                        remaining <- 0
                    | Idle when remaining = 0 ->
                        dispatcherState <- Active
                    | Idle -> // need to bring more work into the pool as we can't fill the work queue from what we have
                        // If we're going to fill the write queue with random work, we should bring all read events into the state first
                        // Hence we potentially take more than one batch at a time based on maxBatches (but less buffered work is more optimal)
                        waitForPending <- tryIngest maxBatches
                    | Slipstreaming -> // only do one round of slipstreaming
                        remaining <- 0
                    | Active | Full -> failwith "Not handled here"
                    if remaining = 0 && hasCapacity then waitForPending <- true
                    if remaining = 0 && not hasCapacity && not wakeForResults then waitForCapacity <- true
                // While the loop can take a long time, we don't attempt logging of stats per iteration on the basis that the maxCycles should be low
                let sw = startSw () in dispatcher.DumpStats(pendingCount()); st <- st + sw.Elapsed
                // 3. Record completion state once per full iteration; dumping streams is expensive so needs to be done infrequently
                let dumped = dispatcher.TryDumpState(dispatcherState, streams, totalPurged, dt, ft, mt, it, st, zt)
                zt <- TimeSpan.Zero
                if dumped then maybePurge ()
                elif idle then
                    // 4. Do a minimal sleep so we don't run completely hot when empty (unless we did something non-trivial)
                    let wakeConditions : Task array = [|
                        if wakeForResults then awaitResults ct
                        elif waitForCapacity then dispatcher.AwaitCapacity()
                        if waitForPending then awaitPending ct
                        Task.Delay(int sleepIntervalMs) |]
                    let sw = System.Diagnostics.Stopwatch.StartNew()
                    do! Task.WhenAny(wakeConditions) :> Task
                    zt <- sw.Elapsed }

        member _.Submit(x : Batch<_>) =
            writePending x

    type StreamSchedulingEngine =

        static member Create<'Metrics, 'Progress, 'Outcome, 'F>
            (   itemDispatcher : Dispatch.ItemDispatcher<Choice<struct (int64 * 'Metrics * 'Outcome), struct ('Metrics * exn)>, 'F>,
                stats : Stats<struct ('Metrics * 'Outcome), struct ('Metrics * exn)>,
                prepare : struct (FsCodec.StreamName * FsCodec.ITimelineEvent<'F> array) -> struct ('Metrics * struct (FsCodec.StreamName * FsCodec.ITimelineEvent<'F> array)),
                handle : struct (FsCodec.StreamName * FsCodec.ITimelineEvent<'F> array) -> Async<struct ('Progress * 'Outcome)>,
                toIndex : StreamSpan<'F> -> 'Progress -> int64,
                dumpStreams, ?maxBatches, ?purgeInterval, ?wakeForResults, ?idleDelay, ?enableSlipstreaming)
            : StreamSchedulingEngine<struct (int64 * 'Metrics * 'Outcome), struct ('Metrics * 'Outcome), struct ('Metrics * exn), 'F> =

            let dispatcher = Dispatcher.MultiDispatcher<_, _, _, 'F>.Create(itemDispatcher, handle, prepare, toIndex, stats, dumpStreams)
            StreamSchedulingEngine<_, _, _, 'F>(
                dispatcher,
                ?maxBatches = maxBatches, ?purgeInterval = purgeInterval, ?wakeForResults = wakeForResults,
                ?idleDelay = idleDelay, ?enableSlipstreaming = enableSlipstreaming)

        static member Create(dispatcher, ?maxBatches, ?purgeInterval, ?wakeForResults, ?idleDelay, ?enableSlipstreaming)
            : StreamSchedulingEngine<struct (int64 * struct ('Metrics * unit)), struct ('Stats * unit), struct ('Stats * exn), 'F> =
            StreamSchedulingEngine<_, _, _, 'F>(
                dispatcher,
                ?maxBatches = maxBatches, ?purgeInterval = purgeInterval, ?wakeForResults = wakeForResults,
                ?idleDelay = idleDelay, ?enableSlipstreaming = enableSlipstreaming)

[<AbstractClass>]
type Stats<'Outcome>(log : ILogger, statsInterval, statesInterval) =
    inherit Scheduling.Stats<struct (StreamSpan.Metrics * 'Outcome), struct (StreamSpan.Metrics * exn)>(log, statsInterval, statesInterval)
    let okStreams, failStreams, badCats = HashSet(), HashSet(), Stats.CatStats()
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

        static member Start(log, partitionId, maxRead, submit, statsInterval) =
            let submitBatch (items : StreamEvent<_> seq, onCompletion) =
                let items = Array.ofSeq items
                let streams = items |> Seq.map ValueTuple.fst |> HashSet
                let batch : Submission.Batch<_, _> = { source = partitionId; onCompletion = onCompletion; messages = items }
                submit batch
                struct (streams.Count, items.Length)
            Ingestion.Ingester<StreamEvent<_> seq>.Start(log, partitionId, maxRead, submitBatch, statsInterval)

    type StreamsSubmitter =

        static member Create
            (   log : ILogger, maxSubmissionsPerPartition, mapBatch, submitStreamsBatch, statsInterval,
                ?disableCompaction) =
            let submitBatch (x : Buffer.Batch<_>) : int =
                submitStreamsBatch x
                x.RemainingStreamsCount
            let tryCompactQueueImpl (queue : Queue<Buffer.Batch<_>>) =
                let mutable acc, worked = None, false
                for x in queue do
                    match acc with
                    | None -> acc <- Some x
                    | Some a -> if a.TryMerge x then worked <- true
                worked
            let tryCompactQueue = if defaultArg disableCompaction false then None else Some tryCompactQueueImpl
            Submission.SubmissionEngine<_, _, _>(log, maxSubmissionsPerPartition, mapBatch, submitBatch, statsInterval, ?tryCompactQueue=tryCompactQueue)

    type Pipeline =

        static member Start
            (   log : ILogger, pumpDispatcher, pumpScheduler, maxReadAhead, submitStreamsBatch, statsInterval,
                // Limits number of batches passed to the scheduler.
                // Holding items back makes scheduler processing more efficient as less state needs to be traversed.
                // Holding items back is also key to enabling the compaction mechanism in the submitter to take effect.
                // Defaults to holding back 20% of maxReadAhead per partition
                ?maxSubmissionsPerPartition,
                ?ingesterStatsInterval) =
            let ingesterStatsInterval = defaultArg ingesterStatsInterval statsInterval
            let mapBatch onCompletion (x : Submission.Batch<_, StreamEvent<'F>>) : Buffer.Batch<'F> =
                let onCompletion () = x.onCompletion(); onCompletion()
                Buffer.Batch.Create(onCompletion, x.messages) |> fun struct (f, _s) -> f
            let maxSubmissionsPerPartition = defaultArg maxSubmissionsPerPartition (maxReadAhead - maxReadAhead/5) // NOTE needs to handle overflow if maxReadAhead is Int32.MaxValue
            let submitter = StreamsSubmitter.Create(log, maxSubmissionsPerPartition, mapBatch, submitStreamsBatch, statsInterval)
            let startIngester (rangeLog, projectionId) = StreamsIngester.Start(rangeLog, projectionId, maxReadAhead, submitter.Ingest, ingesterStatsInterval)
            Sink.Start(log, pumpDispatcher, pumpScheduler, submitter.Pump, startIngester)

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
            prepare, handle, toIndex,
            stats, statsInterval, eventSize,
            ?maxSubmissionsPerPartition,
            // Tune the number of batches the Scheduler should ingest at a time. Can be useful to compensate for small batches
            ?maxBatches,
            // Frequency of jettisoning Write Position state of inactive streams (held by the scheduler for deduplication purposes) to limit memory consumption
            // NOTE: Purging can impair performance, increase write costs or result in duplicate event emissions due to redundant inputs not being deduplicated
            ?purgeInterval,
            // Request optimal throughput by waking based on handler outcomes even if there is unused dispatch capacity
            ?wakeForResults,
            // Tune the sleep time when there are no items to schedule or responses to process. Default 1s.
            ?idleDelay,
            ?ingesterStatsInterval)
        : Sink<Ingestion.Ingester<StreamEvent<'F> seq>> =
        let dispatcher = Dispatch.ItemDispatcher<_, 'F>(maxConcurrentStreams)
        let streamScheduler =
            Scheduling.StreamSchedulingEngine.Create<_, 'Progress, 'Outcome, 'F>
                (   dispatcher, stats,
                    prepare, handle, toIndex,
                    (fun logStreamStates _log -> logStreamStates eventSize),
                    ?maxBatches = maxBatches, ?purgeInterval = purgeInterval, ?wakeForResults = wakeForResults, ?idleDelay = idleDelay)
        Projector.Pipeline.Start(
            log, dispatcher.Pump, streamScheduler.Pump, maxReadAhead, streamScheduler.Submit, statsInterval,
            ?maxSubmissionsPerPartition = maxSubmissionsPerPartition,
            ?ingesterStatsInterval = ingesterStatsInterval)

    /// Project StreamSpans using a <code>handle</code> function that yields a Write Position representing the next event that's to be handled on this Stream
    static member Start<'Outcome, 'F>
        (   log : ILogger, maxReadAhead, maxConcurrentStreams,
            handle : struct (FsCodec.StreamName * FsCodec.ITimelineEvent<'F> array) -> Async<struct (SpanResult * 'Outcome)>,
            stats, statsInterval, eventSize,
            // Limits number of batches passed to the scheduler.
            // Holding items back makes scheduler processing more efficient as less state needs to be traversed.
            // Holding items back is also key to the compaction mechanism working best.
            // Defaults to holding back 20% of maxReadAhead per partition
            ?maxSubmissionsPerPartition,
            // Tune the number of batches the Scheduler should ingest at a time. Can be useful to compensate for small batches
            ?maxBatches,
            // Frequency of jettisoning Write Position state of inactive streams (held by the scheduler for deduplication purposes) to limit memory consumption
            // NOTE: Purging can impair performance, increase write costs or result in duplicate event emissions due to redundant inputs not being deduplicated
            ?purgeInterval,
            // Request optimal throughput by waking based on handler outcomes even if there is unused dispatch capacity
            ?wakeForResults,
            // Tune the sleep time when there are no items to schedule or responses to process. Default 1s.
            ?idleDelay,
            ?ingesterStatsInterval)
        : Sink<Ingestion.Ingester<StreamEvent<'F> seq>> =
        let prepare struct (streamName, span) =
            let metrics = StreamSpan.metrics eventSize span
            struct (metrics, struct (streamName, span))
        StreamsSink.StartEx<SpanResult, 'Outcome, 'F>(
            log, maxReadAhead, maxConcurrentStreams, prepare, handle, SpanResult.toIndex, stats, statsInterval, eventSize,
            ?maxSubmissionsPerPartition = maxSubmissionsPerPartition,
            ?maxBatches = maxBatches, ?purgeInterval = purgeInterval, ?wakeForResults = wakeForResults, ?idleDelay = idleDelay,
            ?ingesterStatsInterval = ingesterStatsInterval)

    /// Project StreamSpans using a <code>handle</code> function that guarantees to always handles all events in the <code>span</code>
    static member Start<'Outcome, 'F>
        (   log : ILogger, maxReadAhead, maxConcurrentStreams,
            handle : FsCodec.StreamName * FsCodec.ITimelineEvent<'F> array -> Async<'Outcome>,
            stats, statsInterval, eventSize,
            // Limits number of batches passed to the scheduler.
            // Holding items back makes scheduler processing more efficient as less state needs to be traversed.
            // Holding items back is also key to the compaction mechanism working best.
            // Defaults to holding back 20% of maxReadAhead per partition
            ?maxSubmissionsPerPartition,
            // Tune the number of batches the Scheduler should ingest at a time. Can be useful to compensate for small batches
            ?maxBatches,
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
            ?maxSubmissionsPerPartition = maxSubmissionsPerPartition,
            ?maxBatches = maxBatches, ?purgeInterval = purgeInterval, ?wakeForResults = wakeForResults, ?idleDelay = idleDelay,
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
                handle : struct (FsCodec.StreamName * FsCodec.ITimelineEvent<'F> array) -> Async<struct (SpanResult * 'Outcome)>,
                stats : Stats<'Outcome>, statsInterval, sliceSize, eventSize,
                // Default 1 ms
                ?idleDelay,
                // Default 1 MiB
                ?maxBytes,
                // Default 16384
                ?maxEvents,
                // Max scheduling read ahead. Default 128.
                ?maxBatches,
                // Max inner cycles per loop. Default 128.
                ?maxCycles,
                // Hook to wire in external stats
                ?dumpExternalStats,
                // Frequency of jettisoning Write Position state of inactive streams (held by the scheduler for deduplication purposes) to limit memory consumption
                // NOTE: Purging can impair performance, increase write costs or result in duplicate event emissions due to redundant inputs not being deduplicated
                ?purgeInterval)
            : Sink<Ingestion.Ingester<StreamEvent<'F> seq>> =

            let maxBatches, maxEvents, maxBytes = defaultArg maxBatches 128, defaultArg maxEvents 16384, (defaultArg maxBytes (1024 * 1024 - (*fudge*)4096))

            let attemptWrite struct (stream, span : FsCodec.ITimelineEvent<'F> array) ct = task {
                let struct (met, span') = StreamSpan.slice<'F> sliceSize (maxEvents, maxBytes) span
                let sw = System.Diagnostics.Stopwatch.StartNew()
                try let req = struct (stream, span')
                    let! res, outcome = Async.StartAsTask(handle req, cancellationToken = ct)
                    let prepareElapsed = sw.Elapsed
                    let index' = SpanResult.toIndex span' res
                    return struct (index' > span[0].Index, Choice1Of2 struct (index', struct (met, prepareElapsed), outcome))
                with e -> return struct (false, Choice2Of2 struct (met, e)) }

            let interpretWriteResultProgress _streams (stream : FsCodec.StreamName) = function
                | Choice1Of2 struct (i', stats, outcome) ->
                    struct (ValueSome i', Choice1Of2 struct (stats, outcome))
                | Choice2Of2 struct (struct (eventCount, bytesCount) as stats, exn : exn) ->
                    log.Warning(exn, "Handling {events:n0}e {bytes:n0}b for {stream} failed, retrying", eventCount, bytesCount, stream)
                    ValueNone, Choice2Of2 struct (stats, exn)

            let itemDispatcher = Dispatch.ItemDispatcher<_, 'F>(maxConcurrentStreams)
            let dumpStreams logStreamStates log =
                logStreamStates eventSize
                match dumpExternalStats with Some f -> f log | None -> ()

            let dispatcher = Scheduling.Dispatcher.MultiDispatcher<_, _, _, 'F>.Create(itemDispatcher, attemptWrite, interpretWriteResultProgress, stats, dumpStreams)
            let streamScheduler =
                Scheduling.StreamSchedulingEngine<struct (int64 * struct (StreamSpan.Metrics * TimeSpan) * 'Outcome), struct (struct (StreamSpan.Metrics * TimeSpan) * 'Outcome), struct (StreamSpan.Metrics * exn), 'F>
                    (   dispatcher, maxBatches = maxBatches, maxCycles = defaultArg maxCycles 128, ?idleDelay = idleDelay, ?purgeInterval = purgeInterval)

            Projector.Pipeline.Start(
                log, itemDispatcher.Pump, streamScheduler.Pump, maxReadAhead, streamScheduler.Submit, statsInterval, maxSubmissionsPerPartition = maxBatches)

module Default =

    /// Canonical Data/Meta type supplied by the majority of Sources
    type EventBody = ReadOnlyMemory<byte>
    /// A contiguous set of Events from a Ordered stream, using the Canonical Data/Meta type
    type StreamSpan = StreamSpan<EventBody>
    /// A Single Event from an Ordered stream, using the Canonical Data/Meta type
    type StreamEvent = StreamEvent<EventBody>
    let inline private eventBodyBytes (x : EventBody) = x.Length
    let inline private stringBytes (x : string) = match x with null -> 0 | x -> x.Length * sizeof<char>
    let eventDataSize (x : FsCodec.IEventData<EventBody>) = eventBodyBytes x.Data + stringBytes x.EventType + 16
    let eventSize (x : FsCodec.IEventData<EventBody>) = eventDataSize x + eventBodyBytes x.Meta
    let jsonSize (x : FsCodec.IEventData<EventBody>) = eventSize x + 80

    type Sink = Sink<Ingestion.Ingester<StreamEvent seq>>

    type Config =

        static member Start<'Outcome>
            (   log, maxReadAhead, maxConcurrentStreams,
                handle : struct (FsCodec.StreamName * StreamSpan) -> Async<struct (SpanResult * 'Outcome)>,
                stats, statsInterval,
                ?maxSubmissionsPerPartition, ?maxBatches,
                ?purgeInterval,
                ?wakeForResults, ?idleDelay, ?ingesterStatsInterval)
            : Sink =
            StreamsSink.Start<'Outcome, EventBody>(
                log, maxReadAhead, maxConcurrentStreams, handle, stats, statsInterval, eventSize,
                ?maxSubmissionsPerPartition = maxSubmissionsPerPartition, ?maxBatches = maxBatches, ?purgeInterval = purgeInterval,
                ?wakeForResults = wakeForResults, ?idleDelay = idleDelay, ?ingesterStatsInterval = ingesterStatsInterval)
