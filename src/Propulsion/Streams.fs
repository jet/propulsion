﻿namespace Propulsion.Streams

open Propulsion.Internal
open Serilog
open System
open System.Collections.Generic

module Log =

    [<NoEquality; NoComparison>]
    type BufferMetric = { cats: int; streams: int; events: int; bytes: int64 }
    [<RequireQualifiedAccess; NoEquality; NoComparison>]
    type Metric =
        /// Summary of data held in an Ingester buffer, not yet submitted to the Scheduler
        | BufferReport of BufferMetric
        /// Buffer Report, classified based on the current State from the perspective of the scheduling algorithms
        | SchedulerStateReport of
            /// All ingested data has been processed
            synced: int *
            /// Work is currently been carried for streams in this state
            busy: BufferMetric *
            /// Buffered data is ready to be processed, but not yet scheduled
            ready: BufferMetric *
            /// Some buffered data is available, but data from start of stream has not yet been loaded (only relevant for striped readers)
            buffering: BufferMetric *
            /// Buffered data is held, but Sink has reported it as unprocessable (i.e., categorized as poison messages)
            malformed: BufferMetric
        /// Time spent on the various aspects of the scheduler loop; used to analyze performance issue
        | SchedulerCpu of
            merge: TimeSpan * // mt: Time spent merging input events and streams prior to ingestion
            ingest: TimeSpan * // it: Time spent ingesting streams from the ingester into the Scheduler's buffers
            dispatch: TimeSpan * // ft: Time spent preparing requests (filling) for the Dispatcher
            results: TimeSpan * // dt: Time spent handling results (draining) from the Dispatcher's response queue
            stats: TimeSpan // st: Time spent emitting statistics per period (default period is 1m)
        /// Scheduler attempt latency reports
        | HandlerResult of
            /// "exception" or "ok"
            kind: string *
            /// handler duration in s
            latency: float
        | StreamsBusy of
            /// "active", "failing" or "stalled"
            kind: string *
            count: int *
            /// age in S
            oldest: float *
            /// age in S
            newest: float

    let [<Literal>] PropertyTag = "propulsionEvent"
    /// Attach a property to the captured event record to hold the metric information
    let internal withMetric (value: Metric) = Log.withScalarProperty PropertyTag value
    let tryGetScalar<'t> key (logEvent: Serilog.Events.LogEvent): 't voption =
        match logEvent.Properties.TryGetValue key with true, Log.ScalarValue (:? 't as e) -> ValueSome e | _ -> ValueNone
    let [<Literal>] GroupTag = "group"
    let [<return: Struct>] (|MetricEvent|_|) logEvent =
        match tryGetScalar<Metric> PropertyTag logEvent with
        | ValueSome m -> ValueSome (m, tryGetScalar<string> GroupTag logEvent)
        | ValueNone -> ValueNone

module StreamName =

    /// Despite conventions, there's no guarantee that an arbitrary Kafka `key`, EventStore `StreamId` etc.
    /// will necessarily adhere to the `{category}-{streamId}` form
    /// This helper ensures there's always a dash (even if that means having a defaultStringId)
    let parseWithDefaultCategory defaultCategory (rawStreamName: string): FsCodec.StreamName =
        if rawStreamName.IndexOf '-' = -1 then String.Concat(defaultCategory, "-", rawStreamName)
        else rawStreamName
        |> FSharp.UMX.UMX.tag

    /// Guard against inputs that don't adhere to "{category}-{streamId}" by prefixing with `-`  })
    let internalParseSafe rawStreamName: FsCodec.StreamName = parseWithDefaultCategory "" rawStreamName

    /// Because we coerce all stream names to be well-formed, we can split it too
    /// where there's no category, we use the full streamId instead
    let categorize: FsCodec.StreamName -> string = function
        | FsCodec.StreamName.Category "" as sn -> (FsCodec.StreamName.toString sn).Substring(1)
        | FsCodec.StreamName.Category cat -> cat

/// Manipulates contiguous set of Events from a Ordered stream, as held internally within this module
module StreamSpan =

    type Metrics = (struct (int * int * int))
    let metrics eventSize (xs: FsCodec.ITimelineEvent<'F>[]): Metrics =
        (struct (0, 0, 0), xs) ||> Seq.fold (fun struct (es, us, bs) x ->
            let s = eventSize x
            if x.IsUnfold then es, us + 1, bs + s
            else es + 1, us, bs + s)
    let private trimEvents<'F> eventSize (maxEvents, maxBytes) (span: FsCodec.ITimelineEvent<'F>[]) =
        let mutable countBudget, bytesBudget = maxEvents, maxBytes
        let fitsInBudget (x: FsCodec.ITimelineEvent<_>) =
            countBudget <- countBudget - 1
            bytesBudget <- bytesBudget - eventSize x
            (countBudget >= 0 && bytesBudget >= 0 && not x.IsUnfold) // Stop at unfolds; if they belong, we need to supply all
            || (countBudget = maxEvents - 1) // We need to guarantee to yield at least one Event, even if it's outside of the size limit
        let trimmedEvents = span |> Array.takeWhile fitsInBudget
        // takeWhile terminated either because it hit the first Unfold, or the size limit
        match span |> Array.tryItem trimmedEvents.Length with
        | Some successor when successor.IsUnfold -> span // If takeWhile stopped on an Unfold we all remaining belong with the preceding event
        | _ -> trimmedEvents
    let slice<'F> eventSize limits (span: FsCodec.ITimelineEvent<'F>[]): struct (FsCodec.ITimelineEvent<'F>[] * Metrics) =
        let trimmed =
            // we must always send one event, even if it exceeds the limit (if the handler throws, the the Stats can categorize the problem to surface it)
            if span.Length = 1 || span[0].IsUnfold || span[1].IsUnfold then span
            // If we have 2 or more (non-Unfold) events, then we limit the batch size
            else trimEvents<'F> eventSize limits span
        trimmed, metrics eventSize trimmed

    let inline index (span: FsCodec.ITimelineEvent<'F>[]) = span[0].Index
    let inline next (span: FsCodec.ITimelineEvent<'F>[]) = let l = span[span.Length - 1] in if l.IsUnfold then l.Index else l.Index + 1L
    let inline dropBefore i = function
        | [||] as xs -> xs
        | xs when next xs < i -> Array.empty
        | xs ->
            match index xs with
            | xi when xi = i -> xs
            | xi -> xs |> Array.skip (i - xi |> int)
    let private coalesce xs =
        xs |> Array.sortInPlaceBy index
        let mutable outputs, acc = null, xs[0]
        for x in xs |> Seq.skip 1 do
            match next acc with
            | accNext when index x > accNext -> // Gap
                if acc |> Seq.exists (_.IsUnfold >> not) then
                    if outputs = null then outputs <- ResizeArray(xs.Length)
                    outputs.Add(acc |> Array.filter (_.IsUnfold >> not))
                acc <- x
            | accNext when next x >= accNext -> // Overlapping; join
                match dropBefore accNext x with
                | [||] -> ()
                | news -> acc <- [| yield! acc |> Seq.filter (_.IsUnfold >> not); yield! news |]
            | _ -> ()
        match acc with
        | [||] when outputs = null -> null
        | [||] -> outputs.ToArray()
        | unified when outputs = null -> Array.singleton unified
        | tail -> outputs.Add tail; outputs.ToArray()
    let private normalize (inputs: FsCodec.ITimelineEvent<_>[][]) =
        match inputs |> Array.filter (function null | [||] -> false | _ -> true) with
        | [||] -> null
        | [| _ |] as alreadyUnified -> alreadyUnified
        | multiple -> coalesce multiple
    let merge min (inputs: FsCodec.ITimelineEvent<_>[][]) =
        inputs |> Array.map (dropBefore min) |> normalize
    let stripUnfolds (xq: FsCodec.ITimelineEvent<_>[][]) =
        if xq = null then xq
        else xq |> Array.map (Array.filter (fun x -> not x.IsUnfold)) |> normalize

/// A Single Event from an Ordered stream being supplied for ingestion into the internal data structures
type StreamEvent<'Format> = (struct (FsCodec.StreamName * FsCodec.ITimelineEvent<'Format>))

module Buffer =

    type Revision = int<revision>
    and [<Measure>] revision
    module Revision =
        open FSharp.UMX
        let initial: Revision = % -1
        let increment (x: Revision): Revision = % (% x + 1)
    type HandlerProgress = (struct (int64 * Revision))
    module HandlerProgress =
        let ofPos pos: HandlerProgress = (pos, Revision.initial)
        let ofMetricsAndPos revision ((_es, us, _bs): StreamSpan.Metrics) pos: HandlerProgress = if us <> 0 then (pos, revision) else ofPos pos

    let [<Literal>] WritePosUnknown = -2L // sentinel value for write position signifying `None` (no write position yet established)
    let [<Literal>] WritePosMalformed = -3L // sentinel value for malformed data
    /// <summary>Buffers events for a stream, tolerating gaps and out of order arrival (see <c>requireAll</c> for scenarios dictating this need)</summary>
    /// <remarks>Optimized Representation as this is the dominant one in terms of memory usage - takes it from 24b to a cache-friendlier 16b</remarks>
    [<NoComparison; NoEquality; Struct>]
    type StreamState<'Format> = private { write: int64; revision: Revision; queue: FsCodec.ITimelineEvent<'Format>[][] } with
        static member Create(write, queue, revision) = { write = defaultValueArg write WritePosUnknown; revision = revision; queue = queue }
        static member Create(write, queue, revision, malformed) = StreamState<'Format>.Create((if malformed then ValueSome WritePosMalformed else write), queue, revision)
        member x.IsEmpty = LanguagePrimitives.PhysicalEquality null x.queue
        member x.EventsSumBy(f) = x.queue |> Seq.map (Seq.sumBy f) |> Seq.sum |> int64
        member x.EventsCount = x.queue |> Seq.sumBy Array.length

        member x.HeadSpan = x.queue[0]
        member x.IsMalformed = not x.IsEmpty && WritePosMalformed = x.write
        member x.QueuedIsAtWritePos = match x.write with WritePosUnknown -> x.HeadSpan[0].Index = 0L | w -> w = x.HeadSpan[0].Index

        member x.WritePos = match x.write with WritePosUnknown | WritePosMalformed -> ValueNone | w -> ValueSome w
        // Count of number of times the the Unfolds held in the queue have changed (typically due to events such as the CosmosDB ChangeFeed delivering a new edition of an Item)
        member x.QueueRevision = x.revision
        member x.TailHasUnfoldAtIndex(index) =
            let tailSpan = x.queue |> Array.last
            let tailEvent = tailSpan |> Array.last
            tailEvent.IsUnfold && tailEvent.Index = index

    type ProgressRequirement = (struct (int64 * Revision voption))
    // example: when we reach position 1 on the stream (having handled event 0), and the required position was 1, we remove the requirement
    // NOTE Any unfolds that accompany event 0 will also bear Index 0
    // NOTE 2: subsequent updates to Unfolds will bear the same Index of 0 until there is an Event with Index 1
    module ProgressRequirement =
        let ofPos pos: ProgressRequirement = pos, ValueNone
        let ofPosUnfoldRevision pos rev: ProgressRequirement = pos, ValueSome rev
        let isSatisfiedBy ((updatedPos, dispatchedRevision): HandlerProgress): ProgressRequirement -> bool = function
            | xPos, _ when updatedPos > xPos -> true
            | xPos, ValueNone -> updatedPos = xPos
            | xPos, ValueSome xRev when updatedPos = xPos -> dispatchedRevision >= xRev
            | _ -> false
        let inline compute index hadUnfold (x: StreamState<'Format>): ProgressRequirement voption =
            // If the queue is empty, or the write position is already beyond the requirement, it has already been handled
            if x.IsEmpty || x.WritePos |> ValueOption.exists (fun wp -> wp > index) then ValueNone
            // If there's an unfold at the tail, we can't checkpoint until it's been handled, a fresher unfold has been handled, or a successor event has moved the position past it
            elif hadUnfold && x.TailHasUnfoldAtIndex index then ofPosUnfoldRevision index x.QueueRevision |> ValueSome
            else ofPos index |> ValueSome

    module StreamState =

        let combine (s1: StreamState<_>) (s2: StreamState<_>): StreamState<'Format> =
            let malformed = s1.IsMalformed || s2.IsMalformed
            let writePos = max s1.WritePos s2.WritePos
            let queue =
                let any1 = not (isNull s1.queue)
                let any2 = not (isNull s2.queue)
                if any1 || any2 then
                    let items = if any1 && any2 then Array.append s1.queue s2.queue elif any1 then s1.queue else s2.queue
                    StreamSpan.merge (defaultValueArg writePos 0L) items
                else null
            let maybeLastUnfold = function null -> ValueNone | q -> let (li: FsCodec.ITimelineEvent<_>) = Array.last q |> Array.last in if li.IsUnfold then ValueSome li else ValueNone
            let changed =
                match maybeLastUnfold queue, maybeLastUnfold s1.queue with
                | ValueNone, ValueNone -> false
                | ValueNone, ValueSome _
                | ValueSome _, ValueNone -> true
                | ValueSome l1, ValueSome l2 -> LanguagePrimitives.PhysicalEquality l1 l2 |> not
            let revision = if changed then Revision.increment s1.revision else s1.revision
            StreamState<'Format>.Create(writePos, queue, revision, malformed)
        let tryTrimUnfoldsIffPosAndRevisionStill ((pos, revision): HandlerProgress) ({ write = xw; revision = xr; queue = xq } as x) =
            if xw <> pos || xr <> revision then ValueNone
            else ValueSome { x with revision = Revision.increment xr; queue = StreamSpan.stripUnfolds xq }

    type Streams<'Format>() =
        let states = Dictionary<FsCodec.StreamName, StreamState<'Format>>()
        let merge stream (state: StreamState<_>) =
            match states.TryGetValue stream with
            | true, current -> states[stream] <- StreamState.combine current state
            | false, _ -> states.Add(stream, state)

        member internal _.States = states :> seq<KeyValuePair<FsCodec.StreamName, StreamState<'Format>>>
        member _.Merge(other: Streams<'Format>) = for x in other.States do merge x.Key x.Value
        member _.Merge(stream, events: FsCodec.ITimelineEvent<'Format>[]) = merge stream (StreamState<'Format>.Create(ValueNone, [| events |], Revision.initial))

        member _.Dump(log: ILogger, estimateSize, categorize) =
            let mutable waiting, waitingE, waitingB = 0, 0, 0L
            let waitingCats, waitingStreams = Stats.Counters(), Stats.Counters()
            for KeyValue (stream, state) in states do
                if not state.IsEmpty then
                    let sz = estimateSize state
                    waitingCats.Ingest(categorize stream)
                    if sz <> 0L then
                        let sn, wp = FsCodec.StreamName.toString stream, defaultValueArg state.WritePos 0L
                        waitingStreams.Ingest(sprintf "%s@%dx%d" sn wp state.HeadSpan.Length, (sz + 512L) / 1024L)
                    waiting <- waiting + 1
                    waitingE <- waitingE + state.EventsCount
                    waitingB <- waitingB + sz
            let m = Log.Metric.BufferReport { cats = waitingCats.Count; streams = waiting; events = waitingE; bytes = waitingB }
            (log |> Log.withMetric m).Information(" Streams Waiting {busy:n0}/{busyMb:n1}MB", waiting, Log.miB waitingB)
            if waitingCats.Any then log.Information(" Waiting Categories, events {@readyCats}", Seq.truncate 5 waitingCats.StatsDescending)
            if waitingCats.Any then log.Information(" Waiting Streams, KB {@readyStreams}", Seq.truncate 5 waitingStreams.StatsDescending)

    [<NoComparison; NoEquality>]
    type Batch private (onCompletion, reqs: Dictionary<FsCodec.StreamName, int64>, unfoldReqs: ISet<FsCodec.StreamName>, eventsCount, unfoldsCount) =
        static member Create(onCompletion, streamEvents: StreamEvent<'Format> seq) =
            let streams, reqs, unfoldReqs = Streams<'Format>(), Dictionary<FsCodec.StreamName, int64>(), HashSet<FsCodec.StreamName>()
            let mutable eventsCount, unfoldsCount = 0, 0
            for struct (stream, eventsAndUnfolds) in streamEvents |> ValueTuple.groupWith Seq.toArray do
                let unfolds, events = eventsAndUnfolds |> Array.partition _.IsUnfold
                let mutable hwm = -1L
                // for events, we tolerate mis-ordered items within a batch (but they should not be there and this only makes sense for requireAll mode)
                for event in events do
                    let asBatch = [| event |]
                    streams.Merge(stream, asBatch)
                    hwm <- asBatch |> StreamSpan.next |> max hwm
                eventsCount <- eventsCount + events.Length
                match unfolds with
                | [||] -> ()
                | unfolds ->
                    unfoldsCount <- unfoldsCount + unfolds.Length
                    unfoldReqs.Add stream |> ignore
                    let next = unfolds |> StreamSpan.next
                    // Drop all but the last set
                    let unfolds = unfolds |> Array.filter (fun x -> x.Index = next)
                    hwm <- max hwm next
                    streams.Merge(stream, unfolds)
                reqs.Add(stream, hwm)
            struct (streams, Batch(onCompletion, reqs, unfoldReqs, eventsCount, unfoldsCount))
        member val OnCompletion = onCompletion
        member val StreamsCount = reqs.Count
        member val EventsCount = eventsCount
        member val UnfoldsCount = unfoldsCount
        member val Reqs = reqs :> seq<KeyValuePair<FsCodec.StreamName, int64>>
        member val UnfoldReqs = unfoldReqs

type [<RequireQualifiedAccess; Struct; NoEquality; NoComparison>] OutcomeKind = Ok | Tagged of string | Exn
module OutcomeKind =
    let [<Literal>] OkTag = "ok"
    let [<Literal>] ExnTag = "exception"
    let Timeout = OutcomeKind.Tagged "timeout"
    let RateLimited = OutcomeKind.Tagged "rateLimited"
    let classify: exn -> OutcomeKind = function
        | :? TimeoutException -> Timeout
        | _ -> OutcomeKind.Exn
    let tag = function OutcomeKind.Ok -> OkTag | OutcomeKind.Tagged g -> g | OutcomeKind.Exn -> ExnTag
    let isOk = function OutcomeKind.Ok -> true | OutcomeKind.Tagged _ | OutcomeKind.Exn -> false
    let isException = function OutcomeKind.Exn -> true | OutcomeKind.Ok | OutcomeKind.Tagged _ -> false

/// Details of name, time since first failure, and number of attempts for a Failing or Stuck stream
type FailingStreamDetails = (struct (FsCodec.StreamName * TimeSpan * int))

/// <summary>Raised by <c>Stats</c>'s <c>HealthCheck</c> to terminate a Sink's processing when the <c>abendThreshold</c> has been exceeded.</summary>
type HealthCheckException(oldestStuck, oldestFailing, stuckStreams, failingStreams) =
    inherit exn()
    override x.Message = $"Failure Threshold exceeded; Oldest stuck stream: {oldestStuck}, Oldest failing stream {oldestFailing}"
    /// Duration for which oldest stream that's active but failing has failed to progress
    member val TimeStuck: TimeSpan = oldestStuck
    /// Duration for which oldest stream that's active but failing to progress (though not erroring)
    member val OldestFailing: TimeSpan = oldestFailing
    /// Details of name, time since first attempt, and number of attempts for Streams not making progress
    member val StuckStreams: FailingStreamDetails[] = stuckStreams
    /// Details of name, time since first failure, and number of attempts for Streams experiencing continual non-transient exceptions
    member val FailingStreams: FailingStreamDetails[] = failingStreams

module Scheduling =

    open Buffer
    type StreamStates<'Format>() =
        let states = Dictionary<FsCodec.StreamName, StreamState<'Format>>()
        let merge stream (state: StreamState<_>) =
            match states.TryGetValue stream with
            | true, current ->
                let updated = StreamState.combine current state
                states[stream] <- updated
                updated
            | false, _ ->
                states.Add(stream, state)
                state
        let updateStreamState stream = function
            | Error malformed ->
                // Flag that the data at the head of the stream is triggering a non-transient error condition from the handler, preventing any further handler dispatches for `stream`
                merge stream (StreamState<'Format>.Create(ValueNone, null, Revision.initial, malformed = malformed)) |> ignore
            | Ok (updatedPos, _dispatchedRevision as up: HandlerProgress)  ->
                // Ensure we have a position (in case it got purged); Drop any events or unfolds implied by updatedPos
                merge stream (StreamState<'Format>.Create(ValueSome updatedPos, null, Revision.initial))
                // Strip unfolds out of the queue if the handler reported the position as unchanged, but the unfolds were included in the invocation
                |> StreamState.tryTrimUnfoldsIffPosAndRevisionStill up |> ValueOption.iter (fun trimmed -> states[ stream ] <- trimmed)
        let purge () =
            let mutable purged = 0
            for x in states do
                let streamState = x.Value
                if streamState.IsEmpty then
                    states.Remove x.Key |> ignore // Safe to do while iterating on netcore >= 3.0
                    purged <- purged + 1
            states.Count, purged

        let busy = HashSet<FsCodec.StreamName>()
        let markBusy stream = busy.Add stream |> ignore
        let markNotBusy stream = busy.Remove stream |> ignore

        member _.ToProgressRequirement(stream, index, hadUnfold): ProgressRequirement voption =
            match states.TryGetValue stream with
            | false, _ -> ValueNone // if there's no state for the stream, then it's all already written (and purged)
            | true, ss -> ProgressRequirement.compute index hadUnfold ss

        member _.HeadSpanSizeBy(f: _ -> int) stream =
            match states.TryGetValue stream with
            | true, state when not state.IsEmpty -> state.HeadSpan |> Array.sumBy f |> int64
            | _ -> 0L

        member _.ChooseDispatchable(s: FsCodec.StreamName, requireAll): StreamState<'Format> voption =
            match states.TryGetValue s with
            | true, ss when not ss.IsEmpty && not ss.IsMalformed && (not requireAll || ss.QueuedIsAtWritePos) && not (busy.Contains s) -> ValueSome ss
            | _ -> ValueNone

        member _.Merge(buffered: Streams<'Format>) = for kv in buffered.States do merge kv.Key kv.Value |> ignore
        member _.Purge() = purge ()

        member _.LockForWrite stream =
            markBusy stream
        member _.DropHandledEventsAndUnlock(stream, outcome) =
            updateStreamState stream outcome
            markNotBusy stream
        member _.Dump(log: ILogger, totalPurged: int, eventSize) =
            let mutable (busyCount, busyE, busyB), (ready, readyE, readyB), synced = (0, 0, 0L), (0, 0, 0L), 0
            let mutable (waiting, waitingE, waitingB), (malformed, malformedE, malformedB) = (0, 0, 0L), (0, 0, 0L)
            let busyCats, readyCats, readyStreams = Stats.Counters(), Stats.Counters(), Stats.Counters()
            let waitingCats, waitingStreams, malformedCats, malformedStreams = Stats.Counters(), Stats.Counters(), Stats.Counters(), Stats.Counters()
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
                    let cat = StreamName.categorize stream
                    let label = let hs = state.HeadSpan in sprintf "%s@%dx%d" (FsCodec.StreamName.toString stream) (StreamSpan.index hs) hs.Length
                    if state.IsMalformed then
                        malformedCats.Ingest(cat)
                        malformedStreams.Ingest(label, Log.miB sz |> int64)
                        malformed <- malformed + 1
                        malformedB <- malformedB + sz
                        malformedE <- malformedE + state.EventsCount
                    elif not state.IsEmpty && not state.QueuedIsAtWritePos then
                        waitingCats.Ingest(cat)
                        waitingStreams.Ingest(label, kb sz)
                        waiting <- waiting + 1
                        waitingB <- waitingB + sz
                        waitingE <- waitingE + state.EventsCount
                    else
                        readyCats.Ingest(cat)
                        readyStreams.Ingest(label, kb sz)
                        ready <- ready + 1
                        readyB <- readyB + sz
                        readyE <- readyE + state.EventsCount
            let busyStats: Log.BufferMetric = { cats = busyCats.Count; streams = busyCount; events = busyE; bytes = busyB }
            let readyStats: Log.BufferMetric = { cats = readyCats.Count; streams = readyStreams.Count; events = readyE; bytes = readyB }
            let waitingStats: Log.BufferMetric = { cats = waitingCats.Count; streams = waitingStreams.Count; events = waitingE; bytes = waitingB }
            let malformedStats: Log.BufferMetric = { cats = malformedCats.Count; streams = malformedStreams.Count; events = malformedE; bytes = malformedB }
            let m = Log.Metric.SchedulerStateReport (synced, busyStats, readyStats, waitingStats, malformedStats)
            (log |> Log.withMetric m).Information("STATE Synced {synced:n0} Purged {purged:n0} Active {busy:n0}/{busyMb:n1}MB Ready {ready:n0}/{readyMb:n1}MB Waiting {waiting}/{waitingMb:n1}MB Malformed {malformed}/{malformedMb:n1}MB",
                                                  synced, totalPurged, busyCount, Log.miB busyB, ready, Log.miB readyB, waiting, Log.miB waitingB, malformed, Log.miB malformedB)
            if busyCats.Any then log.Information(" Active Categories, events {@busyCats}", Seq.truncate 5 busyCats.StatsDescending)
            if readyCats.Any then log.Information(" Ready Categories, events {@readyCats}", Seq.truncate 5 readyCats.StatsDescending)
                                  log.Information(" Ready Streams, KB {@readyStreams}", Seq.truncate 5 readyStreams.StatsDescending)
            if waitingStreams.Any then log.Information(" Waiting Streams, KB {@waitingStreams}", Seq.truncate 5 waitingStreams.StatsDescending)
            if malformedStreams.Any then log.Information(" Malformed Streams, MB {@malformedStreams}", malformedStreams.StatsDescending)
            waitingStreams.Any

    type [<Struct; NoEquality; NoComparison>] BufferState = Idle | Active | Full

    module Stats =

        /// Manages state used to generate metrics (and summary logs) regarding streams currently being processed by a Handler
        module Busy =

            type private StreamState = { ts: int64; mutable count: int }
            let inline private ticksSince effectiveTimestamp (x: StreamState) = effectiveTimestamp - x.ts
            let inline private ageInTicks () = let currentTs = Stopwatch.timestamp () in ticksSince currentTs
            let private walkAges (state: Dictionary<_, _>) =
                if state.Count = 0 then Seq.empty else
                let ticksOld = ageInTicks ()
                seq { for x in state.Values -> struct (ticksOld x, x.count) }
            let private renderStats (state: Dictionary<_, _>) =
                let ticksOld = ageInTicks ()
                [|  for kv in state ->
                        let sn = kv.Key
                        let age = ticksOld kv.Value |> Stopwatch.ticksToTimeSpan
                        struct (sn, age, kv.Value.count) |]
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
                member x.MaxAge = let _, struct (oldest, _) = x.State in oldest
                member _.TryGetAgeS sn =
                    match state.TryGetValue sn with
                    | false, _ -> ValueNone
                    | true, ss -> ageInTicks () ss |> Stopwatch.ticksToSeconds |> int |> ValueSome
            /// Represents state of streams where the handler did not make progress on the last execution either intentionally or due to an exception
            type private Repeating() =
                let state = Dictionary<FsCodec.StreamName, StreamState>()
                member _.HandleResult(sn, isStuck, startTs) =
                    if not isStuck then state.Remove sn |> ignore
                    else match state.TryGetValue sn with
                         | true, v -> v.count <- v.count + 1
                         | false, _ -> state.Add(sn, { ts = startTs; count = 1 })
                member _.State = walkAges state |> renderState
                member _.Stats = renderStats state
                member _.Contains sn = state.ContainsKey sn
                member x.MaxAge = let _, struct (oldest, _) = x.State in oldest
                member _.TryGet sn = match state.TryGetValue sn with true, v -> ValueSome v.count | _ -> ValueNone

            type [<Struct; NoEquality; NoComparison>] State = Running | Slow of s: int | Failing of c: int | Stuck of c2: int | Waiting
            /// Collates all state and reactions to manage the list of busy streams based on callbacks/notifications from the Dispatcher
            type Monitor() =
                let active, failing, stuck = Active(), Repeating(), Repeating()
                let emit (log: ILogger) level state struct (streams, attempts) struct (oldest: TimeSpan, newest: TimeSpan) =
                    log.Write(level, " {state,-7} {streams,3} for {newest:n1}-{oldest:n1}s, {attempts} attempts",
                              state, streams, newest.TotalSeconds, oldest.TotalSeconds, attempts)
                member _.HandleStarted(sn, ts) =
                    active.HandleStarted(sn, ts)
                member _.HandleResult(sn, succeeded, progressed) =
                    let startTs = active.TakeFinished(sn)
                    failing.HandleResult(sn, not succeeded, startTs)
                    stuck.HandleResult(sn, succeeded && not progressed, startTs)
                member _.Classify(longRunningThreshold, sn) =
                    match failing.TryGet sn with
                    | ValueSome count -> Failing count
                    | ValueNone ->
                        match stuck.TryGet sn with
                        | ValueSome count -> Stuck count
                        | ValueNone ->
                            match active.TryGetAgeS sn with
                            | ValueSome age -> if age > longRunningThreshold then Slow age else Running
                            | ValueNone -> Waiting
                member _.OldestStuck = stuck.MaxAge
                member _.OldestActive = active.MaxAge
                member _.OldestFailing = failing.MaxAge
                member _.StuckStreamDetails = stuck.Stats
                member _.FailingStreamDetails = failing.Stats
                member _.DumpState(log: ILogger) =
                    let dump level state struct (streams, attempts) ages =
                        if streams <> 0 then
                            emit log level state (streams, attempts) ages
                    stuck.State ||> dump LogEventLevel.Error "stalled"
                    failing.State ||> dump LogEventLevel.Warning "failing"
                    active.State ||> dump LogEventLevel.Information "active"
                member _.EmitMetrics(log: ILogger) =
                    let report state struct (streams, attempts) struct (oldest: TimeSpan, newest: TimeSpan) =
                        let m = Log.Metric.StreamsBusy (state, streams, oldest.TotalSeconds, newest.TotalSeconds)
                        emit (log |> Log.withMetric m) LogEventLevel.Information state (streams, attempts) (oldest, newest)
                    stuck.State ||> report "stalled"
                    failing.State ||> report "failing"
                    active.State ||> report "active"

        type [<NoComparison; NoEquality>] Timers() =
            let mutable results, dispatch, merge, ingest, stats, sleep = 0L, 0L, 0L, 0L, 0L, 0L
            let sw = Stopwatch.start()
            member _.RecordResults ts = results <- results + Stopwatch.elapsedTicks ts
            member _.RecordDispatch ts = dispatch <- dispatch + Stopwatch.elapsedTicks ts
            member _.RecordMerge ts = merge <- merge + Stopwatch.elapsedTicks ts
            member _.RecordIngest ts = ingest <- ingest + Stopwatch.elapsedTicks ts
            member _.RecordStats ts = stats <- stats + Stopwatch.elapsedTicks ts
            member _.RecordSleep ts = sleep <- sleep + Stopwatch.elapsedTicks ts
            member _.Dump(log: ILogger) =
                let tot = Stopwatch.ticksToTimeSpan (results + dispatch - merge - ingest + stats + sleep) |> max TimeSpan.Zero
                let it, mt = Stopwatch.ticksToTimeSpan ingest, Stopwatch.ticksToTimeSpan merge
                let dt = Stopwatch.ticksToTimeSpan dispatch - it - mt |> max TimeSpan.Zero
                let rt, st, zt = Stopwatch.ticksToTimeSpan results, Stopwatch.ticksToTimeSpan stats, Stopwatch.ticksToTimeSpan sleep
                let log = log |> Log.withMetric (Log.Metric.SchedulerCpu (mt, it, dt, rt, st))
                log.Information("Cpu Dispatch {dispatch:n1}s streams {streams:n1}s batches {batches:n1}s Results {results:n1}s Stats {stats:n1}s Sleep {sleep:n1}s Total {total:n1}s Interval {int:n1}s",
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
                let t = Stats.Counters()
                if idle > 0   then t.Ingest(nameof Idle, idle)
                if active > 0 then t.Ingest(nameof Active, active)
                if full > 0   then t.Ingest(nameof Full, full)
                t.StatsDescending

    [<Struct; NoComparison; NoEquality>]
    type Res<'R> = { duration: TimeSpan; stream: FsCodec.StreamName; index: int64; event: string; index': int64; result: 'R }

    type LatencyStats() =
        let outcomes, outcomesAcc = Stats.LatencyStatsSet(), Stats.LatencyStatsSet()
        let okCats, exnCats = Stats.LatencyStatsSet(), Stats.LatencyStatsSet()
        let okCatsAcc, exnCatsAcc = Stats.LatencyStatsSet(), Stats.LatencyStatsSet()
        member val Categorize = false with get, set
        member _.RecordOutcome(tag, duration) =
            outcomes.Record(tag, duration)
            outcomesAcc.Record(tag, duration)
        member _.RecordOk(category, duration) =
            okCats.Record(category, duration)
            okCatsAcc.Record(category, duration)
        member _.RecordExn(tag, duration) =
            exnCats.Record(tag, duration)
            exnCatsAcc.Record(tag, duration)
        member internal x.DumpStats log =
            outcomes.Dump(log, labelSortOrder = function OutcomeKind.OkTag -> String.Empty | x -> x)
            okCats.Dump(log, totalLabel = "OK")
            exnCats.Dump(log, totalLabel = "ERROR")
            outcomes.Clear(); okCats.Clear(); exnCats.Clear()
        member internal x.DumpState(log, purge) =
            outcomesAcc.Dump(log, labelSortOrder = function OutcomeKind.OkTag -> String.Empty | x -> x)
            okCatsAcc.Dump(log, totalLabel = "ΣOK")
            exnCatsAcc.Dump(log, totalLabel = "ΣERROR")
            if purge then outcomesAcc.Clear(); okCatsAcc.Clear(); exnCatsAcc.Clear()
        member internal x.RecordOutcome(streamName, kind, duration) =
            let tag = OutcomeKind.tag kind
            if tag = OutcomeKind.OkTag && x.Categorize then
                let cat = StreamName.categorize streamName
                x.RecordOk(cat, duration)
            else
                x.RecordOutcome(tag, duration)
            tag

    /// Gathers stats pertaining to the core projection/ingestion activity
    [<AbstractClass>]
    type Stats<'R, 'E>(log: ILogger, statsInterval: TimeSpan, stateInterval: TimeSpan,
                       [<O; D null>] ?longRunningThreshold, [<O; D null>] ?failThreshold, [<O; D null>] ?abendThreshold,
                       [<O; D null>] ?logExternalStats: Action<ILogger>) =
        let failThreshold = defaultArg failThreshold statsInterval
        let longRunningThresholdS = defaultArg longRunningThreshold stateInterval |> _.TotalSeconds |> int
        let metricsLog = log.ForContext("isMetric", true)
        let monitor, monitorInterval = Stats.Busy.Monitor(), IntervalTimer(TimeSpan.FromSeconds 1.)
        let stateStats = Stats.StateStats()
        let lats = LatencyStats()
        let mutable cycles, batchesCompleted, batchesStarted, streams, skipped, unfolded, events, unfolds = 0, 0, 0, 0, 0, 0, 0, 0

        member val Log = log
        member val Latency = lats
        member _.Categorize with get () = lats.Categorize and set value = lats.Categorize <- value
        member val StatsInterval = IntervalTimer statsInterval
        member val StateInterval = IntervalTimer stateInterval
        member val Timers = Stats.Timers()

        member x.DumpStats(struct (dispatchActive, dispatchMax), struct (batchesWaiting, batchesRunning), abend) =
            let batchesCompleted = System.Threading.Interlocked.Exchange(&batchesCompleted, 0)
            log.Information("Batches Waiting {waiting} Started {started} {streams:n0}s ({skipped:n0} skipped {streamsUnfolded:n0} unfolded) {events:n0}e {unfolds:n0}u | Completed {completed} Running {active}",
                            batchesWaiting, batchesStarted, streams, skipped, unfolded, events, unfolds, batchesCompleted, batchesRunning)
            batchesStarted <- 0; streams <- 0; skipped <- 0; unfolded <- 0; events <- 0; unfolds <- 0; (*batchesCompleted <- 0*)
            x.Timers.Dump log
            log.Information("Scheduler {cycles} cycles {@states} Running {busy}/{processors}",
                            cycles, stateStats.StatsDescending, dispatchActive, dispatchMax)
            cycles <- 0; stateStats.Clear()
            monitor.DumpState log
            x.RunHealthCheck abend
            lats.DumpStats log
            x.DumpStats()
            logExternalStats |> Option.iter (fun (f: Action<ILogger>) -> f.Invoke log)

        member _.IsFailing = monitor.OldestFailing > failThreshold || monitor.OldestStuck > TimeSpan.Zero
        member _.HasLongRunning = monitor.OldestFailing.TotalSeconds > longRunningThresholdS
        member _.Classify sn = monitor.Classify(longRunningThresholdS, sn)

        member _.RecordIngested(streamsCount, streamsSkipped, streamsUnfolded, eventCount, unfoldCount) =
            batchesStarted <- batchesStarted + 1
            streams <- streams + streamsCount
            skipped <- skipped + streamsSkipped
            unfolded <- unfolded + streamsUnfolded
            events <- events + eventCount
            unfolds <- unfolds + unfoldCount

        member _.RecordBatchCompletion() =
            System.Threading.Interlocked.Increment(&batchesCompleted) |> ignore

        member x.RecordStats() =
            cycles <- cycles + 1
            x.StatsInterval.IsDue

        member x.RecordState(state) =
            stateStats.Ingest(state)
            x.StateInterval.IfDueRestart()

        /// Allows an ingester or projector to wire in custom stats since the last interval (typically based on data gathered in a `Handle` override)
        abstract DumpStats: unit -> unit
        default _.DumpStats() = ()

        /// Allows an ingester or projector to trigger dumping of accumulated statistics (less frequent than DumpStats)
        abstract DumpState: bool -> unit
        default _.DumpState(purge) = lats.DumpState(log, purge)


        /// Allows serialization of the emission of statistics where multiple Schedulers are active (via an externally managed lock object)
        abstract Serialize: (unit -> unit) -> unit
        default _.Serialize(f) = f ()

        member _.HandleStarted(stream, stopwatchTicks) =
            monitor.HandleStarted(stream, stopwatchTicks)

        abstract member Handle: Res<Result<'R, 'E>> -> unit

        member private x.RecordOutcomeKind(r, k, progressed) =
            monitor.HandleResult(r.stream, succeeded = OutcomeKind.isOk k, progressed = progressed)
            let kindTag = lats.RecordOutcome(r.stream, k, r.duration)
            if metricsLog.IsEnabled LogEventLevel.Information then
                let m = Log.Metric.HandlerResult (kindTag, r.duration.TotalSeconds)
                (metricsLog |> Log.withMetric m).Information("Outcome {kind} in {ms:n0}ms, progressed: {progressed}",
                                                             kindTag, r.duration.TotalMilliseconds, progressed)
                if monitorInterval.IfDueRestart() then monitor.EmitMetrics metricsLog
        member x.RecordOk(r, force) = x.RecordOutcomeKind(r, OutcomeKind.Ok, r.index' > r.index || force)
        member x.RecordExn(r, k, log, exn) =
            x.RecordOutcomeKind(r, k, progressed = false)
            if OutcomeKind.isException k then
                x.HandleExn(log, exn)

        abstract member HandleExn: ILogger * exn -> unit

        abstract GenerateHealthCheckException: oldestStuck: TimeSpan * oldestFailing: TimeSpan * stuck: FailingStreamDetails[] * failing: FailingStreamDetails[] -> exn
        default _.GenerateHealthCheckException(oldestStuck, oldestFailing, stuck, failing) =
            HealthCheckException(oldestStuck, oldestFailing, stuck, failing)
        abstract HealthCheck: oldestStuck: TimeSpan * oldestFailing: TimeSpan -> exn option
        default x.HealthCheck(oldestStuck, oldestFailing) =
            match abendThreshold with
            | Some limit when oldestStuck > limit || oldestFailing > limit ->
                x.GenerateHealthCheckException(oldestStuck, oldestFailing, monitor.StuckStreamDetails, monitor.FailingStreamDetails) |> Some
            | _ -> None
        member x.RunHealthCheck(abend) =
            x.HealthCheck(monitor.OldestStuck, monitor.OldestFailing) |> Option.iter abend

    module Progress =

        type [<Struct; NoComparison; NoEquality>] BatchState = private { markCompleted: unit -> unit; reqs: Dictionary<FsCodec.StreamName, ProgressRequirement> }

        type ProgressState<'Pos>() =
            let pending = Queue<BatchState>()

            let trim () =
                let mutable batch = Unchecked.defaultof<_>
                while pending.TryPeek &batch && batch.reqs.Count = 0 do
                    pending.Dequeue().markCompleted ()
            member _.RunningCount = pending.Count
            member _.EnumPending(): seq<BatchState> =
                trim ()
                pending
            member _.IngestBatch(markCompleted, reqs) =
                let fresh = { markCompleted = markCompleted; reqs = reqs }
                pending.Enqueue fresh
                trim ()
                if pending.Count = 0 then ValueNone // If already complete, avoid triggering stream ingestion or a dispatch cycle
                else ValueSome fresh

            member _.RemoveAttainedRequirements(stream, updatedPosAndDispatchedRevision) =
                for x in pending do
                    match x.reqs.TryGetValue stream with
                    | true, req when ProgressRequirement.isSatisfiedBy updatedPosAndDispatchedRevision req ->
                        x.reqs.Remove stream |> ignore
                    | _ -> ()

            member _.Dump(log: ILogger, lel, classify: FsCodec.StreamName -> Stats.Busy.State) =
                if log.IsEnabled lel && pending.Count <> 0 then
                    let stuck, failing, slow, running, waiting = ResizeArray(), ResizeArray(), ResizeArray(), ResizeArray(), ResizeArray()
                    let h = pending.Peek()
                    for x in h.reqs do
                        match classify x.Key with
                        | Stats.Busy.Stuck count -> stuck.Add struct(x.Key, x.Value, count)
                        | Stats.Busy.Failing count -> failing.Add struct(x.Key, x.Value, count)
                        | Stats.Busy.Slow seconds -> slow.Add struct (x.Key, x.Value, seconds)
                        | Stats.Busy.Running -> if running.Count < 10 then running.Add(ValueTuple.ofKvp x)
                        | Stats.Busy.Waiting -> if waiting.Count < 10 then waiting.Add(ValueTuple.ofKvp x)
                    log.Write(lel, " Active Batch (sn, version[, attempts|ageS]) Stuck {stuck} Failing {failing} Slow {slow} Running {running} Waiting {waiting}",
                              stuck, failing, slow |> Seq.sortByDescending (fun struct (_, _, s) -> s) |> Seq.truncate 10, running, waiting)

        // We potentially traverse the pending streams thousands of times per second so we reuse buffers for better L2 caching properties
        // NOTE internal reuse of `sortBuffer` and `streamsBuffer` means it's critical to never have >1 of these in flight
        type StreamsPrioritizer(getStreamWeight) =

            let streamsSuggested = HashSet()
            let collectUniqueStreams (xs: IEnumerator<BatchState>) = seq {
                while xs.MoveNext() do
                    let x = xs.Current
                    for s in x.reqs.Keys do
                        if streamsSuggested.Add s then
                            yield s }

            let sortBuffer = ResizeArray()
            // Within the head batch, expedite work on longest streams, where requested
            let prioritizeHead (getStreamWeight: FsCodec.StreamName -> int64) (head: BatchState): seq<FsCodec.StreamName> =
                // sortBuffer is reused per invocation, but the result is lazy so we can only clear on entry
                sortBuffer.Clear()
                let weight s = -getStreamWeight s |> int
                for s in head.reqs.Keys do
                    if streamsSuggested.Add s then
                        let w = weight s
                        sortBuffer.Add(struct (s, w))
                if sortBuffer.Count > 0 then
                    let c = Comparer<_>.Default
                    sortBuffer.Sort(fun struct (_, _aw) struct (_, _bw) -> c.Compare(_aw, _bw))
                sortBuffer |> Seq.map ValueTuple.fst

            member _.CollectStreams(batches: BatchState seq): seq<FsCodec.StreamName> =
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
        [<CLIEvent>] abstract member Result: IEvent<InternalRes<Result<'P, 'E>>>
        abstract member Pump: CancellationToken -> Task<unit>
        abstract member State: struct (int * int)
        abstract member HasCapacity: bool with get
        abstract member AwaitCapacity: CancellationToken -> Task
        abstract member TryReplenish: pending: seq<Item<'F>> * handleStarted: (FsCodec.StreamName * int64 -> unit) -> struct (bool * bool)
        abstract member InterpretProgress: Result<'P, 'E> -> ResProgressAndMalformed<Result<'R, 'E>>
    and [<Struct; NoComparison; NoEquality>]
        Item<'Format> = { stream: FsCodec.StreamName; writePos: int64 voption; span: FsCodec.ITimelineEvent<'Format>[]; revision: Revision }
    and [<Struct; NoComparison; NoEquality>] InternalRes<'R> = { stream: FsCodec.StreamName; index: int64; event: string; duration: TimeSpan; result: 'R }
    and ResProgressAndMalformed<'O> = (struct ('O * Buffer.HandlerProgress voption * bool))
    module InternalRes =
        let inline create (i: Item<_>, d, r) =
            let h = i.span[0]
            { stream = i.stream; index = h.Index; event = h.EventType; duration = d; result = r }

    /// Consolidates ingested events into streams; coordinates dispatching of these to projector/ingester in the order implied by the submission order
    /// a) does not itself perform any reading activities
    /// b) triggers synchronous callbacks as batches complete; writing of progress is managed asynchronously by the TrancheEngine(s)
    /// c) submits work to the supplied Dispatcher (which it triggers pumping of)
    /// d) periodically reports state (with hooks for ingestion engines to report same)
    type Engine<'P, 'R, 'E, 'F>
        (   dispatcher: IDispatcher<'P, 'R, 'E, 'F>, stats: Stats<'R,'E>, dumpState,
            // Number of batches to buffer locally; rest held by submitter to enable fair ordering of submission as partitions are added/removed
            pendingBufferSize,
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
            // Constrain event handling dispatch to a) always start from event 0 per stream b) never permit gaps in the sequence of events for a stream
            // Cannot be combined with purging in the current implementation, so supplying a <c>purgeInterval</c> alongside throws <c>NotSupportedException</c>
            // <remarks>Gaps can naturally arise where a lease is lost, regained lost again, leading to a partial redelivery of earlier events.
            // Thus normal dispatch needs to tolerate gaps in the sequence (in addition to the standard dropping of events prior to current write pos).
            // NOTE This absolutely does not mean that an event store should be permitted to deliver events out of order or with gaps. The Equinox DynamoStore,
            //      CosmosStore, EventStoreDb, LibSql, and MessageDb stores have absolute guarantees of NEVER presenting events out of order, or even temporary
            //      gaps in the stream order. The entirely unforced error of relaxing this core assumption pushes problems up into the application:
            //      1. having to second guess whether your handler might not have been presented an event (or had them delivered out of order)
            //      2. adding junk logging or validation logic (requiring state, distributed storage, and/or manual log correlation) "just in case"
            // Example use cases:
            // - if a feed is known to potentially have out of order events _but you are processing all events in one shot on a single processing node_
            //   e.g., a ChangeFeedProcessor where calved documents have been hand-edited that is being reprocessed in full without multiple processing nodes
            // - if your ingestion pipeline is composing a synthetic stream from multiple sources, but they deliver events into the Ingester independently
            //   e.g., if you are merging user info from 20 tables into one virtual user stream for processing, allocating event Indexes dynamically, you
            //         don't want the arrival of event 2 first to cause events 0 and 1 to be dropped</remarks>
            ?requireAll) =
        let writeResult, awaitResults, tryApplyResults =
            let r, w = let c = Channel.unboundedSr in c.Reader, c.Writer
            Channel.write w, Channel.awaitRead r, Channel.apply r
        let enqueueStreams, applyStreams =
            let r, w = let c = Channel.unboundedSr<Streams<_>> in c.Reader, c.Writer
            Channel.write w, Channel.apply r
        let waitToSubmit, tryWritePending, waitingCount, awaitPending, tryPending =
            let r, w = let c = Channel.boundedSw<Batch>(pendingBufferSize) in c.Reader, c.Writer // Actually SingleReader too, but Count throws if you use that
            Channel.waitToWrite w, Channel.tryWrite w, (fun () -> r.Count), Channel.awaitRead r, Channel.tryRead r
        let streams = StreamStates<'F>()
        let batches = Progress.ProgressState()
        let batchesWaitingAndRunning () = struct (waitingCount (), batches.RunningCount)
        // Enumerates the active batches; when the caller pulls beyond that, more get ingested on the fly
        let enumBatches tryIngestBatch = seq {
            yield! batches.EnumPending()
            // We'll get here as soon as the dispatching process has exhausted the currently queued items
            let mutable cont = true in while cont do match tryIngestBatch () with ValueSome x -> yield x | ValueNone -> cont <- false }
        let priority = Progress.StreamsPrioritizer(prioritizeStreamsBy |> Option.map streams.HeadSpanSizeBy)
        let chooseDispatchable =
            let requireAll = defaultArg requireAll false
            if requireAll && Option.isSome purgeInterval then invalidArg (nameof requireAll) "Cannot be combined with a purgeInterval"
            fun stream ->
                streams.ChooseDispatchable(stream, requireAll)
                |> ValueOption.map (fun ss -> { stream = stream; writePos = ss.WritePos; span = ss.HeadSpan; revision = ss.QueueRevision })
        let tryDispatch enumBatches =
            let candidateItems: seq<Item<_>> = enumBatches |> priority.CollectStreams |> Seq.chooseV chooseDispatchable
            let handleStarted (stream, ts) = stats.HandleStarted(stream, ts); streams.LockForWrite(stream)
            dispatcher.TryReplenish(candidateItems, handleStarted)

        // Ingest information to be gleaned from processing the results into `streams` (i.e. remove stream requirements as they are completed)
        let handleResult ({ stream = stream; index = i; event = et; duration = duration; result = r }: InternalRes<_>) =
            match dispatcher.InterpretProgress r with
            | Ok _ as r, ValueSome (index', _ as updatedPosAndDispatchedRevision), _malformed ->
                batches.RemoveAttainedRequirements(stream, updatedPosAndDispatchedRevision)
                streams.DropHandledEventsAndUnlock(stream, Ok updatedPosAndDispatchedRevision)
                stats.Handle { duration = duration; stream = stream; index = i; event = et; index' = index'; result = r }
            | Ok _ as r, ValueNone, malformed
            | (Error _ as r), _, malformed ->
                streams.DropHandledEventsAndUnlock(stream, Error malformed)
                stats.Handle { duration = duration; stream = stream; index = i; event = et; index' = i; result = r }
        let tryHandleResults () = tryApplyResults handleResult

        // Take an incoming batch of events, correlating it against our known stream state to yield a set of required work before we can complete/checkpoint it
        let ingest (batch: Batch) =
            let reqs = Dictionary()
            for item in batch.Reqs do
                streams.ToProgressRequirement(item.Key, item.Value, batch.UnfoldReqs.Contains item.Key)
                |> ValueOption.iter (fun req -> reqs[item.Key] <- req)
            stats.RecordIngested(reqs.Count, batch.StreamsCount - reqs.Count, batch.UnfoldReqs.Count, batch.EventsCount, batch.UnfoldsCount)
            let onCompletion () =
                batch.OnCompletion ()
                stats.RecordBatchCompletion()
            batches.IngestBatch(onCompletion, reqs)
        let tryIngestBatch ingestStreams () = tryPending () |> ValueOption.bind (fun b -> ingestStreams (); ingest b)

        let recordAndPeriodicallyLogStats exiting abend =
            if stats.RecordStats() || exiting then
                stats.Serialize(fun () -> stats.DumpStats(dispatcher.State, batchesWaitingAndRunning (), abend))
                stats.StatsInterval.Restart() // manual restart only after we've serviced the call so observers can await completion

        let purgeDue: unit -> bool =
            match purgeInterval with
            | Some ts -> IntervalTimer(ts).IfDueRestart
            | None -> fun () -> false
        let mutable totalPurged = 0
        let purge () =
            let remaining, purged = streams.Purge()
            totalPurged <- totalPurged + purged
            let l = if purged = 0 then LogEventLevel.Debug else LogEventLevel.Information
            Log.Write(l, "PURGED Remaining {buffered:n0} Purged now {count:n0} Purged total {total:n0}", remaining, purged, totalPurged)
        let recordAndPeriodicallyLogState exiting dispatcherState =
            if stats.RecordState(dispatcherState) || exiting then
                let log = stats.Log
                let dumpStreamStates (eventSize: FsCodec.ITimelineEvent<'F> -> int) =
                    let hasWaiting = streams.Dump(log, totalPurged, eventSize)
                    let lel =
                        if exiting || hasWaiting || stats.IsFailing then LogEventLevel.Warning
                        elif stats.HasLongRunning then LogEventLevel.Information
                        else LogEventLevel.Debug
                    batches.Dump(log, lel, stats.Classify)
                dumpState dumpStreamStates log
                let runPurge = not exiting && purgeDue ()
                stats.DumpState(runPurge)
                if runPurge then purge ()
        let sleepIntervalMs = match idleDelay with Some ts -> TimeSpan.toMs ts | None -> 1000
        let wakeForResults = defaultArg wakeForResults false

        member _.Pump(abend, ct: CancellationToken) = task {
            use _ = dispatcher.Result.Subscribe writeResult
            Task.start (fun () -> task { try do! dispatcher.Pump ct
                                         with e -> abend e })
            let inline ts () = Stopwatch.timestamp ()
            let t = stats.Timers
            let tryHandleResults () = let ts = ts () in let r = tryHandleResults () in t.RecordResults ts; r
            // NOTE Timers subtracts merge time from dispatch time, so callers (inc ingestStreamsInLieuOfDispatch) *must* RecordDispatch the gross time
            let mergeStreams_ () = let ts, r = ts (), applyStreams streams.Merge in t.RecordMerge ts; r
            let tryIngestStreamsInLieuOfDispatch () = let ts = ts () in let r = mergeStreams_ () in t.RecordDispatch ts; r
            let tryIngestBatch () = let ts, b = ts (), tryIngestBatch (mergeStreams_ >> ignore) () in t.RecordIngest ts; b
            let tryDispatch () = let ts = ts () in let r = tryDispatch (enumBatches tryIngestBatch) in t.RecordDispatch ts; r

            let mutable exiting = false
            while not exiting do
                exiting <- ct.IsCancellationRequested
                // 1. propagate write write outcomes to buffer (can mark batches completed etc)
                let processedResults = tryHandleResults ()
                // 2. top up provisioning of writers queue
                // On each iteration, we try to fill the in-flight queue, taking the oldest and/or heaviest streams first
                // Where there is insufficient work in the queue, we trigger ingestion of batches as needed
                let struct (dispatched, hasCapacity) = if not dispatcher.HasCapacity then false, false else tryDispatch ()
                // 3. Report the stats per stats interval
                let statsTs = ts ()
                if exiting then
                    tryHandleResults () |> ignore
                    batches.EnumPending() |> ignore
                recordAndPeriodicallyLogStats exiting abend; t.RecordStats statsTs
                // 4. Do a minimal sleep so we don't run completely hot when empty (unless we did something non-trivial)
                let idle = not processedResults && not dispatched && not (tryIngestStreamsInLieuOfDispatch ())
                if idle && not exiting then
                    let sleepTs = ts ()
                    do! Task.runWithCancellation ct (fun ct ->
                            Task.WhenAny[| if hasCapacity then awaitPending ct :> Task
                                           if wakeForResults then awaitResults ct
                                           elif not hasCapacity then dispatcher.AwaitCapacity(ct)
                                           Task.Delay(sleepIntervalMs, ct) |])
                    t.RecordSleep sleepTs
                // 5. Record completion state once per iteration; dumping streams is expensive so needs to be done infrequently
                let dispatcherState = if not hasCapacity then Full elif idle then Idle else Active
                recordAndPeriodicallyLogState exiting dispatcherState }

        member internal _.SubmitStreams(x: Streams<_>) =
            enqueueStreams x
        member internal _.WaitToSubmit(ct) =
            waitToSubmit ct
        member internal _.TrySubmit(x: Batch) =
            tryWritePending x

module Dispatcher =

    /// Coordinates the dispatching of work and emission of results, subject to the maxDop concurrent processors constraint
    type internal DopDispatcher<'R>(maxDop: int) =
        let tryWrite, wait, apply =
            let c = Channel.unboundedSwSr<CancellationToken -> Task<'R>> in let r, w = c.Reader, c.Writer
            w.TryWrite, Channel.awaitRead r, Channel.apply r >> ignore
        let result = Event<'R>()
        let dop = Sem maxDop

        let runHandler struct (computation: CancellationToken -> Task<'R>, ct) = task {
            let! res = computation ct
            dop.Release()
            result.Trigger res }

        [<CLIEvent>] member _.Result = result.Publish
        member _.State = dop.State
        member _.HasCapacity = dop.HasCapacity
        member _.AwaitButRelease(ct) = dop.WaitButRelease(ct)
        // NOTE computation is required/trusted to have an outer catch (or results would not be posted and dop would leak)
        member _.TryAdd(computation) = dop.TryTake() && tryWrite computation

        member _.Pump(ct: CancellationToken) = task {
            while not ct.IsCancellationRequested do
                try do! wait ct :> Task
                with :? OperationCanceledException -> ()
                let run (f: CancellationToken -> Task<'R>) = Task.start (fun () -> runHandler (f, ct))
                apply run }

    /// Kicks off enough work to fill the inner Dispatcher up to capacity
    type internal ItemDispatcher<'R, 'F>(maxDop) =
        let inner = DopDispatcher<Scheduling.InternalRes<'R>>(maxDop)

        // On each iteration, we try to fill the in-flight queue, taking the oldest and/or heaviest streams first
        let tryFillDispatcher (potential: seq<Scheduling.Item<'F>>) markStarted project =
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

        [<CLIEvent>] member _.Result = inner.Result
        member _.Pump ct = inner.Pump ct
        member _.State = inner.State
        member _.HasCapacity = inner.HasCapacity
        member _.AwaitCapacity(ct) = inner.AwaitButRelease(ct)
        member _.TryReplenish(pending, markStarted, project) = tryFillDispatcher pending markStarted project

    /// Implementation of IDispatcher that feeds items to an item dispatcher that maximizes concurrent requests (within a limit)
    type Concurrent<'P, 'R, 'E, 'F> internal
        (   inner: ItemDispatcher<Result<'P, 'E>, 'F>,
            project: struct (int64 * Scheduling.Item<'F>) -> CancellationToken -> Task<Scheduling.InternalRes<Result<'P, 'E>>>,
            interpretProgress: Result<'P, 'E> -> Scheduling.ResProgressAndMalformed<Result<'R, 'E>>) =
        static member Create
            (   maxDop,
                // NOTE `project` must not throw under any circumstances, or the exception will go unobserved, and DOP will leak in the dispatcher
                project: FsCodec.StreamName -> FsCodec.ITimelineEvent<'F>[] -> Buffer.Revision -> CancellationToken -> Task<Result<'P, 'E>>,
                interpretProgress: Result<'P, 'E> -> Scheduling.ResProgressAndMalformed<Result<'R, 'E>>) =
            let project struct (startTs, item: Scheduling.Item<'F>) (ct: CancellationToken) = task {
                let! res = project item.stream item.span item.revision ct
                return Scheduling.InternalRes.create (item, Stopwatch.elapsed startTs, res) }
            Concurrent<_, _, _, _>(ItemDispatcher(maxDop), project, interpretProgress)
        static member Create(maxDop, prepare: Func<FsCodec.StreamName, FsCodec.ITimelineEvent<_>[], _>, handle: Func<FsCodec.StreamName, FsCodec.ITimelineEvent<_>[], CancellationToken, Task<struct ('Outcome * int64)>>) =
            let project stream span revision ct = task {
                let struct (span: FsCodec.ITimelineEvent<'F>[], met) = prepare.Invoke(stream, span)
                try let! struct (outcome, index') = handle.Invoke(stream, span, ct)
                    return Ok struct (outcome, Buffer.HandlerProgress.ofMetricsAndPos revision met index', met)
                with e -> return Error struct (e, false, met) }
            let interpretProgress = function
                | Ok struct (outcome, hp, met) -> struct (Ok struct (outcome, met), ValueSome hp, false)
                | Error struct (exn, malformed, met) -> Error struct (exn, malformed, met), ValueNone, malformed
            Concurrent<_, _, _, 'F>.Create(maxDop, project, interpretProgress = interpretProgress)
        interface Scheduling.IDispatcher<'P, 'R, 'E, 'F> with
            [<CLIEvent>] override _.Result = inner.Result
            override _.Pump ct = inner.Pump ct
            override _.State = inner.State
            override _.HasCapacity = inner.HasCapacity
            override _.AwaitCapacity(ct) = inner.AwaitCapacity(ct)
            override _.TryReplenish(pending, handleStarted) = inner.TryReplenish(pending, handleStarted, project)
            override _.InterpretProgress res = interpretProgress res

    type ResProgressAndMetrics<'O> = (struct ('O * Buffer.HandlerProgress voption * StreamSpan.Metrics))
    type ExnAndMetrics = (struct(exn * bool * StreamSpan.Metrics))
    type NextIndexAndMetrics = (struct(int64 * StreamSpan.Metrics))

    /// Implementation of IDispatcher that allows a supplied handler select work and declare completion based on arbitrarily defined criteria
    type Batched<'F>
        (   select: Func<Scheduling.Item<'F> seq, Scheduling.Item<'F>[]>,
            // NOTE `handle` must not throw under any circumstances, or the exception will go unobserved
            handle: Scheduling.Item<'F>[] -> CancellationToken -> Task<Scheduling.InternalRes<Result<NextIndexAndMetrics, ExnAndMetrics>>[]>) =
        let inner = DopDispatcher 1
        let result = Event<Scheduling.InternalRes<Result<NextIndexAndMetrics, ExnAndMetrics>>>()

        // On each iteration, we offer the ordered work queue to the selector
        // we propagate the selected streams to the handler
        let trySelect (potential: seq<Scheduling.Item<'F>>) markStarted =
            let mutable hasCapacity, dispatched = true, false
            let streams: Scheduling.Item<'F>[] = select.Invoke potential
            if Array.any streams then
                let res = inner.TryAdd(handle streams)
                if not res then failwith "Checked we can add, what gives?"
                let startTs = Stopwatch.timestamp ()
                for x in streams do markStarted (x.stream, startTs)
                dispatched <- true // if we added any request, we'll skip sleeping
                hasCapacity <- false
            struct (dispatched, hasCapacity)

        interface Scheduling.IDispatcher<NextIndexAndMetrics, struct (unit * StreamSpan.Metrics), ExnAndMetrics, 'F> with
            [<CLIEvent>] override _.Result = result.Publish
            override _.Pump ct = task {
                use _ = inner.Result.Subscribe(Array.iter result.Trigger)
                return! inner.Pump ct }
            override _.State = inner.State
            override _.HasCapacity = inner.HasCapacity
            override _.AwaitCapacity(ct) = inner.AwaitButRelease(ct)
            override _.TryReplenish(pending, handleStarted) = trySelect pending handleStarted
            override _.InterpretProgress(res: Result<_, _>) =
                match res with
                | Ok (pos', met) -> Ok ((), met), ValueSome (Buffer.HandlerProgress.ofPos pos'), false
                | Error (exn, malformed, met) -> Error (exn, malformed, met), ValueNone, malformed

[<AbstractClass>]
type Stats<'Outcome>(log: ILogger, statsInterval, statesInterval,
                     [<O; D null>] ?failThreshold, [<O; D null>] ?abendThreshold, [<O; D null>] ?logExternalStats) =
    inherit Scheduling.Stats<struct ('Outcome * StreamSpan.Metrics), Dispatcher.ExnAndMetrics>(
        log, statsInterval, statesInterval, ?failThreshold = failThreshold, ?abendThreshold = abendThreshold, ?logExternalStats = logExternalStats)
    let mutable okStreams, okEvents, okUnfolds, okBytes, exnStreams, exnCats, exnEvents, exnUnfolds, exnBytes = HashSet(), 0, 0, 0L, HashSet(), Stats.Counters(), 0, 0, 0L
    let mutable resultOk, resultExn = 0, 0
    override _.DumpStats() =
        if resultOk <> 0 then
            log.Information("Projected {mb:n0}MB {completed:n0}r {streams:n0}s {events:n0}e {unfolds:n0}u ({ok:n0} ok)",
                        Log.miB okBytes, resultOk, okStreams.Count, okEvents, okUnfolds, resultOk)
            okStreams.Clear(); resultOk <- 0; okEvents <- 0; okUnfolds <- 0; okBytes <- 0L
        if resultExn <> 0 then
            log.Warning(" Exceptions {mb:n0}MB {fails:n0}r {streams:n0}s {events:n0}e {unfolds:n0}u",
                        Log.miB exnBytes, resultExn, exnStreams.Count, exnEvents, exnUnfolds)
            resultExn <- 0; exnStreams.Clear(); exnBytes <- 0L; exnEvents <- 0; exnUnfolds <- 0
            log.Warning("  Affected cats {@badCats}", exnCats.StatsDescending)
            exnCats.Clear()

    abstract member Classify: exn -> OutcomeKind
    default _.Classify e = OutcomeKind.classify e

    override this.Handle res =
        match res with
        | { stream = stream; result = Ok (outcome, (es, us, bs)) } ->
            okStreams.Add stream |> ignore
            okEvents <- okEvents + es
            okUnfolds <- okUnfolds + us
            okBytes <- okBytes + int64 bs
            resultOk <- resultOk + 1
            base.RecordOk(res, us <> 0)
            this.HandleOk outcome
        | { duration = duration; stream = stream; index = index; event = et; result = Error (Exception.Inner exn, _malformed, (es, us, bs)) } ->
            exnCats.Ingest(StreamName.categorize stream)
            exnStreams.Add stream |> ignore
            exnEvents <- exnEvents + es
            exnUnfolds <- exnUnfolds + us
            exnBytes <- exnBytes + int64 bs
            resultExn <- resultExn + 1
            base.RecordExn(res, this.Classify exn, log.ForContext("stream", stream).ForContext("index", index).ForContext("eventType", et).ForContext("events", es).ForContext("unfolds", us).ForContext("duration", duration), exn)

    abstract member HandleOk: outcome: 'Outcome -> unit

[<AbstractClass; Sealed>]
type Factory private () =

    static member private StartIngester(log, partitionId, maxRead, submit, statsInterval, [<O; D null>] ?commitInterval) =
        let submitBatch (items: StreamEvent<'F> seq, onCompletion) =
            let items = Array.ofSeq items
            let streams = items |> Seq.map ValueTuple.fst |> HashSet
            let batch: Propulsion.Submission.Batch<_, _> = { partitionId = partitionId; onCompletion = onCompletion; messages = items }
            submit batch
            struct (streams.Count, items.Length)
        Propulsion.Ingestion.Ingester<StreamEvent<'F> seq>.Start(log, partitionId, maxRead, submitBatch, statsInterval, ?commitInterval = commitInterval)

    static member CreateSubmitter(log: ILogger, mapBatch, streamsScheduler: Scheduling.Engine<_, _, _, _>, statsInterval) =
        let trySubmitBatch (x: Buffer.Batch): int voption =
            if streamsScheduler.TrySubmit x then ValueSome x.StreamsCount
            else ValueNone
        Propulsion.Submission.SubmissionEngine<_, _, _, _>(log, statsInterval, mapBatch, streamsScheduler.SubmitStreams, streamsScheduler.WaitToSubmit, trySubmitBatch)

    static member Start(log: ILogger, pumpScheduler, maxReadAhead, streamsScheduler, ingesterStateInterval, [<O; D null>] ?commitInterval) =
        let mapBatch onCompletion (x: Propulsion.Submission.Batch<_, StreamEvent<'F>>): struct (Buffer.Streams<'F> * Buffer.Batch) =
            Buffer.Batch.Create(onCompletion, x.messages)
        let submitter = Factory.CreateSubmitter(log, mapBatch, streamsScheduler, ingesterStateInterval)
        let startIngester (rangeLog, partitionId: int) = Factory.StartIngester(rangeLog, partitionId, maxReadAhead, submitter.Ingest, ingesterStateInterval, ?commitInterval = commitInterval)
        Propulsion.PipelineFactory.StartSink(log, pumpScheduler, submitter.Pump, startIngester)

[<AbstractClass; Sealed>]
type Concurrent private () =

    /// Custom projection mechanism that divides work into a <code>prepare</code> phase that selects the prefix of the queued StreamSpan to handle,
    /// and a <code>handle</code> function that yields a Write Position representing the next event that's to be handled on this Stream
    static member StartEx<'Outcome, 'F>
        (   log: ILogger, maxReadAhead, maxConcurrentStreams,
            prepare: Func<FsCodec.StreamName, FsCodec.ITimelineEvent<'F>[], struct (FsCodec.ITimelineEvent<'F>[] * StreamSpan.Metrics)>,
            handle: Func<FsCodec.StreamName, FsCodec.ITimelineEvent<'F>[], CancellationToken, Task<struct ('Outcome * int64)>>,
            eventSize, stats: Scheduling.Stats<_, _>,
            // Configure max number of batches to buffer within the scheduler; Default: Same as maxReadAhead
            [<O; D null>] ?pendingBufferSize,
            // Frequency of jettisoning Write Position state of inactive streams (held by the scheduler for deduplication purposes) to limit memory consumption
            // NOTE: Purging can impair performance, increase write costs or result in duplicate event emissions due to redundant inputs not being deduplicated
            [<O; D null>] ?purgeInterval,
            // Request optimal throughput by waking based on handler outcomes even if there is no unused dispatch capacity
            [<O; D null>] ?wakeForResults,
            // Tune the sleep time when there are no items to schedule or responses to process. Default 1s.
            [<O; D null>] ?idleDelay, [<O; D null>] ?requireAll,
            [<O; D null>] ?ingesterStateInterval, [<O; D null>] ?commitInterval)
        : Propulsion.SinkPipeline<Propulsion.Ingestion.Ingester<StreamEvent<'F> seq>> =
        let dispatcher: Scheduling.IDispatcher<_, _, _, _> = Dispatcher.Concurrent<_, _, _, 'F>.Create(maxConcurrentStreams, prepare = prepare, handle = handle)
        let dumpStreams logStreamStates _log = logStreamStates eventSize
        let scheduler = Scheduling.Engine(dispatcher, stats, dumpStreams,
                                          defaultArg pendingBufferSize maxReadAhead, ?purgeInterval = purgeInterval, ?wakeForResults = wakeForResults,
                                          ?idleDelay = idleDelay, ?requireAll = requireAll)
        Factory.Start(log, scheduler.Pump, maxReadAhead, scheduler,
                      ingesterStateInterval = defaultArg ingesterStateInterval stats.StateInterval.Period, ?commitInterval = commitInterval)

    /// Project Events using a <code>handle</code> function that yields a Write Position representing the next event that's to be handled on this Stream
    static member Start<'Outcome, 'F>
        (   log: ILogger, maxReadAhead, maxConcurrentStreams,
            handle: Func<FsCodec.StreamName, FsCodec.ITimelineEvent<'F>[], CancellationToken, Task<struct ('Outcome * int64)>>,
            eventSize, stats,
            // Configure max number of batches to buffer within the scheduler; Default: Same as maxReadAhead
            [<O; D null>] ?pendingBufferSize,
            // Frequency of jettisoning Write Position state of inactive streams (held by the scheduler for deduplication purposes) to limit memory consumption
            // NOTE: Purging can impair performance, increase write costs or result in duplicate event emissions due to redundant inputs not being deduplicated
            [<O; D null>] ?purgeInterval,
            // Request optimal throughput by waking based on handler outcomes even if there is unused dispatch capacity
            [<O; D null>] ?wakeForResults,
            // Tune the sleep time when there are no items to schedule or responses to process. Default 1s.
            [<O; D null>] ?idleDelay, [<O; D null>] ?requireAll,
            [<O; D null>] ?ingesterStateInterval, [<O; D null>] ?commitInterval)
        : Propulsion.SinkPipeline<Propulsion.Ingestion.Ingester<StreamEvent<'F> seq>> =
        let prepare _streamName span =
            let metrics = StreamSpan.metrics eventSize span
            struct (span, metrics)
        Concurrent.StartEx<'Outcome, 'F>(
            log, maxReadAhead, maxConcurrentStreams, prepare, handle, eventSize, stats,
            ?pendingBufferSize = pendingBufferSize, ?purgeInterval = purgeInterval, ?wakeForResults = wakeForResults,
            ?idleDelay = idleDelay, ?requireAll = requireAll,
            ?ingesterStateInterval = ingesterStateInterval, ?commitInterval = commitInterval)

[<AbstractClass; Sealed>]
type Batched private () =

    /// Establishes a Sink pipeline that continually dispatches to a single instance of a <c>handle</c> function
    /// Prior to the dispatch, the potential streams to include in the batch are identified by the <c>select</c> function
    static member Start<'Outcome, 'F>
        (   log: ILogger, maxReadAhead,
            select: Func<Scheduling.Item<'F> seq, Scheduling.Item<'F>[]>,
            handle: Func<Scheduling.Item<'F>[], CancellationToken, Task<seq<struct (Result<int64, exn> * TimeSpan)>>>,
            eventSize, stats: Scheduling.Stats<struct (unit * StreamSpan.Metrics), Dispatcher.ExnAndMetrics>,
            [<O; D null>] ?pendingBufferSize,
            [<O; D null>] ?purgeInterval, [<O; D null>] ?wakeForResults, [<O; D null>] ?idleDelay, [<O; D null>] ?requireAll,
            [<O; D null>] ?ingesterStateInterval, [<O; D null>] ?commitInterval)
        : Propulsion.SinkPipeline<Propulsion.Ingestion.Ingester<StreamEvent<'F> seq>> =
        let handle (items: Scheduling.Item<'F>[]) ct
            : Task<Scheduling.InternalRes<Result<Dispatcher.NextIndexAndMetrics, Dispatcher.ExnAndMetrics>>[]> = task {
            let start = Stopwatch.timestamp ()
            let err ts e (x: Scheduling.Item<_>) =
                let met = StreamSpan.metrics eventSize x.span
                Scheduling.InternalRes.create (x, ts, Error struct (e, false, met))
            try let! results = handle.Invoke(items, ct)
                return Array.ofSeq (Seq.zip items results |> Seq.map (function
                    | item, (Ok index', ts) ->
                        let used = item.span |> Seq.takeWhile (fun e -> e.Index <> index') |> Array.ofSeq
                        let met = StreamSpan.metrics eventSize used
                        Scheduling.InternalRes.create (item, ts, Ok struct (index', met))
                    | item, (Error e, ts) -> err ts e item))
            with e ->
                let ts = Stopwatch.elapsed start
                return items |> Array.map (err ts e) }
        let dispatcher = Dispatcher.Batched(select, handle)
        let dumpStreams logStreamStates _log = logStreamStates eventSize
        let scheduler = Scheduling.Engine(dispatcher, stats, dumpStreams,
                                          defaultArg pendingBufferSize maxReadAhead, ?purgeInterval = purgeInterval, ?wakeForResults = wakeForResults,
                                          ?idleDelay = idleDelay, ?requireAll = requireAll)
        Factory.Start(log, scheduler.Pump, maxReadAhead, scheduler,
                      ingesterStateInterval = defaultArg ingesterStateInterval stats.StateInterval.Period, ?commitInterval = commitInterval)
