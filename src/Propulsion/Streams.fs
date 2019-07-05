namespace Propulsion.Streams

open MathNet.Numerics.Statistics
open Propulsion
open Serilog
open System
open System.Collections.Concurrent
open System.Collections.Generic
open System.Diagnostics
open System.Threading

/// An Event from a Stream
type IEvent<'Format> =
    /// The Event Type, used to drive deserialization
    abstract member EventType : string
    /// Event body, as UTF-8 encoded json ready to be injected into the Store
    abstract member Data : 'Format
    /// Optional metadata (null, or same as Data, not written if missing)
    abstract member Meta : 'Format
    /// The Event's Creation Time (as defined by the writer, i.e. in a mirror, this is intended to reflect the original time)
    abstract member Timestamp : System.DateTimeOffset

/// A Single Event from an Ordered stream
[<NoComparison; NoEquality>]
type StreamEvent<'Format> = { stream: string; index: int64; event: IEvent<'Format> } with
    interface IEvent<'Format> with
        member __.EventType = __.event.EventType
        member __.Data = __.event.Data
        member __.Meta = __.event.Meta
        member __.Timestamp = __.event.Timestamp

/// Span of events from an Ordered Stream
[<NoComparison; NoEquality>]
type StreamSpan<'Format> = { index: int64; events: IEvent<'Format>[] }

module Internal =

    type EventData<'Format> =
        { eventType : string; data : 'Format; meta : 'Format; timestamp: DateTimeOffset }
        interface IEvent<'Format> with
            member __.EventType = __.eventType
            member __.Data = __.data
            member __.Meta = __.meta
            member __.Timestamp = __.timestamp

    type EventData =
        static member Create(eventType, data, ?meta, ?timestamp) =
            {   eventType = eventType
                data = data
                meta = defaultArg meta null
                timestamp = match timestamp with Some ts -> ts | None -> DateTimeOffset.UtcNow }

    /// Gathers stats relating to how many items of a given category have been observed
    type CatStats() =
        let cats = Dictionary<string,int64>()
        member __.Ingest(cat,?weight) = 
            let weight = defaultArg weight 1L
            match cats.TryGetValue cat with
            | true, catCount -> cats.[cat] <- catCount + weight
            | false, _ -> cats.[cat] <- weight
        member __.Any = cats.Count <> 0
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
        let sortedLatencies = xs |> Seq.map (fun r -> r.TotalMilliseconds) |> Seq.sort |> Seq.toArray

        let pc p = SortedArrayStatistics.Percentile(sortedLatencies, p) |> TimeSpan.FromMilliseconds
        let l = {
            avg = ArrayStatistics.Mean sortedLatencies |> TimeSpan.FromMilliseconds
            stddev =
                let stdDev = ArrayStatistics.StandardDeviation sortedLatencies
                // stdev of singletons is NaN
                if Double.IsNaN stdDev then None else TimeSpan.FromMilliseconds stdDev |> Some

            min = SortedArrayStatistics.Minimum sortedLatencies |> TimeSpan.FromMilliseconds
            max = SortedArrayStatistics.Maximum sortedLatencies |> TimeSpan.FromMilliseconds
            p50 = pc 50
            p95 = pc 95
            p99 = pc 99 }
        let inline sec (t:TimeSpan) = t.TotalSeconds
        let stdDev = match l.stddev with None -> Double.NaN | Some d -> sec d
        log.Information(" {kind} {count} : max={1:n3}s p99={2:n3}s p95={3:n3}s p50={4:n3}s min={5:n3}s avg={6:n3}s stddev={7:n3}s",
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

open Internal

[<AutoOpen>]
module private Impl =
    let (|NNA|) xs = if obj.ReferenceEquals(null,xs) then Array.empty else xs
    let inline arrayBytes (x: _ []) = if obj.ReferenceEquals(null,x) then 0 else x.Length
    let inline eventSize (x : IEvent<byte[]>) = arrayBytes x.Data + arrayBytes x.Meta + x.EventType.Length + 16
    let inline mb x = float x / 1024. / 1024.
    let inline accStopwatch (f : unit -> 't) at =
        let sw = Stopwatch.StartNew()
        let r = f ()
        at sw.Elapsed
        r

#nowarn "52" // see tmp.Sort

module Progress =

    type [<NoComparison; NoEquality>] internal BatchState = { markCompleted: unit -> unit; streamToRequiredIndex : Dictionary<string,int64> }

    type ProgressState<'Pos>() =
        let pending = Queue<_>()
        let trim () =
            while pending.Count <> 0 && pending.Peek().streamToRequiredIndex.Count = 0 do
                let batch = pending.Dequeue()
                batch.markCompleted()
        member __.AppendBatch(markCompleted, reqs : Dictionary<string,int64>) =
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
                        tmp.Add((s,(batch,-getStreamWeight s)))
            let c = Comparer<_>.Default
            tmp.Sort(fun (_,_a) ((_,_b)) -> c.Compare(_a,_b))
            tmp |> Seq.map (fun ((s,_)) -> s)

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
        let inline estimateBytesAsJsonUtf8 (x: IEvent<'Format[]>) = arrayBytes x.Data + arrayBytes x.Meta + (x.EventType.Length * 2) + 96
        let stats (x : StreamSpan<_>) =
            x.events.Length, x.events |> Seq.sumBy estimateBytesAsJsonUtf8
        let slice (maxEvents,maxBytes) streamSpan =
            let mutable count,bytes = 0, 0
            let mutable countBudget, bytesBudget = maxEvents,maxBytes
            let withinLimits y =
                countBudget <- countBudget - 1
                let eventBytes = estimateBytesAsJsonUtf8 y
                bytesBudget <- bytesBudget - eventBytes
                // always send at least one event in order to surface the problem and have the stream marked malformed
                let res = count = 0 || (countBudget > 0 && bytesBudget > 0)
                if res then count <- count + 1; bytes <- bytes + eventBytes
                res
            let trimmed = { streamSpan with events = streamSpan.events |> Array.takeWhile withinLimits }
            stats trimmed, trimmed

    [<NoComparison; NoEquality>] 
    type StreamState<'Format> = { isMalformed: bool; write: int64 option; queue: StreamSpan<'Format>[] } with
        member __.IsEmpty = obj.ReferenceEquals(null, __.queue)
        member __.HasValid = not __.IsEmpty && not __.isMalformed
        member __.IsReady =
            if not __.HasValid then false else

            match __.write, Array.head __.queue with
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
        let inline private optionCombine f (r1: 'a option) (r2: 'a option) =
            match r1, r2 with
            | Some x, Some y -> f x y |> Some
            | None, None -> None
            | None, x | x, None -> x
        let combine (s1: StreamState<_>) (s2: StreamState<_>) : StreamState<_> =
            let writePos = optionCombine max s1.write s2.write
            let items = let (NNA q1, NNA q2) = s1.queue, s2.queue in Seq.append q1 q2
            { write = writePos; queue = StreamSpan.merge (defaultArg writePos 0L) items; isMalformed = s1.isMalformed || s2.isMalformed }

    type Streams<'Format>() =
        let states = Dictionary<string, StreamState<'Format>>()
        let merge stream (state : StreamState<_>) =
            match states.TryGetValue stream with
            | false, _ ->
                states.Add(stream, state)
            | true, current ->
                let updated = StreamState.combine current state
                states.[stream] <- updated

        member __.Merge(item : StreamEvent<'Format>) =
            merge item.stream { isMalformed = false; write = None; queue = [| { index = item.index; events = [| item.event |] } |] }
        member private __.States = states
        member __.Items = states :> seq<KeyValuePair<string,StreamState<'Format>>>
        member __.Merge(other: Streams<'Format>) =
            for x in other.States do
                merge x.Key x.Value

        member __.Dump(log : ILogger, estimateSize, categorize) =
            let mutable waiting, waitingB = 0, 0L
            let waitingCats, waitingStreams = CatStats(), CatStats()
            for KeyValue (stream,state) in states do
                let sz = estimateSize state
                waitingCats.Ingest(categorize stream)
                waitingStreams.Ingest(sprintf "%s@%dx%d" stream (defaultArg state.write 0L) state.queue.[0].events.Length, (sz + 512L) / 1024L)
                waiting <- waiting + 1
                waitingB <- waitingB + sz
            if waiting <> 0 then log.Information(" Streams Waiting {busy:n0}/{busyMb:n1}MB ", waiting, mb waitingB)
            if waitingCats.Any then log.Information(" Waiting Categories, events {@readyCats}", Seq.truncate 5 waitingCats.StatsDescending)
            if waitingCats.Any then log.Information(" Waiting Streams, KB {@readyStreams}", Seq.truncate 5 waitingStreams.StatsDescending)

module Scheduling =

    open Buffering

    type StreamStates<'Format>() =
        let states = Dictionary<string, StreamState<'Format>>()
        let update stream (state : StreamState<_>) =
            match states.TryGetValue stream with
            | false, _ ->
                states.Add(stream, state)
                stream, state
            | true, current ->
                let updated = StreamState.combine current state
                states.[stream] <- updated
                stream, updated
        let updateWritePos stream isMalformed pos span = update stream { isMalformed = isMalformed; write = pos; queue = span }
        let markCompleted stream index = updateWritePos stream false (Some index) null |> ignore
        let merge (buffer : Streams<_>) =
            for x in buffer.Items do
                update x.Key x.Value |> ignore

        let busy = HashSet<string>()
        let pending trySlipstreamed (requestedOrder : string seq) : seq<int64 option*string*StreamSpan<_>> = seq {
            let proposed = HashSet()
            for s in requestedOrder do
                let state = states.[s]
                if state.HasValid && not (busy.Contains s) then
                    proposed.Add s |> ignore
                    yield state.write, s, Array.head state.queue
            if trySlipstreamed then
                // [lazily] Slipstream in further events that are not yet referenced by in-scope batches
                for KeyValue(s,v) in states do
                    if v.HasValid && not (busy.Contains s) && proposed.Add s then
                        yield v.write, s, Array.head v.queue }
        let markBusy stream = busy.Add stream |> ignore
        let markNotBusy stream = busy.Remove stream |> ignore

        member __.InternalMerge buffer = merge buffer
        member __.InternalUpdate stream pos queue = update stream { isMalformed = false; write = Some pos; queue = queue }
        member __.Add(stream, index, event, ?isMalformed) =
            updateWritePos stream (defaultArg isMalformed false) None [| { index = index; events = [| event |] } |]
        member __.Add(stream, span: StreamSpan<_>, isMalformed) =
            updateWritePos stream isMalformed None [| span |]
        member __.SetMalformed(stream, isMalformed) =
            updateWritePos stream isMalformed None [| { index = 0L; events = null } |]
        member __.Item(stream) =
            states.[stream]
        member __.MarkBusy stream =
            markBusy stream
        member __.MarkCompleted(stream, index) =
            markNotBusy stream
            markCompleted stream index
        member __.MarkFailed stream =
            markNotBusy stream
        member __.Pending(trySlipstreamed, byQueuedPriority : string seq) : (int64 option * string * StreamSpan<'Format>) seq =
            pending trySlipstreamed byQueuedPriority
        member __.Dump(log : ILogger, estimateSize, categorize) =
            let mutable busyCount, busyB, ready, readyB, unprefixed, unprefixedB, malformed, malformedB, synced = 0, 0L, 0, 0L, 0, 0L, 0, 0L, 0
            let busyCats, readyCats, readyStreams, unprefixedStreams, malformedStreams = CatStats(), CatStats(), CatStats(), CatStats(), CatStats()
            let kb sz = (sz + 512L) / 1024L
            for KeyValue (stream,state) in states do
                match estimateSize state with
                | 0L ->
                    synced <- synced + 1
                | sz when busy.Contains stream ->
                    busyCats.Ingest(categorize stream)
                    busyCount <- busyCount + 1
                    busyB <- busyB + sz
                | sz when state.isMalformed ->
                    malformedStreams.Ingest(stream, mb sz |> int64)
                    malformed <- malformed + 1
                    malformedB <- malformedB + sz
                | sz when not state.IsReady ->
                    unprefixedStreams.Ingest(stream, mb sz |> int64)
                    unprefixed <- unprefixed + 1
                    unprefixedB <- unprefixedB + sz
                | sz ->
                    readyCats.Ingest(categorize stream)
                    readyStreams.Ingest(sprintf "%s@%dx%d" stream (defaultArg state.write 0L) state.queue.[0].events.Length, kb sz)
                    ready <- ready + 1
                    readyB <- readyB + sz
            log.Information("Streams Synced {synced:n0} Active {busy:n0}/{busyMb:n1}MB Ready {ready:n0}/{readyMb:n1}MB Waiting {waiting}/{waitingMb:n1}MB Malformed {malformed}/{malformedMb:n1}MB",
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
        | Added of streams: int * skip: int * events: int
        /// Result of processing on stream - result (with basic stats) or the `exn` encountered
        | Result of duration : TimeSpan * (string * 'R)
       
    type BufferState = Idle | Busy | Full | Slipstreaming

    /// Gathers stats pertaining to the core projection/ingestion activity
    type StreamSchedulerStats<'R,'E>(log : ILogger, statsInterval : TimeSpan, stateInterval : TimeSpan) =
        let cycles, fullCycles, states, oks, exns = ref 0, ref 0, CatStats(), LatencyStats("ok"), LatencyStats("exceptions")
        let batchesPended, streamsPended, eventsSkipped, eventsPended = ref 0, ref 0, ref 0, ref 0
        let statsDue, stateDue = intervalCheck statsInterval, intervalCheck stateInterval
        let mutable dt,ft,it,st,mt = TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero
        let dumpStats (dispatchActive,dispatchMax) batchesWaiting =
            log.Information("Scheduler {cycles} cycles ({fullCycles} full) {@states} Running {busy}/{processors}",
                !cycles, !fullCycles, states.StatsDescending, dispatchActive, dispatchMax)
            cycles := 0; fullCycles := 0; states.Clear()
            oks.Dump log; exns.Dump log
            log.Information(" Batches Holding {batchesWaiting} Started {batches} ({streams:n0}s {events:n0}-{skipped:n0}e)",
                batchesWaiting, !batchesPended, !streamsPended, !eventsSkipped + !eventsPended, !eventsSkipped)
            batchesPended := 0; streamsPended := 0; eventsSkipped := 0; eventsPended := 0
            log.Information(" Cpu Streams {mt:n1}s Batches {it:n1}s Dispatch {ft:n1}s Results {dt:n1}s Stats {st:n1}s",
                mt.TotalSeconds, it.TotalSeconds, ft.TotalSeconds, dt.TotalSeconds, st.TotalSeconds)
            dt <- TimeSpan.Zero; ft <- TimeSpan.Zero; it <- TimeSpan.Zero; st <- TimeSpan.Zero; mt <- TimeSpan.Zero
        abstract member Handle : InternalMessage<Choice<'R,'E>> -> unit
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
        member __.DumpStats((used,max), batchesWaiting) =
            incr cycles
            if statsDue () then
                dumpStats (used,max) batchesWaiting
                __.DumpStats()
        member __.TryDumpState(state,dump,(_dt,_ft,_mt,_it,_st)) =
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
    type Dispatcher<'R>(maxDop) =
        // Using a Queue as a) the ordering is more correct, favoring more important work b) we are adding from many threads so no value in ConcurrentBag'sthread-affinity
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

    [<NoComparison; NoEquality>]
    type StreamsBatch<'Format> private (onCompletion, buffer, reqs) =
        let mutable buffer = Some buffer
        static member Create(onCompletion, items : StreamEvent<'Format> seq) =
            let buffer, reqs = Buffering.Streams<'Format>(), Dictionary<string,int64>()
            let mutable itemCount = 0
            for item in items do
                itemCount <- itemCount + 1
                buffer.Merge(item)
                match reqs.TryGetValue(item.stream), item.index + 1L with
                | (false, _), required -> reqs.[item.stream] <- required
                | (true, actual), required when actual < required -> reqs.[item.stream] <- required
                | (true,_), _ -> () // replayed same or earlier item
            let batch = StreamsBatch(onCompletion, buffer, reqs)
            batch,(batch.RemainingStreamsCount,itemCount)
        member __.OnCompletion = onCompletion
        member __.Reqs = reqs :> seq<KeyValuePair<string,int64>>
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
    type StreamSchedulingEngine<'R,'E>
        (   dispatcher : Dispatcher<string*Choice<'R,'E>>, stats : StreamSchedulerStats<'R,'E>,
            project : int64 option * string * StreamSpan<byte[]> -> Async<Choice<_,_>>, interpretProgress, dumpStreams,
            /// Tune number of batches to ingest at a time. Default 5.
            ?maxBatches,
            /// Tune the max number of check/dispatch cyles. Default 16.
            ?maxCycles,
            /// Tune the sleep time when there are no items to schedule or responses to process. Default 2ms.
            ?idleDelay,
            /// Opt-in to allowing items to be processed independent of batch sequencing - requires upstream/projection function to be able to identify gaps. Default false.
            ?enableSlipstreaming) =
        let idleDelay = defaultArg idleDelay (TimeSpan.FromMilliseconds 2.)
        let sleepIntervalMs = int idleDelay.TotalMilliseconds
        let maxCycles, maxBatches, slipstreamingEnabled = defaultArg maxCycles 16, defaultArg maxBatches 5, defaultArg enableSlipstreaming false
        let work = ConcurrentStack<InternalMessage<Choice<'R,'E>>>() // dont need them ordered so Queue is unwarranted; usage is cross-thread so Bag is not better
        let pending = ConcurrentQueue<StreamsBatch<byte[]>>() // Queue as need ordering
        let streams = StreamStates<byte[]>()
        let progressState = Progress.ProgressState()
        
        let weight stream =
            let state = streams.Item stream
            let firstSpan = Array.head state.queue
            let mutable acc = 0
            for x in firstSpan.events do acc <- acc + eventSize x
            int64 acc

        // ingest information to be gleaned from processing the results into `streams`
        static let workLocalBuffer = Array.zeroCreate 1024
        let tryDrainResults feedStats =
            let mutable worked, more = false, true
            while more do
                let c = work.TryPopRange(workLocalBuffer)
                if c = 0 then more <- false
                else worked <- true
                for i in 0..c-1 do
                    let x = workLocalBuffer.[i]
                    match x with
                    | Added _ -> () // Only processed in Stats (and actually never enters this queue)
                    | Result (_duration,(stream,res)) ->
                        match interpretProgress streams stream res with
                        | None -> streams.MarkFailed stream
                        | Some index ->
                            progressState.MarkStreamProgress(stream,index)
                            streams.MarkCompleted(stream,index)
                    feedStats x
            worked
        // On each iteration, we try to fill the in-flight queue, taking the oldest and/or heaviest streams first
        let tryFillDispatcher includeSlipstreamed =
            let mutable hasCapacity, dispatched = dispatcher.HasCapacity, false
            if hasCapacity then
                let potential = streams.Pending(includeSlipstreamed, progressState.InScheduledOrder weight)
                let xs = potential.GetEnumerator()
                while xs.MoveNext() && hasCapacity do
                    let (_,s,_) as item = xs.Current
                    let succeeded = dispatcher.TryAdd(async { let! r = project item in return s, r })
                    if succeeded then streams.MarkBusy s
                    dispatched <- dispatched || succeeded // if we added any request, we'll skip sleeping
                    hasCapacity <- succeeded
            hasCapacity, dispatched
        // Take an incoming batch of events, correlating it against our known stream state to yield a set of remaining work
        let ingestPendingBatch feedStats (markCompleted, items : seq<KeyValuePair<string,int64>>) = 
            let inline validVsSkip (streamState : StreamState<_>) required =
                match streamState.write with
                | Some cw when cw >= required -> 0, 1
                | _ -> 1, 0
            let reqs = Dictionary()
            let mutable count, skipCount = 0, 0
            for item in items do
                let streamState = streams.Item item.Key
                match validVsSkip streamState item.Value with
                | 0, skip ->
                    skipCount <- skipCount + skip
                | required, _ ->
                    count <- count + required
                    reqs.[item.Key] <- item.Value
            progressState.AppendBatch(markCompleted,reqs)
            feedStats <| Added (reqs.Count,skipCount,count)

        member __.Pump _abend = async {
            use _ = dispatcher.Result.Subscribe(Result >> work.Push)
            let! ct = Async.CancellationToken
            while not ct.IsCancellationRequested do
                let mutable idle, dispatcherState, remaining = true, Idle, maxCycles
                let mutable dt,ft,mt,it,st = TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero
                while remaining <> 0 do
                    remaining <- remaining - 1
                    // 1. propagate write write outcomes to buffer (can mark batches completed etc)
                    let processedResults = (fun () -> tryDrainResults stats.Handle) |> accStopwatch <| fun x -> dt <- dt + x
                    // 2. top up provisioning of writers queue
                    let hasCapacity, dispatched = (fun () -> tryFillDispatcher (dispatcherState = Slipstreaming)) |> accStopwatch <| fun x -> ft <- ft + x
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
                                (fun () -> ingestPendingBatch stats.Handle (batch.OnCompletion, batch.Reqs)) |> accStopwatch <| fun t -> it <- it + t
                                batchesTaken <- batchesTaken + 1
                                more <- batchesTaken < maxBatches
                            | false,_ when batchesTaken <> 0  ->
                                more <- false
                            | false,_ when (*batchesTaken = 0 && *)slipstreamingEnabled ->
                                dispatcherState <- Slipstreaming
                                more <- false
                            | false,_  ->
                                remaining <- 0
                                more <- false
                    | Slipstreaming -> // only do one round of slipstreaming
                        remaining <- 0
                    | Busy | Full -> failwith "Not handled here"
                    // This loop can take a long time; attempt logging of stats per iteration
                    (fun () -> stats.DumpStats(dispatcher.State,pending.Count)) |> accStopwatch <| fun t -> st <- st + t
                // 3. Record completion state once per full iteration; dumping streams is expensive so needs to be done infrequently
                if not (stats.TryDumpState(dispatcherState,dumpStreams streams,(dt,ft,mt,it,st))) && idle then
                    // 4. Do a minimal sleep so we don't run completely hot when empty (unless we did something non-trivial)
                    Thread.Sleep sleepIntervalMs } // Not Async.Sleep so we don't give up the thread
        member __.Submit(x : StreamsBatch<_>) =
            pending.Enqueue(x)

    type StreamSchedulingEngine =
        static member Create<'Stats,'Req,'Outcome>
            (   dispatcher : Dispatcher<string*Choice<int64*'Stats*'Outcome,'Stats*exn>>, stats : StreamSchedulerStats<int64*'Stats*'Outcome,'Stats*exn>,
                prepare : string*StreamSpan<byte[]> -> 'Stats*'Req, handle : 'Req -> Async<int*'Outcome>,
                dumpStreams, ?maxBatches, ?idleDelay, ?enableSlipstreaming)
            : StreamSchedulingEngine<int64*'Stats*'Outcome,'Stats*exn> =
            let project (_maybeWritePos, stream, span) : Async<Choice<int64*'Stats*'Outcome, 'Stats*exn>> = async {
                let stats,req = prepare (stream,span)
                try let! count,outcome = handle req
                    return Choice1Of2 ((span.index + int64 count),stats,outcome)
                with e -> return Choice2Of2 (stats,e) }
            let interpretProgress (_streams : StreamStates<_>) _stream : Choice<int64*'Stats*'Outcome,'Stats*exn> -> Option<int64> = function
                | Choice1Of2 (index,_stats,_outcome) -> Some index
                | Choice2Of2 _ -> None
            StreamSchedulingEngine
                (   dispatcher, stats, project, interpretProgress, dumpStreams,
                    ?maxBatches = maxBatches, ?idleDelay = idleDelay, ?enableSlipstreaming = enableSlipstreaming)

module Projector =

    type OkResult<'R> = int64 * (int*int) * 'R
    type FailResult = (int*int) * exn

    type Stats<'R>(log : ILogger, categorize, statsInterval, statesInterval) =
        inherit Scheduling.StreamSchedulerStats<OkResult<'R>,FailResult>(log, statsInterval, statesInterval)
        let okStreams, failStreams, badCats, resultOk, resultExnOther = HashSet(), HashSet(), CatStats(), ref 0, ref 0
        let mutable okEvents, okBytes, exnEvents, exnBytes = 0, 0L, 0, 0L

        override __.DumpStats() =
            log.Information("Projected {mb:n0}MB {completed:n0}m {streams:n0}s {events:n0}e ({ok:n0} ok)",
                mb okBytes, !resultOk, okStreams.Count, okEvents, !resultOk)
            okStreams.Clear(); resultOk := 0; okEvents <- 0; okBytes <- 0L
            if !resultExnOther <> 0 then
                log.Warning("Exceptions {mb:n0}MB {fails:n0}r {streams:n0}s {events:n0}e",
                    mb exnBytes, !resultExnOther, failStreams.Count, exnEvents)
                resultExnOther := 0; failStreams.Clear(); exnBytes <- 0L; exnEvents <- 0
                log.Warning("Malformed cats {@badCats}", badCats.StatsDescending)
                badCats.Clear()

        override __.Handle message =
            let inline adds x (set:HashSet<_>) = set.Add x |> ignore
            let inline bads x (set:HashSet<_>) = badCats.Ingest(categorize x); adds x set
            base.Handle message
            match message with
            | Scheduling.Added _ -> () // Processed by standard logging already; we have nothing to add
            | Scheduling.Result (_duration, (stream, Choice1Of2 (_,(es,bs),_r))) ->
                adds stream okStreams
                okEvents <- okEvents + es
                okBytes <- okBytes + int64 bs
                incr resultOk
            | Scheduling.Result (_duration, (stream, Choice2Of2 ((es,bs),exn))) ->
                bads stream failStreams
                exnEvents <- exnEvents + es
                exnBytes <- exnBytes + int64 bs
                incr resultExnOther
                log.Warning(exn,"Failed processing {b:n0} bytes {e:n0}e in stream {stream}", bs, es, stream)

    type StreamsIngester =
        static member Start(log, partitionId, maxRead, submit, ?statsInterval, ?sleepInterval) =
            let makeBatch onCompletion (items : StreamEvent<_> seq) =
                let items = Array.ofSeq items
                let streams = HashSet(seq { for x in items -> x.stream })
                let batch : Submission.SubmissionBatch<_> = { partitionId = partitionId; onCompletion = onCompletion; messages = items }
                batch,(streams.Count,items.Length)
            Ingestion.Ingester<StreamEvent<_> seq,Submission.SubmissionBatch<StreamEvent<_>>>.Start(log, maxRead, makeBatch, submit, ?statsInterval = statsInterval, ?sleepInterval = sleepInterval)

    type StreamsSubmitter =
        static member Create(log : Serilog.ILogger, mapBatch, submitStreamsBatch, statsInterval, ?maxSubmissionsPerPartition, ?pumpInterval) =
            let maxSubmissionsPerPartition = defaultArg maxSubmissionsPerPartition 5
            let submitBatch (x : Scheduling.StreamsBatch<_>) : int =
                submitStreamsBatch x
                x.RemainingStreamsCount
            let tryCompactQueue (queue : Queue<Scheduling.StreamsBatch<_>>) =
                let mutable acc, worked = None, false
                for x in queue do
                    match acc with
                    | None -> acc <- Some x
                    | Some a -> if a.TryMerge x then worked <- true
                worked
            Submission.SubmissionEngine<_,_>(log, maxSubmissionsPerPartition, mapBatch, submitBatch, statsInterval, tryCompactQueue=tryCompactQueue, ?pumpInterval=pumpInterval)

    type StreamsProjectorPipeline =
        static member Start(log : Serilog.ILogger, pumpDispatcher, pumpScheduler, maxReadAhead, submitStreamsBatch, statsInterval, ?ingesterStatsInterval, ?maxSubmissionsPerPartition) =
            let mapBatch onCompletion (x : Submission.SubmissionBatch<StreamEvent<_>>) : Scheduling.StreamsBatch<_> =
                let onCompletion () = x.onCompletion(); onCompletion()
                Scheduling.StreamsBatch.Create(onCompletion, x.messages) |> fst
            let submitter = StreamsSubmitter.Create(log, mapBatch, submitStreamsBatch, statsInterval, ?maxSubmissionsPerPartition=maxSubmissionsPerPartition)
            let startIngester (rangeLog, projectionId) = StreamsIngester.Start(rangeLog, projectionId, maxReadAhead, submitter.Ingest, ?statsInterval = ingesterStatsInterval)
            ProjectorPipeline.Start(log, pumpDispatcher, pumpScheduler, submitter.Pump(), startIngester)

type StreamsProjector =
    static member Start<'Req,'Outcome>(log : ILogger, maxReadAhead, maxConcurrentStreams, prepare, project, categorize, ?statsInterval, ?stateInterval)
        : ProjectorPipeline<_> =
        let statsInterval, stateInterval = defaultArg statsInterval (TimeSpan.FromMinutes 5.), defaultArg stateInterval (TimeSpan.FromMinutes 5.)
        let projectionStats = Projector.Stats<'Outcome>(log.ForContext<Projector.Stats<'Outcome>>(), categorize, statsInterval, stateInterval)
        let dispatcher = Scheduling.Dispatcher<_>(maxConcurrentStreams)
        let streamScheduler = Scheduling.StreamSchedulingEngine.Create<_,'Req,'Outcome>(dispatcher, projectionStats, prepare, project, fun s l -> s.Dump(l, Buffering.StreamState.eventsSize, categorize))
        Projector.StreamsProjectorPipeline.Start(log, dispatcher.Pump(), streamScheduler.Pump, maxReadAhead, streamScheduler.Submit, statsInterval)

    static member Start<'Outcome>(log : ILogger, maxReadAhead, maxConcurrentStreams, handle, categorize, ?statsInterval, ?stateInterval)
        : ProjectorPipeline<_> =
        let prepare (streamName,span) =
            let stats = Buffering.StreamSpan.stats span
            stats,(streamName,span)
        let handle (streamName,span : StreamSpan<_>) = async {
            let! res = handle (streamName,span)
            return span.events.Length,res }
        StreamsProjector.Start<_,'Outcome>(log, maxReadAhead, maxConcurrentStreams, prepare, handle, categorize, ?statsInterval=statsInterval, ?stateInterval=stateInterval)