/// Maintains a sequence of Ingested StreamSpans representing groups of stream writes that were indexed at the same time
/// Ingestion takes care of deduplicating each batch of writes with reference to the existing spans recorded for this Epoch (only)
/// When the maximum events count is reached, the Epoch is closed, and writes transition to the successor Epoch
/// The Reader module reads Ingested events forward from a given Index on the Epoch's stream
/// The Checkpoint per Index consists of the pair of 1. EpochId 2. Event Index within that Epoch (see `module Checkpoint` for detail)
module Propulsion.DynamoStore.AppendsEpoch

open Propulsion.Internal
open System.Collections.Generic
open System.Collections.Immutable

/// The absolute upper limit of number of streams that can be indexed within a single Epoch (defines how Checkpoints are encoded, so cannot be changed)
let [<Literal>] MaxItemsPerEpoch = Checkpoint.MaxItemsPerEpoch
let [<Literal>] Category = "$AppendsEpoch"
#if !PROPULSION_DYNAMOSTORE_NOTIFIER
let streamId = Equinox.StreamId.gen2 AppendsPartitionId.toString AppendsEpochId.toString
#endif
let [<return: Struct>] (|StreamName|_|) = function
    | FsCodec.StreamName.CategoryAndIds (Category, [| pid; eid |]) -> ValueSome struct (AppendsPartitionId.parse pid, AppendsEpochId.parse eid)
    | _ -> ValueNone
// NB - these types and the union case names reflect the actual storage formats and hence need to be versioned with care
[<RequireQualifiedAccess>]
module Events =

#if PROPULSION_DYNAMOSTORE_NOTIFIER
    let isEventTypeClosed (et : string) = et = "Closed"
#else
    // NOTE while the `i` values in `app` could be inferred, we redundantly store them to enable optimal tailing
    //      without having to read and/or process/cache all preceding events
    type Ingested =             { add : StreamSpan[]; app : StreamSpan[] }
                                // Structure mapped from DynamoStore.Batch.Schema: p: stream, i: index, c: array of event types
     and [<Struct>] StreamSpan = { p : IndexStreamId; i : int64; c : string[] }
    type Event =
        | Ingested of           Ingested
        | Closed
        interface TypeShape.UnionContract.IUnionContract
    let codec = Store.Codec.gen<Event>
    let isEventTypeClosed (et: string) = et = nameof Closed
#endif

#if !PROPULSION_DYNAMOSTORE_NOTIFIER
let next (x : Events.StreamSpan) = int x.i + x.c.Length
/// Aggregates all spans per stream into a single Span from the lowest index to the highest
let flatten : Events.StreamSpan seq -> Events.StreamSpan seq =
    Seq.groupBy (fun x -> x.p)
    >> Seq.map (fun (p, xs) ->
        let mutable i = -1L
        let c = ResizeArray()
        for x in xs do
            if i = -1 then i <- x.i
            let n = i + int64 c.Count
            let overlap = n - x.i
            if overlap < 0 then invalidOp (sprintf "Invalid gap of %d at %d in '%O'" -overlap n p)
            c.AddRange(Seq.skip (int overlap) x.c)
        { p = p; i = i; c = c.ToArray() })

module Fold =

    [<NoComparison; NoEquality>]
    type State =
        { versions : ImmutableDictionary<IndexStreamId, int>; closed : bool }
        member state.With(e : Events.Ingested) =
            let news = seq { for x in e.add -> KeyValuePair(x.p, next x) }
            let updates = seq { for x in e.app -> KeyValuePair(x.p, next x) }
            { state with versions = state.versions.AddRange(news).SetItems(updates)  }
        member state.WithClosed() = { state with closed = true }
    let initial = { versions = ImmutableDictionary.Create(); closed = false }
    let private evolve (state : State) = function
        | Events.Ingested e ->  state.With(e)
        | Events.Closed ->      state.WithClosed()
    let fold = Array.fold evolve

module Ingest =

    let (|Start|Append|Discard|Gap|) ({ versions = cur } : Fold.State, eventSpan : Events.StreamSpan) =
        match cur.TryGetValue eventSpan.p with
        | false, _ -> Start eventSpan
        | true, curNext ->
            match next eventSpan - curNext with
            | appendLen when appendLen > eventSpan.c.Length -> Gap (appendLen - eventSpan.c.Length)
            | appendLen when appendLen > 0 -> Append ({ p = eventSpan.p; i = curNext; c = Array.skip (eventSpan.c.Length - appendLen) eventSpan.c } : Events.StreamSpan)
            | _ -> Discard

    /// Takes a set of spans, flattens them and trims them relative to the currently established per-stream high-watermarks
    let tryToIngested onlyWarnOnGap state (inputs: Events.StreamSpan seq): Events.Ingested option =
        let started, appended = ResizeArray<Events.StreamSpan>(), ResizeArray<Events.StreamSpan>()
        for eventSpan in flatten inputs do
            match state, eventSpan with
            | Start es -> started.Add es
            | Append es -> appended.Add es
            | Gap g ->
                match onlyWarnOnGap with
                | Some (log: Serilog.ILogger) ->
                    log.Warning("Gap of {gap} at {pos} in {stream}, expect stale state.", g, eventSpan.i, eventSpan.p)
                    appended.Add eventSpan
                | None ->
                    invalidOp $"Invalid gap of %d{g} at %d{eventSpan.i} in '{eventSpan.p}'"
            | Discard -> ()
        match started.ToArray(), appended.ToArray() with
        | [||], [||] -> None
        | s, a -> Some { add = s; app = a }
    /// Trims the supplied inputs, removing items that overlap with this Epoch's per-stream max index
    let removeDuplicates state inputs : Events.StreamSpan[] =
        [| for eventSpan in flatten inputs do
            match state, eventSpan with
            | Start es
            | Append es -> es
            | Gap _ -> eventSpan
            | Discard -> () |]
    let decide onlyWarnOnGap shouldClose (inputs : Events.StreamSpan seq) : _ -> _ * _ = function
        | ({ closed = false; versions = cur } as state : Fold.State) ->
            let closed, ingested, events =
                match tryToIngested onlyWarnOnGap state inputs with
                | None -> false, Array.empty, [||]
                | Some diff ->
                    let closing = shouldClose (diff.app.Length + diff.add.Length + cur.Count)
                    let ingestEvent = Events.Ingested diff
                    let ingested = (seq { for x in diff.add -> x.p }, seq { for x in diff.app -> x.p }) ||> Seq.append |> Array.ofSeq
                    closing, ingested, [| ingestEvent ; if closing then Events.Closed |]
            let res : ExactlyOnceIngester.IngestResult<_, _> = { accepted = ingested; closed = closed; residual = [||] }
            res, events
        | { closed = true } as state ->
            { accepted = [||]; closed = true; residual = removeDuplicates state inputs }, [||]

type Service internal (onlyWarnOnGap, shouldClose, resolve: AppendsPartitionId * AppendsEpochId -> Equinox.Decider<Events.Event, Fold.State>) =

    member _.Ingest(partitionId, epochId, spans: Events.StreamSpan[]) : Async<ExactlyOnceIngester.IngestResult<_, _>> =
        let decider = resolve (partitionId, epochId)
        if Array.isEmpty spans then async { return { accepted = [||]; closed = false; residual = [||] } } else // special-case null round-trips

        let isSelf p = match IndexStreamId.toStreamName p with FsCodec.StreamName.Category c -> c = Category
        if spans |> Array.exists (function { p = p } -> isSelf p) then invalidArg (nameof spans) "Writes to indices should be filtered prior to indexing"
        let decide (c: Equinox.ISyncContext<_>) = Ingest.decide onlyWarnOnGap (shouldClose (c.StreamEventBytes, c.Version)) spans c.State
        decider.TransactEx(decide, Equinox.AnyCachedValue)

module Factory =

    let private createCategory (context, cache) = Store.Dynamo.createUnoptimized Category Events.codec Fold.initial Fold.fold (context, Some cache)
    let create log (maxBytes: int, maxVersion: int64, maxStreams: int, onlyWarnOnGap) store =
        let resolve = createCategory store |> Equinox.Decider.forStream log
        let shouldClose (totalBytes : int64 voption, version) totalStreams =
            let closing = totalBytes.Value > maxBytes || version >= maxVersion || totalStreams >= maxStreams
            if closing then log.Information("Epoch Closing v{version}/{maxVersion} {streams}/{maxStreams} streams {kib:f0}/{maxKib:f0} KiB",
                                            version, maxVersion, totalStreams, maxStreams, float totalBytes.Value / 1024., float maxBytes / 1024.)
            closing
        Service((if onlyWarnOnGap then Some log else None), shouldClose, streamId >> resolve)

/// Manages the loading of Ingested Span Batches in a given Epoch from a given position forward
/// In the case where we are polling the tail, this should mean we typically do a single round-trip for a point read of the Tip
/// only deserializing events pertaining to things we have not seen before
module Reader =

    type Event = (struct (int64 * Events.Event))
    let codec : FsCodec.IEventCodec<Event, _, _> = Streams.decWithIndex<Events.Event>

    type State = { changes : struct (int * Events.StreamSpan[])[]; closed : bool }
    let initial = { changes = Array.empty; closed = false }
    let fold (state : State) (events : Event seq) =
        let mutable closed = state.closed
        let changes = ResizeArray(state.changes)
        for x in events do
            match x with
            | _, Events.Closed -> closed <- true
            | i, Events.Ingested e -> changes.Add(int i, Array.append e.add e.app)
        { changes = changes.ToArray(); closed = closed }

    type Service internal (resolve : AppendsPartitionId * AppendsEpochId * int64 -> Equinox.Decider<Event, State>) =

        member _.Read(partitionId, epochId, (*inclusive*)minIndex) : Async<int64 voption * int64 * State> =
            let decider = resolve (partitionId, epochId, minIndex)
            decider.QueryEx(fun c -> c.StreamEventBytes, c.Version, c.State)

        member _.ReadVersion(partitionId, epochId) : Async<int64> =
            let decider = resolve (partitionId, epochId, System.Int64.MaxValue)
            decider.QueryEx(fun c -> c.Version)

    module Factory =

        let private createCategory context minIndex = Store.Dynamo.createWithOriginIndex Category codec initial fold context minIndex
        let create log context =
            let resolve minIndex = Equinox.Decider.forStream log (createCategory context minIndex)
            Service(fun (pid, eid, minIndex) -> streamId (pid, eid) |> resolve minIndex)
#endif
