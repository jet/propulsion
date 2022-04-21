/// Maintains a sequence of Ingested StreamSpans representing groups of stream writes that were indexed at the same time
/// Ingestion takes care of deduplicating each batch of writes with reference to the existing spans recorded for this Epoch (only)
/// When the maximum events count is reached, the Epoch is closed, and writes transition to the successor Epoch
/// The Reader module reads Ingested events forward from a given Index on the Epoch's stream
/// The Checkpoint per Index consists of the pair of 1. EpochId 2. Event Index within that Epoch (see `module Checkpoint` for detail)
module Propulsion.DynamoStore.AppendsEpoch

open System.Collections.Generic
open System.Collections.Immutable

let [<Literal>] Category = "$AppendsEpoch"
let streamName (tid, eid) = FsCodec.StreamName.compose Category [AppendsTrancheId.toString tid; AppendsEpochId.toString eid]

// NB - these types and the union case names reflect the actual storage formats and hence need to be versioned with care
[<RequireQualifiedAccess>]
module Events =

    // NOTE while the `i` values in `appended` could be inferred, we redundantly store them to enable optimal tailing
    //      without having to read and/or process/cache all preceding events
    type Ingested =             { started : StreamSpan array; appended : StreamSpan array }
                                // Structure mapped from DynamoStore.Batch.Schema: p: stream, i: index, c: array of event types
     and [<Struct>] StreamSpan = { p : IndexStreamId; i : int64; c : string[] }
    type Event =
        | Ingested of           Ingested
        | Closed
        interface TypeShape.UnionContract.IUnionContract
    let codec = EventCodec.create<Event>()

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
            let news = seq { for x in e.started -> KeyValuePair(x.p, next x) }
            let updates = seq { for x in e.appended -> KeyValuePair(x.p, next x) }
            { state with versions = state.versions.AddRange(news).SetItems(updates)  }
        member state.WithClosed() = { state with closed = true }
    let initial = { versions = ImmutableDictionary.Create(); closed = false }
    let private evolve (state : State) = function
        | Events.Ingested e ->  state.With(e)
        | Events.Closed ->      state.WithClosed()
    let fold : State -> Events.Event seq -> State = Seq.fold evolve

module Ingest =

    let (|Start|Append|Discard|) ({ versions = cur } : Fold.State, eventSpan : Events.StreamSpan) =
        match cur.TryGetValue eventSpan.p with
        | false, _ -> Start eventSpan
        | true, curNext ->
            match next eventSpan - curNext with
            | appendLen when appendLen > 0 -> Append ({ p = eventSpan.p; i = curNext; c = Array.skip (eventSpan.c.Length - appendLen) eventSpan.c } : Events.StreamSpan)
            | _ -> Discard

    /// Takes a set of spans, flattens them and trims them relative to the currently established per-stream high-watermarks
    let tryToIngested state (inputs : Events.StreamSpan seq) : Events.Ingested option =
        let started, appended = ResizeArray<Events.StreamSpan>(), ResizeArray<Events.StreamSpan>()
        for eventSpan in flatten inputs do
            match state, eventSpan with
            | Start es -> started.Add es
            | Append es -> appended.Add es
            | Discard -> ()
        match started.ToArray(), appended.ToArray() with
        | [||], [||] -> None
        | s, a -> Some { started = s; appended = a }
    /// Trims the supplied inputs, removing items that overlap with this Epoch's per-stream max index
    let removeDuplicates state inputs : Events.StreamSpan array =
        [| for eventSpan in flatten inputs do
            match state, eventSpan with
            | Start es
            | Append es -> es
            | Discard -> () |]
    let decide shouldClose (inputs : Events.StreamSpan seq) = function
        | ({ closed = false; versions = cur } as state : Fold.State) ->
            let closed, ingested, events =
                match tryToIngested state inputs with
                | None -> false, Array.empty, []
                | Some diff ->
                    let closing = shouldClose (diff.appended.Length + diff.started.Length + cur.Count)
                    let ingestEvent = Events.Ingested diff
                    let ingested = (seq { for x in diff.started -> x.p }, seq { for x in diff.appended -> x.p }) ||> Seq.append |> Array.ofSeq
                    closing, ingested, [ ingestEvent ; if closing then Events.Closed ]
            let res : ExactlyOnceIngester.IngestResult<_, _> = { accepted = ingested; closed = closed; residual = [||] }
            res, events
        | { closed = true } as state ->
            { accepted = [||]; closed = true; residual = removeDuplicates state inputs }, []

type Service internal (shouldClose, resolve : AppendsTrancheId * AppendsEpochId -> Equinox.Decider<Events.Event, Fold.State>) =

    member _.Ingest(trancheId, epochId, spans : Events.StreamSpan[], ?assumeEmpty) : Async<ExactlyOnceIngester.IngestResult<_, _>> =
        let decider = resolve (trancheId, epochId)
        if Array.isEmpty spans then async { return { accepted = [||]; closed = false; residual = [||] } } else // special-case null round-trips

        let isSelf p = IndexStreamId.toStreamName p |> FsCodec.StreamName.splitCategoryAndId |> fst = Category
        if spans |> Array.exists (function { p = p } -> isSelf p) then invalidArg (nameof spans) "Writes to indices should be filtered prior to indexing"
        decider.Transact(Ingest.decide shouldClose spans, if assumeEmpty = Some true then Equinox.AssumeEmpty else Equinox.AllowStale)

module Config =

    let private resolveStream (context, cache) =
        let cat = Config.createUnoptimized Events.codec Fold.initial Fold.fold (context, Some cache)
        cat.Resolve
    let create maxItemsPerEpoch store =
        let shouldClose totalItems = totalItems >= maxItemsPerEpoch
        let resolve = streamName >> resolveStream store >> Config.createDecider
        Service(shouldClose, resolve)

/// Manages the loading of Ingested Span Batches in a given Epoch from a given position forward
/// In the case where we are polling the tail, this should mean we typically do a single round-trip for a point read of the Tip
/// only deserializing events pertaining to things we have not seen before
module Reader =

    type Event = int64 * Events.Event
    let codec : FsCodec.IEventCodec<Event, _, _> = EventCodec.withIndex<Events.Event>

    type State = { changes : struct (int * Events.StreamSpan array) array; closed : bool }
    let initial = { changes = Array.empty; closed = false }
    let fold (state : State) (events : Event seq) =
        let mutable closed = state.closed
        let changes = ResizeArray(state.changes)
        for x in events do
            match x with
            | _, Events.Closed -> closed <- true
            | i, Events.Ingested e -> changes.Add(int i, Array.append e.started e.appended)
        { changes = changes.ToArray(); closed = closed }

    type Service internal (resolve : AppendsTrancheId * AppendsEpochId * int64 -> Equinox.Decider<Event, State>) =

        member _.Read(trancheId, epochId, (*inclusive*)minIndex) : Async<State> =
            let decider = resolve (trancheId, epochId, minIndex)
            decider.Query(id)

    module Config =

        let private resolveStream context minIndex =
            let cat = Config.createWithOriginIndex codec initial fold context minIndex
            cat.Resolve
        let create context =
            let resolve minIndex = streamName >> resolveStream context minIndex >> Config.createDecider
            Service(fun (tid, eid, minIndex) -> resolve minIndex (tid, eid) )
