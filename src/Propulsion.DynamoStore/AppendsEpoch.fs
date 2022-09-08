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
let [<Literal>] MaxItemsPerEpoch = 1_000_000
let [<Literal>] Category = "$AppendsEpoch"
let streamName struct (tid, eid) = struct (Category, FsCodec.StreamName.createStreamId [AppendsTrancheId.toString tid; AppendsEpochId.toString eid])

// NB - these types and the union case names reflect the actual storage formats and hence need to be versioned with care
[<RequireQualifiedAccess>]
module Events =

    // NOTE while the `i` values in `app` could be inferred, we redundantly store them to enable optimal tailing
    //      without having to read and/or process/cache all preceding events
    type Ingested =             { add : StreamSpan array; app : StreamSpan array }
                                // Structure mapped from DynamoStore.Batch.Schema: p: stream, i: index, c: array of event types
     and [<Struct>] StreamSpan = { p : IndexStreamId; i : int64; c : string array }
    type Event =
        | Ingested of           Ingested
        | Closed
        interface TypeShape.UnionContract.IUnionContract
    let codec = EventCodec.gen<Event>

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
        | s, a -> Some { add = s; app = a }
    /// Trims the supplied inputs, removing items that overlap with this Epoch's per-stream max index
    let removeDuplicates state inputs : Events.StreamSpan array =
        [| for eventSpan in flatten inputs do
            match state, eventSpan with
            | Start es
            | Append es -> es
            | Discard -> () |]
    let decide shouldClose (inputs : Events.StreamSpan seq) : _ -> _ * _ = function
        | ({ closed = false; versions = cur } as state : Fold.State) ->
            let closed, ingested, events =
                match tryToIngested state inputs with
                | None -> false, Array.empty, []
                | Some diff ->
                    let closing = shouldClose (diff.app.Length + diff.add.Length + cur.Count)
                    let ingestEvent = Events.Ingested diff
                    let ingested = (seq { for x in diff.add -> x.p }, seq { for x in diff.app -> x.p }) ||> Seq.append |> Array.ofSeq
                    closing, ingested, [ ingestEvent ; if closing then Events.Closed ]
            let res : ExactlyOnceIngester.IngestResult<_, _> = { accepted = ingested; closed = closed; residual = [||] }
            res, events
        | { closed = true } as state ->
            { accepted = [||]; closed = true; residual = removeDuplicates state inputs }, []

type Service internal (shouldClose, resolve : struct (AppendsTrancheId * AppendsEpochId) -> Equinox.Decider<Events.Event, Fold.State>) =

    member _.Ingest(trancheId, epochId, spans : Events.StreamSpan[], ?assumeEmpty) : Async<ExactlyOnceIngester.IngestResult<_, _>> =
        let decider = resolve (trancheId, epochId)
        if Array.isEmpty spans then async { return { accepted = [||]; closed = false; residual = [||] } } else // special-case null round-trips

        let isSelf p = match IndexStreamId.toStreamName p with FsCodec.StreamName.Category c -> c = Category
        if spans |> Array.exists (function { p = p } -> isSelf p) then invalidArg (nameof spans) "Writes to indices should be filtered prior to indexing"
        decider.TransactEx((fun c -> (Ingest.decide (shouldClose (c.StreamEventBytes, c.Version))) spans c.State), if assumeEmpty = Some true then Equinox.AssumeEmpty else Equinox.AllowStale)

module Config =

    let private createCategory (context, cache) = Config.createUnoptimized Events.codec Fold.initial Fold.fold (context, Some cache)
    let create log (maxBytes : int, maxVersion : int64, maxStreams : int) store =
        let resolve = streamName >> (createCategory store |> Equinox.Decider.resolve log)
        let shouldClose (totalBytes : int64 voption, version) totalStreams =
            let closing = totalBytes.Value > maxBytes || version >= maxVersion || totalStreams >= maxStreams
            if closing then log.Information("Epoch Closing v{version}/{maxVersion} {streams}/{maxStreams} streams {kib:f0}/{maxKib:f0} KiB",
                                            version, maxVersion, totalStreams, maxStreams, float totalBytes.Value / 1024., float maxBytes / 1024.)
            closing
        Service(shouldClose, resolve)

/// Manages the loading of Ingested Span Batches in a given Epoch from a given position forward
/// In the case where we are polling the tail, this should mean we typically do a single round-trip for a point read of the Tip
/// only deserializing events pertaining to things we have not seen before
module Reader =

    type Event = (struct (int64 * Events.Event))
    let codec : FsCodec.IEventCodec<Event, _, _> = EventCodec.withIndex<Events.Event>

    type State = { changes : struct (int * Events.StreamSpan array) array; closed : bool }
    let initial = { changes = Array.empty; closed = false }
    let fold (state : State) (events : Event seq) =
        let mutable closed = state.closed
        let changes = ResizeArray(state.changes)
        for x in events do
            match x with
            | _, Events.Closed -> closed <- true
            | i, Events.Ingested e -> changes.Add(int i, Array.append e.add e.app)
        { changes = changes.ToArray(); closed = closed }

    type Service internal (resolve : AppendsTrancheId * AppendsEpochId * int64 -> Equinox.Decider<Event, State>) =

        member _.Read(trancheId, epochId, (*inclusive*)minIndex) : Async<int64 voption * int64 * State> =
            let decider = resolve (trancheId, epochId, minIndex)
            decider.QueryEx(fun c -> c.StreamEventBytes, c.Version, c.State)

        member _.ReadVersion(trancheId, epochId) : Async<int64> =
            let decider = resolve (trancheId, epochId, System.Int64.MaxValue)
            decider.QueryEx(fun c -> c.Version)

    module Config =

        let private createCategory context minIndex = Config.createWithOriginIndex codec initial fold context minIndex
        let create log context =
            let resolve minIndex = Equinox.Decider.resolve log (createCategory context minIndex)
            Service(fun (tid, eid, minIndex) -> streamName (tid, eid) |> resolve minIndex)
