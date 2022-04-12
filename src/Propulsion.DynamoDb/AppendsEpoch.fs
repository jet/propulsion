module Propulsion.DynamoDb.AppendsEpoch

open System.Collections.Generic
open System.Collections.Immutable

let [<Literal>] Category = "AppendsEpoch"
let streamName (tid, eid) = FsCodec.StreamName.compose Category [AppendsTrancheId.toString tid; AppendsEpochId.toString eid]

// NB - these types and the union case names reflect the actual storage formats and hence need to be versioned with care
[<RequireQualifiedAccess>]
module Events =

    type [<Struct>] StreamSpan ={ p : IndexStreamId; i : int64; c : int } // stream, index, count
    // NOTE while the `i` values in appended could be inferred, we redundantly store them here to enable optimal tailing
    type Ingested =             { started : StreamSpan[]; appended : StreamSpan[] }
    type Event =
        | Ingested of           Ingested
        | Closed
        interface TypeShape.UnionContract.IUnionContract
    let codec = EventCodec.create<Event>()

module Fold =

    type State =
        { versions : ImmutableDictionary<IndexStreamId, Span>; closed : bool }
        member state.With(e : Events.Ingested) =
            let news = seq { for x in e.started -> KeyValuePair(x.p, Span.Of x) }
            let updates = seq { for e in e.appended -> let cur = state.versions[e.p] in KeyValuePair(e.p, { cur with count = cur.count + e.c }) }
            { state with versions = state.versions.AddRange(news).SetItems(updates)  }
        member state.WithClosed() = { state with closed = true }
    and [<Struct>] Span =
        { index : int; count : int }
        static member Of(x : Events.StreamSpan) = { index = int x.i; count = x.c }
        member x.Next = x.index + x.count |> int64
    let initial = { versions = ImmutableDictionary.Create(); closed = false }
    let private evolve (state : State) = function
        | Events.Ingested e ->  state.With(e)
        | Events.Closed ->      state.WithClosed()
    let fold : State -> Events.Event seq -> State = Seq.fold evolve

module Ingest =

    let next (x : Events.StreamSpan) = x.i + int64 x.c
    let flatten : Events.StreamSpan seq -> Events.StreamSpan seq =
        Seq.groupBy (fun x -> x.p)
        >> Seq.map (fun (p, xs) ->
            let i = xs |> Seq.map (fun x -> int64 x.i) |> Seq.min
            let n = xs |> Seq.map next |> Seq.max |> int
            { p = p; i = i; c = n - int i })
    let tryToIngested ({ versions = cur } : Fold.State) (inputs : Events.StreamSpan seq) : Events.Ingested option =
        let started, appended = ResizeArray<Events.StreamSpan>(), ResizeArray<Events.StreamSpan>()
        for eventSpan in flatten inputs do
            match cur.TryGetValue(eventSpan.p) with
            | false, _ -> started.Add eventSpan
            | true, curSpan ->
                match next eventSpan - curSpan.Next with
                | appLen when appLen > 0 -> appended.Add { p = eventSpan.p; i = curSpan.Next; c = int appLen }
                | _ -> ()
        match started.ToArray(), appended.ToArray() with
        | [||], [||] -> None
        | s, a -> Some { started = s; appended = a }
    let removeDuplicates ({ versions = cur } : Fold.State) inputs : Events.StreamSpan[] =
        [| for eventSpan in flatten inputs do
            match cur.TryGetValue(eventSpan.p) with
            | false, _ -> ()
            | true, curSpan ->
                match next eventSpan - curSpan.Next with
                | appLen when appLen > 0 -> yield { p = eventSpan.p; i = curSpan.Next; c = int appLen }
                | _ -> yield eventSpan |]
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

    member _.Ingest(trancheId, epochId, items, ?assumeEmpty) : Async<ExactlyOnceIngester.IngestResult<_, _>> =
        let decider = resolve (trancheId, epochId)
        decider.Transact(Ingest.decide shouldClose items, if assumeEmpty = Some true then Equinox.AssumeEmpty else Equinox.AllowStale)

module Config =

    let private resolveStream (context, cache) =
        let cat = Config.createUnoptimized Events.codec Fold.initial Fold.fold (context, cache)
        cat.Resolve
    let create maxItemsPerEpoch store =
        let shouldClose totalItems = totalItems >= maxItemsPerEpoch
        let resolve = streamName >> resolveStream store >> Config.createDecider
        Service(shouldClose, resolve)

module Reader =

    type Event = int64 * Events.Event
    let codec : FsCodec.IEventCodec<Event, _, _> = EventCodec.withIndex<Events.Event>

    type State = { changes : struct (int * Events.StreamSpan[])[]; closed : bool }
    let initial = { changes = Array.empty; closed = false }
    let fold (state : State) (events : Event seq) =
        let mutable closed = state.closed
        let changes = ResizeArray(state.changes)
        let pos = Dictionary()
        let index (x : Events.StreamSpan) = pos[x.p] <- int x.i + x.c
        state.changes |> Array.iter (fun struct (_,xs) -> xs |> Array.iter index)
        for x in events do
            match x with
            | _, Events.Closed -> closed <- true
            | i, Events.Ingested e ->
                let spans = ResizeArray(e.started.Length + e.appended.Length)
                spans.AddRange e.started
                for x in e.appended do
                    spans.Add { p = x.p; i = pos[x.p]; c = x.c }
                spans |> Seq.iter index
                changes.Add(int i, spans.ToArray())
        { changes = changes.ToArray(); closed = closed }

    type Service internal (resolve : AppendsTrancheId * AppendsEpochId -> Equinox.Decider<Event, State>) =

        member _.Read(trancheId, epochId) : Async<State> =
            let decider = resolve (trancheId, epochId)
            decider.Query(id)

    module Config =

        let private resolveStream context =
            let cat = Config.createUncachedUnoptimized codec initial fold context
            cat.Resolve
        let create store =
            let resolve = streamName >> resolveStream store >> Config.createDecider
            Service(resolve)
