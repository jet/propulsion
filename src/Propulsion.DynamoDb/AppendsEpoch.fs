module Propulsion.DynamoDb.AppendsEpoch

open System.Collections.Generic
open System.Collections.Immutable

let [<Literal>] Category = "AppendsEpoch"
let streamName (tid, eid) = FsCodec.StreamName.compose Category [AppendsTrancheId.toString tid; AppendsEpochId.toString eid]

// NB - these types and the union case names reflect the actual storage formats and hence need to be versioned with care
[<RequireQualifiedAccess>]
module Events =

    type [<Struct>] StreamSpan ={ p : IndexStreamId; i : int64; c : int } // stream, index, count
    type [<Struct>] Added =     { p : IndexStreamId; c : int } // stream, count
    type Ingested =             { started : StreamSpan[]; appended : Added[] }
    type Event =
        | Ingested of           Ingested
        | Closed
        interface TypeShape.UnionContract.IUnionContract
    open FsCodec.SystemTextJson
    let codec = Codec.Create<Event>().ToByteArrayCodec()

module Fold =

    type State =
        { versions : ImmutableDictionary<IndexStreamId, Span>; closed : bool }
        member state.With(e : Events.Ingested) =
            let news = seq { for x in e.started -> KeyValuePair(x.p, Span.Of x) }
            let updates = seq { for e in e.appended -> let cur = state.versions[e.p] in KeyValuePair(e.p, { cur with count = cur.count + e.c }) }
            { state with versions = state.versions.AddRange(news).SetItems(updates)  }
    and [<Struct>] Span =
        { index : int; count : int }
        static member Of(x : Events.StreamSpan) = { index = int x.i; count = x.c }
        member x.Next = x.index + x.count |> int64
    let initial = { versions = ImmutableDictionary.Create(); closed = false }
    let private evolve (state : State) = function
        | Events.Ingested e ->  state.With(e)
        | Events.Closed ->      { state with closed = true }
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
        let started, appended = ResizeArray<Events.StreamSpan>(), ResizeArray<Events.Added>()
        for eventSpan in flatten inputs do
            match cur.TryGetValue(eventSpan.p) with
            | false, _ -> started.Add eventSpan
            | true, curSpan ->
                match next eventSpan - curSpan.Next with
                | appLen when appLen > 0 -> appended.Add { p = eventSpan.p; c = int appLen }
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

//    member _.Read epochId : Async<Fold.State> =
//        let decider = resolve epochId
//        decider.Query(id, Equinox.AllowStale)

module Config =

    let private resolveStream (context, cache) =
        let cacheStrategy = Equinox.DynamoStore.CachingStrategy.SlidingWindow (cache, System.TimeSpan.FromMinutes 20.)
        let accessStrategy = Equinox.DynamoStore.AccessStrategy.Unoptimized
        let cat = Equinox.DynamoStore.DynamoStoreCategory(context, Events.codec, Fold.fold, Fold.initial, cacheStrategy, accessStrategy)
        cat.Resolve
    let private resolveDecider stream = Equinox.Decider(Serilog.Log.Logger, stream, maxAttempts = 3)
    let create maxItemsPerEpoch store =
        let shouldClose totalItems = totalItems >= maxItemsPerEpoch
        let resolve = streamName >> resolveStream store >> resolveDecider
        Service(shouldClose, resolve)
