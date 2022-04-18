/// Maintains a pointer for into the  chain for each Tranche
/// Allows an Ingester to quickly determine the current Epoch which it should commence writing into
/// As an Epoch is marked `Closed`, `module Index` will mark a new epoch `Started` on this aggregate
module Propulsion.DynamoStore.AppendsIndex

let [<Literal>] Category = "$AppendsIndex"
let streamName iid = FsCodec.StreamName.create Category (IndexId.toString iid)

// NB - these types and the union case names reflect the actual storage formats and hence need to be versioned with care
[<RequireQualifiedAccess>]
module Events =

    type Event =
        | Started of {| tranche : AppendsTrancheId; epoch : AppendsEpochId |}
        | Snapshotted of {| active : Map<AppendsTrancheId, AppendsEpochId> |}
        interface TypeShape.UnionContract.IUnionContract
    let codec = EventCodec.create<Event>()

module Fold =

    type State = Map<AppendsTrancheId, AppendsEpochId>

    let initial = Map.empty
    let evolve state = function
        | Events.Started e -> state |> Map.add e.tranche e.epoch
        | Events.Snapshotted e -> e.active
    let fold : State -> Events.Event seq -> State = Seq.fold evolve

    let isOrigin = function Events.Snapshotted _ -> true | _ -> false
    let toSnapshot s = Events.Snapshotted {| active = s |}

let readEpochId trancheId (state : Fold.State) =
    state
    |> Map.tryFind trancheId

let interpret (trancheId, epochId) (state : Fold.State) =
    [if state |> readEpochId trancheId |> Option.forall (fun cur -> cur < epochId) && epochId >= AppendsEpochId.initial then
        yield Events.Started {| tranche = trancheId; epoch = epochId |}]

type Service internal (resolve : unit -> Equinox.Decider<Events.Event, Fold.State>) =

    /// Determines the current active epoch for the specified Tranche
    member _.ReadIngestionEpochId(trancheId) : Async<AppendsEpochId> =
        let decider = resolve ()
        decider.Query(readEpochId trancheId >> Option.defaultValue AppendsEpochId.initial, Equinox.AllowStale)

    /// Mark specified `epochId` as live for the purposes of ingesting commits for the specified Tranche
    /// Writers are expected to react to having writes to an epoch denied (due to it being Closed) by anointing the successor via this
    member _.MarkIngestionEpochId(trancheId, epochId) : Async<unit> =
        let decider = resolve ()
        decider.Transact(interpret (trancheId, epochId), Equinox.AllowStale)

module Config =

    let private resolveStream store =
        let cat = Config.createSnapshotted Events.codec Fold.initial Fold.fold (Fold.isOrigin, Fold.toSnapshot) store
        cat.Resolve
    let internal resolveDecider store () = streamName IndexId.wellKnownId |> resolveStream store |> Config.createDecider
    let create (context, cache) = Service(resolveDecider (context, Some cache))

/// On the Reading Side, there's no advantage to caching (as we have snapshots, and it's Dynamo)
module Reader =

    let readKnownTranches (state : Fold.State) : AppendsTrancheId[] =
        state |> Map.keys |> Array.ofSeq

    type Service internal (resolve : unit -> Equinox.Decider<Events.Event, Fold.State>) =

        member _.ReadKnownTranches() : Async<AppendsTrancheId[]> =
            let decider = resolve ()
            decider.Query(readKnownTranches)

    let create context = Service(Config.resolveDecider (context, None))
