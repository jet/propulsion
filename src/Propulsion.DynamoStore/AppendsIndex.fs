/// Maintains a pointer for into the chain for each Partition
/// Allows an Ingester to quickly determine the current Epoch which it should commence writing into
/// As an Epoch is marked `Closed`, `module Index` will mark a new epoch `Started` on this aggregate
module Propulsion.DynamoStore.AppendsIndex

let [<Literal>] Category = "$AppendsIndex"
#if !PROPULSION_DYNAMOSTORE_NOTIFIER
let streamId () = Equinox.StreamId.gen IndexId.toString IndexId.wellKnownId

// NB - these types and the union case names reflect the actual storage formats and hence need to be versioned with care
[<RequireQualifiedAccess>]
module Events =

    type Event =
        | [<System.Runtime.Serialization.DataMember(Name = "Started")>] StartedPreview of {| tranche : AppendsPartitionId; epoch : AppendsEpochId |}
        // Since rc.3, the logic has only ever produced Started2 events https://github.com/jet/propulsion/issues/201
        | [<System.Runtime.Serialization.DataMember(Name = "Started2")>] Started of {| partition : AppendsPartitionId; epoch : AppendsEpochId |}
        | Snapshotted of {| active : Map<AppendsPartitionId, AppendsEpochId> |}
        interface TypeShape.UnionContract.IUnionContract
    let codec = EventCodec.gen<Event>

module Fold =

    type State = Map<AppendsPartitionId, AppendsEpochId>

    let initial = Map.empty
    let evolve state = function
        | Events.StartedPreview e -> state |> Map.add e.tranche e.epoch
        | Events.Started e -> state |> Map.add e.partition e.epoch
        | Events.Snapshotted e -> e.active
    let fold : State -> Events.Event seq -> State = Seq.fold evolve

    let isOrigin = function Events.Snapshotted _ -> true | _ -> false
    let toSnapshot s = Events.Snapshotted {| active = s |}

let readEpochId partitionId (state : Fold.State) =
    state
    |> Map.tryFind partitionId

let interpret (partitionId, epochId) (state : Fold.State) =
    [if state |> readEpochId partitionId |> Option.forall (fun cur -> cur < epochId) && epochId >= AppendsEpochId.initial then
        yield Events.Started {| partition = partitionId; epoch = epochId |}]

type Service internal (resolve : unit -> Equinox.Decider<Events.Event, Fold.State>) =

    /// Determines the current active epoch for the specified Partition
    member _.ReadIngestionEpochId(partitionId) : Async<AppendsEpochId> =
        let decider = resolve ()
        decider.Query(readEpochId partitionId >> Option.defaultValue AppendsEpochId.initial, Equinox.AllowStale)

    /// Mark specified `epochId` as live for the purposes of ingesting commits for the specified Partition
    /// Writers are expected to react to having writes to an epoch denied (due to it being Closed) by anointing the successor via this
    member _.MarkIngestionEpochId(partitionId, epochId) : Async<unit> =
        let decider = resolve ()
        decider.Transact(interpret (partitionId, epochId), Equinox.AllowStale)

module Config =

    let private createCategory store = Config.createSnapshotted Events.codec Fold.initial Fold.fold (Fold.isOrigin, Fold.toSnapshot) store
    let resolve log store = createCategory store |> Equinox.Decider.resolve log
    let create log (context, cache) = Service(streamId >> resolve log (context, Some cache) Category)

/// On the Reading Side, there's no advantage to caching (as we have snapshots, and it's Dynamo)
module Reader =

    let readKnownPartitions (state : Fold.State) : AppendsPartitionId array =
        state |> Map.toSeq |> Seq.map fst |> Array.ofSeq

    let readIngestionEpochId partitionId (state : Fold.State) =
        state |> Map.tryFind partitionId |> Option.defaultValue AppendsEpochId.initial

    type Service internal (resolve : unit -> Equinox.Decider<Events.Event, Fold.State>) =

        member _.Read() : Async<Fold.State> =
            let decider = resolve ()
            decider.Query(id)

        member _.ReadKnownPartitions() : Async<AppendsPartitionId array> =
            let decider = resolve ()
            decider.Query(readKnownPartitions)

        member _.ReadIngestionEpochId(partitionId) : Async<AppendsEpochId> =
            let decider = resolve ()
            decider.Query(readIngestionEpochId partitionId)

    let create log context = Service(streamId >> Config.resolve log (context, None) Category)
#endif
