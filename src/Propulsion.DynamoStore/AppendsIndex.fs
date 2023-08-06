/// Maintains a pointer for into the chain for each Partition
/// Allows an Ingester to quickly determine the current Epoch which it should commence writing into
/// As an Epoch is marked `Closed`, `module Index` will mark a new epoch `Started` on this aggregate
module Propulsion.DynamoStore.AppendsIndex

module Stream =
    let [<Literal>] Category = "$AppendsIndex"
#if !PROPULSION_DYNAMOSTORE_NOTIFIER
    let id () = FsCodec.StreamId.gen IndexId.toString IndexId.wellKnownId
    let name = id >> FsCodec.StreamName.create Category

// NB - these types and the union case names reflect the actual storage formats and hence need to be versioned with care
[<RequireQualifiedAccess>]
module Events =

    [<System.Text.Json.Serialization.JsonConverter(typeof<StartedUpConverter>)>]
    type Started = { partition : AppendsPartitionId; epoch : AppendsEpochId }
    /// <= rc.2 used a tranche field. >= rc.3 can accept either a tranche or a partition field in the event body, but will only write a partition
    and StartedUpConverter() =
        inherit FsCodec.SystemTextJson.JsonIsomorphism<Started, StartedBackcompatPreRc2>()
        override _.Pickle e = { partition = Some e.partition; tranche = None; epoch = e.epoch }
        override _.UnPickle e = { partition = (match e.partition with Some p -> p | _ -> e.tranche.Value); epoch = e.epoch }
    and StartedBackcompatPreRc2 = { partition : AppendsPartitionId option; tranche : AppendsPartitionId option; epoch : AppendsEpochId }

    type Event =
        | Started of Started
        | Snapshotted of {| active : Map<AppendsPartitionId, AppendsEpochId> |}
        interface TypeShape.UnionContract.IUnionContract
    let codec = Store.Codec.gen<Event>

module Fold =

    type State = Map<AppendsPartitionId, AppendsEpochId>
    let initial = Map.empty

    module Snapshot =

        let private generate (s: State) = Events.Snapshotted {| active = s |}
        let private isOrigin = function Events.Snapshotted _ -> true | _ -> false
        let config = isOrigin, generate

    let private evolve state = function
        | Events.Started e -> state |> Map.add e.partition e.epoch
        | Events.Snapshotted e -> e.active
    let fold = Array.fold evolve

let readEpochId partitionId (state : Fold.State) =
    state
    |> Map.tryFind partitionId

let interpret (partitionId, epochId) (state : Fold.State) = [|
    if state |> readEpochId partitionId |> Option.forall (fun cur -> cur < epochId) && epochId >= AppendsEpochId.initial then
        Events.Started { partition = partitionId; epoch = epochId } |]

type Service internal (resolve: unit -> Equinox.Decider<Events.Event, Fold.State>) =

    /// Determines the current active epoch for the specified Partition
    member _.ReadIngestionEpochId(partitionId) : Async<AppendsEpochId> =
        let decider = resolve ()
        decider.Query(readEpochId partitionId >> Option.defaultValue AppendsEpochId.initial, Equinox.AnyCachedValue)

    /// Mark specified `epochId` as live for the purposes of ingesting commits for the specified Partition
    /// Writers are expected to react to having writes to an epoch denied (due to it being Closed) by anointing the successor via this
    member _.MarkIngestionEpochId(partitionId, epochId) : Async<unit> =
        let decider = resolve ()
        decider.Transact(interpret (partitionId, epochId), Equinox.AnyCachedValue)

module Factory =

    let private createCategory store = Store.Dynamo.createSnapshotted Stream.Category Events.codec Fold.initial Fold.fold Fold.Snapshot.config store
    let resolve log store = createCategory store |> Equinox.Decider.forStream log
    let create log (context, cache) = Service(Stream.id >> resolve log (context, Some cache))

/// On the Reading Side, there's no advantage to caching (as we have snapshots, and it's Dynamo)
module Reader =

    let readKnownPartitions (state : Fold.State) : AppendsPartitionId[] =
        state |> Map.toSeq |> Seq.map fst |> Array.ofSeq

    let readIngestionEpochId partitionId (state : Fold.State) =
        state |> Map.tryFind partitionId |> Option.defaultValue AppendsEpochId.initial

    type Service internal (resolve : unit -> Equinox.Decider<Events.Event, Fold.State>) =

        member _.Read() : Async<Fold.State> =
            let decider = resolve ()
            decider.Query(id)

        member _.ReadKnownPartitions() : Async<AppendsPartitionId[]> =
            let decider = resolve ()
            decider.Query(readKnownPartitions)

        member _.ReadIngestionEpochId(partitionId) : Async<AppendsEpochId> =
            let decider = resolve ()
            decider.Query(readIngestionEpochId partitionId)

    let create log context = Service(Stream.id >> Factory.resolve log (context, None))
#endif
