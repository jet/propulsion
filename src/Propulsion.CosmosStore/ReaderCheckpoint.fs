module Propulsion.Feed.ReaderCheckpoint

open Propulsion.Internal
open System
open System.Threading.Tasks

#if COSMOSV3
let streamName (source, tranche, consumerGroupName : string) =
    if consumerGroupName = null then
        let Category = "ReaderCheckpoint"
        // This form is only used for interop with the V3 Propulsion.Feed.FeedSource - anyone starting with V4 should only ever encounter tripartite names
        FsCodec.StreamName.compose Category [| SourceId.toString source; TrancheId.toString tranche |]
    else
        let (*[<Literal>]*) Category = "$ReaderCheckpoint"
        FsCodec.StreamName.compose Category [| SourceId.toString source; TrancheId.toString tranche; consumerGroupName |]
#else
module Stream =
    let [<Literal>] Category = "$ReaderCheckpoint"
    let id = FsCodec.StreamId.gen3 SourceId.toString TrancheId.toString (*consumerGroupName*)id
    let name = id >> FsCodec.StreamName.create Category
#endif

// NB - these schemas reflect the actual storage formats and hence need to be versioned with care
module Events =

    type Config =       { checkpointFreqS : int }
    type Checkpoint =   { at : DateTimeOffset; nextCheckpointDue : DateTimeOffset; pos : Position }

    type Started =      { config : Config; origin : Checkpoint }
    type Updated =      { config : Config; pos : Checkpoint }
    type Snapshotted =  { config : Config; state : Checkpoint }

    type Event =
        | Started       of Started
        | Overrode      of Updated
        | Checkpointed  of Updated
        // Updated events are not actually written to the store when storing in Cosmos (see `transmute`, below)
        // While we could remove the `nextCheckpointDue` and `config` values, we won't do that, so people can use AnyKnownEvent
        //  access modes and/or just load the most recent event
        | Updated       of Updated
        | Snapshotted   of Snapshotted
        interface TypeShape.UnionContract.IUnionContract
#if MEMORYSTORE
    let codec = FsCodec.Box.Codec.Create<Event>()
#else
#if DYNAMOSTORE
    let codec = FsCodec.SystemTextJson.Codec.Create<Event>() |> FsCodec.Deflate.EncodeUncompressed
#else
#if !COSMOSV3
    let codec = FsCodec.SystemTextJson.CodecJsonElement.Create<Event>()
#else
    let codec = FsCodec.NewtonsoftJson.Codec.Create<Event>()
#endif
#endif
#endif

module Fold =

    type State = NotStarted | Running of Events.Snapshotted

    let initial : State = NotStarted
    let private evolve _state = function
        | Events.Started { config = cfg; origin=originState } -> Running { config = cfg; state = originState }
        | Events.Updated e | Events.Checkpointed e | Events.Overrode e -> Running { config = e.config; state = e.pos }
        | Events.Snapshotted runningState -> Running runningState
    let fold: State -> Events.Event seq -> State = Seq.fold evolve // NOTE Leave as Seq for interop with COSMOSV3

    let isOrigin _state = true // we can build a state from any of the events and/or an unfold

    let private toSnapshot state =
        match state with
        | NotStarted -> failwith "should never produce a NotStarted state"
        | Running state -> Events.Snapshotted {config = state.config; state = state.state}

    /// We only want to generate a first class event every N minutes, while efficiently writing contingent on the current etag value
    /// So, we post-process the events to remove `Updated` events (as opposed to `Checkpointed` ones),
    /// knowing that the state already has that Updated event folded into it when we snapshot
#if COSMOSV3
    let transmute events state : Events.Event list * Events.Event list =
        match events, state with
        | [Events.Updated _], state -> [], [toSnapshot state]
        | xs, state ->                 xs, [toSnapshot state]
#else
    let transmute events state : Events.Event[] * Events.Event[] =
        match events, state with
        | [| Events.Updated _ |], state -> [||], [| toSnapshot state |]
        | xs, state ->                     xs, [| toSnapshot state |]
#endif
let private mkCheckpoint at next pos = { at = at; nextCheckpointDue = next; pos = pos } : Events.Checkpoint
let private mk (at : DateTimeOffset) (interval : TimeSpan) pos : Events.Config * Events.Checkpoint =
    let next = at.Add interval
    { checkpointFreqS = int interval.TotalSeconds }, mkCheckpoint at next pos
let private configFreq (config : Events.Config) =
    config.checkpointFreqS |> float |> TimeSpan.FromSeconds

let decideStart establishOrigin at freq state = async {
    match state with
    | Fold.NotStarted ->
        let! origin = establishOrigin
        let config, checkpoint = mk at freq origin
        return checkpoint.pos, [| Events.Started { config = config; origin = checkpoint } |]
    | Fold.Running s ->
        return s.state.pos, [||] }

let decideOverride at (freq : TimeSpan) pos = function
    | Fold.Running s when s.state.pos = pos && s.config.checkpointFreqS = int freq.TotalSeconds -> [||]
    | _ ->
        let config, checkpoint = mk at freq pos
        [| Events.Overrode { config = config; pos = checkpoint } |]

let decideUpdate at pos = function
    | Fold.NotStarted -> failwith "Cannot Commit a checkpoint for a series that has not been Started"
    | Fold.Running state -> [|
        if at < state.state.nextCheckpointDue then
            if pos <> state.state.pos then // No checkpoint due, pos changed => Write, but maintain same nextCheckpointDue
                Events.Updated { config = state.config; pos = mkCheckpoint at state.state.nextCheckpointDue pos }
        else // Checkpoint due => Force a write every N seconds regardless of whether the position has actually changed
            let freq = TimeSpan.FromSeconds(float state.config.checkpointFreqS)
            let config, checkpoint = mk at freq pos
            Events.Checkpointed { config = config; pos = checkpoint } |]

#if COSMOSV3
module Equinox = module LoadOption = let AnyCachedValue = ()
type Equinox.Decider<'e, 's> with
    member x.TransactAsync(decide, load : unit): Async<'r> =
        x.TransactAsync(fun s -> async { let! r, es = decide s in return r, Array.toList es })
    member x.Transact(decide, load : unit): Async<'r> =
        x.Transact(decide >> function r, es -> r, Array.toList es)
    member x.Transact(decide, ?load : unit): Async<unit> =
        x.Transact(decide >> Array.toList)
#endif

type Service internal (resolve: SourceId * TrancheId * string -> Equinox.Decider<Events.Event, Fold.State>, consumerGroupName, defaultCheckpointFrequency) =

    interface IFeedCheckpointStore with

        /// Start a checkpointing series with the supplied parameters
        /// Yields the checkpoint interval and the starting position
        member _.Start(source, tranche, establishOrigin, ct) : Task<Position> =
            let decider = resolve (source, tranche, consumerGroupName)
            let establishOrigin = match establishOrigin with None -> async { return Position.initial } | Some f -> Async.call f.Invoke
            decider.TransactAsync(decideStart establishOrigin DateTimeOffset.UtcNow defaultCheckpointFrequency, load = Equinox.LoadOption.AnyCachedValue)
            |> Async.executeAsTask ct

        /// Ingest a position update
        /// NB fails if not already initialized; caller should ensure correct initialization has taken place via Read -> Start
        member _.Commit(source, tranche, pos : Position, ct) =
            let decider = resolve (source, tranche, consumerGroupName)
            decider.Transact(decideUpdate DateTimeOffset.UtcNow pos, load = Equinox.LoadOption.AnyCachedValue)
            |> Async.executeAsTask ct :> _

    /// Override a checkpointing series with the supplied parameters
    member _.Override(source, tranche, pos : Position, ct): Task<unit> =
        let decider = resolve (source, tranche, consumerGroupName)
        decider.Transact(decideOverride DateTimeOffset.UtcNow defaultCheckpointFrequency pos) |> Async.executeAsTask ct

#if MEMORYSTORE
module MemoryStore =

    open Equinox.MemoryStore

    let create log (consumerGroupName, defaultCheckpointFrequency) context =
        let cat = MemoryStoreCategory(context, Stream.Category, Events.codec, Fold.fold, Fold.initial)
        let resolve = Equinox.Decider.forStream log cat
        Service(Stream.id >> resolve, consumerGroupName, defaultCheckpointFrequency)
#else
let private defaultCacheDuration = TimeSpan.FromMinutes 20.
#if COSMOSV3
let private cacheStrategy cache = Equinox.CosmosStore.CachingStrategy.SlidingWindow (cache, defaultCacheDuration)
#else
let private cacheStrategy cache = Equinox.CachingStrategy.SlidingWindow (cache, defaultCacheDuration)
#endif
#if DYNAMOSTORE
module DynamoStore =

    open Equinox.DynamoStore

    let accessStrategy = AccessStrategy.Custom (Fold.isOrigin, Fold.transmute)
    let create log (consumerGroupName, defaultCheckpointFrequency) (context, cache) =
        let cat = DynamoStoreCategory(context, Stream.Category, Events.codec, Fold.fold, Fold.initial, accessStrategy, cacheStrategy cache)
        let resolve = Equinox.Decider.forStream log cat
        Service(Stream.id >> resolve, consumerGroupName, defaultCheckpointFrequency)
#else
#if !COSMOSV3
module CosmosStore =

    open Equinox.CosmosStore

    let accessStrategy = AccessStrategy.Custom (Fold.isOrigin, Fold.transmute)
    let create log (consumerGroupName, defaultCheckpointFrequency) (context, cache) =
        let cat = CosmosStoreCategory(context, Stream.Category, Events.codec, Fold.fold, Fold.initial, accessStrategy, cacheStrategy cache)
        let resolve = Equinox.Decider.forStream log cat
        Service(Stream.id >> resolve, consumerGroupName, defaultCheckpointFrequency)
#else
let private create log defaultCheckpointFrequency resolveStream =
    let resolve id = Equinox.Decider(log, resolveStream Equinox.AllowStale (streamName id), maxAttempts = 3)
    Service(resolve, null, defaultCheckpointFrequency)

#if COSMOSV3
module CosmosStore =

    open Equinox.CosmosStore

    let accessStrategy = AccessStrategy.Custom (Fold.isOrigin, Fold.transmute)
    let create log defaultCheckpointFrequency (context, cache) =
        let cat = CosmosStoreCategory(context, Events.codec, Fold.fold, Fold.initial, cacheStrategy cache, accessStrategy)
        let resolveStream opt sn = cat.Resolve(sn, opt)
        create log defaultCheckpointFrequency resolveStream
#endif
#endif
#endif
#endif
