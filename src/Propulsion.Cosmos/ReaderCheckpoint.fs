module Propulsion.Feed.ReaderCheckpoint

open System

let [<Literal>] Category = "ReaderCheckpoint"
let streamName (source, tranche) = FsCodec.StreamName.compose Category [SourceId.toString source; TrancheId.toString tranche]

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
    let codec = FsCodec.NewtonsoftJson.Codec.Create<Event>()

module Fold =

    type State = NotStarted | Running of Events.Snapshotted

    let initial : State = NotStarted
    let private evolve _state = function
        | Events.Started { config = cfg; origin=originState } -> Running { config = cfg; state = originState }
        | Events.Updated e | Events.Checkpointed e | Events.Overrode e -> Running { config = e.config; state = e.pos }
        | Events.Snapshotted runningState -> Running runningState
    let fold : State -> Events.Event seq -> State = Seq.fold evolve

    let isOrigin _state = true // we can build a state from any of the events and/or an unfold

    let private toSnapshot state =
        match state with
        | NotStarted -> failwith "should never produce a NotStarted state"
        | Running state -> Events.Snapshotted {config = state.config; state = state.state}

    /// We only want to generate a first class event every N minutes, while efficiently writing contingent on the current etag value
    /// So, we post-process the events to remove `Updated` events (as opposed to `Checkpointed` ones),
    /// knowing that the state already has that Updated event folded into it when we snapshot
    let transmute events state : Events.Event list * Events.Event list =
        match events, state with
        | [Events.Updated _], state -> [], [toSnapshot state]
        | xs, state ->                 xs, [toSnapshot state]

type Command =
    | Start of    at : DateTimeOffset * checkpointFreq : TimeSpan * pos : Position
    | Override of at : DateTimeOffset * checkpointFreq : TimeSpan * pos : Position
    | Update of   at : DateTimeOffset                             * pos : Position

let interpret command (state : Fold.State) =
    let mkCheckpoint at next pos = { at = at; nextCheckpointDue = next; pos = pos } : Events.Checkpoint
    let mk (at : DateTimeOffset) (interval : TimeSpan) pos : Events.Config * Events.Checkpoint =
        let freq = int interval.TotalSeconds
        let next = at.AddSeconds(float freq)
        { checkpointFreqS = freq }, mkCheckpoint at next pos

    match command, state with
    | Start (at, freq, pos), Fold.NotStarted ->
        let config, checkpoint = mk at freq pos
        [Events.Started { config = config; origin = checkpoint}]
    | Override (at, freq, pos), Fold.Running _ ->
        let config, checkpoint = mk at freq pos
        [Events.Overrode { config = config; pos = checkpoint}]
    | Update (at, pos), Fold.Running state ->
        if at < state.state.nextCheckpointDue then
            if pos = state.state.pos then [] // No checkpoint due, pos unchanged => No write
            else // No checkpoint due, pos changed => Write, but maintain same nextCheckpointDue
                [Events.Updated { config = state.config; pos = mkCheckpoint at state.state.nextCheckpointDue pos }]
        else // Checkpoint due => Force a write every N seconds regardless of whether the position has actually changed
            let freq = TimeSpan.FromSeconds(float state.config.checkpointFreqS)
            let config, checkpoint = mk at freq pos
            [Events.Checkpointed { config = config; pos = checkpoint }]
    | c, s -> failwithf "Command %A invalid when %A" c s

#if COSMOSSTORE
type Decider<'e, 's> = Equinox.Decider<'e, 's>
#else
type Decider<'e, 's> = Equinox.Stream<'e, 's>
#endif

type Service internal (resolve : SourceId * TrancheId -> Decider<Events.Event, Fold.State>) =

    /// Start a checkpointing series with the supplied parameters
    /// NB will fail if already existing; caller should select to `Start` or `Override` based on whether Read indicates state is Running Or NotStarted
    member _.Start(source, tranche, freq : TimeSpan, pos : Position) =
        let decider = resolve (source, tranche)
        decider.Transact(interpret (Command.Start (DateTimeOffset.UtcNow, freq, pos)))

    /// Override a checkpointing series with the supplied parameters
    /// NB fails if not already initialized; caller should select to `Start` or `Override` based on whether Read indicates state is Running Or NotStarted
    member _.Override(source, tranche, freq : TimeSpan, pos : Position) =
        let decider = resolve (source, tranche)
        decider.Transact(interpret (Command.Override (DateTimeOffset.UtcNow, freq, pos)))

    interface IFeedCheckpointStore with

        /// Determines the present Checkpointed Position (if any)
        member _.ReadPosition(source, tranche) : Async<Position option> =
            let decider = resolve (source, tranche)
            decider.Query(function
                | Fold.NotStarted -> None
                | Fold.Running r -> Some r.state.pos)

        /// Ingest a position update
        /// NB fails if not already initialized; caller should ensure correct initialization has taken place via Read -> Start
        member _.Commit(source, tranche, pos : Position) =
            let decider = resolve (source, tranche)
            decider.Transact(interpret (Command.Update (DateTimeOffset.UtcNow, pos)))

let private create log resolveStream =
    let resolve id = Decider(log, resolveStream Equinox.AllowStale (streamName id), maxAttempts = 3)
    Service(resolve)

#if COSMOSSTORE
module CosmosStore =

    open Equinox.CosmosStore

    let accessStrategy = AccessStrategy.Custom (Fold.isOrigin, Fold.transmute)
    let private resolve (context, cache) =
        let cacheStrategy = CachingStrategy.SlidingWindow (cache, TimeSpan.FromMinutes 20.)
        let resolver = CosmosStoreCategory(context, Events.codec, Fold.fold, Fold.initial, cacheStrategy, accessStrategy)
        fun opt sn -> resolver.Resolve(sn, opt)
    let create log (context, cache) =
        create log (resolve (context, cache))
#else
module Cosmos =

    open Equinox.Cosmos

    let accessStrategy = AccessStrategy.Custom (Fold.isOrigin, Fold.transmute)
    let private resolve (context, cache) =
        let cacheStrategy = CachingStrategy.SlidingWindow (cache, TimeSpan.FromMinutes 20.)
        let resolver = Resolver(context, Events.codec, Fold.fold, Fold.initial, cacheStrategy, accessStrategy)
        fun opt sn -> resolver.Resolve(sn, opt)
    let create log (context, cache) =
        create log (resolve (context, cache))
#endif
