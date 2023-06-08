module Propulsion.EventStore.Checkpoint

open FSharp.UMX
open Propulsion.Internal
open System // must shadow UMX to use DateTimeOffSet
open System.Threading.Tasks

type CheckpointSeriesId = string<checkpointSeriesId>
and [<Measure>] checkpointSeriesId
module CheckpointSeriesId =
    let ofGroupName (groupName : string) = UMX.tag groupName
    let toString (x : CheckpointSeriesId) = UMX.untag x

let [<Literal>] Category = "Sync"
let [<Literal>] Category_ = Category
let streamId = Equinox.StreamId.gen CheckpointSeriesId.toString

// NB - these schemas reflect the actual storage formats and hence need to be versioned with care
module Events =

    type Checkpoint = { at : DateTimeOffset; nextCheckpointDue : DateTimeOffset; pos : int64 }
    type Config = { checkpointFreqS : int }
    type Started = { config : Config; origin : Checkpoint }
    type Pos = { config : Config; pos : Checkpoint }
    type Snapshotted = { config : Config; state : Checkpoint }

    type Event =
        | Started of Started
        | Checkpointed of Pos
        | Overrode of Pos
        // Updated events are not actually written to the store when storing in Cosmos (see `transmute`, below)
        // While we could remove the `nextCheckpointDue` and `config` values, we won't do that, so people can use AnyKnownEvent
        //  access modes and/or save just load the most recent event
        | Updated of Pos
        | [<System.Runtime.Serialization.DataMember(Name = "state-v1")>]
            Snapshotted of Snapshotted
        interface TypeShape.UnionContract.IUnionContract
    // Avoid binding to a specific serializer as a) nothing else is binding to it in here b) it should serialize with any serializer so we defer
    // let codec = FsCodec.NewtonsoftJson.Codec.Create<Event>()

module Fold =

    type State = NotStarted | Running of Events.Snapshotted

    let initial : State = NotStarted
    let private evolve _state = function
        | Events.Started { config = cfg; origin=originState } -> Running { config = cfg; state = originState }
        | Events.Updated e | Events.Checkpointed e | Events.Overrode e -> Running { config = e.config; state = e.pos }
        | Events.Snapshotted runningState -> Running runningState
    let fold : State -> Events.Event seq -> State = Seq.fold evolve

    let isOrigin _state = true // we can build a state from any of the events and/or an unfold

    let private snapshot state =
        match state with
        | NotStarted -> failwith "should never produce a NotStarted state"
        | Running state -> Events.Snapshotted {config = state.config; state = state.state}

    /// We only want to generate a first class event every N minutes, while efficiently writing contingent on the current etag value
    /// So, we post-process the events to remove `Updated` events (as opposed to `Checkpointed` ones),
    /// knowing that the state already has that updated folded into it when we snapshot from it
    let transmute events state : Events.Event list * Events.Event list =
        match events, state with
        | [Events.Updated _], state -> [], [snapshot state]
        | xs, state ->                 xs, [snapshot state]

type Command =
    | Start of at : DateTimeOffset * checkpointFreq : TimeSpan * pos : int64
    | Override of at : DateTimeOffset * checkpointFreq : TimeSpan * pos : int64
    | Update of at : DateTimeOffset * pos : int64

let interpret command (state: Fold.State): seq<Events.Event> =
    let mkCheckpoint at next pos = { at = at; nextCheckpointDue = next; pos = pos } : Events.Checkpoint
    let mk (at : DateTimeOffset) (interval : TimeSpan) pos : Events.Config * Events.Checkpoint =
        let next = at.Add interval
        { checkpointFreqS = int interval.TotalSeconds }, mkCheckpoint at next pos

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

open Equinox // to disambiguate DeciderCore \/ -- TODO remove as this brings Category into scope and makes a mess
type Service internal (resolve : CheckpointSeriesId -> DeciderCore<Events.Event, Fold.State>) =

    /// Determines the present state of the CheckpointSequence
    member _.Read(series, ct) =
        let decider = resolve series
        decider.Query(id, load = Equinox.AnyCachedValue, ct = ct)

    /// Start a checkpointing series with the supplied parameters
    /// NB will fail if already existing; caller should select to `Start` or `Override` based on whether Read indicates state is Running Or NotStarted
    member _.Start(series, freq : TimeSpan, pos : int64, ct) : Task<unit> =
        let decider = resolve series
        decider.Transact(interpret (Command.Start(DateTimeOffset.UtcNow, freq, pos)), load = Equinox.AnyCachedValue, ct = ct)

    /// Override a checkpointing series with the supplied parameters
    /// NB fails if not already initialized; caller should select to `Start` or `Override` based on whether Read indicates state is Running Or NotStarted
    member _.Override(series, freq : TimeSpan, pos : int64, ct) =
        let decider = resolve series
        decider.Transact(interpret (Command.Override(DateTimeOffset.UtcNow, freq, pos)), load = Equinox.AnyCachedValue, ct = ct)

    /// Ingest a position update
    /// NB fails if not already initialized; caller should ensure correct initialization has taken place via Read -> Start
    member _.Commit(series, pos : int64, ct) =
        let decider = resolve series
        decider.Transact(interpret (Command.Update(DateTimeOffset.UtcNow, pos)), load = Equinox.AnyCachedValue, ct = ct)

let create resolve = Service(streamId >> resolve Category_)

// General pattern is that an Equinox Service is a singleton and calls pass an identifier for a stream per call
// This light wrapper means we can adhere to that general pattern yet still end up with legible code while we in practice only maintain a single checkpoint series per running app
type CheckpointSeries(groupName, resolve, ?log) =
    let seriesId = CheckpointSeriesId.ofGroupName groupName
    let log = match log with Some x -> x | None -> Serilog.Log.ForContext<Service>()
    let inner = create (resolve log)

    member _.Read(ct): Task<Fold.State> = inner.Read(seriesId, ct)
    member _.Start(freq, pos, ct): Task<unit> = inner.Start(seriesId, freq, pos, ct)
    member _.Override(freq, pos, ct): Task<unit> = inner.Override(seriesId, freq, pos, ct)
    member _.Commit(pos, ct): Task<unit> = inner.Commit(seriesId, pos, ct)
