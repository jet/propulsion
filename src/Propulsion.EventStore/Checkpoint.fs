module Propulsion.EventStore.Checkpoint

open FSharp.UMX
open System // must shadow UMX to use DateTimeOffSet

type CheckpointSeriesId = string<checkpointSeriesId>
and [<Measure>] checkpointSeriesId
module CheckpointSeriesId = let ofGroupName (groupName : string) = UMX.tag groupName

// NB - these schemas reflect the actual storage formats and hence need to be versioned with care
module Events =
    type Checkpoint = { at: DateTimeOffset; nextCheckpointDue: DateTimeOffset; pos: int64 }
    type Config = { checkpointFreqS: int }
    type Started = { config: Config; origin: Checkpoint }
    type Checkpointed = { config: Config; pos: Checkpoint }
    type Unfolded = { config: Config; state: Checkpoint }
    type Event =
        | Started of Started
        | Checkpointed of Checkpointed
        | Overrode of Checkpointed
        | [<System.Runtime.Serialization.DataMember(Name="state-v1")>]
            Unfolded of Unfolded
        interface TypeShape.UnionContract.IUnionContract

module Folds =
    type State = NotStarted | Running of Events.Unfolded

    let initial : State = NotStarted
    let private evolve _ignoreState = function
        | Events.Started { config = cfg; origin=originState } -> Running { config = cfg; state = originState }
        | Events.Checkpointed e | Events.Overrode e -> Running { config = e.config; state = e.pos }
        | Events.Unfolded runningState -> Running runningState
    let fold (state: State) = Seq.fold evolve state
    let isOrigin _state = true // we can build a state from any of the events and/or an unfold
    let private unfold state =
        match state with
        | NotStarted -> failwith "should never produce a NotStarted state"
        | Running state -> Events.Unfolded {config = state.config; state=state.state}

    /// We only want to generate a first class event every N minutes, while efficiently writing contingent on the current etag value
    let transmute events state =
        let checkpointEventIsRedundant (e: Events.Checkpointed) (s: Events.Unfolded) =
            s.state.nextCheckpointDue = e.pos.nextCheckpointDue
            && s.state.pos <> e.pos.pos
        match events, state with
        | [Events.Checkpointed e], (Running state as s) when checkpointEventIsRedundant e state ->
            [],unfold s
        | xs, state ->
            xs,unfold state

type Command =
    | Start of at: DateTimeOffset * checkpointFreq: TimeSpan * pos: int64
    | Override of at: DateTimeOffset * checkpointFreq: TimeSpan * pos: int64
    | Update of at: DateTimeOffset * pos: int64

module Commands =
    let interpret command (state : Folds.State) =
        let mkCheckpoint at next pos = { at=at; nextCheckpointDue = next; pos = pos } : Events.Checkpoint
        let mk (at : DateTimeOffset) (interval: TimeSpan) pos : Events.Config * Events.Checkpoint=
            let freq = int interval.TotalSeconds
            let next = at.AddSeconds(float freq)
            { checkpointFreqS = freq }, mkCheckpoint at next pos
        match command, state with
        | Start (at, freq, pos), Folds.NotStarted ->
            let config, checkpoint = mk at freq pos
            [Events.Started { config = config; origin = checkpoint}]
        | Override (at, freq, pos), Folds.Running _ ->
            let config, checkpoint = mk at freq pos
            [Events.Overrode { config = config; pos = checkpoint}]
        | Update (at,pos), Folds.Running state ->
            // Force a write every N seconds regardless of whether the position has actually changed
            if state.state.pos = pos && at < state.state.nextCheckpointDue then [] else
            let freq = TimeSpan.FromSeconds <| float state.config.checkpointFreqS
            let config, checkpoint = mk at freq pos
            [Events.Checkpointed { config = config; pos = checkpoint}]
        | c, s -> failwithf "Command %A invalid when %A" c s

type Service(log, resolveStream, ?maxAttempts) =
    let (|AggregateId|) (id : CheckpointSeriesId) = Equinox.AggregateId ("Sync", % id)
    let (|Stream|) (AggregateId id) = Equinox.Stream(log, resolveStream id, defaultArg maxAttempts 3)
    let execute (Stream stream) cmd = stream.Transact(Commands.interpret cmd)

    /// Determines the present state of the CheckpointSequence
    member __.Read(Stream stream) =
        stream.Query id

    /// Start a checkpointing series with the supplied parameters
    /// NB will fail if already existing; caller should select to `Start` or `Override` based on whether Read indicates state is Running Or NotStarted
    member __.Start(id, freq: TimeSpan, pos: int64) =
        execute id <| Command.Start(DateTimeOffset.UtcNow, freq, pos)

    /// Override a checkpointing series with the supplied parameters
    /// NB fails if not already initialized; caller should select to `Start` or `Override` based on whether Read indicates state is Running Or NotStarted
    member __.Override(id, freq: TimeSpan, pos: int64) =
        execute id <| Command.Override(DateTimeOffset.UtcNow, freq, pos)

    /// Ingest a position update
    /// NB fails if not already initialized; caller should ensure correct initialization has taken place via Read -> Start
    member __.Commit(id, pos: int64) =
        execute id <| Command.Update(DateTimeOffset.UtcNow, pos)

// General pattern is that an Equinox Service is a singleton and calls pass an inentifier for a stream per call
// This light wrapper means we can adhere to that general pattern yet still end up with lef=gible code while we in practice only maintain a single checkpoint series per running app
type CheckpointSeries(name, log, resolveStream) =
    let seriesId = CheckpointSeriesId.ofGroupName name
    let inner = Service(log, resolveStream)
    member __.Read = inner.Read seriesId
    member __.Start(freq, pos) = inner.Start(seriesId, freq, pos)
    member __.Override(freq, pos) = inner.Override(seriesId, freq, pos)
    member __.Commit(pos) = inner.Commit(seriesId, pos)