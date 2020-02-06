namespace Propulsion.EventStore

open Propulsion.Streams
open System

type StartPos = Absolute of int64 | Chunk of int | Percentage of float | TailOrCheckpoint | StartOrCheckpoint

type ReaderSpec =
    {   /// Identifier for this projection and it's state
        groupName : string
        /// Indicates user has specified that they wish to restart from the indicated position as opposed to resuming from the checkpoint position
        forceRestart : bool
        /// Start position from which forward reading is to commence // Assuming no stored position
        start : StartPos
        /// Frequency at which to update the Checkpoint store
        checkpointInterval : TimeSpan
        /// Delay when reading yields an empty batch
        tailInterval : TimeSpan
        /// Specify initial phase where interleaved reading stripes a 256MB chunk apart attain a balance between good reading speed and not killing the server
        gorge : int option
        /// Maximum number of striped readers to permit when tailing; this dictates how many stream readers will be used to perform catchup work on streams
        ///   that are missing a prefix (e.g. due to not starting from the start of the $all stream, and/or deleting data from the destination store)
        streamReaders : int // TODO
        /// Initial batch size to use when commencing reading
        batchSize : int
        /// Smallest batch size to degrade to in the presence of failures
        minBatchSize : int }

type StartMode = Starting | Resuming | Overridding

[<AutoOpen>]
module Mapping =
    open EventStore.ClientAPI

    type RecordedEvent with
        member __.Timestamp = DateTimeOffset.FromUnixTimeMilliseconds(__.CreatedEpoch)

    let (|PropulsionTimelineEvent|) (x : RecordedEvent) : FsCodec.ITimelineEvent<_> =
        let inline len0ToNull (x : _[]) = match x with null -> null | x when x.Length = 0 -> null | x -> x
        FsCodec.Core.TimelineEvent.Create(x.EventNumber, x.EventType, len0ToNull x.Data, len0ToNull x.Metadata, timestamp = x.Timestamp) :> _

    let (|PropulsionStreamEvent|) (x : RecordedEvent) : Propulsion.Streams.StreamEvent<_> =
        { stream = StreamName.internalParseSafe x.EventStreamId; event = (|PropulsionTimelineEvent|) x }

type EventStoreSource =
    static member Run
        (   log : Serilog.ILogger, sink : Propulsion.ProjectorPipeline<_>, checkpoints : Checkpoint.CheckpointSeries,
            connect, spec, tryMapEvent,
            maxReadAhead, statsInterval) = async {
        let conn = connect ()
        let! maxInParallel = Async.StartChild <| Reader.establishMax conn
        let! initialCheckpointState = checkpoints.Read
        let! maxPos = maxInParallel

        let! startPos = async {
            let mkPos x = EventStore.ClientAPI.Position(x, 0L)
            let requestedStartPos =
                match spec.start with
                | Absolute p -> mkPos p
                | Chunk c -> Reader.posFromChunk c
                | Percentage pct -> Reader.posFromPercentage (pct, maxPos)
                | TailOrCheckpoint -> maxPos
                | StartOrCheckpoint -> EventStore.ClientAPI.Position.Start
            let! startMode, startPos, checkpointFreq = async {
                match initialCheckpointState, requestedStartPos with
                | Checkpoint.Fold.NotStarted, r ->
                    if spec.forceRestart then invalidOp "Cannot specify --forceRestart when no progress yet committed"
                    do! checkpoints.Start(spec.checkpointInterval, r.CommitPosition)
                    return Starting, r, spec.checkpointInterval
                | Checkpoint.Fold.Running s, _ when not spec.forceRestart ->
                    return Resuming, mkPos s.state.pos, TimeSpan.FromSeconds(float s.config.checkpointFreqS)
                | Checkpoint.Fold.Running _, r ->
                    do! checkpoints.Override(spec.checkpointInterval, r.CommitPosition)
                    return Overridding, r, spec.checkpointInterval }
            log.Information("Sync {mode} {groupName} @ {pos} (chunk {chunk}, {pct:p1}) checkpointing every {checkpointFreq:n1}m",
                startMode, spec.groupName, startPos.CommitPosition, Reader.chunk startPos, float startPos.CommitPosition / float maxPos.CommitPosition,
                checkpointFreq.TotalMinutes)
            return startPos }

        let ingester = sink.StartIngester(log.ForContext("Tranche", "Ingester"), 0)

        let initialSeriesId, conns, dop =  
            log.Information("Tailing every {intervalS:n1}s TODO with {streamReaders} stream catchup-readers", spec.tailInterval.TotalSeconds, spec.streamReaders)
            match spec.gorge with
            | Some factor ->
                log.Information("Commencing Gorging with {stripes} $all reader stripes covering a 256MB chunk each", factor)
                let extraConns = Seq.init (factor- 1 ) (ignore >> connect)
                let conns = [| yield conn; yield! extraConns |]
                Reader.chunk startPos |> int, conns, (max (conns.Length) (spec.streamReaders+1))
            | None ->
                0, [|conn|], spec.streamReaders + 1

        let striper = StripedIngester(log.ForContext("Tranche", "Stripes"), ingester, maxReadAhead, initialSeriesId, statsInterval)
        let! _pumpStripes = Async.StartChild striper.Pump // will die with us, which is only after Reader finishes :point_down:

        let post = function
            | Reader.Res.EndOfChunk seriesId -> striper.Submit <| Message.CloseSeries seriesId
            | Reader.Res.Batch (seriesId, pos, xs) ->
                let cp = pos.CommitPosition
                striper.Submit <| Message.Batch(seriesId, cp, checkpoints.Commit cp, xs)

        let reader = Reader.EventStoreReader(conns, spec.batchSize, spec.minBatchSize, tryMapEvent, post, spec.tailInterval, dop)
        do! reader.Pump(initialSeriesId, startPos, maxPos) }
