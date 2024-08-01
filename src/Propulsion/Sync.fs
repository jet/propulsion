/// Implements a Sink that synchronizes events from a source to a target that can convey a write position after each set of events are reconciled with a stream
module Propulsion.Sync

open Propulsion.Internal
open Propulsion.Streams
open Serilog
open System
open System.Collections.Generic

[<AbstractClass>]
type Stats<'Outcome>(log: ILogger, statsInterval, stateInterval, [<O; D null>] ?failThreshold) =
    inherit Scheduling.Stats<struct ('Outcome * StreamSpan.Metrics * TimeSpan), Dispatcher.ExnAndMetrics>(log, statsInterval, stateInterval, ?failThreshold = failThreshold)
    let mutable okStreams, okEvents, okUnfolds, okBytes, exnStreams, exnEvents, exnUnfolds, exnBytes = HashSet(), 0, 0, 0L, HashSet(), 0, 0, 0L
    let prepareStats = Stats.LatencyStats("prepare")
    override _.DumpStats() =
        if okStreams.Count <> 0 && exnStreams.Count <> 0 then
            log.Information("Completed {okMb:n0}MB {okStreams:n0}s {okEvents:n0}e {okUnfolds:n0}u Exceptions {exnMb:n0}MB {exnStreams:n0}s {exnEvents:n0}e {exnUnfolds:n0}u",
                            Log.miB okBytes, okStreams.Count, okEvents, okUnfolds, Log.miB exnBytes, exnStreams.Count, exnEvents, exnUnfolds)
        okStreams.Clear(); okEvents <- 0; okUnfolds <- 0; okBytes <- 0L; exnStreams.Clear(); exnBytes <- 0; exnEvents <- 0; exnUnfolds <- 0
        prepareStats.Dump log

    abstract member Classify: exn -> OutcomeKind
    default _.Classify e = OutcomeKind.classify e

    override this.Handle message =
        match message with
        | { stream = stream; result = Ok (outcome, (es, us, bs), prepareElapsed) } ->
            okStreams.Add stream |> ignore
            okEvents <- okEvents + es
            okUnfolds <- okUnfolds + us
            okBytes <- okBytes + int64 bs
            prepareStats.Record prepareElapsed
            base.RecordOk(message, us <> 0)
            this.HandleOk outcome
        | { stream = stream; result = Error (Exception.Inner exn, _malformed, (es, us, bs)) } ->
            exnStreams.Add stream |> ignore
            exnEvents <- exnEvents + es
            exnUnfolds <- exnUnfolds + us
            exnBytes <- exnBytes + int64 bs
            base.RecordExn(message, this.Classify exn, log.ForContext("stream", stream).ForContext("events", es).ForContext("unfolds", us), exn)

    abstract member HandleOk: outcome: 'Outcome -> unit

[<AbstractClass; Sealed>]
type Factory private () =

    static member StartAsync
        (   log: ILogger, maxReadAhead, maxConcurrentStreams,
            handle: Func<FsCodec.StreamName, FsCodec.ITimelineEvent<'F>[], CancellationToken, Task<struct ('Outcome * int64)>>,
            stats: Stats<'Outcome>, sliceSize, eventSize,
            ?dumpExternalStats, ?idleDelay, ?maxBytes, ?maxEvents, ?purgeInterval, ?ingesterStateInterval, ?commitInterval)
        : SinkPipeline<Ingestion.Ingester<StreamEvent<'F> seq>> =

        let maxEvents, maxBytes = defaultArg maxEvents 16384, (defaultArg maxBytes (1024 * 1024 - (*fudge*)4096))

        let attemptWrite stream (events: FsCodec.ITimelineEvent<'F>[]) revision ct = task {
            let struct (trimmed, met) = StreamSpan.slice<'F> sliceSize (maxEvents, maxBytes) events
            let prepareTs = Stopwatch.timestamp ()
            try let! outcome, index' = handle.Invoke(stream, trimmed, ct)
                return Ok struct (outcome, Buffer.HandlerProgress.ofMetricsAndPos revision met index', met, Stopwatch.elapsed prepareTs)
            with e -> return Error struct (e, false, met) }

        let interpretProgress = function
            | Ok struct (outcome, hp, met: StreamSpan.Metrics, prep) -> struct (Ok struct (outcome, met, prep), ValueSome hp, false)
            | Error struct (exn: exn, malformed, met) -> Error struct (exn, malformed, met), ValueNone, malformed

        let dispatcher: Scheduling.IDispatcher<_, _, _, _> = Dispatcher.Concurrent<_, _, _, _>.Create(maxConcurrentStreams, attemptWrite, interpretProgress = interpretProgress)
        let dumpStreams logStreamStates log =
            logStreamStates eventSize
            match dumpExternalStats with Some f -> f log | None -> ()
        let scheduler =
            Scheduling.Engine<struct ('Outcome * Buffer.HandlerProgress * StreamSpan.Metrics * TimeSpan), struct ('Outcome * StreamSpan.Metrics * TimeSpan), Dispatcher.ExnAndMetrics, 'F>
                (dispatcher, stats, dumpStreams, pendingBufferSize = maxReadAhead, ?idleDelay = idleDelay, ?purgeInterval = purgeInterval)

        Factory.Start(log, scheduler.Pump, maxReadAhead, scheduler,
                      ingesterStateInterval = defaultArg ingesterStateInterval stats.StateInterval.Period, ?commitInterval = commitInterval)
