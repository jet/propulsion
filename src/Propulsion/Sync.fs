/// Implements a Sink that synchronizes events from a source to a target that can convey a write position after each set of events are reconciled with a stream
module Propulsion.Sync

open Propulsion.Internal
open Propulsion.Streams
open Serilog
open System
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks

[<AbstractClass>]
type Stats<'Outcome>(log : ILogger, statsInterval, stateInterval) =
    inherit Scheduling.Stats<struct (struct (StreamSpan.Metrics * TimeSpan) * 'Outcome), struct (StreamSpan.Metrics * exn)>(log, statsInterval, stateInterval)
    let okStreams, failStreams = HashSet(), HashSet()
    let prepareStats = Stats.LatencyStats("prepare")
    let mutable okEvents, okBytes, exnEvents, exnBytes = 0, 0L, 0, 0L

    override _.DumpStats() =
        if okStreams.Count <> 0 && failStreams.Count <> 0 then
            log.Information("Completed {okMb:n0}MB {okStreams:n0}s {okEvents:n0}e Exceptions {exnMb:n0}MB {exnStreams:n0}s {exnEvents:n0}e",
                            Log.miB okBytes, okStreams.Count, okEvents, Log.miB exnBytes, failStreams.Count, exnEvents)
        okStreams.Clear(); okEvents <- 0; okBytes <- 0L; failStreams.Clear(); exnBytes <- 0; exnEvents <- 0
        prepareStats.Dump log

    abstract member Classify : exn -> OutcomeKind
    default _.Classify e = OutcomeKind.classify e

    override this.Handle message =
        let inline adds x (set : HashSet<_>) = set.Add x |> ignore
        match message with
        | { stream = stream; result = Ok (((es, bs), prepareElapsed), outcome) } ->
            adds stream okStreams
            okEvents <- okEvents + es
            okBytes <- okBytes + int64 bs
            prepareStats.Record prepareElapsed
            base.RecordOk message
            this.HandleOk outcome
        | { stream = stream; result = Error ((es, bs), Exception.Inner exn) } ->
            adds stream failStreams
            exnEvents <- exnEvents + es
            exnBytes <- exnBytes + int64 bs
            base.RecordExn(message, this.Classify exn, log.ForContext("stream", stream).ForContext("events", es), exn)

    abstract member HandleOk : outcome : 'Outcome -> unit

type Factory private () =

    static member StartAsync
        (   log : ILogger, maxReadAhead, maxConcurrentStreams,
            handle : Func<FsCodec.StreamName, FsCodec.ITimelineEvent<'F>[], CancellationToken, Task<struct ('R * 'Outcome)>>,
            toIndex : Func<FsCodec.ITimelineEvent<'F>[], 'R, int64>,
            stats : Stats<'Outcome>, sliceSize, eventSize,
            ?dumpExternalStats, ?idleDelay, ?maxBytes, ?maxEvents, ?purgeInterval)
        : Sink<Ingestion.Ingester<StreamEvent<'F> seq>> =

        let maxEvents, maxBytes = defaultArg maxEvents 16384, (defaultArg maxBytes (1024 * 1024 - (*fudge*)4096))

        let attemptWrite stream (events : FsCodec.ITimelineEvent<'F>[]) ct = task {
            let struct (met, span') = StreamSpan.slice<'F> sliceSize (maxEvents, maxBytes) events
            let prepareTs = Stopwatch.timestamp ()
            try let! res, outcome = handle.Invoke(stream, span', ct)
                let index' = toIndex.Invoke(span', res)
                return struct (index' > events[0].Index, Ok struct (index', struct (met, Stopwatch.elapsed prepareTs), outcome))
            with e -> return struct (false, Error struct (met, e)) }

        let interpretWriteResultProgress _streams (stream : FsCodec.StreamName) = function
            | Ok struct (i', stats, outcome) ->
                struct (ValueSome i', Ok struct (stats, outcome))
            | Error struct (struct (eventCount, bytesCount) as stats, exn : exn) ->
                log.Warning(exn, "Handling {events:n0}e {bytes:n0}b for {stream} failed, retrying", eventCount, bytesCount, stream)
                ValueNone, Error struct (stats, exn)

        let dispatcher : Scheduling.IDispatcher<_, _, _, _> = Dispatcher.Concurrent<_, _, _, _>.Create(maxConcurrentStreams, attemptWrite, interpretWriteResultProgress)
        let dumpStreams logStreamStates log =
            logStreamStates eventSize
            match dumpExternalStats with Some f -> f log | None -> ()
        let scheduler =
            Scheduling.Engine<struct (int64 * struct (StreamSpan.Metrics * TimeSpan) * 'Outcome), struct (struct (StreamSpan.Metrics * TimeSpan) * 'Outcome), struct (StreamSpan.Metrics * exn), 'F>
                (dispatcher, stats, dumpStreams, pendingBufferSize = maxReadAhead, ?idleDelay = idleDelay, ?purgeInterval = purgeInterval)

        Projector.Pipeline.Start(log, scheduler.Pump, maxReadAhead, scheduler, stats.StatsInterval.Period)
