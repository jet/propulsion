namespace Propulsion.Kafka

open Propulsion
open Propulsion.Streams
open Serilog
open System
open System.Collections.Generic

type ParallelProducerSink =
    static member Start(maxReadAhead, maxConcurrentStreams, render, producers : Producers, ?statsInterval)
        : ProjectorPipeline<_> =
        let statsInterval = defaultArg statsInterval (TimeSpan.FromMinutes 5.)
        let handle item = async {
            let key, value = render item
            do! Async.Ignore <| producers.ProduceAsync(key, value) }
        Parallel.ParallelProjector.Start(Log.Logger, maxReadAhead, maxConcurrentStreams, handle >> Async.Catch, statsInterval=statsInterval, logExternalStats = producers.DumpStats)

type StreamsProducerStats(log : ILogger, statsInterval, stateInterval) =
    inherit Streams.Scheduling.StreamSchedulerStats<OkResult<TimeSpan>,FailResult>(log, statsInterval, stateInterval)
    let okStreams, failStreams = HashSet(), HashSet()
    let jsonStats = Streams.Internal.LatencyStats("json")
    let mutable okEvents, okBytes, exnEvents, exnBytes = 0, 0L, 0, 0L

    override __.DumpStats() =
        if okStreams.Count <> 0 && failStreams.Count <> 0 then
            log.Information("Completed {okMb:n0}MB {okStreams:n0}s {okEvents:n0}e Exceptions {exnMb:n0}MB {exnStreams:n0}s {exnEvents:n0}e",
                mb okBytes, okStreams.Count, okEvents, mb exnBytes, failStreams.Count, exnEvents)
        okStreams.Clear(); okEvents <- 0; okBytes <- 0L
        jsonStats.Dump log

    override __.Handle message =
        let inline adds x (set:HashSet<_>) = set.Add x |> ignore
        base.Handle message
        match message with
        | Scheduling.InternalMessage.Added _ -> () // Processed by standard logging already; we have nothing to add
        | Scheduling.InternalMessage.Result (_duration, (stream, Choice1Of2 (_,(es,bs),jsonElapsed))) ->
            adds stream okStreams
            okEvents <- okEvents + es
            okBytes <- okBytes + int64 bs
            jsonStats.Record jsonElapsed
        | Scheduling.InternalMessage.Result (_duration, (stream, Choice2Of2 ((es,bs),_exn))) ->
            adds stream failStreams
            exnEvents <- exnEvents + es
            exnBytes <- exnBytes + int64 bs

type StreamsProducerSink =
    static member Start(log : ILogger, maxReadAhead, maxConcurrentStreams, render, producers : Producers, categorize, ?statsInterval, ?stateInterval)
        : ProjectorPipeline<_> =
        let statsInterval, stateInterval = defaultArg statsInterval (TimeSpan.FromMinutes 5.), defaultArg stateInterval (TimeSpan.FromMinutes 5.)
        let stats = StreamsProducerStats(log.ForContext<StreamsProducerStats>(), statsInterval, stateInterval)
        let attemptWrite (_writePos,stream,fullBuffer : Streams.StreamSpan<_>) = async {
            let maxEvents, maxBytes = 16384, 1_000_000 - (*fudge*)4096
            let (eventCount,bytesCount),span = Streams.Buffering.StreamSpan.slice (maxEvents,maxBytes) fullBuffer
            let sw = System.Diagnostics.Stopwatch.StartNew()
            let spanJson = render (stream, span)
            let jsonElapsed = sw.Elapsed
            try let! _res = producers.ProduceAsync(stream,spanJson)
                return Choice1Of2 (span.index + int64 eventCount,(eventCount,bytesCount),jsonElapsed)
            with e -> return Choice2Of2 ((eventCount,bytesCount),e) }
        let interpretWriteResultProgress _streams _stream = function
            | Choice1Of2 (i',_,_) -> Some i'
            | Choice2Of2 (_,_) -> None
        let dispatcher = Streams.Scheduling.Dispatcher<_>(maxConcurrentStreams)
        let streamScheduler =
            Streams.Scheduling.StreamSchedulingEngine<OkResult<TimeSpan>,FailResult>(
                dispatcher, stats, attemptWrite, interpretWriteResultProgress,
                fun s l ->
                    s.Dump(l, Streams.Buffering.StreamState.eventsSize, categorize)
                    producers.DumpStats l)
        Streams.Projector.StreamsProjectorPipeline.Start(log, dispatcher.Pump(), streamScheduler.Pump, maxReadAhead, streamScheduler.Submit, statsInterval)