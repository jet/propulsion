namespace Propulsion.Kafka

open Propulsion
open Propulsion.Streams
open Serilog
open System
open System.Collections.Generic

type ParallelProducerSink =
    static member Start(maxReadAhead, maxDop, render, producer : Producer, ?statsInterval)
        : ProjectorPipeline<_> =
        let statsInterval = defaultArg statsInterval (TimeSpan.FromMinutes 5.)
        let handle item = async {
            let key, value = render item
            do! Bindings.produceAsync producer.ProduceAsync (key, value) }
        Parallel.ParallelProjector.Start(Log.Logger, maxReadAhead, maxDop, handle >> Async.Catch, statsInterval=statsInterval, logExternalStats = producer.DumpStats)

type StreamsProducerStats(log : ILogger, statsInterval, stateInterval) =
    inherit Streams.Scheduling.StreamSchedulerStats<OkResult<TimeSpan>,FailResult>(log, statsInterval, stateInterval)
    let okStreams, failStreams = HashSet(), HashSet()
    let prepareStats = Streams.Internal.LatencyStats("prepare")
    let mutable okEvents, okBytes, exnEvents, exnBytes = 0, 0L, 0, 0L

    override __.DumpStats() =
        if okStreams.Count <> 0 && failStreams.Count <> 0 then
            log.Information("Completed {okMb:n0}MB {okStreams:n0}s {okEvents:n0}e Exceptions {exnMb:n0}MB {exnStreams:n0}s {exnEvents:n0}e",
                mb okBytes, okStreams.Count, okEvents, mb exnBytes, failStreams.Count, exnEvents)
        okStreams.Clear(); okEvents <- 0; okBytes <- 0L
        prepareStats.Dump log

    override __.Handle message =
        let inline adds x (set:HashSet<_>) = set.Add x |> ignore
        base.Handle message
        match message with
        | Scheduling.InternalMessage.Added _ -> () // Processed by standard logging already; we have nothing to add
        | Scheduling.InternalMessage.Result (_duration, (stream, Choice1Of2 (_,(es,bs),prepareElapsed))) ->
            adds stream okStreams
            okEvents <- okEvents + es
            okBytes <- okBytes + int64 bs
            prepareStats.Record prepareElapsed
        | Scheduling.InternalMessage.Result (_duration, (stream, Choice2Of2 ((es,bs),_exn))) ->
            adds stream failStreams
            exnEvents <- exnEvents + es
            exnBytes <- exnBytes + int64 bs

type StreamsProducerSink =
    static member Start
        (   log : ILogger, maxReadAhead, maxConcurrentStreams, prepare, producer : Producer, categorize,
            ?statsInterval, ?stateInterval,
            /// Default .5 ms
            ?idleDelay,
            /// Default 1 MiB
            ?maxBytes,
            /// Default 16384
            ?maxEvents,
            /// Max scheduling readahead. Default 128.
            ?maxBatches,
            /// Max inner cycles per loop. Default 128.
            ?maxCycles)
        : ProjectorPipeline<_> =
        let statsInterval, stateInterval = defaultArg statsInterval (TimeSpan.FromMinutes 5.), defaultArg stateInterval (TimeSpan.FromMinutes 5.)
        let maxBatches, maxEvents, maxBytes = defaultArg maxBatches 128, defaultArg maxEvents 16384, (defaultArg maxBytes (1024*1024 - (*fudge*)4096))
        let stats = StreamsProducerStats(log.ForContext<StreamsProducerStats>(), statsInterval, stateInterval)
        let attemptWrite (_writePos,stream,fullBuffer : Streams.StreamSpan<_>) = async {
            let (eventCount,bytesCount),span = Streams.Buffering.StreamSpan.slice (maxEvents,maxBytes) fullBuffer
            let sw = System.Diagnostics.Stopwatch.StartNew()
            let! (message : string) = prepare (stream, span)
            let prepareElapsed = sw.Elapsed
            match message.Length with
            | x when x > maxBytes ->
                log.Warning("Message on {stream} had String.Length {length} using {events}/{availableEvents}",
                    stream, x, span.events.Length, fullBuffer.events.Length)
            | _ -> ()
            try do! Bindings.produceAsync producer.ProduceAsync (stream,message)
                return Choice1Of2 (span.index + int64 eventCount,(eventCount,bytesCount),prepareElapsed)
            with e -> return Choice2Of2 ((eventCount,bytesCount),e) }
        let interpretWriteResultProgress _streams (stream : string) = function
            | Choice1Of2 (i',_,_) -> Some i'
            | Choice2Of2 ((eventCount,bytesCount),exn : exn) ->
                log.Warning(exn,"Writing   {events:n0}e {bytes:n0}b for {stream} failed, retrying", eventCount, bytesCount, stream)
                None
        let dispatcher = Streams.Scheduling.Dispatcher<_>(maxConcurrentStreams)
        let streamScheduler =
            Streams.Scheduling.StreamSchedulingEngine<OkResult<TimeSpan>,FailResult>
                (   dispatcher, stats, attemptWrite, interpretWriteResultProgress,
                    (fun s l ->
                        s.Dump(l, Streams.Buffering.StreamState.eventsSize, categorize)
                        producer.DumpStats l),
                    maxBatches=maxBatches, maxCycles=defaultArg maxCycles 128,
                    idleDelay=defaultArg idleDelay (TimeSpan.FromMilliseconds 0.5))
        Streams.Projector.StreamsProjectorPipeline.Start(
            log, dispatcher.Pump(), streamScheduler.Pump, maxReadAhead, streamScheduler.Submit, statsInterval,
            maxSubmissionsPerPartition=maxBatches)