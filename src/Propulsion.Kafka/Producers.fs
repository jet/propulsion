namespace Propulsion.Kafka

open Confluent.Kafka
open Jet.ConfluentKafka.FSharp
open Serilog
open System

type ParallelProducer =
    static member Start(log : ILogger, maxReadAhead, maxConcurrentStreams, clientId, broker, topic, render, ?statsInterval, ?customize)
        : Propulsion.ProjectorPipeline<_> =
        let statsInterval = defaultArg statsInterval (TimeSpan.FromMinutes 5.)
        let cfg = KafkaProducerConfig.Create(clientId, broker, Acks.Leader, compression = CompressionType.Lz4, linger = TimeSpan.Zero, maxInFlight = 1_000_000, ?customize = customize)
        let producer = KafkaProducer.Create(log, cfg, topic)
        let handle item = async {
            let key, value = render item
            let! _res = producer.ProduceAsync(key, value)
            return () }
        Propulsion.Parallel.ParallelProjector.Start(Log.Logger, maxReadAhead, maxConcurrentStreams, handle >> Async.Catch, statsInterval=statsInterval)

type StreamsProducer =
    static member Start(log : ILogger, maxReadAhead, maxConcurrentStreams, clientId, broker, topic, render, categorize, ?statsInterval, ?stateInterval, ?customize)
        : Propulsion.ProjectorPipeline<_> =
        let statsInterval, stateInterval = defaultArg statsInterval (TimeSpan.FromMinutes 5.), defaultArg stateInterval (TimeSpan.FromMinutes 5.)
        let projectionAndKafkaStats = Propulsion.Streams.Projector.Stats(log.ForContext<Propulsion.Streams.Projector.Stats>(), categorize, statsInterval, stateInterval)
        let cfg = KafkaProducerConfig.Create(clientId, broker, Acks.Leader, compression = CompressionType.Lz4, linger = TimeSpan.Zero, maxInFlight = 1_000_000, ?customize = customize)
        let producers = Array.init 2(*Environment.ProcessorCount*) (fun _i -> KafkaProducer.Create(log, cfg, topic))
        let robin = 0
        let s = Propulsion.Streams.Internal.ConcurrentLatencyStats("json")
        let due = Propulsion.Internal.intervalCheck (TimeSpan.FromMinutes 1.)
        let attemptWrite (_writePos,stream,fullBuffer : Propulsion.Streams.StreamSpan<_>) = async {
            let maxEvents, maxBytes = 16384, 1_000_000 - (*fudge*)4096
            let ((eventCount,_) as stats), span = Propulsion.Streams.Buffering.StreamSpan.slice (maxEvents,maxBytes) fullBuffer
            let sw = System.Diagnostics.Stopwatch.StartNew()
            let spanJson = render (stream, span)
            let jsonElapsed = sw.Elapsed
            s.Record jsonElapsed
            if due () then s.Dump log
            let producer = producers.[System.Threading.Interlocked.Increment(&robin) % producers.Length]
            try let! _res = producer.ProduceAsync(stream,spanJson)
                return Choice1Of2 (span.index + int64 eventCount,stats,())
            with e -> return Choice2Of2 (stats,e) }
        let interpretWriteResultProgress _streams _stream = function
            | Choice1Of2 (i',_, _) -> Some i'
            | Choice2Of2 (_,_) -> None
        let dispatcher = Propulsion.Streams.Scheduling.Dispatcher<_>(maxConcurrentStreams)
        let streamScheduler =
            Propulsion.Streams.Scheduling.StreamSchedulingEngine<_,_>(
                dispatcher, projectionAndKafkaStats, attemptWrite, interpretWriteResultProgress,
                fun s l -> s.Dump(l, Propulsion.Streams.Buffering.StreamState.eventsSize, categorize))
        Propulsion.Streams.Projector.StreamsProjectorPipeline.Start(log, dispatcher.Pump(), streamScheduler.Pump, maxReadAhead, streamScheduler.Submit, statsInterval)