namespace Propulsion.Kafka

open Confluent.Kafka
open Jet.ConfluentKafka.FSharp
open Serilog
open System

type ParallelProducer =
    static member Start(log : ILogger, maxReadAhead, maxConcurrentStreams, clientId, broker, topic, render, ?statsInterval)
            : Propulsion.ProjectorPipeline<_> =
        let producer =
            let producerConfig = KafkaProducerConfig.Create(clientId, broker, Acks.Leader, compression = CompressionType.Lz4, maxInFlight = 1_000_000, linger = TimeSpan.Zero)
            KafkaProducer.Create(log, producerConfig, topic)
        let handle item = async {
            let key, value = render item
            let! _res = producer.ProduceAsync(key, value)
            return () }
        Propulsion.Parallel.ParallelProjector.Start(Log.Logger, maxReadAhead, maxConcurrentStreams, handle >> Async.Catch, ?statsInterval=statsInterval)

type StreamsProducer =
    static member Start(log : ILogger, maxReadAhead, maxConcurrentStreams, clientId, broker, topic, render, categorize, ?statsInterval, ?stateInterval)
            : Propulsion.ProjectorPipeline<_> =
        let statsInterval, stateInterval = defaultArg statsInterval (TimeSpan.FromMinutes 5.), defaultArg stateInterval (TimeSpan.FromMinutes 5.)
        let projectionAndKafkaStats = Propulsion.Streams.Projector.Stats(log.ForContext<Propulsion.Streams.Projector.Stats>(), categorize, statsInterval, stateInterval)
        let producerConfig = KafkaProducerConfig.Create(clientId, broker, Acks.Leader, compression = CompressionType.Lz4, maxInFlight = 1_000_000, linger = TimeSpan.Zero)
        let producer = KafkaProducer.Create(log, producerConfig, topic)
        let attemptWrite (_writePos,stream,fullBuffer : Propulsion.Streams.StreamSpan<_>) = async {
            let maxEvents, maxBytes = 16384, 1_000_000 - (*fudge*)4096
            let ((eventCount,_) as stats), span = Propulsion.Streams.Buffering.Span.slice (maxEvents,maxBytes) fullBuffer
            let spanJson = render (stream, span)
            try let! _res = producer.ProduceAsync(stream,spanJson)
                let res = ()
                return Choice1Of2 (span.index + int64 eventCount,stats,res)
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