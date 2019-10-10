namespace Propulsion.Kafka

open Propulsion
open Propulsion.Streams
open Serilog
open System

type ParallelProducerSink =
    static member Start(maxReadAhead, maxDop, render, producer : Producer, ?statsInterval)
        : ProjectorPipeline<_> =
        let statsInterval = defaultArg statsInterval (TimeSpan.FromMinutes 5.)
        let handle item = async {
            let key, value = render item
            do! Bindings.produceAsync producer.ProduceAsync (key, value) }
        Parallel.ParallelProjector.Start(Log.Logger, maxReadAhead, maxDop, handle >> Async.Catch, statsInterval=statsInterval, logExternalStats = producer.DumpStats)

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
            let maxBytes =  (defaultArg maxBytes (1024*1024 - (*fudge*)4096))
            let handle (stream, span) = async {
                let! (message : string) = prepare (stream, span)
                match message.Length with
                | x when x > maxBytes -> log.Warning("Message on {stream} had String.Length {length} Queue length {queueLen}", stream, x, span.events.Length)
                | _ -> ()
                let! _ = producer.ProduceAsync(stream,message)
                return span.index + span.events.LongLength
            }
            Sync.StreamsSync.Start
                (    log, maxReadAhead, maxConcurrentStreams, handle, categorize,
                     maxBytes=maxBytes, ?statsInterval = statsInterval, ?stateInterval = stateInterval, ?idleDelay=idleDelay,
                     ?maxEvents=maxEvents, ?maxBatches=maxBatches, ?maxCycles=maxCycles, dumpExternalStats=producer.DumpStats)