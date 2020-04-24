namespace Propulsion.Kafka

open FsCodec
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
            do! producer.Produce (key, value) }
        Parallel.ParallelProjector.Start(Log.Logger, maxReadAhead, maxDop, handle >> Async.Catch, statsInterval=statsInterval, logExternalStats = producer.DumpStats)

type StreamsProducerSink =

   static member Start
        (   log : ILogger, maxReadAhead, maxConcurrentStreams,
            prepare : StreamName * StreamSpan<_> -> Async<(string*string) option * 'Outcome>,
            producer : Producer,
            stats : Streams.Sync.Stats<'Outcome>, projectorStatsInterval,
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
            let handle (stream : StreamName, span) = async {
                let! (maybeMsg, outcome : 'Outcome) = prepare (stream, span)
                match maybeMsg with
                | Some (key : string, message : string) ->
                    match message.Length with
                    | x when x > maxBytes -> log.Warning("Message on {stream} had String.Length {length} Queue length {queueLen}", stream, x, span.events.Length)
                    | _ -> ()
                    do! producer.Produce(key, message)
                | None -> ()
                return span.index + span.events.LongLength, outcome
            }
            Sync.StreamsSync.Start
                (    log, maxReadAhead, maxConcurrentStreams, handle, stats, projectorStatsInterval = projectorStatsInterval,
                     maxBytes=maxBytes, ?idleDelay=idleDelay,
                     ?maxEvents=maxEvents, ?maxBatches=maxBatches, ?maxCycles=maxCycles, dumpExternalStats=producer.DumpStats)

   static member Start
        (   log : ILogger, maxReadAhead, maxConcurrentStreams,
            prepare : StreamName * StreamSpan<_> -> Async<string*string>,
            producer : Producer,
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
            let stats = Streams.Sync.Stats<unit>(log.ForContext<Sync.Stats<unit>>(), statsInterval, stateInterval)
            let prepare (stream,span) = async {
                let! k,v = prepare (stream,span)
                return Some (k,v), ()
            }
            StreamsProducerSink.Start
                (    log, maxReadAhead, maxConcurrentStreams,
                     prepare, producer, stats, projectorStatsInterval = statsInterval,
                     ?idleDelay=idleDelay, ?maxBytes = maxBytes,
                     ?maxEvents=maxEvents, ?maxBatches=maxBatches, ?maxCycles=maxCycles)
