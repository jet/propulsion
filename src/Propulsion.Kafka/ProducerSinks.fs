namespace Propulsion.Kafka

open FsCodec
open Propulsion
open Propulsion.Internal
open Propulsion.Streams
open Serilog
open System

type ParallelProducerSink =
    static member Start(maxReadAhead, maxDop, render, producer : Producer, ?statsInterval)
        : Sink<Ingestion.Ingester<'F seq>> =
        let statsInterval = defaultArg statsInterval (TimeSpan.FromMinutes 5.)
        let handle item ct = task {
            let key, value = render item
            do! producer.Produce(key, value, ?headers = None, ct = ct) }
        Parallel.ParallelSink.Start(Log.Logger, maxReadAhead, maxDop, (fun x ct -> handle x ct |> Task.Catch),
                                    statsInterval = statsInterval, logExternalStats = producer.DumpStats)

type StreamsProducerSink =

   static member Start
        (   log : ILogger, maxReadAhead, maxConcurrentStreams,
            prepare : struct (StreamName * Default.StreamSpan) -> Async<struct ((struct (string * string)) voption * 'Outcome)>,
            producer : Producer,
            stats : Sync.Stats<'Outcome>, statsInterval,
            // Frequency with which to jettison Write Position information for inactive streams in order to limit memory consumption
            // NOTE: Can impair performance and/or increase costs of writes as it inhibits the ability of the ingester to discard redundant inputs
            ?purgeInterval,
            // Default 1 ms
            ?idleDelay,
            // Default 1 MiB
            ?maxBytes,
            // Default 16384
            ?maxEvents)
        : Default.Sink =
            let maxBytes = defaultArg maxBytes (1024*1024 - (*fudge*)4096)
            let handle (stream : StreamName) span ct = task {
                let! (maybeMsg, outcome : 'Outcome) = prepare (stream, span)
                match maybeMsg with
                | ValueSome (key : string, message : string) ->
                    match message.Length with
                    | x when x > maxBytes -> log.Warning("Message on {stream} had String.Length {length} Queue length {queueLen}", stream, x, span.Length)
                    | _ -> ()
                    do! producer.Produce(key, message, ct = ct)
                | ValueNone -> ()
                return struct (SpanResult.AllProcessed, outcome)
            }
            Sync.StreamsSync.Start
                (    log, maxReadAhead, maxConcurrentStreams, (fun s e ct -> handle s e ct),
                     stats, statsInterval, Default.jsonSize, Default.eventSize,
                     maxBytes = maxBytes, ?idleDelay = idleDelay,?purgeInterval = purgeInterval,
                     ?maxEvents = maxEvents, dumpExternalStats = producer.DumpStats)

   static member Start
        (   log : ILogger, maxReadAhead, maxConcurrentStreams,
            prepare : struct (StreamName * Default.StreamSpan) -> Async<struct (string * string)>,
            producer : Producer,
            stats : Sync.Stats<unit>, statsInterval,
            // Frequency with which to jettison Write Position information for inactive streams in order to limit memory consumption
            // NOTE: Can impair performance and/or increase costs of writes as it inhibits the ability of the ingester to discard redundant inputs
            ?purgeInterval,
            // Default 1 ms
            ?idleDelay,
            // Default 1 MiB
            ?maxBytes,
            // Default 16384
            ?maxEvents)
        : Default.Sink =
            let prepare struct (stream, span) = async {
                let! kv = prepare (stream, span)
                return struct (ValueSome kv, ())
            }
            StreamsProducerSink.Start
                (    log, maxReadAhead, maxConcurrentStreams, prepare, producer,
                     stats, statsInterval,
                     ?idleDelay = idleDelay, ?purgeInterval = purgeInterval, ?maxBytes = maxBytes,
                     ?maxEvents = maxEvents)
