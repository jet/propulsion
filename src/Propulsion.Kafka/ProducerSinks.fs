namespace Propulsion.Kafka

open FsCodec
open Propulsion
open Propulsion.Internal
open Propulsion.Sinks
open Serilog
open System
open System.Threading
open System.Threading.Tasks

type ParallelProducerSink =

    static member Start(maxReadAhead, maxDop, render : Func<'F, struct(string * string)>, producer : Producer, ?statsInterval)
        : Sink<Ingestion.Ingester<'F seq>> =
        let statsInterval = defaultArg statsInterval (TimeSpan.FromMinutes 5.)
        let handle item ct = task {
            let struct (key, value) = render.Invoke item
            do! producer.Produce(key, value, ?headers = None, ct = ct) }
        Parallel.Factory.Start(Log.Logger, maxReadAhead, maxDop, (fun x ct -> handle x ct |> Task.Catch),
                               statsInterval = statsInterval, logExternalStats = producer.DumpStats)

type StreamsProducerSink =

    static member StartAsync
        (   log : ILogger, maxReadAhead,
            maxConcurrentStreams, prepare : Func<StreamName, Event[], CancellationToken, Task<struct (struct (string * string) voption * 'Outcome)>>,
            producer : Producer,
            stats : Sync.Stats<'Outcome>,
            // Frequency with which to jettison Write Position information for inactive streams in order to limit memory consumption
            // NOTE: Can impair performance and/or increase costs of writes as it inhibits the ability of the ingester to discard redundant inputs
            ?purgeInterval,
            // Default 1 ms
            ?idleDelay,
            // Default 1 MiB
            ?maxBytes,
            // Default 16384
            ?maxEvents)
        : Sink =
            let maxBytes = defaultArg maxBytes (1024*1024 - (*fudge*)4096)
            let handle (stream : StreamName) span ct = task {
                let! (maybeMsg, outcome : 'Outcome) = prepare.Invoke(stream, span, ct)
                match maybeMsg with
                | ValueSome (key : string, message : string) ->
                    match message.Length with
                    | x when x > maxBytes -> log.Warning("Message on {stream} had String.Length {length} Queue length {queueLen}", stream, x, span.Length)
                    | _ -> ()
                    do! producer.Produce(key, message, ct = ct)
                | ValueNone -> ()
                return struct (StreamResult.AllProcessed, outcome)
            }
            Sync.Factory.StartAsync
                (   log, maxReadAhead, maxConcurrentStreams, handle, StreamResult.toIndex,
                    stats, Event.renderedSize, Event.storedSize,
                    maxBytes = maxBytes, ?idleDelay = idleDelay, ?purgeInterval = purgeInterval,
                    ?maxEvents = maxEvents, dumpExternalStats = producer.DumpStats)

    static member Start
        (   log, maxReadAhead,
            maxConcurrentStreams, prepare : StreamName -> Event[] -> Async<(string * string) option * 'Outcome>,
            producer,
            stats,
            ?purgeInterval, ?idleDelay, ?maxBytes, ?maxEvents)
        : Sink =
        let prepare' s xs ct = task {
            let! r, o = prepare s xs |> Async.executeAsTask ct
            let r' = r |> ValueOption.ofOption |> ValueOption.map ValueTuple.Create
            return struct (r', o) }
        StreamsProducerSink.StartAsync(
            log, maxReadAhead, maxConcurrentStreams, prepare', producer, stats,
            ?purgeInterval = purgeInterval, ?idleDelay = idleDelay, ?maxBytes = maxBytes, ?maxEvents = maxEvents)

    static member StartAsync
        (   log : ILogger, maxReadAhead,
            maxConcurrentStreams, prepare : Func<StreamName, Event[], CancellationToken, Task<struct (string * string)>>,
            producer : Producer, stats : Sync.Stats<unit>,
            // Frequency with which to jettison Write Position information for inactive streams in order to limit memory consumption
            // NOTE: Can impair performance and/or increase costs of writes as it inhibits the ability of the ingester to discard redundant inputs
            ?purgeInterval,
            // Default 1 ms
            ?idleDelay,
            // Default 1 MiB
            ?maxBytes,
            // Default 16384
            ?maxEvents)
        : Sink =
            let prepare' stream span ct = task {
                let! kv = prepare.Invoke(stream, span, ct)
                return struct (ValueSome kv, ())
            }
            StreamsProducerSink.StartAsync
                (   log, maxReadAhead, maxConcurrentStreams, prepare', producer, stats,
                    ?idleDelay = idleDelay, ?purgeInterval = purgeInterval, ?maxBytes = maxBytes,
                    ?maxEvents = maxEvents)

    static member Start
        (   log, maxReadAhead,
            maxConcurrentStreams, prepare : StreamName -> Event[] -> Async<string * string>,
            producer,
            stats,
            ?purgeInterval, ?idleDelay, ?maxBytes, ?maxEvents)
        : Sink =
        let prepare' s xs ct = task {
            let! k, v = prepare s xs |> Async.executeAsTask ct
            return struct (k, v) }
        StreamsProducerSink.StartAsync(
            log, maxReadAhead, maxConcurrentStreams, prepare', producer, stats,
            ?purgeInterval = purgeInterval, ?idleDelay = idleDelay, ?maxBytes = maxBytes, ?maxEvents = maxEvents)
