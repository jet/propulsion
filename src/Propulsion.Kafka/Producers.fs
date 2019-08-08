namespace Propulsion.Kafka

open Confluent.Kafka
#if KAFKA0
open Propulsion.Kafka0.Confluent.Kafka
open Propulsion.Kafka0.Jet.ConfluentKafka.FSharp
#else
open Jet.ConfluentKafka.FSharp
#endif
open Propulsion
open Serilog
open System

/// Methods are intended to be used safely from multiple threads concurrently
type Producer
    (   log : ILogger, clientId, broker, topic, ?customize,
        // Deprecated; there's a good chance this will be removed
        ?degreeOfParallelism) =
    let cfg =
        KafkaProducerConfig.Create(
            clientId, broker, Acks.Leader,
            compression = CompressionType.Lz4, linger = TimeSpan.Zero, maxInFlight = 1_000_000,
            ?customize = customize)
    // NB having multiple producers has yet to be proved necessary at this point
    // - the theory is that because each producer gets a dedicated rdkafka context, compression thread and set of sockets, better throughput can be attained
    // - we should consider removing the degreeOfParallism argument and this associated logic unless we actually get to the point of leaning on this
    let producers = Array.init (defaultArg degreeOfParallelism 1) (fun _i -> KafkaProducer.Create(log, cfg, topic))
    let produceStats = Streams.Internal.ConcurrentLatencyStats(sprintf "producers(%d)" producers.Length)
    let mutable robin = 0

    member __.DumpStats log = produceStats.Dump log

    member __.ProduceAsync(key, value) = async {
        let producer = producers.[System.Threading.Interlocked.Increment(&robin) % producers.Length]
        let sw = System.Diagnostics.Stopwatch.StartNew()
        let! res = producer.ProduceAsync(key, value)
        produceStats.Record sw.Elapsed
        return res }