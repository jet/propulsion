namespace Propulsion.Kafka

open Confluent.Kafka
open Jet.ConfluentKafka.FSharp
open Propulsion
open Serilog
open System

/// Methods are intended to be used safely from multiple threads concurrently
type Producers(log : ILogger, clientId, broker, topic, ?customize, ?producerParallelism) =
    let cfg =
        KafkaProducerConfig.Create(
            clientId, broker, Acks.Leader,
            compression = CompressionType.Lz4, linger = TimeSpan.Zero, maxInFlight = 1_000_000,
            ?customize = customize)
    let producers = Array.init (defaultArg producerParallelism 1) (fun _i -> KafkaProducer.Create(log, cfg, topic))
    let produceStats = Streams.Internal.ConcurrentLatencyStats(sprintf "producers(%d)" producers.Length)
    let mutable robin = 0

    member __.DumpStats log = produceStats.Dump log

    member __.ProduceAsync(key, value) = async {
        let producer = producers.[System.Threading.Interlocked.Increment(&robin) % producers.Length]
        let sw = System.Diagnostics.Stopwatch.StartNew()
        let! res = producer.ProduceAsync(key, value)
        produceStats.Record sw.Elapsed
        return res }