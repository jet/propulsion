namespace Propulsion.Kafka

open Confluent.Kafka // required for shimming
open Propulsion.Internal
open Serilog
open System

/// Methods are intended to be used safely from multiple threads concurrently
type Producer
    (   log : ILogger, clientId, bootstrapServers, acks, topic,
        // Linger period (larger values improve compression value and throughput, lower values improve best case latency). Default 5ms (librdkafka < 1.5 default: 0.5ms, librdkafka >= 1.5 default: 5ms)
        ?linger,
        // Default: LZ4
        ?compression,
        // Deprecated; there's a good chance this will be removed
        ?degreeOfParallelism,
        // Miscellaneous configuration parameters to be passed to the underlying Confluent.Kafka producer configuration. Same as constructor argument for Confluent.Kafka >=1.2.
        ?config,
        // Miscellaneous configuration parameters to be passed to the underlying Confluent.Kafka producer configuration.
        ?custom,
        // Postprocesses the ProducerConfig after the rest of the rules have been applied
        ?customize) =
    let batching =
        let linger = defaultArg linger (TimeSpan.FromMilliseconds 5.)
        FsKafka.Batching.Linger linger
    let compression = defaultArg compression CompressionType.Lz4
    let cfg = FsKafka.KafkaProducerConfig.Create(clientId, bootstrapServers, acks, batching, compression, ?config = config, ?custom = custom, ?customize=customize)
    // NB having multiple producers has yet to be proved necessary at this point
    // - the theory is that because each producer gets a dedicated rdkafka context, compression thread and set of sockets, better throughput can be attained
    // - we should consider removing the degreeOfParallelism argument and this associated logic unless we actually get to the point of leaning on this
    let producers = Array.init (defaultArg degreeOfParallelism 1) (fun _i -> FsKafka.KafkaProducer.Create(log, cfg, topic))
    let produceStats = Stats.ConcurrentLatencyStats(sprintf "producers(%d)" producers.Length)
    let mutable robin = 0

    member _.DumpStats log = produceStats.Dump log

    /// Execute a producer operation, including recording of the latency statistics for the operation
    /// NOTE: the `execute` function is expected to throw in the event of a failure to produce (this is the standard semantic for all Confluent.Kafka ProduceAsync APIs)
    member _.Produce(execute : FsKafka.KafkaProducer -> Async<'r>) : Async<'r> = async {
        let producer = producers[System.Threading.Interlocked.Increment(&robin) % producers.Length]
        let sw = Stopwatch.start ()
        let! res = execute producer
        produceStats.Record sw.Elapsed
        return res }

    /// Throws if producing fails, per normal Confluent.Kafka 1.x semantics
    member x.Produce(key, value, ?headers) =
        match headers with
        | Some h -> x.Produce(fun producer -> producer.ProduceAsync(key, value, h) |> Async.Ignore)
        | None -> x.Produce(fun producer -> producer.ProduceAsync(key, value) |> Async.Ignore)
