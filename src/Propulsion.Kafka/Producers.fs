namespace Propulsion.Kafka

open Confluent.Kafka // required for shimming
open FsKafka
open Propulsion
open Serilog
open System

/// Methods are intended to be used safely from multiple threads concurrently
type Producer
    (   log : ILogger, clientId, broker, topic, ?customize,
        /// Default: Leader
        ?acks,
        /// Linger period (larger values improve compression value and throughput, lower values improve best case latency). Default 5ms (librdkafka < 1.5 default: 0.5ms, librdkafka >= 1.5 default: 5ms)
        ?linger,
        /// Default: LZ4
        ?compression,
        // Deprecated; there's a good chance this will be removed
        ?degreeOfParallelism) =
    let acks = defaultArg acks Acks.Leader
    let batching =
        let linger = defaultArg linger (TimeSpan.FromMilliseconds 5.)
        FsKafka.Batching.Linger linger
    let compression = defaultArg compression CompressionType.Lz4
    let cfg = KafkaProducerConfig.Create(clientId, broker, acks, batching, compression, ?customize=customize)
    // NB having multiple producers has yet to be proved necessary at this point
    // - the theory is that because each producer gets a dedicated rdkafka context, compression thread and set of sockets, better throughput can be attained
    // - we should consider removing the degreeOfParallism argument and this associated logic unless we actually get to the point of leaning on this
    let producers = Array.init (defaultArg degreeOfParallelism 1) (fun _i -> KafkaProducer.Create(log, cfg, topic))
    let produceStats = Streams.Internal.ConcurrentLatencyStats(sprintf "producers(%d)" producers.Length)
    let mutable robin = 0

    member __.DumpStats log = produceStats.Dump log

    /// Execute a producer operation, including recording of the latency statistics for the operation
    /// NOTE: the `execute` function is expected to throw in the event of a failure to produce (this is the standard semantic for all Confluent.Kafka ProduceAsync APIs)
#if KAFKA0
    /// NOTE: Confluent.Kafka0 APIs are expected to adhere to V1 semantics (i.e. throwing in the event of a failure to produce)
    ///       However, a failure check is incorporated here as a backstop
    member __.Produce(execute : KafkaProducer -> Async<Message<_,_>>) : Async<Message<_,_>> = async {
#else
    member __.Produce(execute : KafkaProducer -> Async<'r>) : Async<'r> = async {
#endif
        let producer = producers.[System.Threading.Interlocked.Increment(&robin) % producers.Length]
        let sw = System.Diagnostics.Stopwatch.StartNew()
        let! res = execute producer
#if KAFKA0
        if res.Error.HasError then return invalidOp res.Error.Reason // CK 1.x throws, we do the same here for consistency
#endif
        produceStats.Record sw.Elapsed
        return res }

#if KAFKA0
    /// Throws if producing fails, for consistency with Confluent.Kafka >= 1.0 APIs
    /// NOTE The underlying 0.11.x Confluent.Kafka drivers do not throw; This implementation throws if the response `.Error.HasError` for symmetry with the Confluent.Kafka >= 1 behavior
    /// NOTE Propulsion.Kafka (i.e. not using Propulsion.Kafka0) adds an optional `headers` argument
    member __.Produce(key, value) =
        __.Produce(fun producer -> producer.ProduceAsync(key, value)) |> Async.Ignore
#else
    /// Throws if producing fails, per normal Confluent.Kafka 1.x semantics
    member __.Produce(key, value, ?headers) =
        __.Produce(fun producer -> producer.ProduceAsync(key, value, ?headers=headers) |> Async.Ignore)
#endif

    [<Obsolete("Please migrate code to an appropriate Produce overload")>]
    /// Throws if producing fails, per normal Confluent.Kafka 1.x semantics
    member __.ProduceAsync(key, value) =
        __.Produce(fun x -> x.ProduceAsync(key, value))
