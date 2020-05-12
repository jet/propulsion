// Shims for stuff that's present in FsKafka 1.x
namespace FsKafka

open Confluent.Kafka
open Newtonsoft.Json
open Newtonsoft.Json.Linq
open Propulsion.Kafka.Internal // Async Helpers
open Propulsion.Kafka0.Confluent.Kafka
open Serilog
open System
open System.Threading
open System.Threading.Tasks
open System.Collections.Generic
open System.Collections.Concurrent

// Cloned from FsKafka master branch
module Core =
    [<NoComparison>]
    type ConsumerBufferingConfig = { minInFlightBytes : int64; maxInFlightBytes : int64; maxBatchSize : int; maxBatchDelay : TimeSpan }

    module Constants =
        let messageCounterSourceContext = "FsKafka.Core.InFlightMessageCounter"

    type InFlightMessageCounter(log : ILogger, minInFlightBytes : int64, maxInFlightBytes : int64) =
        do  if minInFlightBytes < 1L then invalidArg "minInFlightBytes" "must be positive value"
            if maxInFlightBytes < 1L then invalidArg "maxInFlightBytes" "must be positive value"
            if minInFlightBytes > maxInFlightBytes then invalidArg "maxInFlightBytes" "must be greater than minInFlightBytes"

        let mutable inFlightBytes = 0L

        member __.InFlightMb = float inFlightBytes / 1024. / 1024.
        member __.Delta(numBytes : int64) = Interlocked.Add(&inFlightBytes, numBytes) |> ignore
        member __.IsOverLimitNow() = Volatile.Read(&inFlightBytes) > maxInFlightBytes
        member __.AwaitThreshold busyWork =
            if __.IsOverLimitNow() then
                log.Information("Consuming... breached in-flight message threshold (now ~{max:n0}B), quiescing until it drops to < ~{min:n1}GB",
                    inFlightBytes, float minInFlightBytes / 1024. / 1024. / 1024.)
                while Volatile.Read(&inFlightBytes) > minInFlightBytes do
                    busyWork ()
                log.Verbose "Consumer resuming polling"

/// See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md for documentation on the implications of specific settings
[<NoComparison>]
type KafkaProducerConfig private (inner, bootstrapServers) =
    member __.Inner : ProducerConfig = inner
    member __.BootstrapServers = bootstrapServers

    member __.Acks = let v = inner.Acks in v.Value
    member __.MaxInFlight = let v = inner.MaxInFlight in v.Value
    member __.Compression = let v = inner.CompressionType in v.GetValueOrDefault(CompressionType.None)

    /// Creates and wraps a Confluent.Kafka ProducerConfig with the specified settings
    static member Create
        (   clientId : string, bootstrapServers, acks,
            /// Message compression. Defaults to None.
            ?compression,
            /// Maximum in-flight requests. Default: 1_000_000.
            /// NB <> 1 implies potential reordering of writes should a batch fail and then succeed in a subsequent retry
            ?maxInFlight,
            /// Time to wait for other items to be produced before sending a batch. Default: 0ms
            /// NB the linger setting alone does provide any hard guarantees; see BatchedProducer.CreateWithConfigOverrides
            ?linger : TimeSpan,
            /// Number of retries. Confluent.Kafka default: 2. Default: 60.
            ?retries,
            /// Backoff interval. Confluent.Kafka default: 100ms. Default: 1s.
            ?retryBackoff,
            /// Statistics Interval. Default: no stats.
            ?statisticsInterval,
            /// Confluent.Kafka default: false. Defaults to true.
            ?socketKeepAlive,
            /// Partition algorithm. Default: `ConsistentRandom`.
            ?partitioner,
            ?config : IDictionary<string,string>,
            /// Miscellaneous configuration parameters to be passed to the underlying Confluent.Kafka producer configuration.
            ?custom,
            /// Postprocesses the ProducerConfig after the rest of the rules have been applied
            ?customize) =
        let c =
            ProducerConfig(
                ClientId = clientId, BootstrapServers = bootstrapServers,
                RetryBackoffMs = Nullable (match retryBackoff with Some (t : TimeSpan) -> int t.TotalMilliseconds | None -> 1000), // CK default 100ms
                MessageSendMaxRetries = Nullable (defaultArg retries 60), // default 2
                Acks = Nullable acks,
                SocketKeepaliveEnable = Nullable (defaultArg socketKeepAlive true), // default: false
                LogConnectionClose = Nullable false, // https://github.com/confluentinc/confluent-kafka-dotnet/issues/124#issuecomment-289727017
                MaxInFlight = Nullable (defaultArg maxInFlight 1_000_000)) // default 1_000_000
        config |> Option.iter (fun xs -> for KeyValue (k,v) in xs do c.Set(k,v))
        linger |> Option.iter<TimeSpan> (fun x -> c.LingerMs <- Nullable (int x.TotalMilliseconds)) // default 0
        partitioner |> Option.iter (fun x -> c.Partitioner <- Nullable x)
        compression |> Option.iter (fun x -> c.CompressionType <- Nullable x)
        statisticsInterval |> Option.iter<TimeSpan> (fun x -> c.StatisticsIntervalMs <- Nullable (int x.TotalMilliseconds))
        custom |> Option.iter (fun xs -> for KeyValue (k,v) in xs do c.Set(k,v))
        customize |> Option.iter (fun f -> f c)
        KafkaProducerConfig(c, bootstrapServers)

[<AutoOpen>]
module Impl =
    let encoding = System.Text.Encoding.UTF8
    let mkSerializer() = Confluent.Kafka.Serialization.StringSerializer(encoding)
    let mkDeserializer() = Confluent.Kafka.Serialization.StringDeserializer(encoding)

/// Creates and wraps a Confluent.Kafka Producer with the supplied configuration
type KafkaProducer private (inner : Producer<string, string>, topic : string, unsub) =
    member __.Inner = inner
    member __.Topic = topic

    interface IDisposable with member __.Dispose() = unsub(); inner.Dispose()

    /// Produces a single item, yielding a response upon completion/failure of the ack
    /// <remarks>
    ///     There's no assurance of ordering [without dropping `maxInFlight` down to `1` and annihilating throughput].
    ///     Thus its critical to ensure you don't submit another message for the same key until you've had a success / failure response from the call.<remarks/>
    member __.ProduceAsync(key, value) : Async<Message<_,_>>= async {
        let! res = inner.ProduceAsync(topic, key = key, ``val`` = value) |> Async.AwaitTaskCorrect
        // Propulsion.Kafka.Producer duplicates this check, but this one should remain for consistency with Confluent.Kafka v1
        if res.Error.HasError then return failwithf "ProduceAsync error %O" res.Error
        return res }

    static member Create(log : ILogger, config : KafkaProducerConfig, topic : string): KafkaProducer =
        if String.IsNullOrEmpty topic then nullArg "topic"
        log.Information("Producing... {bootstrapServers} / {topic} compression={compression} maxInFlight={maxInFlight} acks={acks}",
            config.BootstrapServers, topic, config.Compression, config.MaxInFlight, config.Acks)
        let p = new Producer<string, string>(config.Inner.Render(), mkSerializer (), mkSerializer())
        let d1 = p.OnLog.Subscribe(fun m -> log.Information("Producing... {message} level={level} name={name} facility={facility}", m.Message, m.Level, m.Name, m.Facility))
        let d2 = p.OnError.Subscribe(fun e -> log.Error("Producing... {reason} code={code} isBrokerError={isBrokerError}", e.Reason, e.Code, e.IsBrokerError))
        new KafkaProducer(p, topic, fun () -> for x in [d1;d2] do x.Dispose())

type IProducer<'K,'V> = Confluent.Kafka.Producer<'K,'V>
type DeliveryReport<'K,'V> = Confluent.Kafka.Message<'K,'V>

type BatchedProducer private (log: ILogger, inner : IProducer<string, string>, topic : string) =
    member __.Inner = inner
    member __.Topic = topic

    interface IDisposable with member __.Dispose() = inner.Dispose()

    /// Produces a batch of supplied key/value messages. Results are returned in order of writing (which may vary from order of submission).
    /// <throws>
    ///    1. if there is an immediate local config issue
    ///    2. upon receipt of the first failed `DeliveryReport` (NB without waiting for any further reports, which can potentially leave some results in doubt should a 'batch' get split) </throws>
    /// <remarks>
    ///    Note that the delivery and/or write order may vary from the supplied order unless `maxInFlight` is 1 (which massively constrains throughput).
    ///    Thus it's important to note that supplying >1 item into the queue bearing the same key without maxInFlight=1 risks them being written out of order onto the topic.<remarks/>
    member __.ProduceBatch(keyValueBatch : (string * string)[]) = async {
        if Array.isEmpty keyValueBatch then return [||] else

        let! ct = Async.CancellationToken

        let tcs = TaskCompletionSource<DeliveryReport<_,_>[]>()
        let numMessages = keyValueBatch.Length
        let results = Array.zeroCreate<DeliveryReport<_,_>> numMessages
        let numCompleted = ref 0

        use _ = ct.Register(fun _ -> tcs.TrySetCanceled() |> ignore)

        let handler (m : DeliveryReport<string,string>) =
            if m.Error.HasError then
                let errorMsg = exn (sprintf "Error on message topic=%s code=%O reason=%s" m.Topic m.Error.Code m.Error.Reason)
                tcs.TrySetException errorMsg |> ignore
            else
                let i = Interlocked.Increment numCompleted
                results.[i - 1] <- m
                if i = numMessages then tcs.TrySetResult results |> ignore
        let handler' =
            { new IDeliveryHandler<string, string> with
                member __.MarshalData = false
                member __.HandleDeliveryReport m = handler m }
        for key,value in keyValueBatch do
            inner.ProduceAsync(topic, key, value, blockIfQueueFull = true, deliveryHandler = handler')
        log.Debug("Produced {count}",!numCompleted)
        return! Async.AwaitTaskCorrect tcs.Task }

    /// Creates and wraps a Confluent.Kafka Producer that affords a batched production mode.
    /// The default settings represent a best effort at providing batched, ordered delivery semantics
    /// NB See caveats on the `ProduceBatch` API for further detail as to the semantics
    static member CreateWithConfigOverrides
        (   log : ILogger, config : KafkaProducerConfig, topic : string,
            /// Default: 1
            /// NB Having a <> 1 value for maxInFlight runs two risks due to the intrinsic lack of
            /// batching mechanisms within the Confluent.Kafka client:
            /// 1) items within the initial 'batch' can get written out of order in the face of timeouts and/or retries
            /// 2) items beyond the linger period may enter a separate batch, which can potentially get scheduled for transmission out of order
            ?maxInFlight,
            /// Having a non-zero linger is critical to items getting into the correct groupings
            /// (even if it of itself does not guarantee anything based on Kafka's guarantees). Default: 100ms
            ?linger: TimeSpan) : BatchedProducer =
        let lingerMs = match linger with Some x -> int x.TotalMilliseconds | None -> 100
        log.Information("Producing... Using batch Mode with linger={lingerMs}", lingerMs)
        config.Inner.LingerMs <- Nullable lingerMs
        config.Inner.MaxInFlight <- Nullable (defaultArg maxInFlight 1)
        let inner = KafkaProducer.Create(log, config, topic)
        new BatchedProducer(log, inner.Inner, topic)

/// See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md for documentation on the implications of specfic settings
[<NoComparison>]
type KafkaConsumerConfig = private { inner: ConsumerConfig; topics: string list; buffering: Core.ConsumerBufferingConfig } with
    member __.Buffering  = __.buffering
    member __.Inner  = __.inner
    member __.Topics = __.topics

    /// Builds a Kafka Consumer Config suitable for KafkaConsumer.Start*
    static member Create
        (   /// Identify this consumer in logs etc
            clientId, bootstrapServers, topics,
            /// Consumer group identifier.
            groupId,
            /// Specifies handling when Consumer Group does not yet have an offset recorded. Confluent.Kafka default: start from Latest. Default: start from Earliest.
            ?autoOffsetReset,
            /// Default 100kB.
            ?fetchMaxBytes,
            /// Minimum number of bytes to wait for (subject to timeout with default of 100ms). Default 1B.
            ?fetchMinBytes,
            /// Stats reporting interval for the consumer. Default: no reporting.
            ?statisticsInterval : TimeSpan,
            ?config : IDictionary<string,string>,
            /// Misc configuration parameter to be passed to the underlying CK consumer.
            ?custom,
            /// Postprocesses the ConsumerConfig after the rest of the rules have been applied
            ?customize,
            ?autoCommitInterval,

            (* Client-side batching / limiting of reading ahead to constrain memory consumption *)

            /// Minimum total size of consumed messages in-memory for the consumer to attempt to fill. Default 2/3 of maxInFlightBytes.
            ?minInFlightBytes,
            /// Maximum total size of consumed messages in-memory before broker polling is throttled. Default 24MiB.
            ?maxInFlightBytes,
            /// Message batch linger time. Default 500ms.
            ?maxBatchDelay,
            /// Maximum number of messages to group per batch on consumer callbacks for BatchedConsumer. Default 1000.
            ?maxBatchSize) =
        let maxInFlightBytes = defaultArg maxInFlightBytes (16L * 1024L * 1024L)
        let minInFlightBytes = defaultArg minInFlightBytes (maxInFlightBytes * 2L / 3L)
        let fetchMaxBytes = defaultArg fetchMaxBytes 100_000
        let c =
            ConsumerConfig(
                ClientId = clientId, BootstrapServers = bootstrapServers, GroupId = groupId,
                AutoOffsetReset = Nullable (defaultArg autoOffsetReset AutoOffsetReset.Earliest), // default: latest
                FetchMaxBytes = Nullable fetchMaxBytes, // default: 524_288_000
                EnableAutoCommit = Nullable true, // at AutoCommitIntervalMs interval, write value supplied by StoreOffset call
                EnableAutoOffsetStore = Nullable false, // explicit calls to StoreOffset are the only things that effect progression in offsets
                LogConnectionClose = Nullable false) // https://github.com/confluentinc/confluent-kafka-dotnet/issues/124#issuecomment-289727017
        config |> Option.iter (fun xs -> for KeyValue (k,v) in xs do c.Set(k,v))
        fetchMinBytes |> Option.iter (fun x -> c.FetchMinBytes <- x) // Fetch waits for this amount of data for up to FetchWaitMaxMs (100)
        statisticsInterval |> Option.iter<TimeSpan> (fun x -> c.StatisticsIntervalMs <- Nullable <| int x.TotalMilliseconds)
        autoCommitInterval |> Option.iter<TimeSpan> (fun x -> c.AutoCommitIntervalMs  <- Nullable <| int x.TotalMilliseconds)
        custom |> Option.iter (fun xs -> for KeyValue (k,v) in xs do c.Set(k,v))
        customize |> Option.iter<ConsumerConfig -> unit> (fun f -> f c)
        {   inner = c
            topics = match Seq.toList topics with [] -> invalidArg "topics" "must be non-empty collection" | ts -> ts
            buffering = {
                maxBatchDelay = defaultArg maxBatchDelay (TimeSpan.FromMilliseconds 500.); maxBatchSize = defaultArg maxBatchSize 1000
                minInFlightBytes = minInFlightBytes; maxInFlightBytes = maxInFlightBytes } }

// Stats format: https://github.com/edenhill/librdkafka/blob/master/STATISTICS.md
type KafkaPartitionMetrics =
    {   partition: int
        [<JsonProperty("fetch_state")>]
        fetchState: string
        [<JsonProperty("next_offset")>]
        nextOffset: int64
        [<JsonProperty("stored_offset")>]
        storedOffset: int64
        [<JsonProperty("committed_offset")>]
        committedOffset: int64
        [<JsonProperty("lo_offset")>]
        loOffset: int64
        [<JsonProperty("hi_offset")>]
        hiOffset: int64
        [<JsonProperty("consumer_lag")>]
        consumerLag: int64 }

type OffsetValue =
    | Unset
    | Valid of value: int64
    override this.ToString() =
        match this with
        | Unset -> "Unset"
        | Valid value -> value.ToString()
module OffsetValue =
    let ofOffset (offset : Offset) =
        match offset.Value with
        | _ when offset = Offset.Invalid -> Unset
        | valid -> Valid valid

type ConsumerBuilder =
    static member private WithLogging(log: ILogger, c : Consumer<_,_>, ?onRevoke) =
        let d1 = c.OnLog.Subscribe(fun m ->
            log.Information("Consuming... {message} level={level} name={name} facility={facility}", m.Message, m.Level, m.Name, m.Facility))
        let d2 = c.OnError.Subscribe(fun e ->
            log.Error("Consuming... Error reason={reason} code={code} broker={isBrokerError}", e.Reason, e.Code, e.IsBrokerError))
        let d3 = c.OnPartitionsAssigned.Subscribe(fun tps ->
            for topic,partitions in tps |> Seq.groupBy (fun p -> p.Topic) |> Seq.map (fun (t,ps) -> t, [| for p in ps -> p.Partition |]) do
                log.Information("Consuming... Assigned {topic:l} {partitions}", topic, partitions)
            c.Assign tps)
        let d4 = c.OnPartitionsRevoked.Subscribe(fun tps ->
            for topic,partitions in tps |> Seq.groupBy (fun p -> p.Topic) |> Seq.map (fun (t,ps) -> t, [| for p in ps -> p.Partition |]) do
                log.Information("Consuming... Revoked {topic:l} {partitions}", topic, partitions)
            c.Unassign ()
            onRevoke |> Option.iter (fun f -> f tps))
        let d5 = c.OnPartitionEOF.Subscribe(fun tpo ->
            log.Verbose("Consuming... EOF {topic} partition={partition} offset={offset}", tpo.Topic, tpo.Partition, let o = tpo.Offset in o.Value))
        let d6 = c.OnOffsetsCommitted.Subscribe(fun cos ->
            for t,ps in cos.Offsets |> Seq.groupBy (fun p -> p.Topic) do
                let o = seq { for p in ps -> p.Partition, OffsetValue.ofOffset p.Offset(*, fmtError p.Error*) }
                let e = cos.Error
                if not e.HasError then log.Information("Consuming... Committed {topic} {offsets}", t, o)
                else log.Warning("Consuming... Committed {topic} {offsets} reason={error} code={code} isBrokerError={isBrokerError}", t, o, e.Reason, e.Code, e.IsBrokerError))
        let d7 = c.OnStatistics.Subscribe(fun json ->
            let stats = JToken.Parse json
            for t in stats.Item("topics").Children() do
                if t.HasValues && c.Subscription |> Seq.exists (fun ct -> ct = t.First.Item("topic").ToString()) then
                    let topic, partitions = let tm = t.First in tm.Item("topic").ToString(), tm.Item("partitions").Children()
                    let metrics = [|
                        for tm in partitions do
                            if tm.HasValues then
                                let kpm = tm.First.ToObject<KafkaPartitionMetrics>()
                                if kpm.partition <> -1 then
                                    yield kpm |]
                    let totalLag = metrics |> Array.sumBy (fun x -> x.consumerLag)
                    log.Information("Consuming... Stats {topic:l} totalLag {totalLag} {@stats}", topic, totalLag, metrics))
        fun () -> for d in [d1;d2;d3;d4;d5;d6;d7] do d.Dispose()
    static member WithLogging(log : ILogger, config : ConsumerConfig, ?onRevoke) =
        let consumer = new Consumer<_,_>(config.Render(), mkDeserializer(), mkDeserializer())
        let unsubLog = ConsumerBuilder.WithLogging(log, consumer, ?onRevoke = onRevoke)
        consumer, unsubLog

module Bindings =

    let inline storeOffset (log : ILogger) (consumer : Consumer<_,_>) (highWaterMark : Confluent.Kafka.Message<string,string>) =
        try let e = consumer.StoreOffset(highWaterMark)
            if e.Error.HasError then log.Error("Consuming... storing offsets failed {@e}", e.Error)
        with e -> log.Error(e, "Consuming... storing offsets failed")

module private ConsumerImpl =
    /// guesstimate approximate message size in bytes
    let approximateMessageBytes (result : Message<string, string>) =
        let inline len (x:string) = match x with null -> 0 | x -> sizeof<char> * x.Length
        16 + len result.Key + len result.Value |> int64

    type BlockingCollection<'T> with
        member bc.FillBuffer(buffer : 'T[], maxDelay : TimeSpan) : int =
            let cts = new CancellationTokenSource()
            do cts.CancelAfter maxDelay

            let n = buffer.Length
            let mutable i = 0
            let mutable t = Unchecked.defaultof<'T>

            while i < n && not cts.IsCancellationRequested do
                if bc.TryTake(&t, 5 (* ms *)) then
                    buffer.[i] <- t ; i <- i + 1
                    while i < n && not cts.IsCancellationRequested && bc.TryTake(&t) do
                        buffer.[i] <- t ; i <- i + 1
            i

    type PartitionedBlockingCollection<'Key, 'Message when 'Key : equality>(?perPartitionCapacity : int) =
        let collections = new ConcurrentDictionary<'Key, Lazy<BlockingCollection<'Message>>>()
        let onPartitionAdded = new Event<'Key * BlockingCollection<'Message>>()

        let createCollection() =
            match perPartitionCapacity with
            | None -> new BlockingCollection<'Message>()
            | Some c -> new BlockingCollection<'Message>(boundedCapacity = c)

        [<CLIEvent>]
        member __.OnPartitionAdded = onPartitionAdded.Publish

        member __.Add (key : 'Key, message : 'Message) =
            let factory key = lazy(
                let coll = createCollection()
                onPartitionAdded.Trigger(key, coll)
                coll)

            let buffer = collections.GetOrAdd(key, factory)
            buffer.Value.Add message

        member __.Revoke(key : 'Key) =
            match collections.TryRemove key with
            | true, coll -> Task.Delay(10000).ContinueWith(fun _ -> coll.Value.CompleteAdding()) |> ignore
            | _ -> ()

    let mkBatchedMessageConsumer (log: ILogger) (buf : Core.ConsumerBufferingConfig) (ct : CancellationToken) (consumer : Consumer<string, string>)
            (partitionedCollection : PartitionedBlockingCollection<TopicPartition, Confluent.Kafka.Message<string, string>>)
            (handler : Confluent.Kafka.Message<string,string>[] -> Async<unit>) = async {
        let tcs = TaskCompletionSource<unit>()
        use cts = CancellationTokenSource.CreateLinkedTokenSource(ct)
        use _ = ct.Register(fun _ -> tcs.TrySetResult () |> ignore)

        use _ = consumer

        let mcLog = log.ForContext(Serilog.Core.Constants.SourceContextPropertyName, Core.Constants.messageCounterSourceContext)
        let counter = Core.InFlightMessageCounter(mcLog, buf.minInFlightBytes, buf.maxInFlightBytes)

        // starts a tail recursive loop that dequeues batches for a given partition buffer and schedules the user callback
        let consumePartition (collection : BlockingCollection<Confluent.Kafka.Message<string, string>>) =
            let buffer = Array.zeroCreate buf.maxBatchSize
            let nextBatch () =
                let count = collection.FillBuffer(buffer, buf.maxBatchDelay)
                if count <> 0 then log.Debug("Consuming {count}", count)
                let batch = Array.init count (fun i -> buffer.[i])
                Array.Clear(buffer, 0, count)
                batch

            let rec loop () = async {
                if not collection.IsCompleted then
                    try match nextBatch() with
                        | [||] -> ()
                        | batch ->
                            // run the handler function
                            do! handler batch

                            // store completed offsets
                            let lastItem = batch |> Array.maxBy (fun m -> let o = m.Offset in o.Value)
                            Bindings.storeOffset log consumer lastItem

                            // decrement in-flight message counter
                            let batchSize = batch |> Array.sumBy approximateMessageBytes
                            counter.Delta(-batchSize)
                    with e ->
                        tcs.TrySetException e |> ignore
                        cts.Cancel()
                    return! loop() }

            Async.Start(loop(), cts.Token)

        use _ = partitionedCollection.OnPartitionAdded.Subscribe (fun (_key,buffer) -> consumePartition buffer)

        // run the consumer
        let ct = cts.Token
        try while not ct.IsCancellationRequested do
                counter.AwaitThreshold(fun () -> Thread.Sleep 1)
                try let mutable message = null
                    if consumer.Consume(&message, 5) then
                        if message.Error.HasError then log.Warning("Consuming... error {e}", message.Error)
                        else
                            counter.Delta(+approximateMessageBytes message)
                            partitionedCollection.Add(message.TopicPartition, message)
                with| :? System.OperationCanceledException -> log.Warning("Consuming... cancelled {name}", consumer.Name)
        finally
            consumer.Dispose()

        // await for handler faults or external cancellation
        return! Async.AwaitTaskCorrect tcs.Task
    }

/// Creates and wraps a Confluent.Kafka IConsumer, wrapping it to afford a batched consumption mode with implicit offset progression at the end of each
/// (parallel across partitions, sequenced/monotonic within) batch of processing carried out by the `partitionHandler`
/// Conclusion of the processing (when a `partitionHandler` throws and/or `Stop()` is called) can be awaited via `AwaitCompletion()`
type BatchedConsumer private (inner : Consumer<string, string>, task : Task<unit>, triggerStop) =

    member __.Inner = inner

    interface IDisposable with member __.Dispose() = __.Stop()
    /// Request cancellation of processing
    member __.Stop() =  triggerStop ()
    /// Inspects current status of processing task
    member __.Status = task.Status
    member __.RanToCompletion = task.Status = System.Threading.Tasks.TaskStatus.RanToCompletion
    /// Asynchronously awaits until consumer stops or is faulted
    member __.AwaitCompletion() = Async.AwaitTaskCorrect task

    /// Starts a Kafka consumer with the provided configuration. Batches are grouped by topic partition.
    /// Batches belonging to the same topic partition will be scheduled sequentially and monotonically; however batches from different partitions can run concurrently.
    /// Completion of the `partitionHandler` saves the attained offsets so the auto-commit can mark progress; yielding an exception terminates the processing
    static member Start(log : ILogger, config : KafkaConsumerConfig, partitionHandler : Message<string,string>[] -> Async<unit>) =
        if List.isEmpty config.topics then invalidArg "config" "must specify at least one topic"
        log.Information("Consuming... {bootstrapServers} {topics} {groupId} autoOffsetReset={autoOffsetReset} fetchMaxBytes={fetchMaxB} maxInFlight={maxInFlightGB:n1}GB maxBatchDelay={maxBatchDelay}s maxBatchSize={maxBatchSize}",
            config.inner.BootstrapServers, config.topics, config.inner.GroupId, (let x = config.inner.AutoOffsetReset in x.Value), config.inner.FetchMaxBytes,
            float config.buffering.maxInFlightBytes / 1024. / 1024. / 1024., (let t = config.buffering.maxBatchDelay in t.TotalSeconds), config.buffering.maxBatchSize)
        let partitionedCollection = ConsumerImpl.PartitionedBlockingCollection<TopicPartition, Message<string, string>>()
        let onRevoke (xs : seq<TopicPartition>) =
            for x in xs do
                partitionedCollection.Revoke(x)
        let consumer, unsubLog = ConsumerBuilder.WithLogging(log, config.inner, onRevoke = onRevoke)
        let cts = new CancellationTokenSource()
        let triggerStop () =
            log.Information("Consuming... Stopping {name}", consumer.Name)
            cts.Cancel()
        let task = ConsumerImpl.mkBatchedMessageConsumer log config.buffering cts.Token consumer partitionedCollection partitionHandler |> Async.StartAsTask
        let c = new BatchedConsumer(consumer, task, triggerStop)
        consumer.Subscribe config.topics
        c

    /// Starts a Kafka consumer instance that schedules handlers grouped by message key. Additionally accepts a global degreeOfParallelism parameter
    /// that controls the number of handlers running concurrently across partitions for the given consumer instance.
    static member StartByKey(log: ILogger, config : KafkaConsumerConfig, degreeOfParallelism : int, keyHandler : Message<_,_> [] -> Async<unit>) =
        let semaphore = new SemaphoreSlim(degreeOfParallelism)
        let partitionHandler (results : Message<_,_>[]) = async {
            return!
                results
                |> Seq.groupBy (fun r -> r.Key)
                |> Seq.map (fun (_,gp) -> async {
                    let! ct = Async.CancellationToken
                    let! _ = semaphore.WaitAsync ct |> Async.AwaitTaskCorrect
                    try do! keyHandler (Seq.toArray gp)
                    finally semaphore.Release() |> ignore })
                |> Async.Parallel
                |> Async.Ignore
        }

        BatchedConsumer.Start(log, config, partitionHandler)
