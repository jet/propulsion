namespace Jet.ConfluentKafka

open System
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks

open NLog
open Confluent.Kafka
open System.Reactive.Linq

[<AutoOpen>]
module private Helpers =

    let logger = LogManager.GetLogger __SOURCE_FILE__
    let encoding = System.Text.Encoding.UTF8

    type Logger with
        static member Create name = LogManager.GetLogger name

        member inline ts.log (format, level) =
            let inline trace (message:string) = ts.Log(level, message)
            Printf.kprintf trace format

        member inline ts.log (message:string, level) = ts.Log(level, message)
        member inline ts.info format = ts.log (format, LogLevel.Info)
        member inline ts.warn format = ts.log (format, LogLevel.Warn)
        member inline ts.error format = ts.log (format, LogLevel.Error)
        member inline ts.verbose format = ts.log (format, LogLevel.Debug)
        member inline ts.trace format = ts.log (format, LogLevel.Trace)
        member inline ts.critical format = ts.log (format, LogLevel.Fatal)

    type Async with
        static member AwaitTaskCorrect (task : Task<'T>) : Async<'T> =
            Async.FromContinuations <| fun (k,ek,_) ->
                task.ContinueWith (fun (t:Task<'T>) ->
                    if t.IsFaulted then
                        let e = t.Exception
                        if e.InnerExceptions.Count = 1 then ek e.InnerExceptions.[0]
                        else ek e
                    elif t.IsCanceled then ek (TaskCanceledException("Task wrapped with Async has been cancelled."))
                    elif t.IsCompleted then k t.Result
                    else ek(Exception "invalid Task state!"))
                |> ignore

[<Struct; RequireQualifiedAccess>]
type Acks =
    | Zero
    | One
    | All

[<Struct; RequireQualifiedAccess>]
type Compression =
    | None
    | GZip
    | Snappy
    | LZ4

[<Struct; RequireQualifiedAccess>]
type AutoOffsetReset =
    | Earliest
    | Latest
    | None

[<Struct; RequireQualifiedAccess>]
type Partitioner =
    | Random
    | Consistent
    | ConsistentRandom

type KafkaConfiguration = KeyValuePair<string, obj>

type ConfigKey<'T> = Key of id:string * render:('T -> obj)
with
    member k.Id = let (Key(id,_)) = k in id

    static member (==>) (Key(id, render) : ConfigKey<'T>, value : 'T) = 
        match render value with
        | null -> nullArg id
        | :? string as str when String.IsNullOrWhiteSpace str -> nullArg id
        | obj -> KeyValuePair(id, obj)

[<RequireQualifiedAccess>]
module Config =

    // A small DSL for interfacing with Kafka configuration
    // c.f. https://kafka.apache.org/documentation/#configuration

    let private mkKey id render = Key(id, render >> box)
    let private ms (t : TimeSpan) = int t.TotalMilliseconds

    // shared keys
    let broker = mkKey "bootstrap.servers" (fun (u:Uri) -> string u)
    let clientId = mkKey "client.id" id<string>
    let maxInFlight = mkKey "max.in.flight.requests.per.connection" id<int>
    let logConnectionClose = mkKey "log.connection.close" id<bool>
    let apiVersionRequest = mkKey "api.version.request" id<bool>
    let brokerVersionFallback = mkKey "broker.version.fallback" id<string>
    let rebalanceTimeout = mkKey "rebalance.timeout.ms" ms

    // producers
    let linger = mkKey "linger.ms" ms
    let acks = mkKey "acks" (function Acks.Zero -> 0 | Acks.One -> 1 | Acks.All -> -1)
    let batchNumMessages = mkKey "batch.num.messages" id<int>
    let retries = mkKey "retries" id<int>
    let retryBackoff = mkKey "retry.backoff.ms" ms
    let timeout = mkKey "request.timeout.ms" ms
    let messageSendMaxRetries = mkKey "message.send.max.retries" id<int>
    let statisticsInterval = mkKey "statistics.interval.ms" ms
    let compression = 
        mkKey "compression.type"
            (function
            | Compression.None -> "none"
            | Compression.GZip -> "gzip"
            | Compression.Snappy -> "snappy"
            | Compression.LZ4 -> "lz4")

    let partitioner =
        mkKey "partitioner"
            (function
            | Partitioner.Random -> "random"
            | Partitioner.Consistent -> "consistent"
            | Partitioner.ConsistentRandom -> "consistent_random")

    // consumers
    let enableAutoCommit = mkKey "enable.auto.commit" id<bool>
    let enableAutoOffsetStore = mkKey "enable.auto.offset.store" id<bool>
    let autoCommitInterval = mkKey "auto.commit.interval.ms" ms
    let groupId = mkKey "group.id" id<string>
    let fetchMaxBytes = mkKey "fetch.message.max.bytes" id<int>
    let fetchMinBytes = mkKey "fetch.min.bytes" id<int>
    let fetchMaxWait = mkKey "fetch.wait.max.ms" ms
    let checkCrc = mkKey "check.crcs" id<bool>
    let heartbeatInterval = mkKey "heartbeat.interval.ms" ms
    let sessionTimeout = mkKey "session.timeout.ms" ms
    let topicConfig = mkKey "default.topic.config" id : ConfigKey<seq<KafkaConfiguration>>
    let autoOffsetReset = 
        mkKey "auto.offset.reset"
            (function
            | AutoOffsetReset.Earliest -> "earliest"
            | AutoOffsetReset.Latest -> "lastest"
            | AutoOffsetReset.None -> "none")

// NB we deliberately wrap all types exposed by Confluent.Kafka
// to avoid needing to reference librdkafka everywehere
[<Struct>]
type KafkaMessage internal (message : Message) =
    member internal __.UnderlyingMessage = message
    member __.Topic = message.Topic
    member __.Partition = message.Partition
    member __.Offset = message.Offset.Value
    member __.Key = encoding.GetString message.Key
    member __.Value = encoding.GetString message.Value

type KafkaProducer private (producer : Producer, topic : string) =

    let d1 = producer.OnLog.Subscribe (fun m -> logger.info "producer_info|%s level=%d name=%s facility=%s" m.Message m.Level m.Name m.Facility)
    let d2 = producer.OnError.Subscribe (fun e -> logger.error "producer_error|%s code=%O isBrokerError=%b" e.Reason e.Code e.IsBrokerError)

    let tryWrap (msg : Message) =
        if msg.Error.HasError then
            let errorMsg = sprintf "Error writing message topic=%s code=%O reason=%s" topic msg.Error.Code msg.Error.Reason
            Choice2Of2(Exception errorMsg)
        else
            Choice1Of2(KafkaMessage msg)

    /// https://github.com/edenhill/librdkafka/wiki/Statistics
    [<CLIEvent>]
    member __.OnStatistics = producer.OnStatistics
    member __.Topic = topic
    member __.Produce(key : string, value : string) = async {
        let keyBytes = match key with null -> [||] | _ -> encoding.GetBytes key
        let valueBytes = encoding.GetBytes value
        let! msg = 
            producer.ProduceAsync(topic, keyBytes, valueBytes) 
            |> Async.AwaitTaskCorrect

        match tryWrap msg with
        | Choice1Of2 msg -> return msg
        | Choice2Of2 e -> return raise e
    }

    /// <summary>
    ///   Produces a batch of supplied key/value messages. Results are returned in order of writing.  
    /// </summary>
    /// <param name="keyValueBatch"></param>
    /// <param name="marshalDeliveryReportData">Enable marshalling of data in returned messages. Defaults to false.</param>
    member __.ProduceBatch(keyValueBatch : seq<string * string>, ?marshalDeliveryReportData : bool) = async {
        match Seq.toArray keyValueBatch with
        | [||] -> return [||]
        | keyValueBatch ->

        let! ct = Async.CancellationToken

        let tcs = new TaskCompletionSource<KafkaMessage[]>()
        let numMessages = keyValueBatch.Length
        let results = Array.zeroCreate<KafkaMessage> numMessages
        let numCompleted = ref 0

        use _ = ct.Register(fun _ -> tcs.TrySetCanceled() |> ignore)

        let handler =
            { new IDeliveryHandler with
                member __.MarshalData = defaultArg marshalDeliveryReportData false
                member __.HandleDeliveryReport m =
                    match tryWrap m with
                    | Choice2Of2 e ->
                        tcs.TrySetException e |> ignore

                    | Choice1Of2 m ->
                        let i = Interlocked.Increment numCompleted
                        results.[i - 1] <- m
                        if i = numMessages then tcs.TrySetResult results |> ignore }


        do for key,value in keyValueBatch do
            let keyBytes = encoding.GetBytes key
            let valueBytes = encoding.GetBytes value
            producer.ProduceAsync(topic, 
                        keyBytes, 0, keyBytes.Length, 
                        valueBytes, 0, valueBytes.Length, 
                        blockIfQueueFull = true,
                        deliveryHandler = handler)


        return! Async.AwaitTaskCorrect tcs.Task
    }

    interface IDisposable with member __.Dispose() = for d in [d1;d2;producer:>_] do d.Dispose()

    /// <summary>
    ///     Creates a Kafka producer instance with supplied configuration
    /// </summary>
    /// <param name="broker"></param>
    /// <param name="topic"></param>
    /// <param name="compression">Message compression. Defaults to 'none'.</param>
    /// <param name="retries">Number of retries. Defaults to 60.</param>
    /// <param name="retryBackoff">Backoff interval. Defaults to 1 second.</param>
    /// <param name="statisticsInterval">Statistics Interval. Defaults to no stats.</param>
    /// <param name="linger">Linger time. Defaults to 200 milliseconds.</param>
    /// <param name="acks">Acks setting. Defaults to 'One'.</param>
    /// <param name="partitioner">Partitioner setting. Defaults to 'consistent_random'.</param>
    /// <param name="kafunkCompatibility">A magical flag that attempts to provide compatibility for Kafunk consumers. Defalts to true.</param>
    /// <param name="miscConfig">Misc configuration parameter to be passed to the underlying CK producer.</param>
    static member Create(clientId : string, broker : Uri, topic : string, 
                            ?compression, ?retries, ?retryBackoff, ?statisticsInterval,
                            ?linger, ?acks, ?partitioner, ?kafunkCompatibility : bool, ?miscConfig) =
        if String.IsNullOrEmpty topic then nullArg "topic"

        let linger = defaultArg linger (TimeSpan.FromMilliseconds 200.)
        let compression = defaultArg compression Compression.None
        let retries = defaultArg retries 60
        let retryBackoff = defaultArg retryBackoff (TimeSpan.FromSeconds 1.)
        let partitioner = defaultArg partitioner Partitioner.ConsistentRandom
        let acks = defaultArg acks Acks.One
        let miscConfig = defaultArg miscConfig [||]
        let kafunkCompatibility = defaultArg kafunkCompatibility true

        let config =
            [|
                yield Config.clientId ==> clientId
                yield Config.broker ==> broker

                match statisticsInterval with Some t -> yield Config.statisticsInterval ==> t | None -> ()
                yield Config.linger ==> linger
                yield Config.compression ==> compression
                yield Config.partitioner ==> partitioner
                yield Config.retries ==> retries
                yield Config.retryBackoff ==> retryBackoff
                yield Config.acks ==> acks

                if kafunkCompatibility then
                    yield Config.apiVersionRequest ==> false
                    yield Config.brokerVersionFallback ==> "0.9.0"

                // https://github.com/confluentinc/confluent-kafka-dotnet/issues/124#issuecomment-289727017
                yield Config.logConnectionClose ==> false

                yield! miscConfig
            |]

        logger.info "Starting Kafka Producer on topic=%s broker=%O compression=%O acks=%O" topic broker compression acks
        let producer = new Producer(config, manualPoll = false, disableDeliveryReports = false)
        new KafkaProducer(producer, topic)

type KafkaConsumerConfig =
    {
        /// Client identifier; should typically be superhero name
        clientId : string
        broker : Uri
        topics : string list
        /// Consumer group identifier
        groupId : string

        autoOffsetReset : AutoOffsetReset
        fetchMinBytes : int
        fetchMaxBytes : int
        retryBackoff : TimeSpan
        retries : int
        statisticsInterval : TimeSpan option

        miscConfig : KafkaConfiguration list

        /// Poll timeout used by the Confluent.Kafka consumer
        pollTimeout : TimeSpan
        /// Maximum number of messages to group per batch on consumer callbacks
        maxBatchSize : int
        /// Message batch linger time
        maxBatchDelay : TimeSpan
        /// Maximum total size of consumed messages in-memory before polling is throttled.
        maxInFlightBytes : int64
        /// Consumed offsets commit interval
        offsetCommitInterval : TimeSpan
    }
with
    /// <summary>
    ///     Creates a Kafka consumer configuration object.
    /// </summary>
    /// <param name="clientId">Should typically be superhero name.</param>
    /// <param name="broker"></param>
    /// <param name="topics"></param>
    /// <param name="groupId">Consumer group id.</param>
    /// <param name="autoOffsetReset">Auto offset reset. Defaults to Earliest.</param>
    /// <param name="fetchMaxBytes">Fetch max bytes. Defaults to 100KB.</param>
    /// <param name="fetchMinBytes">Fetch min bytes. Defaults to 10B.</param>
    /// <param name="retries">Max retries. Defaults to 60.</param>
    /// <param name="retryBackoff">Retry backoff interval. Defaults to 1 second.</param>
    /// <param name="pollTimeout">Poll timeout used by the CK consumer. Defaults to 200ms.</param>
    /// <param name="maxBatchSize">Max number of messages to use in batched consumers. Defaults to 1000.</param>
    /// <param name="maxBatchDelay">Max batch linger time. Defaults to 1 second.</param>
    /// <param name="offsetCommitInterval">Offset commit interval. Defaults to 2 seconds.</param>
    /// <param name="maxInFlightBytes">Maximum total size of consumed messages in-memory before polling is throttled. Defaults to 64MiB.</param>
    /// <param name="miscConfiguration">Misc configuration parameters to pass to the underlying CK consumer.</param>
    static member Create(clientId, broker, topics, groupId,
                            ?autoOffsetReset, ?fetchMaxBytes, ?fetchMinBytes,
                            ?retries, ?retryBackoff, ?statisticsInterval,
                            ?pollTimeout, ?maxBatchSize, ?maxBatchDelay, ?offsetCommitInterval, ?maxInFlightBytes,
                            ?miscConfiguration) =
        {
            clientId = clientId
            broker = broker
            topics = 
                match Seq.toList topics with
                | [] -> invalidArg "topics" "must be non-empty collection"
                | ts -> ts

            groupId = groupId

            miscConfig = match miscConfiguration with None -> [] | Some c -> Seq.toList c

            autoOffsetReset = defaultArg autoOffsetReset AutoOffsetReset.Earliest
            fetchMaxBytes = defaultArg fetchMaxBytes 100000
            fetchMinBytes = defaultArg fetchMinBytes 10 // TODO check if sane default
            retries = defaultArg retries 60
            retryBackoff = defaultArg retryBackoff (TimeSpan.FromSeconds 1.)
            statisticsInterval = statisticsInterval

            pollTimeout = defaultArg pollTimeout (TimeSpan.FromMilliseconds 200.)
            maxBatchSize = defaultArg maxBatchSize 1000
            maxBatchDelay = defaultArg maxBatchDelay (TimeSpan.FromMilliseconds 500.)
            maxInFlightBytes = defaultArg maxInFlightBytes (64L * 1024L * 1024L)
            offsetCommitInterval = defaultArg offsetCommitInterval (TimeSpan.FromSeconds 10.)
        }

module private ConsumerImpl =

    type KafkaConsumerConfig with
        member c.ToKeyValuePairs() =
            [|
                yield Config.clientId ==> c.clientId
                yield Config.broker ==> c.broker
                yield Config.groupId ==> c.groupId

                yield Config.fetchMaxBytes ==> c.fetchMaxBytes
                yield Config.fetchMinBytes ==> c.fetchMinBytes
                yield Config.retries ==> c.retries
                yield Config.retryBackoff ==> c.retryBackoff
                match c.statisticsInterval with None -> () | Some t -> yield Config.statisticsInterval ==> t

                yield Config.enableAutoCommit ==> true
                yield Config.enableAutoOffsetStore ==> false
                yield Config.autoCommitInterval ==> c.offsetCommitInterval

                yield Config.topicConfig ==> 
                    [
                        Config.autoOffsetReset ==> AutoOffsetReset.Earliest
                    ]

                // https://github.com/confluentinc/confluent-kafka-dotnet/issues/124#issuecomment-289727017
                yield Config.logConnectionClose ==> false

                yield! c.miscConfig
            |]


    /// used for calculating approximate message size in bytes
    let getMessageSize (message : Message) =
        let inline len (x:byte[]) = match x with null -> 0L | _ -> x.LongLength
        16L + len message.Key + len message.Value

    let getBatchOffset (batch : KafkaMessage[]) =
        let maxOffset = batch |> Array.maxBy (fun m -> m.Offset)
        maxOffset.UnderlyingMessage.TopicPartitionOffset

    let mkMessage (msg : Message) =
        if msg.Error.HasError then
            failwithf "error consuming message topic=%s partition=%d offset=%O code=%O reason=%s"
                            msg.Topic msg.Partition msg.Offset msg.Error.Code msg.Error.Reason

        KafkaMessage(msg)

    let mkBatchedMessage (msgs : IList<Message>) =
        msgs |> Seq.map mkMessage |> Seq.toArray

    type MessageSizeCounter(maxInFlightBytes : int64) =
        do if maxInFlightBytes < 1L then invalidArg "maxInFlightBytes" "must be positive value"
        let mutable count = 0L
        member __.Add(size : int64) = Interlocked.Add(&count, size) |> ignore
        member __.IsThresholdReached = count > maxInFlightBytes
        member __.Count = count
        member __.Max = maxInFlightBytes

    type Consumer with
        static member Create (config : KafkaConsumerConfig) =
            let kvps = config.ToKeyValuePairs()
            let consumer = new Consumer(kvps)
            let _ = consumer.OnPartitionsAssigned.Subscribe(fun m -> consumer.Assign m)
            let _ = consumer.OnPartitionsRevoked.Subscribe(fun m -> consumer.Unassign())
            consumer.Subscribe config.topics
            consumer

        member c.StoreOffset(tpo : TopicPartitionOffset) =
            c.StoreOffsets[| TopicPartitionOffset(tpo.Topic, tpo.Partition, Offset(tpo.Offset.Value + 1L)) |]
            |> ignore

        member c.RunPoll(pollTimeout : TimeSpan, sizeCounter : MessageSizeCounter) =
            let cts = new CancellationTokenSource()
            let poll() = 
                while not cts.IsCancellationRequested do 
                    while sizeCounter.IsThresholdReached do Thread.Sleep 50
                    c.Poll(pollTimeout)

            let _ = Async.StartAsTask(async { poll() })
            { new IDisposable with member __.Dispose() = cts.Cancel() }

        member c.OnBatchedPartitionMessages(maxBatchSize : int, maxDelay : TimeSpan) =
            c.OnMessage
             .GroupBy(fun m -> m.TopicPartition)
             .SelectMany(fun group -> group.Buffer(maxDelay, maxBatchSize))
             .Where(fun batch -> batch.Count > 0)

        member c.WithLogging() =
            let fmtError (e : Error) = if e.HasError then sprintf " reason=%s code=%O isBrokerError=%b" e.Reason e.Code e.IsBrokerError else ""
            let fmtTopicPartitions (topicPartitions : seq<TopicPartition>) =
                topicPartitions 
                |> Seq.groupBy (fun p -> p.Topic)
                |> Seq.map (fun (t,ps) -> sprintf "topic=%s|partitions=[%s]" t (ps |> Seq.map (fun p -> string p.Partition) |> String.concat "; "))

            let fmtTopicPartitionOffsets (topicPartitionOffsets : seq<TopicPartitionOffset>) =
                topicPartitionOffsets 
                |> Seq.groupBy (fun p -> p.Topic)
                |> Seq.map (fun (t,ps) -> sprintf "topic=%s|offsets=[%s]" t (ps |> Seq.map (fun p -> sprintf "%d@%O" p.Partition p.Offset) |> String.concat "; "))

            let fmtTopicPartitionOffsetErrors (topicPartitionOffsetErrors : seq<TopicPartitionOffsetError>) =
                topicPartitionOffsetErrors
                |> Seq.groupBy (fun p -> p.Topic)
                |> Seq.map (fun (t,ps) -> sprintf "topic=%s|offsets=[%s]" t (ps |> Seq.map (fun p -> sprintf "%d@%O%s" p.Partition p.Offset (fmtError p.Error)) |> String.concat "; "))
                    
            let d1 = c.OnLog.Subscribe (fun m -> logger.info "consumer_info|%s level=%d name=%s facility=%s" m.Message m.Level m.Name m.Facility)
            let d2 = c.OnError.Subscribe (fun e -> logger.error "consumer_error|%s" (fmtError e))
            let d3 = c.OnPartitionsAssigned.Subscribe (fun tps -> for fmt in fmtTopicPartitions tps do logger.info "consumer_partitions_assigned|%s" fmt)
            let d4 = c.OnPartitionsRevoked.Subscribe (fun tps -> for fmt in fmtTopicPartitions tps do logger.info "consumer_partitions_revoked|%s" fmt)
            let d5 = c.OnPartitionEOF.Buffer(TimeSpan.FromSeconds 5., 20).Subscribe(fun tpo -> for fmt in fmtTopicPartitionOffsets tpo do logger.verbose "consumer_partition_eof|%s" fmt)
            let d6 = c.OnOffsetsCommitted.Subscribe (fun cos -> for fmt in fmtTopicPartitionOffsetErrors cos.Offsets do logger.info "consumer_committed_offsets|%s%s" fmt (fmtError cos.Error))
            { new IDisposable with member __.Dispose() = for d in [d1;d2;d3;d4;d5;d6] do d.Dispose() }

    let mkMessageConsumer (config : KafkaConsumerConfig) (ct : CancellationToken) (consumer : Consumer) (handler : KafkaMessage -> unit) = async {
        let tcs = new TaskCompletionSource<unit>()
        use _ = ct.Register(fun _ -> tcs.TrySetResult () |> ignore)

        use _ = consumer.WithLogging()

        let counter = new MessageSizeCounter(config.maxInFlightBytes)
        let runHandler (msg : Message) =
            try 
                let size = getMessageSize msg
                counter.Add size
                do handler(mkMessage msg)
                consumer.StoreOffset msg.TopicPartitionOffset
                counter.Add -size
            with e -> 
                tcs.TrySetException e |> ignore

        use _ = consumer.OnMessage.Subscribe runHandler
        use _ = consumer.RunPoll(config.pollTimeout, counter)

        // await for handler faults or external cancellation
        do! Async.AwaitTaskCorrect tcs.Task
    }

    let mkBatchedMessageConsumer (config : KafkaConsumerConfig) (ct : CancellationToken) (consumer : Consumer) (handler : KafkaMessage[] -> Async<unit>) = async {
        let tcs = new TaskCompletionSource<unit>()
        use cts = CancellationTokenSource.CreateLinkedTokenSource(ct)
        use _ = ct.Register(fun _ -> tcs.TrySetResult () |> ignore)

        use _ = consumer.WithLogging()
        
        let counter = new MessageSizeCounter(config.maxInFlightBytes)
        let runHandler batch =
            let wrapper = async {
                try
                    let size = batch |> Seq.sumBy getMessageSize
                    counter.Add size
                    let batch = mkBatchedMessage batch
                    do! handler batch 
                    consumer.StoreOffset(getBatchOffset batch)
                    counter.Add -size
                with e -> 
                    tcs.TrySetException e |> ignore
            }

            Async.StartImmediate(wrapper, cts.Token)

        use _ = consumer.OnBatchedPartitionMessages(config.maxBatchSize, config.maxBatchDelay).Subscribe runHandler
        use _ = consumer.RunPoll(config.pollTimeout, counter)

        // await for handler faults or external cancellation
        do! Async.AwaitTaskCorrect tcs.Task
    }

open ConsumerImpl

type KafkaConsumer private (config : KafkaConsumerConfig, isBatchConsumer : bool, consumeTask : CancellationToken -> Consumer -> Async<unit>) =
    let cts = new CancellationTokenSource()
    let consumer = Consumer.Create config
    let task = Async.StartAsTask(consumeTask cts.Token consumer)

    /// https://github.com/edenhill/librdkafka/wiki/Statistics
    [<CLIEvent>]
    member __.OnStatistics = consumer.OnStatistics
    member __.Config = config
    member __.IsBatchConsumer = isBatchConsumer
    member __.Status = task.Status
    member __.AwaitConsumer() = Async.AwaitTaskCorrect task
    member __.Stop() = cts.Cancel() ; task.Result ; consumer.Dispose()

    interface IDisposable with member __.Dispose() = __.Stop()

    /// Starts a kafka consumer with provided configuration and message handler
    static member Start (config : KafkaConsumerConfig) (handler : KafkaMessage -> unit) =
        let mkConsumer c ct = ConsumerImpl.mkMessageConsumer config c ct handler
        new KafkaConsumer(config, false, mkConsumer)

    /// Starts a kafka consumer with provider configuration and batch message handler.
    /// Message batches are grouped by Kafka partition
    static member StartBatched (config : KafkaConsumerConfig) (handler : KafkaMessage[] -> Async<unit>) =
        if List.isEmpty config.topics then invalidArg "config" "must specify at least one topic"
        logger.info "Starting Kafka consumer on topics=%A groupId=%s broker=%O autoOffsetReset=%O maxBatchSize=%O" config.topics config.groupId config.broker config.autoOffsetReset config.maxBatchSize
        let mkConsumer c ct = ConsumerImpl.mkBatchedMessageConsumer config c ct handler
        new KafkaConsumer(config, true, mkConsumer)