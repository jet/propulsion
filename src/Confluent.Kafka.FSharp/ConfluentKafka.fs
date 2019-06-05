namespace Confluent.Kafka

open System
open System.Collections.Concurrent
open System.Text
open System.Threading
open System.Threading.Tasks
open Confluent.Kafka
open FSharp.Control

[<AutoOpen>]
module internal Prelude =

  let inline isNull (x:obj) = obj.ReferenceEquals (x,null)

  let inline arrayToSegment (xs:'a[]) : ArraySegment<'a> =
    ArraySegment (xs, 0, xs.Length)

  let inline stringToSegment (s:string) : ArraySegment<byte> =
    if isNull s then ArraySegment()
    else Encoding.UTF8.GetBytes s |> arrayToSegment

  module Map =

    let mergeChoice (f:'a -> Choice<'b * 'c, 'b, 'c> -> 'd) (map1:Map<'a, 'b>) (map2:Map<'a, 'c>) : Map<'a, 'd> =
      Set.union (map1 |> Seq.map (fun k -> k.Key) |> set) (map2 |> Seq.map (fun k -> k.Key) |> set)
      |> Seq.map (fun k ->
        match Map.tryFind k map1, Map.tryFind k map2 with
        | Some b, Some c -> k, f k (Choice1Of3 (b,c))
        | Some b, None   -> k, f k (Choice2Of3 b)
        | None,   Some c -> k, f k (Choice3Of3 c)
        | None,   None   -> failwith "invalid state")
      |> Map.ofSeq

  type Async with
    static member AwaitTaskCancellationAsError (t:Task<'a>) : Async<'a> =
        Async.FromContinuations <| fun (ok,err,_) ->
          t.ContinueWith ((fun (t:Task<'a>) ->
            if t.IsFaulted then err t.Exception
            elif t.IsCanceled then err (OperationCanceledException("Task wrapped with Async has been cancelled."))
            elif t.IsCompleted then ok t.Result
            else failwith "unreachable")) |> ignore

/// Operations on messages.
[<CompilationRepresentationAttribute(CompilationRepresentationFlags.ModuleSuffix)>]
module internal Message =

  let internal errorMessage (m:Message) (e:Error) =
    sprintf "message_error|topic=%s partition=%i offset=%i code=%O reason=%s" m.Topic m.Partition m.Offset.Value e.Code e.Reason

  let internal errorException (m:Message) (e:Error) =
    exn(errorMessage m e)

  /// Throws an exception if the message has an error.
  let throwIfError (m:Message) =
    if m.Error.HasError then
      raise (errorException m m.Error)

  /// Returns the message key as a UTF8 string.
  let keyString (m:Message) = Encoding.UTF8.GetString m.Key

  /// Returns the message value as a UTF8 string.
  let valueString (m:Message) = Encoding.UTF8.GetString m.Value

/// Kafka client configuration.
module Config =
    let private enumToString e =
      match Microsoft.FSharp.Reflection.FSharpValue.GetUnionFields (e, e.GetType()) with
      | case, _ -> case.Name.ToLower()

    module DebugFlags =
      type DebugFlag =
        | Generic
        | Broker
        | Topic
        | Metadata
        | Feature
        | Queue
        | Msg
        | Protocol
        | Cgrp
        | Security
        | Fetch
        | Interceptor
        | Plugin
        | Consumer
        | All

    //
    // Non-typed
    //
    let remove = Map.remove
    let config<'v> k (v: 'v) config = Map.add k (box v) config

    let configMap<'v> name k (v: 'v) (config: Map<string,obj>): Map<string,obj> =
        match config.TryFind name with
            | Some mapObj ->
                match mapObj with
                    | :? Map<string,obj> as map' ->
                        Map.add name (box (Map.add k (box v) map')) config
                    | _ -> failwithf "Expected config '%s' to be map but got %s" name (mapObj.GetType().Name)
            | None ->
                Map.add name (box (Map([(k,box v)]))) config

    let configs configs map = List.fold (fun m (k, v) -> config k v m) map configs
    let debug (debugFlag: seq<DebugFlags.DebugFlag>) configs =
      let str =
        debugFlag
        |> Seq.map enumToString
        |> String.concat ","
      config "debug" str configs

    //
    // Strong typed
    //
    let clientId = config<string> "client.id"
    let bootstrapServers = config<string> "bootstrap.servers"

    module Consumer =
        let enableAutoCommit = config<bool> "enable.auto.commit"
        let groupId = config<string> "group.id"
        let fetchMaxBytes = config<int> "fetch.message.max.bytes"
        let fetchMinBytes = config<int> "fetch.min.bytes"
        let fetchMaxWaitMs = config<int> "fetch.wait.max.ms"
        let checkCrc = config<bool> "check.crcs"
        let heartbeatInterval = config<int> "heartbeat.interval.ms"
        let sessionTimeout = config<int> "session.timeout.ms"
        let commitIntervalMs = config<int> "auto.commit.interval.ms"

        module Topic =
            type AutoOffsetReset =
                | Beginning
                | End
                | Error

            let autoOffsetReset (reset: AutoOffsetReset) = configMap<string> "default.topic.config" "auto.offset.reset" (enumToString reset)

        let safe =
            Map.empty
            |> Topic.autoOffsetReset Topic.Error
            // Wrapper will store offsets, do not let librdkafka do it:
            // https://github.com/confluentinc/confluent-kafka-dotnet/issues/527
            |> config "enable.auto.offset.store" false

    module Producer =
        type Compression =
            | None
            | Gzip
            | Snappy
            | Lz4

        type RequiredAcks =
            | All
            | None
            | Some of count: int

        type Partitioner =
            | Random
            | Consistent
            | Consistent_Random
            | Murmur2
            | Murmur2_Random

        let maxInFlight = config "max.in.flight.requests.per.connection"
        let lingerMs = config<int> "linger.ms"
        let requiredAcks (acks: RequiredAcks) =
            match acks with
                | All -> -1
                | None -> 0
                | Some count -> count
            |> config "request.required.acks"
        let batchNumMessages = config<int> "batch.num.messages"
        let compression (compression: Compression) = config<string> "compression.codec" (enumToString compression)
        let requestTimeoutMs = config<int> "request.timeout.ms"
        let partitioner (algorithm: Partitioner) = config<string> "partitioner" (enumToString algorithm)
        let messageSendMaxRetries = config<int> "message.send.max.retries"
        let retryBackoffMs = config<int> "retry.backoff.ms"

        /// Safe producer configuration.
        /// - max.inflight = 1
        /// - acks = all
        let safe =
            Map.empty
            |> maxInFlight 1
            |> requiredAcks All
            |> config "produce.offset.report" true

/// Operations on producers.
[<CompilationRepresentationAttribute(CompilationRepresentationFlags.ModuleSuffix)>]
module Producer =

  /// Creates a producer.
  let create (config: Map<string,obj>) =
    let producer = new Producer(config, false, false)
    producer

  let onLog logger (producer: Producer) =
    producer.OnLog
    |> Event.add logger
    producer

  /// Creates an IDeliveryHandler and a Task which completes when the handler receives the specified number of messages.
  /// @expectedMessageCount - the number of messages to be received to complete the Task.
  /// @throwOnError - if true, then sets the Task to the faulted state upon receipt of a Message with an Error.
  let private batchDeliveryHandler (expectedMessageCount:int) (throwOnError:bool) : IDeliveryHandler * Task<Message[]> =
    let tcs = TaskCompletionSource<Message[]>()
    let rs = Array.zeroCreate expectedMessageCount
    let mutable i = -1
    let handler =
      { new IDeliveryHandler with
          member __.HandleDeliveryReport m =
            if throwOnError && m.Error.HasError then
              tcs.TrySetException ((Message.errorException m m.Error)) |> ignore
            else
              let i' = Interlocked.Increment &i
              rs.[i'] <- m
              if i' = expectedMessageCount - 1 then
                tcs.SetResult rs
          member __.MarshalData = false }
    handler,tcs.Task

  let private produceInternal (p:Producer) (topic:string) (throwOnError:bool) (k:ArraySegment<byte>, v:ArraySegment<byte>) : Async<Message> = async {
    let! m = p.ProduceAsync (topic, k.Array, k.Offset, k.Count, v.Array, v.Offset, v.Count, true) |> Async.AwaitTask
    if throwOnError then Message.throwIfError m
    return m }

  let private produceBatchedInternal (p:Producer) (topic:string) (throwOnError:bool) (ms:(ArraySegment<byte> * ArraySegment<byte>)[]) : Async<Message[]> = async {
    if ms.Length <> 0 then
      let handler,result = batchDeliveryHandler ms.Length throwOnError
      for i = 0 to ms.Length - 1 do
        let k,v = ms.[i]
        p.ProduceAsync(topic, k.Array, k.Offset, k.Count, v.Array, v.Offset, v.Count, true, handler)
      return! result |> Async.AwaitTask 
    else
      return Array.empty    
  }

  let produce (p:Producer) (topic:string) (k:ArraySegment<byte>, v:ArraySegment<byte>) : Async<Message> =
    produceInternal p topic true (k,v)

  let produceBytes (p:Producer) (topic:string) (k:byte[], v:byte[]) : Async<Message> =
    produceInternal p topic true (arrayToSegment k, arrayToSegment v)

  let produceString (p:Producer) (topic:string) (k:string, v:string) : Async<Message> =
    produce p topic (stringToSegment k, stringToSegment v)

  /// Produces a batch of messages sequentially.
  /// Short-circuits on message error.
  let produceBatched (p:Producer) (topic:string) (ms:(ArraySegment<byte> * ArraySegment<byte>)[]) : Async<Message[]> =
    produceBatchedInternal p topic true ms

  /// Produces a batch of messages sequentially.
  /// Short-circuits on message error.
  let produceBatchedBytes (p:Producer) (topic:string) (ms:(byte[] * byte[])[]) : Async<Message[]> =
    let ms = ms |> Array.map (fun (k,v) -> arrayToSegment k, arrayToSegment v)
    produceBatched p topic ms

  /// Produces a batch of messages sequentially.
  /// Short-circuits on message error.
  let produceBatchedString (p:Producer) (topic:string) (ms:(string * string)[]) : Async<Message[]> =
    let ms = ms |> Array.map (fun (k,v) -> stringToSegment k, stringToSegment v)
    produceBatched p topic ms

/// A set of messages from a single partition.
type ConsumerMessageSet = {
  topic : string
  partition : int
  messages : Message[]
} with
  static member lastOffset (ms:ConsumerMessageSet) =
    if ms.messages.Length = 0 then -1L
    else ms.messages.[ms.messages.Length - 1].Offset.Value

  static member firstOffset (ms:ConsumerMessageSet) =
    if ms.messages.Length = 0 then -1L
    else ms.messages.[0].Offset.Value

/// Operations on consumers.
[<CompilationRepresentationAttribute(CompilationRepresentationFlags.ModuleSuffix)>]
module Consumer =

  /// Creates a consumer.
  /// - OnPartitionsAssigned -> Assign
  /// - OnPartitionsRevoked -> Unassign
  let create (config: Map<string,obj>) =
    let consumer = new Consumer(config)
    consumer.OnPartitionsAssigned |> Event.add (fun m -> consumer.Assign m)
    consumer.OnPartitionsRevoked |> Event.add (fun _ -> consumer.Unassign ())
    consumer

  /// Consumes messages, buffers them by time and batchSize and calls the specified handler.
  /// The handler is called sequentially within a partition and in parallel across partitions.
  /// @pollTimeoutMs - the consumer poll timeout.
  /// @batchLingerMs - the time to wait to form a per-partition batch; when time limit is reached the handler is called.
  /// @batchSize - the per-partition batch size limit; when the limit is reached, the handler is called.
  let consume
    (c:Consumer)
    (pollTimeoutMs:int)
    (batchLingerMs:int)
    (batchSize:int)
    (handle:ConsumerMessageSet -> Async<unit>) : Async<unit> = async {
    let! externalCancel = Async.CancellationToken
    use localCancel = CancellationTokenSource.CreateLinkedTokenSource externalCancel
    let tcs = TaskCompletionSource<unit>()
    let queues = ConcurrentDictionary<string * int, BlockingCollection<Message>>()
    let close () =
      for kvp in queues do
        try kvp.Value.CompleteAdding() with _ -> ()

    // If task is finished (due to exception), cancel the token
    tcs.Task.ContinueWith(fun (_:Task<unit>) -> localCancel.Cancel()) |> ignore
    // If cancellation token triggers, cancel the task
    use _ = localCancel.Token.Register(fun () ->
        c.Unsubscribe()
        close()
        tcs.TrySetCanceled() |> ignore
    )

    let consumePartition (t:string, p:int) (queue:BlockingCollection<Message>) = async {
      try
        use queue = queue
        do!
          queue.GetConsumingEnumerable ()
          |> AsyncSeq.ofSeq
          |> AsyncSeq.bufferByCountAndTime batchSize batchLingerMs
          |> AsyncSeq.iterAsync (fun ms -> async {
            let! res = handle { ConsumerMessageSet.topic = t ; partition = p ; messages = ms }
            // Update driver's internal position pointer (will be flushed periodically in accordance to 
            // "auto.commit.interval.ms"
            let maxOffsetMsg = ms |> Seq.maxBy (fun msg -> msg.Offset.Value)
            let position =
              new TopicPartitionOffset(
                maxOffsetMsg.TopicPartition,
                new Offset(maxOffsetMsg.Offset.Value + 1L))
            c.StoreOffsets([|position|]) |> ignore
            return res
          })
      with ex ->
        tcs.TrySetException ex |> ignore }
    let poll = async {
      try
        let buf = ResizeArray<_>()
        let mutable m = Unchecked.defaultof<_>
        while (not localCancel.Token.IsCancellationRequested) do
          if c.Consume(&m, pollTimeoutMs) then
            Message.throwIfError m
            buf.Add m
            // drain in-memory buffer
            while (buf.Count <= batchSize) && (not localCancel.Token.IsCancellationRequested) && c.Consume(&m, 0) do
              Message.throwIfError m
              buf.Add m
            buf
            |> Seq.groupBy (fun m -> m.Topic,m.Partition)
            |> Seq.toArray
            |> Array.Parallel.iter (fun (tp,ms) ->
              let mutable queue = Unchecked.defaultof<_>
              if not (queues.TryGetValue (tp,&queue)) then
                queue <- new BlockingCollection<_>(batchSize)
                queues.TryAdd (tp,queue) |> ignore
                Async.Start (consumePartition tp queue, localCancel.Token)
              for m in ms do
                queue.Add m)
            buf.Clear ()
      with ex ->
        tcs.TrySetException ex |> ignore }

    Async.Start (poll, localCancel.Token)
    try return! tcs.Task |> Async.AwaitTaskCancellationAsError finally close () }

  let onLog logger (consumer: Consumer) =
    consumer.OnLog
    |> Event.add logger
    consumer

  // -------------------------------------------------------------------------------------------------
  // AsyncSeq adapters

  /// Represents repeated calls to Consumer.Consume with the specified timeout as an AsyncSeq.
  /// If a message is marked as an error, raises an exception.
  let stream (c:Consumer) (timeoutMs:int) : AsyncSeq<Message> = asyncSeq {
    let mutable m = Unchecked.defaultof<_>
    while true do
      if c.Consume(&m, timeoutMs) then
        Message.throwIfError m
        yield m
        let position = new TopicPartitionOffset(m.TopicPartition, new Offset(m.Offset.Value + 1L))
        c.StoreOffsets([|position|]) |> ignore
  }

  /// Represents repeated calls to Consumer.Consume with the specified timeout as an AsyncSeq.
  /// Buffers messages into buffers bounded by time and count.
  let streamBuffered (c:Consumer) (timeoutMs:int) (batchSize:int) : AsyncSeq<Message[]> = asyncSeq {
    let mutable m = Unchecked.defaultof<_>
    let mutable last = DateTime.UtcNow
    let buf = ResizeArray<_>()
    while true do
      let now = DateTime.UtcNow
      let dt = int (now - last).TotalMilliseconds
      let timeoutMs = max (timeoutMs - dt) 0
      if timeoutMs > 0 && c.Consume(&m, timeoutMs) then
        last <- now
        Message.throwIfError m
        buf.Add m
        if buf.Count >= batchSize then
          yield buf.ToArray()
          buf.Clear ()
      else
        last <- now
        if buf.Count > 0 then
          yield buf.ToArray()
          buf.Clear () }

  /// Query low and high watermaks from broker
  let offsetRange (host: string) (topic:string) (partitions:int seq) : Async<Map<int, int64 * int64>> = async {
    let config =
      Config.Consumer.safe
      |> Config.bootstrapServers host
      |> Config.Consumer.groupId "offset-fetcher"
      |> Config.Consumer.enableAutoCommit false
    use consumer = new Consumer(config)

    // Get list of all partitions of given topic
    let partitions =
      if partitions |> Seq.isEmpty then
        (
          let topicSeq = 
            consumer.GetMetadata(true, TimeSpan.FromSeconds(20.0)).Topics            
            |> Seq.filter(fun t -> t.Topic = topic)
          if topicSeq |> Seq.isEmpty then failwithf "Metadata fetch error | Topic:%O does not exist" topic
          else topicSeq |> Seq.head
        ).Partitions
        |> Seq.map(fun p -> p.PartitionId)
      else
        partitions

    return
      partitions
      |> Seq.map(fun p ->
        let tp = new TopicPartition(topic, p)
        let watermark = consumer.QueryWatermarkOffsets(tp)
        (p, (watermark.Low.Value, watermark.High.Value))
      )
      |> Map.ofSeq
  }

module Legacy =
  // Implement ProducerMessage and ProducerResult, ConsumerMessageSet classes and API to be more backward compatible with kafunk.
  module Binary =
    let empty = ArraySegment<byte>()
    let inline ofArray (bytes : byte[]) : ArraySegment<byte> = ArraySegment(bytes, 0, bytes.Length)

  //
  // Producer
  //
  type ProducerMessage =
    struct
      /// The message payload.
      val value : ArraySegment<byte>
      /// The optional message key.
      val key : ArraySegment<byte>
      new (value:ArraySegment<byte>, key:ArraySegment<byte>) =
        { value = value ; key = key }
    end
      with

        /// Creates a producer message.
        static member ofBytes (value:ArraySegment<byte>, ?key) =
          ProducerMessage(value, defaultArg key Binary.empty)

        /// Creates a producer message.
        static member ofBytes (value:byte[], ?key) =
          let keyBuf = defaultArg (key |> Option.map Binary.ofArray) Binary.empty
          ProducerMessage(Binary.ofArray value, keyBuf)

        /// Creates a producer message.
        static member ofString (value:string, ?key:string) =
          let keyBuf = defaultArg (key |> Option.map (Encoding.UTF8.GetBytes >> Binary.ofArray)) Binary.empty
          ProducerMessage(Binary.ofArray (Encoding.UTF8.GetBytes value), keyBuf)

        /// Returns the size, in bytes, of the message.
        static member internal size (m:ProducerMessage) =
          m.key.Count + m.value.Count

  type ProducerResult =
    struct
      /// The partition to which the message was written.
      val partition : int32

      /// The offset of the first message produced to the partition.
      val offset : Offset

      /// The number of messages in the produced batch for the partition.
      val count : int

      new (p,o,c) = { partition = p ; offset = o ; count = c }
    end

  let produceBatched (p:Producer) (topic: string) (batch:ProducerMessage seq) : Async<ProducerResult[]> = async {
    let segmentToArray (segment: ArraySegment<byte>) =
      if segment.Count = segment.Array.Length then
        segment.Array
      else
        let arr = Array.zeroCreate(segment.Count)
        Array.Copy(segment.Array, segment.Offset, arr, 0, segment.Count)
        arr

    let ms =
      batch
      |> Seq.map(fun pm ->
        let key = segmentToArray pm.key
        let value = segmentToArray pm.value
        (key, value)
      )
      |> Array.ofSeq

    let! messages = Producer.produceBatchedBytes p topic ms

    let ret =
      messages
      |> Seq.groupBy(fun m -> (m.Partition, m.Offset))
      |> Seq.map(fun ((part, offs), vals) ->
        new ProducerResult(part, offs, Seq.length vals)
      )
      |> Array.ofSeq

    return ret
  }

  //
  // Consumer
  //
  type LegacyConfigDefaults = {
    pullTimeoutMs: int
    batchLingerMs: int
    batchSize: int
  }

  type Consumer with
    /// Some configuration settings did not exist in kafunk consumer, such as
    /// pullTimeoutMs, batchLingerMs and batchSize (in producer) but are required in Confluent API.
    /// To make transition easier, this property holds default values, which allow to mimic kafunk API.
    member this.LegacyConfigDefaults = {pullTimeoutMs = 300; batchLingerMs = 300; batchSize = 10000}

  let consume
    (c:Consumer)
    (handler: ConsumerMessageSet -> Async<unit>)
    : Async<unit> =
      Consumer.consume c (c.LegacyConfigDefaults.pullTimeoutMs) (c.LegacyConfigDefaults.batchLingerMs) (c.LegacyConfigDefaults.batchSize) handler

  /// Progress information for a consumer in a group.
  type ConsumerProgressInfo = {

    /// The consumer group id.
    group : string

    /// The name of the kafka topic.
    topic : string

    /// Progress info for each partition.
    partitions : ConsumerPartitionProgressInfo[]

    /// The total lag across all partitions.
    totalLag : int64

    /// The minimum lead across all partitions.
    minLead : int64

  }

  /// Progress information for a consumer in a group, for a specific topic-partition.
  and ConsumerPartitionProgressInfo = {

    /// The partition id within the topic.
    partition : int

    /// The consumer's current offset.
    consumerOffset : Offset

    /// The offset at the current start of the topic.
    earliestOffset : Offset

    /// The offset at the current end of the topic.
    highWatermarkOffset : Offset

    /// The distance between the high watermark offset and the consumer offset.
    lag : int64

    /// The distance between the consumer offset and the earliest offset.
    lead : int64

    /// The number of messages in the partition.
    messageCount : int64

  }

  /// Operations for providing consumer progress information.
  module ConsumerInfo =
    /// Returns consumer progress information.
    /// Passing empty set of partitions returns information for all partitions.
    /// Note that this does not join the group as a consumer instance
    let progress (consumer:Consumer) (topic:string) (ps:int[]) = async {
      let! topicPartitions =
        if ps |> Array.isEmpty then
          async {
            let meta =
              consumer.GetMetadata(false, TimeSpan.FromSeconds(40.0)).Topics
              |> Seq.find(fun t -> t.Topic = topic)

            return
              meta.Partitions
              |> Seq.map(fun p -> new TopicPartition(topic, p.PartitionId))
               }
        else
          async { return ps |> Seq.map(fun p -> new TopicPartition(topic,p)) }

      let committedOffsets =
        consumer.Committed(topicPartitions, TimeSpan.FromSeconds(20.))
        |> Seq.sortBy(fun e -> e.Partition)
        |> Seq.map(fun e -> e.Partition, e)
        |> Map.ofSeq

      let! watermarkOffsets =
        topicPartitions
          |> Seq.map(fun tp -> async {
            return tp.Partition, consumer.QueryWatermarkOffsets(tp)}
          )
          |> Async.Parallel

      let watermarkOffsets =
        watermarkOffsets
        |> Map.ofArray

      let partitions =
        (watermarkOffsets, committedOffsets)
        ||> Map.mergeChoice (fun p -> function
          | Choice1Of3 (hwo,cOffset) ->
            let e,l,o = hwo.Low.Value,hwo.High.Value,cOffset.Offset.Value
            // Consumer offset of (Invalid Offset -1001) indicates that no consumer offset is present.  In this case, we should calculate lag as the high water mark minus earliest offset
            let lag, lead =
              match o with
              | offset when offset = Offset.Invalid.Value -> l - e, 0L
              | _ -> l - o, o - e
            { partition = p ; consumerOffset = cOffset.Offset ; earliestOffset = hwo.Low ; highWatermarkOffset = hwo.High ; lag = lag ; lead = lead ; messageCount = l - e }
          | Choice2Of3 hwo ->
            // in the event there is no consumer offset present, lag should be calculated as high watermark minus earliest
            // this prevents artifically high lags for partitions with no consumer offsets
            let e,l = hwo.Low.Value,hwo.High.Value
            { partition = p ; consumerOffset = Offset.Invalid ; earliestOffset = hwo.Low ; highWatermarkOffset = hwo.High ; lag = l - e ; lead = 0L ; messageCount = l - e }
            //failwithf "unable to find consumer offset for topic=%s partition=%i" topic p
          | Choice3Of3 o ->
            let invalid = Offset.Invalid
            { partition = p ; consumerOffset = o.Offset ; earliestOffset = invalid ; highWatermarkOffset = invalid ; lag = invalid.Value ; lead = invalid.Value ; messageCount = -1L })
        |> Seq.map (fun kvp -> kvp.Value)
        |> Seq.toArray

      return
        {
        topic = topic ; group = consumer.Name ; partitions = partitions ;
        totalLag = partitions |> Seq.sumBy (fun p -> p.lag)
        minLead =
          if partitions.Length > 0 then
            partitions |> Seq.map (fun p -> p.lead) |> Seq.min
          else Offset.Invalid.Value }}