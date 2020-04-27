namespace Propulsion.Kafka.Integration

open Confluent.Kafka // required for shimming
open FsCodec
open FsKafka
open Newtonsoft.Json
open Propulsion.Kafka
open Serilog
open Swensen.Unquote
open System
open System.Collections.Concurrent
open System.Collections.Generic
open System.ComponentModel
open System.Threading
open System.Threading.Tasks
open Xunit

[<AutoOpen>]
[<EditorBrowsable(EditorBrowsableState.Never)>]
module Helpers =

    // Derived from https://github.com/damianh/CapturingLogOutputWithXunit2AndParallelTests
    // NB VS does not surface these atm, but other test runners / test reports do
    type TestOutputAdapter(testOutput : Xunit.Abstractions.ITestOutputHelper) =
        let formatter = Serilog.Formatting.Display.MessageTemplateTextFormatter("{Timestamp:yyyy-MM-dd HH:mm:ss.fff zzz} [{Level}] {Message}{NewLine}{Exception}", null);
        let writeSerilogEvent logEvent =
            use writer = new System.IO.StringWriter()
            formatter.Format(logEvent, writer);
            writer |> string |> testOutput.WriteLine
            writer |> string |> System.Diagnostics.Debug.Write
        interface Serilog.Core.ILogEventSink with member __.Emit logEvent = writeSerilogEvent logEvent

    let createLogger sink =
        LoggerConfiguration()
            .Destructure.FSharpTypes()
            .WriteTo.Sink(sink)
            .WriteTo.Seq("http://localhost:5341")
            .CreateLogger()

    let getTestBroker() =
        match Environment.GetEnvironmentVariable "TEST_KAFKA_BROKER" with
        | x when String.IsNullOrEmpty x -> invalidOp "missing environment variable 'TEST_KAFKA_BROKER'"
        | x -> x

    let newId () = let g = System.Guid.NewGuid() in g.ToString("N")

    type Async with
        static member ParallelThrottled degreeOfParallelism jobs = async {
            let s = new SemaphoreSlim(degreeOfParallelism)
            return!
                jobs
                |> Seq.map (fun j -> async {
                    let! ct = Async.CancellationToken
                    do! s.WaitAsync ct |> Async.AwaitTask
                    try return! j
                    finally s.Release() |> ignore })
                |> Async.Parallel
        }

    type ConsumerPipeline with
        member c.StopAfter(delay : TimeSpan) =
            Task.Delay(delay).ContinueWith(fun (_ : Task) -> c.Stop()) |> ignore

    type TestMeta = { key : string; value : string; partition : int; offset : int64 }
    let mapParallelConsumeResultToKeyValuePair (x : ConsumeResult<_, _>) : KeyValuePair<string, string> =
        let m = Bindings.mapMessage x
        KeyValuePair(m.Key, JsonConvert.SerializeObject { key = m.Key; value = m.Value; partition = Bindings.partitionId x; offset = let o = x.Offset in o.Value })
    type TestMessage = { producerId : int ; messageId : int }
    type ConsumedTestMessage = { consumerId : int ; meta : TestMeta; payload : TestMessage }
    type ConsumerCallback = ConsumerPipeline -> ConsumedTestMessage -> Async<unit>

    let runProducers log bootstrapServers (topic : string) (numProducers : int) (messagesPerProducer : int) = async {
        let runProducer (producerId : int) = async {
            let cfg = KafkaProducerConfig.Create("panther", bootstrapServers, Acks.Leader)
            use producer = BatchedProducer.CreateWithConfigOverrides(log, cfg, topic, maxInFlight=10000)

            let! results =
                [1 .. messagesPerProducer]
                |> Seq.map (fun msgId ->
                    let key = string msgId
                    let value = JsonConvert.SerializeObject { producerId = producerId ; messageId = msgId }
                    key, value)

                |> Seq.chunkBySize 100
                |> Seq.map producer.ProduceBatch
                |> Async.ParallelThrottled 7

            return Array.concat results
        }

        return! Async.Parallel [for i in 1 .. numProducers -> runProducer i]
    }

    type FactIfBroker() =
        inherit FactAttribute()
        override __.Skip = if null <> Environment.GetEnvironmentVariable "TEST_KAFKA_BROKER" then null else "Skipping as no TEST_KAFKA_BROKER supplied"
        override __.Timeout = 60 * 15 * 1000

    let runConsumersParallel log (config : KafkaConsumerConfig) (numConsumers : int) (timeout : TimeSpan option) (handler : ConsumerCallback) = async {
        let mkConsumer (consumerId : int) = async {

            // need to pass the consumer instance to the handler callback
            // do a bit of cyclic dependency fixups
            let consumerCell = ref None
            let rec getConsumer() =
                // avoid potential race conditions by polling
                match !consumerCell with
                | None -> Thread.SpinWait 20; getConsumer()
                | Some c -> c

            let deserialize consumerId (KeyValue (k,v)) : ConsumedTestMessage =
                let d = FsCodec.NewtonsoftJson.Serdes.Deserialize(v)
                let v = FsCodec.NewtonsoftJson.Serdes.Deserialize(d.value)
                { consumerId = consumerId; meta = d; payload = v }
            let handle item = handler (getConsumer()) (deserialize consumerId item)
            let consumer = ParallelConsumer.Start(log, config, 128, mapParallelConsumeResultToKeyValuePair, handle >> Async.Catch, statsInterval=TimeSpan.FromSeconds 10.)

            consumerCell := Some consumer

            timeout |> Option.iter consumer.StopAfter

            do! consumer.AwaitCompletion()
        }

        do! Async.Parallel [for i in 1 .. numConsumers -> mkConsumer i] |> Async.Ignore
    }

    let deserialize consumerId (e : FsCodec.ITimelineEvent<byte[]>) : ConsumedTestMessage =
        let d = FsCodec.NewtonsoftJson.Serdes.Deserialize(System.Text.Encoding.UTF8.GetString e.Data)
        { consumerId = consumerId; meta = d; payload = unbox e.Context }

    type Stats(log, statsInterval, stateInterval) =
        inherit Propulsion.Kafka.StreamsConsumerStats<unit>(log, statsInterval, stateInterval)

        override __.HandleOk res = ()
        override __.HandleExn exn = log.Information(exn, "Unhandled")

    let runConsumersBatch log (config : KafkaConsumerConfig) (numConsumers : int) (timeout : TimeSpan option) (handler : ConsumerCallback) = async {
        let mkConsumer (consumerId : int) = async {
            // need to pass the consumer instance to the handler callback
            // do a bit of cyclic dependency fixups
            let consumerCell = ref None
            let rec getConsumer() =
                // avoid potential race conditions by polling
                match !consumerCell with
                | None -> Thread.SpinWait 20; getConsumer()
                | Some c -> c

            // When offered, take whatever is pending
            let select = Array.ofSeq
            // when processing, declare all items processed each time we're invoked
            let handle (streams : Propulsion.Streams.Scheduling.DispatchItem<byte[]>[]) = async {
                let mutable c = 0
                for stream in streams do
                  for event in stream.span.events do
                      c <- c + 1
                      do! handler (getConsumer()) (deserialize consumerId event)
                (log : Serilog.ILogger).Information("BATCHED CONSUMER Handled {c} events in {l} streams", c, streams.Length )
                return [| for x in streams -> Choice1Of2 (x.span.events.[x.span.events.Length-1].Index+1L) |] |> Seq.ofArray }
            let stats = Stats(log, TimeSpan.FromSeconds 5.,TimeSpan.FromSeconds 5.)
            let messageIndexes = StreamNameSequenceGenerator()
            let consumer =
                BatchesConsumer.Start
                    (   log, config, mapParallelConsumeResultToKeyValuePair, messageIndexes.KeyValueToStreamEvent,
                        select, handle,
                        stats, TimeSpan.FromSeconds 10.)

            consumerCell := Some consumer

            timeout |> Option.defaultValue (TimeSpan.FromMinutes 15.) |> consumer.StopAfter

            do! consumer.AwaitCompletion()
        }

        do! Async.Parallel [for i in 1 .. numConsumers -> mkConsumer i] |> Async.Ignore
    }

    let mapStreamConsumeResultToDataAndContext (x: ConsumeResult<_,string>) : byte[] * obj =
        let m = Bindings.mapMessage x
        System.Text.Encoding.UTF8.GetBytes(m.Value),
        box { key = m.Key; value = m.Value; partition = Bindings.partitionId x; offset = let o = x.Offset in o.Value }

    let runConsumersStream log (config : KafkaConsumerConfig) (numConsumers : int) (timeout : TimeSpan option) (handler : ConsumerCallback) = async {
        let mkConsumer (consumerId : int) = async {
            // need to pass the consumer instance to the handler callback
            // do a bit of cyclic dependency fixups
            let consumerCell = ref None
            let rec getConsumer() =
                // avoid potential race conditions by polling
                match !consumerCell with
                | None -> Thread.SpinWait 20; getConsumer()
                | Some c -> c

            // when processing, declare all items processed each time we're invoked
            let handle (streamName : StreamName, span : Propulsion.Streams.StreamSpan<byte[]>) = async {
                for event in span.events do
                    do! handler (getConsumer()) (deserialize consumerId event)
                return Propulsion.Streams.SpanResult.AllProcessed, () }
            let stats = Stats(log, TimeSpan.FromSeconds 5.,TimeSpan.FromSeconds 5.)
            let messageIndexes = StreamNameSequenceGenerator()
            let consumer =
                 StreamsConsumer.Start<unit>
                    (   log, config, messageIndexes.ConsumeResultToStreamEvent(mapStreamConsumeResultToDataAndContext),
                        handle, 256, stats, TimeSpan.FromSeconds 10.,
                        maxBatches = 50)

            consumerCell := Some consumer

            timeout |> Option.defaultValue (TimeSpan.FromMinutes 15.) |> consumer.StopAfter

            do! consumer.AwaitCompletion()
        }

        do! Async.Parallel [for i in 1 .. numConsumers -> mkConsumer i] |> Async.Ignore
    }

#nowarn "1182" // From hereon in, we may have some 'unused' privates (the tests)

type BatchesConsumer(testOutputHelper) =
    inherit ConsumerIntegration(testOutputHelper, false)

    override __.RunConsumers(log, config, numConsumers, consumerCallback, timeout) : Async<unit> =
        runConsumersBatch log config numConsumers timeout consumerCallback

and StreamsConsumer(testOutputHelper) =
    inherit ConsumerIntegration(testOutputHelper, true)

    override __.RunConsumers(log, config, numConsumers, consumerCallback, timeout) : Async<unit> =
        runConsumersStream log config numConsumers timeout consumerCallback

and ParallelConsumer(testOutputHelper) =
    inherit ConsumerIntegration(testOutputHelper, true)

    let log, bootstrapServers = createLogger (TestOutputAdapter testOutputHelper), getTestBroker ()

    override __.RunConsumers(log, config, numConsumers, consumerCallback, timeout) : Async<unit> =
        runConsumersParallel log config numConsumers timeout consumerCallback

    [<FactIfBroker>]
    member __.``consumer pipeline should have expected exception semantics`` () = async {
        let topic = newId() // dev kafka topics are created and truncated automatically
        let groupId = newId()

        do! __.RunProducers(log, bootstrapServers, topic, 1, 10) // populate the topic with a few messages

        let config = KafkaConsumerConfig.Create("panther", bootstrapServers, [topic], groupId)

        let! r = Async.Catch <| __.RunConsumers(log, config, 1, (fun _ _ -> async { return raise <|IndexOutOfRangeException() }))
        test <@ match r with
                | Choice2Of2 (:? AggregateException as ae) -> ae.InnerExceptions |> Seq.forall (function (:? IndexOutOfRangeException) -> true | _ -> false)
                | x -> failwithf "%A" x @>
    }

and [<AbstractClass>] ConsumerIntegration(testOutputHelper, expectConcurrentScheduling) =
    let log, bootstrapServers = createLogger (TestOutputAdapter testOutputHelper), getTestBroker ()

    member __.RunProducers(log, bootstrapServers, topic, numProducers, messagesPerProducer) : Async<unit> =
        runProducers log bootstrapServers topic numProducers messagesPerProducer |> Async.Ignore
    abstract RunConsumers: Serilog.ILogger * KafkaConsumerConfig *  int * ConsumerCallback * TimeSpan option -> Async<unit>
    member __.RunConsumers(log,config,count,cb) = __.RunConsumers(log,config,count,cb,None)

    [<FactIfBroker>]
    member __.``producer-consumer basic roundtrip`` () = async {
        let numProducers = 10
#if DEBUG
        let numConsumers = 10
#else
        // TODO debug why this is happy locally buy not on the AzureDevOps CI Rig
        let numConsumers = 1
#endif
        let messagesPerProducer = 1000

        let topic = newId() // dev kafka topics are created and truncated automatically
        let groupId = newId()

        let itemsSeen = ConcurrentDictionary<_,_>()
        let consumedBatches = ConcurrentBag<ConsumedTestMessage>()
        let expectedUniqueMessages = numProducers * messagesPerProducer
        let consumerCallback (consumer:ConsumerPipeline) msg = async {
            itemsSeen.[msg.payload] <- ()
            consumedBatches.Add msg
            // signal cancellation if consumed items reaches expected size
            if itemsSeen.Count >= expectedUniqueMessages then
                consumer.Stop()
        }

        // Section: run the test
        let producers = __.RunProducers(log, bootstrapServers, topic, numProducers, messagesPerProducer)

        let config = KafkaConsumerConfig.Create("panther", bootstrapServers, [topic], groupId, statisticsInterval=TimeSpan.FromSeconds 5.)
        let consumers = __.RunConsumers(log, config, numConsumers, consumerCallback)

        let! _ = Async.Parallel [ producers ; consumers ]

        // Section: assertion checks
        let ``consumed batches should be non-empty`` =
            (not << Seq.isEmpty) consumedBatches

        test <@ ``consumed batches should be non-empty`` @> // "consumed batches should all be non-empty")

        let allMessages =
            consumedBatches
            |> Seq.toArray

        let ``all message keys should have expected value`` =
            allMessages |> Array.forall (fun msg -> int msg.meta.key = msg.payload.messageId)

        test <@ ``all message keys should have expected value`` @> // "all message keys should have expected value"

        // ``should have consumed all expected messages`
        let unconsumed =
            allMessages
            |> Array.groupBy (fun msg -> msg.payload.producerId)
            |> Array.map (fun (_, gp) -> gp |> Array.distinctBy (fun msg -> msg.payload.messageId))
            |> Array.where (fun gp -> gp.Length <> messagesPerProducer)
        let unconsumedCounts =
            unconsumed
            |> Seq.map (fun gp -> gp.[0].payload.producerId, gp.Length)
            |> Array.ofSeq
        test <@ Array.isEmpty unconsumedCounts @>
    }

    [<FactIfBroker>]
    member __.``Given a topic different consumer group ids should be consuming the same message set`` () = async {
        let numMessages = 10

        let topic = newId() // dev kafka topics are created and truncated automatically

        do! __.RunProducers(log, bootstrapServers, topic, 1, numMessages) // populate the topic with a few messages

        let messageCount = ref 0
        let groupId1 = newId()
        let config = KafkaConsumerConfig.Create("panther", bootstrapServers, [topic], groupId1)
        do! __.RunConsumers(log, config, 1,
                (fun c _m -> async { if Interlocked.Increment(messageCount) >= numMessages then c.Stop() }))

        test <@ numMessages = !messageCount @>

        let messageCount = ref 0
        let groupId2 = newId()
        let config = KafkaConsumerConfig.Create("panther", bootstrapServers, [topic], groupId2)
        do! __.RunConsumers(log, config, 1,
                (fun c _m -> async { if Interlocked.Increment(messageCount) >= numMessages then c.Stop() }))

        test <@ numMessages = !messageCount @>
    }

    [<FactIfBroker>]
    member __.``Spawning a new consumer with same consumer group id should not receive new messages`` () = async {
        let numMessages = 10
        let topic = newId() // dev kafka topics are created and truncated automatically
        let groupId = newId()
        let config = KafkaConsumerConfig.Create("panther", bootstrapServers, [topic], groupId, autoCommitInterval=TimeSpan.FromSeconds 1.)

        do! __.RunProducers(log, bootstrapServers, topic, 1, numMessages) // populate the topic with a few messages

        // expected to read 10 messages from the first consumer
        let messageCount = ref 0
        do! __.RunConsumers(log, config, 1,
                (fun c _m -> async {
                    if Interlocked.Increment(messageCount) >= numMessages then
                        c.StopAfter(TimeSpan.FromSeconds 5.) })) // cancel after 5 second to allow offsets to be stored

        test <@ numMessages = !messageCount @>

        // expected to read no messages from the subsequent consumer
        let messageCount = ref 0
        do! __.RunConsumers(log, config, 1,
                (fun c _m -> async {
                    if Interlocked.Increment(messageCount) >= numMessages then c.Stop() }),
                Some (TimeSpan.FromSeconds 10.))

        test <@ 0 = !messageCount @>
    }

    [<FactIfBroker>]
    member __.``Committed offsets should not result in missing messages`` () = async {
        let numMessages = 10
        let topic = newId() // dev kafka topics are created and truncated automatically
        let groupId = newId()
        let config = KafkaConsumerConfig.Create("panther", bootstrapServers, [topic], groupId)

        do! __.RunProducers(log, bootstrapServers, topic, 1, numMessages) // populate the topic with a few messages

        // expected to read 10 messages from the first consumer
        let messageCount = ref 0
        do! __.RunConsumers(log, config, 1,
                (fun c _m -> async {
                    if Interlocked.Increment(messageCount) >= numMessages then
                        c.StopAfter(TimeSpan.FromSeconds 1.) })) // cancel after 1 second to allow offsets to be committed)

        test <@ numMessages = !messageCount @>

        do! __.RunProducers(log, bootstrapServers, topic, 1, numMessages) // produce more messages

        // expected to read 10 messages from the subsequent consumer,
        // this is to verify there are no off-by-one errors in how offsets are committed
        let messageCount = ref 0
        do! __.RunConsumers(log, config, 1,
                (fun c _m -> async {
                    if Interlocked.Increment(messageCount) >= numMessages then
                        c.StopAfter(TimeSpan.FromSeconds 1.) })) // cancel after 1 second to allow offsets to be committed)

        test <@ numMessages = !messageCount @>
    }

    [<FactIfBroker>]
    member __.``Consumers should schedule two batches of the same partition concurrently`` () = async {
        // writes 2000 messages down a topic with a shuffled partition key
        // then attempts to consume the topic, checking that batches are
        // monotonic w.r.t. offsets
        let numMessages = 2000
        let maxBatchSize = 20
        let topic = newId() // dev kafka topics are created and truncated automatically
        let groupId = newId()
        let config = KafkaConsumerConfig.Create("panther", bootstrapServers, [topic], groupId, maxBatchSize=maxBatchSize)

        // Produce messages in the topic
        do! __.RunProducers(log, bootstrapServers, topic, 1, numMessages)

        let globalMessageCount = ref 0

        let getPartitionOffset =
            let state = ConcurrentDictionary<int, int64 ref>()
            fun partition -> state.GetOrAdd(partition, fun _ -> ref -1L)

        let getBatchPartitionCount =
            let state = ConcurrentDictionary<int, int ref>()
            fun partition -> state.GetOrAdd(partition, fun _ -> ref 0)

        let concurrentCalls = ref 0
        let foundNonMonotonic = ref false

        do! __.RunConsumers(log, config, 1,
                (fun c m -> async {
                    let partition = m.meta.partition

                    // check per-partition handlers are serialized
                    let concurrentBatchCell = getBatchPartitionCount partition
                    let concurrentBatches = Interlocked.Increment concurrentBatchCell
                    if 1 <> concurrentBatches then Interlocked.Increment(concurrentCalls) |> ignore

                    // check for message monotonicity
                    let offset = getPartitionOffset partition
                    for msg in [|m|] do
                        if msg.meta.offset > !offset then foundNonMonotonic := true
                        offset := msg.meta.offset

                    // Sleeping is really not going to help matters in batched mode
                    if expectConcurrentScheduling then
                        do! Async.Sleep 100

                    let _ = Interlocked.Decrement concurrentBatchCell

                    if Interlocked.Increment(globalMessageCount) >= numMessages then c.Stop() }))

        test <@ !foundNonMonotonic @> //  "offset for partition should be monotonic"
        test <@ if expectConcurrentScheduling then !concurrentCalls > 1 else !concurrentCalls = 0 @> // "partitions should definitely schedule more than one batch concurrently")
        test <@ numMessages = !globalMessageCount @> }
