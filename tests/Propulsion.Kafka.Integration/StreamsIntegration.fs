namespace Propulsion.Kafka.Integration.Streams

open Jet.ConfluentKafka.FSharp
open Newtonsoft.Json
open Propulsion.Kafka
open Propulsion.Kafka.Integration.Helpers
open System
open Swensen.Unquote
open System.Collections.Concurrent
open System.ComponentModel
open System.Threading

[<AutoOpen>]
[<EditorBrowsable(EditorBrowsableState.Never)>]
module Helpers =
    open System.Collections.Generic
    open Confluent.Kafka

    type TestMeta = { key: string; partition: int; offset : int64 }

    /// StreamsConsumer buffers and deduplicates messages from a contiguous stream with each message bearing an index.
    /// The messages we consume don't have such characteristics, so we generate a fake `index` by keeping an int per stream in a dictionary
    type MessagesByArrivalOrder() =
        // we synthesize a monotonically increasing index to render the deduplication facility inert
        static let indices = System.Collections.Generic.Dictionary()
        static let genIndex streamName =
            match indices.TryGetValue streamName with
            | true, v -> let x = v + 1 in indices.[streamName] <- x; x
            | false, _ -> let x = 0 in indices.[streamName] <- x; x

        // Stuff the full content of the message into an Event record - we'll parse it when it comes out the other end in a span
        static member ToStreamEvent (KeyValue (k,v : string), meta : TestMeta) : Propulsion.Streams.StreamEvent<byte[]> seq =
            let index = genIndex k |> int64
            let inline gb (s : string) = System.Text.Encoding.UTF8.GetBytes s
            let d, m = gb v,gb (FsCodec.NewtonsoftJson.Serdes.Serialize meta)
            let e = FsCodec.Core.IndexedEventData(index,false,eventType = String.Empty,data=d,metadata=m,timestamp=DateTimeOffset.UtcNow)
            Seq.singleton { stream=k; event=e }

    type ConsumedTestMessage = { consumerId : int ; meta : TestMeta; payload : TestMessage }
    type ConsumerCallback = ConsumerPipeline -> ConsumedTestMessage -> Async<unit>
    let runConsumers log (config : KafkaConsumerConfig) (numConsumers : int) (timeout : TimeSpan option) (handler : ConsumerCallback) = async {
        let mkConsumer (consumerId : int) = async {
            let deserialize (e : FsCodec.IIndexedEvent<byte[]>) : ConsumedTestMessage =
                let d = FsCodec.NewtonsoftJson.Serdes.Deserialize(System.Text.Encoding.UTF8.GetString e.Data)
                let m = FsCodec.NewtonsoftJson.Serdes.Deserialize(System.Text.Encoding.UTF8.GetString e.Meta)
                { consumerId = consumerId; meta=m; payload=d }

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
            // when processingdeclare all items processed each timer we;re invoked
            let handle (streams : Propulsion.Streams.Scheduling.DispatchItem<byte[]>[]) = async {
                for stream in streams do
                  for event in stream.span.events do
                      do! handler (getConsumer()) (deserialize event)
                return [| for x in streams -> Choice1Of2 x.span.events.[x.span.events.Length-1].Index |] |> Seq.ofArray }
            let stats = Propulsion.Streams.Scheduling.StreamSchedulerStats(log, TimeSpan.FromSeconds 5.,TimeSpan.FromSeconds 5.)
            let mapConsumeResult (x: ConsumeResult<_,_>) : KeyValuePair<string,string>*TestMeta =
                KeyValuePair(x.Key,x.Value), { key = x.Key; partition = Bindings.partitionId x; offset = let o = x.Offset in o.Value }
            let consumer =
                StreamsConsumer.Start
                    (   log, config, mapConsumeResult, MessagesByArrivalOrder.ToStreamEvent,
                        select, handle,
                        stats, categorize = id,
                        pipelineStatsInterval = TimeSpan.FromSeconds 10.)

            consumerCell := Some consumer

            timeout |> Option.iter consumer.StopAfter

            do! consumer.AwaitCompletion()
        }

        do! Async.Parallel [for i in 1 .. numConsumers -> mkConsumer i] |> Async.Ignore
    }

#nowarn "1182" // From hereon in, we may have some 'unused' privates (the tests)

type T1(testOutputHelper) =
    let log, broker = createLogger (TestOutputAdapter testOutputHelper), getTestBroker ()

    let [<FactIfBroker>] ``producer-consumer basic roundtrip`` () = async {
        let numProducers = 10
        let numConsumers = 10
        let messagesPerProducer = 1000

        let topic = newId() // dev kafka topics are created and truncated automatically
        let groupId = newId()

        let consumedBatches = new ConcurrentBag<ConsumedTestMessage>()
        let expectedUniqueMessages = numProducers * messagesPerProducer
        let consumerCallback (consumer:ConsumerPipeline) msg = async {
            do consumedBatches.Add msg
            // signal cancellation if consumed items reaches expected size
            if consumedBatches |> Seq.map (fun x -> x.payload.producerId, x.payload.messageId) |> Seq.distinct |> Seq.length = expectedUniqueMessages then
                consumer.Stop()
        }

        // Section: run the test
        let producers = runProducers log broker topic numProducers messagesPerProducer |> Async.Ignore

        let config = KafkaConsumerConfig.Create("panther", broker, [topic], groupId, statisticsInterval=(TimeSpan.FromSeconds 5.))
        let consumers = runConsumers log config numConsumers None consumerCallback

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

// separated test type to allow the tests to run in parallel
type T2(testOutputHelper) =
    let log, broker = createLogger (TestOutputAdapter testOutputHelper), getTestBroker ()

    let [<FactIfBroker>] ``consumer pipeline should have expected exception semantics`` () = async {
        let topic = newId() // dev kafka topics are created and truncated automatically
        let groupId = newId()

        let! _ = runProducers log broker topic 1 10 // populate the topic with a few messages

        let config = KafkaConsumerConfig.Create("panther", broker, [topic], groupId)

        let! r = Async.Catch <| runConsumers log config 1 None (fun _ _ -> async { return raise <|IndexOutOfRangeException() })
        test <@ match r with
                | Choice2Of2 (:? AggregateException as ae) -> ae.InnerExceptions |> Seq.forall (function (:? IndexOutOfRangeException) -> true | _ -> false)
                | x -> failwithf "%A" x @>
    }

    let [<FactIfBroker>] ``Given a topic different consumer group ids should be consuming the same message set`` () = async {
        let numMessages = 10

        let topic = newId() // dev kafka topics are created and truncated automatically

        let! _ = runProducers log broker topic 1 numMessages // populate the topic with a few messages

        let messageCount = ref 0
        let groupId1 = newId()
        let config = KafkaConsumerConfig.Create("panther", broker, [topic], groupId1)
        do! runConsumers log config 1 None
                (fun c _m -> async { if Interlocked.Increment(messageCount) >= numMessages then c.Stop() })

        test <@ numMessages = !messageCount @>

        let messageCount = ref 0
        let groupId2 = newId()
        let config = KafkaConsumerConfig.Create("panther", broker, [topic], groupId2)
        do! runConsumers log config 1 None
                (fun c _m -> async { if Interlocked.Increment(messageCount) >= numMessages then c.Stop() })

        test <@ numMessages = !messageCount @>
    }

    let [<FactIfBroker>] ``Spawning a new consumer with same consumer group id should not receive new messages`` () = async {
        let numMessages = 10
        let topic = newId() // dev kafka topics are created and truncated automatically
        let groupId = newId()
        let config = KafkaConsumerConfig.Create("panther", broker, [topic], groupId)

        let! _ = runProducers log broker topic 1 numMessages // populate the topic with a few messages

        // expected to read 10 messages from the first consumer
        let messageCount = ref 0
        do! runConsumers log config 1 None
                (fun c _m -> async {
                    if Interlocked.Increment(messageCount) >= numMessages then
                        c.StopAfter(TimeSpan.FromSeconds 1.) }) // cancel after 1 second to allow offsets to be stored

        test <@ numMessages = !messageCount @>

        // Await async committing
        //do! Async.Sleep 10_000

        // expected to read no messages from the subsequent consumer
        let messageCount = ref 0
        do! runConsumers log config 1 (Some (TimeSpan.FromSeconds 10.))
                (fun c _m -> async {
                    if Interlocked.Increment(messageCount) >= numMessages then c.Stop() })

        test <@ 0 = !messageCount @>
    }

// separated test type to allow the tests to run in parallel
type T3(testOutputHelper) =
    let log, broker = createLogger (TestOutputAdapter testOutputHelper), getTestBroker ()

    let [<FactIfBroker>] ``Commited offsets should not result in missing messages`` () = async {
        let numMessages = 10
        let topic = newId() // dev kafka topics are created and truncated automatically
        let groupId = newId()
        let config = KafkaConsumerConfig.Create("panther", broker, [topic], groupId)

        let! _ = runProducers log broker topic 1 numMessages // populate the topic with a few messages

        // expected to read 10 messages from the first consumer
        let messageCount = ref 0
        do! runConsumers log config 1 None
                (fun c _m -> async {
                    if Interlocked.Increment(messageCount) >= numMessages then
                        c.StopAfter(TimeSpan.FromSeconds 1.) }) // cancel after 1 second to allow offsets to be committed)

        test <@ numMessages = !messageCount @>

        let! _ = runProducers log broker topic 1 numMessages // produce more messages

        // expected to read 10 messages from the subsequent consumer,
        // this is to verify there are no off-by-one errors in how offsets are committed
        let messageCount = ref 0
        do! runConsumers log config 1 None
                (fun c _m -> async {
                    if Interlocked.Increment(messageCount) >= numMessages then
                        c.StopAfter(TimeSpan.FromSeconds 1.) }) // cancel after 1 second to allow offsets to be committed)

        test <@ numMessages = !messageCount @>
    }

    let [<FactIfBroker>] ``Consumers should schedule two batches of the same partition concurrently`` () = async {
        // writes 2000 messages down a topic with a shuffled partition key
        // then attempts to consume the topic, checking that batches are
        // monotonic w.r.t. offsets
        let numMessages = 2000
        let maxBatchSize = 5
        let topic = newId() // dev kafka topics are created and truncated automatically
        let groupId = newId()
        let config = KafkaConsumerConfig.Create("panther", broker, [topic], groupId, maxBatchSize = maxBatchSize)

        // Produce messages in the topic
        do! runProducers log broker topic 1 numMessages |> Async.Ignore

        let globalMessageCount = ref 0

        let getPartitionOffset =
            let state = new ConcurrentDictionary<int, int64 ref>()
            fun partition -> state.GetOrAdd(partition, fun _ -> ref -1L)

        let getBatchPartitionCount =
            let state = new ConcurrentDictionary<int, int ref>()
            fun partition -> state.GetOrAdd(partition, fun _ -> ref 0)

        let concurrentCalls = ref 0
        let foundNonMonotonic = ref false

        do! runConsumers log config 1 None
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

                    do! Async.Sleep 100

                    let _ = Interlocked.Decrement concurrentBatchCell

                    if Interlocked.Increment(globalMessageCount) >= numMessages then c.Stop() })

        test <@ !foundNonMonotonic @> //  "offset for partition should be monotonic"
        test <@ !concurrentCalls > 1 @> // "partitions should definitely schedule more than one batch concurrently")
        test <@ numMessages = !globalMessageCount @> }