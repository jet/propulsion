﻿namespace Propulsion.Kafka.Integration.Streams

open Jet.ConfluentKafka.FSharp
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
    /// StreamsConsumer buffers and deduplicates messages from a contiguous stream with each message bearing an index.
    /// The messages we consume don't have such characteristics, so we generate a fake `index` by keeping an int per stream in a dictionary
    type MessagesByArrivalOrder() =
        // we synthesize a monotonically increasing index to render the deduplication facility inert
        let indices = System.Collections.Generic.Dictionary()
        let genIndex streamName =
            match indices.TryGetValue streamName with
            | true, v -> let x = v + 1 in indices.[streamName] <- x; x
            | false, _ -> let x = 0 in indices.[streamName] <- x; x

        // Stuff the full content of the message into an Event record - we'll parse it when it comes out the other end in a span
        member __.ToStreamEvents (KeyValue (k,v : string)) : Propulsion.Streams.StreamEvent<byte[]> seq =
            let index = genIndex k |> int64
            let gb (x : string) = System.Text.Encoding.UTF8.GetBytes x
            let e = FsCodec.Core.IndexedEventData(index,false,eventType = String.Empty,data=gb v,metadata=null,timestamp=DateTimeOffset.UtcNow)
            Seq.singleton { stream=k; event=e }

    let deserialize consumerId (e : FsCodec.IIndexedEvent<byte[]>) : ConsumedTestMessage =
        let d = FsCodec.NewtonsoftJson.Serdes.Deserialize(System.Text.Encoding.UTF8.GetString e.Data)
        let v = FsCodec.NewtonsoftJson.Serdes.Deserialize(d.value)
        { consumerId = consumerId; meta=d; payload=v }

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
            let stats = Propulsion.Streams.Scheduling.StreamSchedulerStats(log, TimeSpan.FromSeconds 5.,TimeSpan.FromSeconds 5.)
            let messageIndexes = MessagesByArrivalOrder()
            let consumer =
                BatchesConsumer.Start
                    (   log, config, mapConsumeResult, messageIndexes.ToStreamEvents,
                        select, handle,
                        stats, categorize = id,
                        pipelineStatsInterval = TimeSpan.FromSeconds 10.)

            consumerCell := Some consumer

            timeout |> Option.defaultValue (TimeSpan.FromMinutes 15.) |> consumer.StopAfter

            do! consumer.AwaitCompletion()
        }

        do! Async.Parallel [for i in 1 .. numConsumers -> mkConsumer i] |> Async.Ignore
    }

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
            let handle (stream : string, span : Propulsion.Streams.StreamSpan<byte[]>) = async {
                for event in span.events do
                    do! handler (getConsumer()) (deserialize consumerId event)
                return span.events.Length,() }
            let stats = Propulsion.Streams.Scheduling.StreamSchedulerStats(log, TimeSpan.FromSeconds 5.,TimeSpan.FromSeconds 5.)
            let messageIndexes = MessagesByArrivalOrder()
            let consumer =
                 Core.StreamsConsumer.Start
                    (   log, config, mapConsumeResult, messageIndexes.ToStreamEvents,
                        handle, 256, stats,
                        maxBatches = 50, categorize = id, pipelineStatsInterval = TimeSpan.FromSeconds 10.)

            consumerCell := Some consumer

            timeout |> Option.defaultValue (TimeSpan.FromMinutes 15.) |> consumer.StopAfter

            do! consumer.AwaitCompletion()
        }

        do! Async.Parallel [for i in 1 .. numConsumers -> mkConsumer i] |> Async.Ignore
    }

#nowarn "1182" // From hereon in, we may have some 'unused' privates (the tests)

type private T1Batch(testOutputHelper) =
    inherit T1(testOutputHelper)

    override __.RunConsumers(log, config, numConsumers, consumerCallback) : Async<unit> =
        runConsumersBatch log config numConsumers None consumerCallback

and private T1Stream(testOutputHelper) =
    inherit T1(testOutputHelper)

    override __.RunConsumers(log, config, numConsumers, consumerCallback) : Async<unit> =
        runConsumersStream log config numConsumers None consumerCallback

and [<AbstractClass>]T1(testOutputHelper) =
    let log, broker = createLogger (TestOutputAdapter testOutputHelper), getTestBroker ()

    member __.RunProducers(log, broker, topic, numProducers, messagesPerProducer) : Async<unit> =
        runProducers log broker topic numProducers messagesPerProducer |> Async.Ignore
    abstract RunConsumers: Serilog.ILogger * KafkaConsumerConfig *  int * ConsumerCallback -> Async<unit>

    [<FactIfBroker>]
    member __.``producer-consumer basic roundtrip`` () = async {
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
        let producers = __.RunProducers(log, broker, topic, numProducers, messagesPerProducer)

        let config = KafkaConsumerConfig.Create("panther", broker, [topic], groupId, statisticsInterval=TimeSpan.FromSeconds 5.)
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

// separated test type to allow the tests to run in parallel
type private T2Batch(testOutputHelper) =
    inherit T2(testOutputHelper)

    override __.RunConsumers(log, config, numConsumers, consumerCallback, timeout) : Async<unit> =
        runConsumersBatch log config numConsumers timeout consumerCallback

and private T2Stream(testOutputHelper) =
    inherit T2(testOutputHelper)

    override __.RunConsumers(log, config, numConsumers, consumerCallback, timeout) : Async<unit> =
        runConsumersStream log config numConsumers timeout consumerCallback

and [<AbstractClass>] T2(testOutputHelper) =
    let log, broker = createLogger (TestOutputAdapter testOutputHelper), getTestBroker ()

    member __.RunProducers(log, broker, topic, numProducers, messagesPerProducer) : Async<unit> =
        runProducers log broker topic numProducers messagesPerProducer |> Async.Ignore
    abstract RunConsumers: Serilog.ILogger * KafkaConsumerConfig *  int * ConsumerCallback * TimeSpan option -> Async<unit>
    member __.RunConsumers(log,config,count,cb) = __.RunConsumers(log,config,count,cb,None)

//    [<FactIfBroker(Skip="Streamwise processing is subject to retries; need to cover that in tests")>]
//    member __.``consumer pipeline should have expected exception semantics`` () = async {
//        let topic = newId() // dev kafka topics are created and truncated automatically
//        let groupId = newId()
//
//        do! __.RunProducers(log, broker, topic, 1, 10) // populate the topic with a few messages
//
//        let config = KafkaConsumerConfig.Create("panther", broker, [topic], groupId)
//
//        let! r = Async.Catch <| __.RunConsumers(log, config, 1, (fun _ _ -> async { return raise <|IndexOutOfRangeException() }))
//        test <@ match r with
//                | Choice2Of2 (:? AggregateException as ae) -> ae.InnerExceptions |> Seq.forall (function (:? IndexOutOfRangeException) -> true | _ -> false)
//                | x -> failwithf "%A" x @>
//    }

    [<FactIfBroker>]
    member __.``Spawning a new consumer with same consumer group id should not receive new messages`` () = async {
        let numMessages = 10
        let topic = newId() // dev kafka topics are created and truncated automatically
        let groupId = newId()
        let config = KafkaConsumerConfig.Create("panther", broker, [topic], groupId, offsetCommitInterval=TimeSpan.FromSeconds 1.)

        do! __.RunProducers(log, broker, topic, 1, numMessages) // populate the topic with a few messages

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

// separated test type to allow the tests to run in parallel
type T3Batch(testOutputHelper) =
    inherit T3(testOutputHelper, false)

    override __.RunConsumers(log, config, numConsumers, consumerCallback, timeout) : Async<unit> =
        runConsumersBatch log config numConsumers timeout consumerCallback

and T3Stream(testOutputHelper) =
    inherit T3(testOutputHelper, true)

    override __.RunConsumers(log, config, numConsumers, consumerCallback, timeout) : Async<unit> =
        runConsumersStream log config numConsumers timeout consumerCallback

and [<AbstractClass>] T3(testOutputHelper, expectConcurrentScheduling) =
    let log, broker = createLogger (TestOutputAdapter testOutputHelper), getTestBroker ()

    member __.RunProducers(log, broker, topic, numProducers, messagesPerProducer) : Async<unit> =
        runProducers log broker topic numProducers messagesPerProducer |> Async.Ignore
    abstract RunConsumers: Serilog.ILogger * KafkaConsumerConfig *  int * ConsumerCallback * TimeSpan option -> Async<unit>
    member __.RunConsumers(log,config,count,cb) = __.RunConsumers(log,config,count,cb,None)

    [<FactIfBroker>]
    member __.``Given a topic different consumer group ids should be consuming the same message set`` () = async {
        let numMessages = 10

        let topic = newId() // dev kafka topics are created and truncated automatically

        do! __.RunProducers(log, broker, topic, 1, numMessages) // populate the topic with a few messages

        let messageCount = ref 0
        let groupId1 = newId()
        let config = KafkaConsumerConfig.Create("panther", broker, [topic], groupId1)
        do! __.RunConsumers(log, config, 1,
                (fun c _m -> async { if Interlocked.Increment(messageCount) >= numMessages then c.Stop() }))

        test <@ numMessages = !messageCount @>

        let messageCount = ref 0
        let groupId2 = newId()
        let config = KafkaConsumerConfig.Create("panther", broker, [topic], groupId2)
        do! __.RunConsumers(log, config, 1,
                (fun c _m -> async { if Interlocked.Increment(messageCount) >= numMessages then c.Stop() }))

        test <@ numMessages = !messageCount @>
    }

    [<FactIfBroker>]
    member __.``Committed offsets should not result in missing messages`` () = async {
        let numMessages = 10
        let topic = newId() // dev kafka topics are created and truncated automatically
        let groupId = newId()
        let config = KafkaConsumerConfig.Create("panther", broker, [topic], groupId)

        do! __.RunProducers(log, broker, topic, 1, numMessages) // populate the topic with a few messages

        // expected to read 10 messages from the first consumer
        let messageCount = ref 0
        do! __.RunConsumers(log, config, 1,
                (fun c _m -> async {
                    if Interlocked.Increment(messageCount) >= numMessages then
                        c.StopAfter(TimeSpan.FromSeconds 1.) })) // cancel after 1 second to allow offsets to be committed)

        test <@ numMessages = !messageCount @>

        do! __.RunProducers(log, broker, topic, 1, numMessages) // produce more messages

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
        let config = KafkaConsumerConfig.Create("panther", broker, [topic], groupId, maxBatchSize = maxBatchSize)

        // Produce messages in the topic
        do! __.RunProducers(log, broker, topic, 1, numMessages)

        let globalMessageCount = ref 0

        let getPartitionOffset =
            let state = new ConcurrentDictionary<int, int64 ref>()
            fun partition -> state.GetOrAdd(partition, fun _ -> ref -1L)

        let getBatchPartitionCount =
            let state = new ConcurrentDictionary<int, int ref>()
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