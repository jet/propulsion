namespace Propulsion.Kafka.Integration.Parallel

open Jet.ConfluentKafka.FSharp
open Newtonsoft.Json
open Propulsion.Kafka
open Serilog
open Swensen.Unquote
open System
open System.Collections.Concurrent
open System.ComponentModel
open System.Threading
open System.Threading.Tasks
open Xunit

[<AutoOpen>]
[<EditorBrowsable(EditorBrowsableState.Never)>]
module Helpers =
    open Confluent.Kafka

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
        | x -> Uri x

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
            Task.Delay(delay).ContinueWith(fun (_:Task) -> c.Stop()) |> ignore

    type TestMessage = { producerId : int ; messageId : int }
    [<NoComparison; NoEquality>]
    type ConsumedTestMessage = { consumerId : int ; raw : ConsumeResult<string,string> ; payload : TestMessage }
    type ConsumerCallback = ConsumerPipeline -> ConsumedTestMessage -> Async<unit>

    let runProducers log (broker : Uri) (topic : string) (numProducers : int) (messagesPerProducer : int) = async {
        let runProducer (producerId : int) = async {
            let cfg = KafkaProducerConfig.Create("panther", broker, Acks.Leader)
            use producer = BatchedProducer.CreateWithConfigOverrides(log, cfg, topic, maxInFlight = 10000)

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

    let runConsumers log (config : KafkaConsumerConfig) (numConsumers : int) (timeout : TimeSpan option) (handler : ConsumerCallback) = async {
        let mkConsumer (consumerId : int) = async {
            let deserialize (raw : ConsumeResult<string,string>) = { consumerId = consumerId ; raw = raw; payload = JsonConvert.DeserializeObject<_> raw.Value }

            // need to pass the consumer instance to the handler callback
            // do a bit of cyclic dependency fixups
            let consumerCell = ref None
            let rec getConsumer() =
                // avoid potential race conditions by polling
                match !consumerCell with
                | None -> Thread.SpinWait 20; getConsumer()
                | Some c -> c

            let handle item = handler (getConsumer()) (deserialize item)
            let consumer = ParallelConsumer.Start(log, config, 1024, id, handle >> Async.Catch, statsInterval = TimeSpan.FromSeconds 10.)

            consumerCell := Some consumer

            timeout |> Option.iter consumer.StopAfter

            do! consumer.AwaitCompletion()
        }

        do! Async.Parallel [for i in 1 .. numConsumers -> mkConsumer i] |> Async.Ignore
    }

type FactIfBroker() =
    inherit FactAttribute()
    override __.Skip = if null <> Environment.GetEnvironmentVariable "TEST_KAFKA_BROKER" then null else "Skipping as no EQUINOX_KAFKA_BROKER supplied"

type T1(testOutputHelper) =
    let log, broker = createLogger (TestOutputAdapter testOutputHelper), getTestBroker ()

    let [<FactIfBroker>] ``producer-consumer basic roundtrip`` () = async {
        let numProducers = 10
        let numConsumers = 10
        let messagesPerProducer = 1000

        let topic = newId() // dev kafka topics are created and truncated automatically
        let groupId = newId()
    
        let consumedBatches = new ConcurrentBag<ConsumedTestMessage>()
        let consumerCallback (consumer:ConsumerPipeline) msg = async {
            do consumedBatches.Add msg
            let messageCount = consumedBatches |> Seq.length
            // signal cancellation if consumed items reaches expected size
            if messageCount >= numProducers * messagesPerProducer then
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
            allMessages |> Array.forall (fun msg -> int msg.raw.Key = msg.payload.messageId)

        test <@ ``all message keys should have expected value`` @> // "all message keys should have expected value"

        test <@ allMessages // ``should have consumed all expected messages`
                |> Array.groupBy (fun msg -> msg.payload.producerId)
                |> Array.map (fun (_, gp) -> gp |> Array.distinctBy (fun msg -> msg.payload.messageId))
                |> Array.forall (fun gp -> gp.Length = messagesPerProducer) @>
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
                    let partition = Bindings.partitionValue m.raw.Partition

                    // check per-partition handlers are serialized
                    let concurrentBatchCell = getBatchPartitionCount partition
                    let concurrentBatches = Interlocked.Increment concurrentBatchCell
                    if 1 <> concurrentBatches then Interlocked.Increment(concurrentCalls) |> ignore

                    // check for message monotonicity
                    let offset = getPartitionOffset partition
                    for msg in [|m|] do
                        if (let o = msg.raw.Offset in o.Value) > !offset then foundNonMonotonic := true
                        offset := let o = msg.raw.Offset in o.Value

                    do! Async.Sleep 100

                    let _ = Interlocked.Decrement concurrentBatchCell

                    if Interlocked.Increment(globalMessageCount) >= numMessages then c.Stop() })

        test <@ !foundNonMonotonic @> //  "offset for partition should be monotonic"
        test <@ !concurrentCalls > 1 @> // "partitions should definitely schedule more than one batch concurrently")
        test <@ numMessages = !globalMessageCount @> }