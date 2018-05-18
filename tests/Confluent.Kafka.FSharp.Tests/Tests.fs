module Jet.ConfluentKafka.Tests.``Kafka Integration Tests``

open System
open System.Threading
open System.Collections.Concurrent
open System.Threading.Tasks
open Newtonsoft.Json
open NUnit.Framework

open Jet.ConfluentKafka

[<AutoOpen>]
module Helpers =

    type KafkaConsumer with
        member c.StopAfter(delay : TimeSpan) =
            Task.Delay(delay).ContinueWith(fun (_:Task) -> c.Stop()) |> ignore

    type TestMessage = { producerId : int ; messageId : int }
    type ConsumedTestMessage = { consumerId : int ; message : KafkaMessage ; payload : TestMessage }
    type ConsumerCallback = 
        | Message of (KafkaConsumer -> ConsumedTestMessage -> unit) 
        | Batched of (KafkaConsumer -> ConsumedTestMessage [] -> Async<unit>)

    let runProducers (broker : Uri) (topic : string) (numProducers : int) (messagesPerProducer : int) = async {
        let runProducer (producerId : int) = async {
            use producer = KafkaProducer.Create("panther", broker, topic)

            let! results =
                [1 .. messagesPerProducer]
                |> Seq.map (fun msgId ->
                    let key = string msgId
                    let value = JsonConvert.SerializeObject { producerId = producerId ; messageId = msgId }
                    key, value)

                |> Seq.chunkBySize 100
                |> Seq.map producer.ProduceBatch
                |> Async.Parallel

            return Array.concat results
        }

        return! Async.Parallel [for i in 1 .. numProducers -> runProducer i]
    }

    let runConsumers (broker : Uri) (topic : string) (groupId : string) (numConsumers : int) (timeout : TimeSpan option) (callback : ConsumerCallback) = async {
        let mkConsumer (consumerId : int) = async {
            let config = KafkaConsumerConfig.Create("panther", broker, [topic], groupId)
            let deserialize (msg : KafkaMessage) = { consumerId = consumerId ; message = msg ; payload = JsonConvert.DeserializeObject<_> msg.Value }

            // need to pass the consumer instance to the handler callback
            // do a bit of cyclic dependency fixups
            let consumerCell = ref None
            let rec getConsumer() =
                // avoid potential race conditions by polling
                match !consumerCell with
                | None -> Thread.SpinWait 20; getConsumer()
                | Some c -> c

            let consumer =
                match callback with
                | Message handler -> KafkaConsumer.Start config (fun msg -> handler (getConsumer()) (deserialize msg))
                | Batched handler -> KafkaConsumer.StartBatched config (fun batch -> handler (getConsumer()) (Array.map deserialize batch))

            consumerCell := Some consumer

            timeout |> Option.iter (fun t -> consumer.StopAfter t)

            do! consumer.AwaitConsumer()
        }

        do! Async.Parallel [for i in 1 .. numConsumers -> mkConsumer i] |> Async.Ignore
    }


let getKafkaTestBroker() =
    match Environment.GetEnvironmentVariable "CONFLUENT_KAFKA_TEST_BROKER" with
    | x when String.IsNullOrWhiteSpace x -> invalidOp "Test runner needs to specify CONFLUENT_KAFKA_TEST_BROKER environment variable"
    | uri -> Uri uri

[<Test; Category("IntegrationTest")>]
let ``ConfluentKafka producer-consumer basic roundtrip`` () = utask {
    let broker = getKafkaTestBroker()

    let numProducers = 10
    let numConsumers = 10
    let messagesPerProducer = 1000

    let topic = newId() // dev kafka topics are created and truncated automatically
    let groupId = newId()

    let consumedBatches = new ConcurrentBag<ConsumedTestMessage[]>()
    let consumerCallback =
        Batched(fun consumer batch ->
            async {
                do consumedBatches.Add batch
                let messageCount = consumedBatches |> Seq.sumBy Array.length
                // signal cancellation if consumed items reaches expected size
                if messageCount >= numProducers * messagesPerProducer then
                    consumer.Stop()
            })

    // Section: run the test
    let producers = runProducers broker topic numProducers messagesPerProducer |> Async.Ignore
    let consumers = runConsumers broker topic groupId numConsumers None consumerCallback

    do!
        [ producers ; consumers ]
        |> Async.Parallel
        |> Async.Ignore

    // Section: assertion checks
    let ``consumed batches should be non-empty`` =
        consumedBatches
        |> Seq.forall (not << Array.isEmpty)

    Assert.IsTrue(``consumed batches should be non-empty``, "consumed batches should all be non-empty")

    let ``batches should be grouped by partition`` =
        consumedBatches
        |> Seq.map (fun batch -> batch |> Seq.distinctBy (fun b -> b.message.Partition) |> Seq.length)
        |> Seq.forall (fun numKeys -> numKeys = 1)
        
    Assert.IsTrue(``batches should be grouped by partition``, "batches should be grouped by partition")

    let allMessages =
        consumedBatches
        |> Seq.concat
        |> Seq.toArray

    let ``all message keys should have expected value`` =
        allMessages |> Array.forall (fun msg -> int msg.message.Key = msg.payload.messageId)

    Assert.IsTrue(``all message keys should have expected value``, "all message keys should have expected value")

    let ``should have consumed all expected messages`` =
        allMessages
        |> Array.groupBy (fun msg -> msg.payload.producerId)
        |> Array.map (fun (_, gp) -> gp |> Array.distinctBy (fun msg -> msg.payload.messageId))
        |> Array.forall (fun gp -> gp.Length = messagesPerProducer)

    Assert.IsTrue(``should have consumed all expected messages``, "should have consumed all expected messages")
}

[<Test; Category("IntegrationTest")>]
let ``ConfluentKafka consumer should have expected exception semantics`` () = utask {
    let broker = getKafkaTestBroker()

    let topic = newId() // dev kafka topics are created and truncated automatically
    let groupId = newId()

    let! _ = runProducers broker topic 1 10 // populate the topic with a few messages

    runConsumers broker topic groupId 1 None (Message (fun _ _ -> raise <| IndexOutOfRangeException()))
    |> Assert.ThrowsAsync<IndexOutOfRangeException, _>
}

[<Test; Category("IntegrationTest")>]
let ``Given a topic different consumer group ids should be consuming the same message set`` () = utask {
    let broker = getKafkaTestBroker()
    let numMessages = 10

    let topic = newId() // dev kafka topics are created and truncated automatically

    let! _ = runProducers broker topic 1 numMessages // populate the topic with a few messages

    let messageCount = ref 0
    let groupId1 = newId()
    let! _ = 
        Message (fun c _ -> if Interlocked.Increment messageCount = numMessages then c.Stop())
        |> runConsumers broker topic groupId1 1 None

    Assert.AreEqual(numMessages, !messageCount)

    let messageCount = ref 0
    let groupId2 = newId()
    let! _ = 
        Message (fun c _ -> if Interlocked.Increment messageCount = numMessages then c.Stop())
        |> runConsumers broker topic groupId2 1 None

    Assert.AreEqual(numMessages, !messageCount)
}

[<Test; Category("IntegrationTest")>]
let ``Spawning a new consumer with same consumer group id should not receive new messages`` () = utask {
    let broker = getKafkaTestBroker()

    let numMessages = 10
    let topic = newId() // dev kafka topics are created and truncated automatically
    let groupId = newId()

    let! _ = runProducers broker topic 1 numMessages // populate the topic with a few messages

    // expected to read 10 messages from the first consumer
    let messageCount = ref 0
    let! _ =
        Message (fun c _ -> 
            if Interlocked.Increment messageCount = numMessages then 
                c.StopAfter(TimeSpan.FromSeconds 1.)) // cancel after 1 second to allow offsets to be committed)
        |> runConsumers broker topic groupId 1 None

    Assert.AreEqual(numMessages, !messageCount)

    // expected to read no messages from the subsequent consumer
    let messageCount = ref 0
    let! _ =
        Message (fun c _ -> if Interlocked.Increment messageCount = numMessages then c.Stop())
        |> runConsumers broker topic groupId 1 (Some (TimeSpan.FromSeconds 10.)) 

    Assert.AreEqual(0, !messageCount)
}

[<Test; Category("IntegrationTest")>]
let ``Commited offsets should not result in missing messages`` () = utask {
    let broker = getKafkaTestBroker()

    let numMessages = 10
    let topic = newId() // dev kafka topics are created and truncated automatically
    let groupId = newId()

    let! _ = runProducers broker topic 1 numMessages // populate the topic with a few messages

    // expected to read 10 messages from the first consumer
    let messageCount = ref 0
    let! _ =
        Message (fun c _ -> 
            if Interlocked.Increment messageCount = numMessages then 
                c.StopAfter(TimeSpan.FromSeconds 1.)) // cancel after 1 second to allow offsets to be committed)
        |> runConsumers broker topic groupId 1 None

    Assert.AreEqual(numMessages, !messageCount)

    let! _ = runProducers broker topic 1 numMessages // produce more messages

    // expected to read 10 messages from the subsequent consumer,
    // this is to verify there are no off-by-one errors in how offsets are committed
    let messageCount = ref 0
    let! _ =
        Message (fun c _ -> 
            if Interlocked.Increment messageCount = numMessages then 
                c.StopAfter(TimeSpan.FromSeconds 1.)) // cancel after 1 second to allow offsets to be committed)
        |> runConsumers broker topic groupId 1 None

    Assert.AreEqual(numMessages, !messageCount)
}