module Propulsion.Kafka.Integration.Monitor

open Confluent.Kafka
open FsKafka
open Propulsion.Internal
open Propulsion.Kafka
open Propulsion.Kafka.Integration
open System
open System.Threading
open Swensen.Unquote

let mkProducer log broker topic =
    // Needs to be random to fill all partitions as we're not producing overly random messages
    let config = KafkaProducerConfig.Create("tiger", broker, Acks.Leader, Batching.Linger (TimeSpan.FromMilliseconds 100.), partitioner=Partitioner.Random)
    KafkaProducer.Create(log, config, topic)
// test config creates topics with 4 partitions
let testPartitionCount = 4
let createConsumerConfig broker topic groupId =
    KafkaConsumerConfig.Create("tiger", broker, [topic], groupId, AutoOffsetReset.Earliest, maxBatchSize = 1)
let startConsumerFromConfig log config handler =
    let handler' r = async {
        do! handler r
        return Choice1Of2 () }
    ParallelConsumer.Start(log, config, testPartitionCount, id, handler')
let startConsumer log broker topic groupId handler =
    let config = createConsumerConfig broker topic groupId
    startConsumerFromConfig log config handler
let mkMonitor log = KafkaMonitor(log, TimeSpan.FromSeconds 3., windowSize=5)

let producerOnePerSecondLoop (producer : KafkaProducer) =
    let rec loop () = async {
        let! _ = producer.ProduceAsync("a","1")
        do! Async.Sleep 1000
        return! loop () }
    loop ()

let onlyConsumeFirstBatchHandler =
    let observedPartitions = System.Collections.Concurrent.ConcurrentDictionary()
    fun (item : ConsumeResult<string,string>) -> async {
        // make first handle succeed to ensure consumer has offsets
        let partitionId = Binding.partitionValue item.Partition
        if not <| observedPartitions.TryAdd(partitionId,()) then do! Async.Sleep Int32.MaxValue }

let startTimeout () = IntervalTimer(TimeSpan.FromMinutes 14)

type IntervalTimer with
    member x.AwaitTimeoutOr(cond) = async {
        while not (cond()) && not x.IsDue do
            do! Async.Sleep 1000 }

type T1(testOutputHelper) =
    let log, broker = createLogger (TestOutputAdapter testOutputHelper), getTestBroker ()

    [<FactIfBroker>]
    let ``Monitor should detect stalled consumer`` () = async {
        let topic, group = newId (), newId () // dev kafka topics are created and truncated automatically
        let producer = mkProducer log broker topic
        let! _producerActivity = Async.StartChild <| producerOnePerSecondLoop producer

        let mutable errorObserved = false
        let observeErrorsMonitorHandler(_topic,states : (int * PartitionResult) list) =
            errorObserved <- errorObserved
                || states |> List.exists (function _,PartitionResult.ErrorPartitionStalled _ -> true | _ -> false)

        // start stalling consumer
        use consumer = startConsumer log broker topic group onlyConsumeFirstBatchHandler
        let monitor = mkMonitor log
        use _ = monitor.OnStatus.Subscribe(observeErrorsMonitorHandler)
        use _ = monitor.Start(consumer.Inner, group)
        do! startTimeout().AwaitTimeoutOr(fun () -> Volatile.Read(&errorObserved))
        errorObserved =! true }

type T2(testOutputHelper) =
    let log, broker = createLogger (TestOutputAdapter testOutputHelper), getTestBroker ()

    [<FactIfBroker>]
    let ``Monitor should continue checking progress after rebalance`` () = async {
        let topic, group = newId (), newId () // dev kafka topics are created and truncated automatically
        let producer = mkProducer log broker topic
        let mutable progressChecked, numPartitions = false, 0

        let partitionsObserver(_topic, errors : (int * PartitionResult) list) =
            progressChecked <- true
            numPartitions <- errors.Length

        let! _producerActivity = Async.StartChild <| producerOnePerSecondLoop producer

        use consumerOne = startConsumer log broker topic group onlyConsumeFirstBatchHandler
        let monitor = mkMonitor log
        use _ = monitor.OnStatus.Subscribe(partitionsObserver)
        use _ = monitor.Start(consumerOne.Inner, group)
        // first consumer is only member of group, should have all partitions
        let timeout = startTimeout ()
        do! timeout.AwaitTimeoutOr(fun () -> Volatile.Read(&numPartitions) = testPartitionCount)

        testPartitionCount =! numPartitions

        // create second consumer and join group to trigger rebalance
        use _consumerTwo = startConsumer log broker topic group onlyConsumeFirstBatchHandler
        progressChecked <- false

        // make sure the progress was checked after rebalance
        do! timeout.AwaitTimeoutOr(fun () -> 2 = Volatile.Read(&numPartitions))

        // with second consumer in group, first consumer should have half of the partitions
        2 =! numPartitions
    }

type T3(testOutputHelper) =
    let log, broker = createLogger (TestOutputAdapter testOutputHelper), getTestBroker ()

    [<FactIfBroker>]
    let ``Monitor should not join consumer group`` () = async {
        let topic, group = newId (), newId () // dev kafka topics are created and truncated automatically
        let noopObserver _ = ()
        let config = createConsumerConfig broker topic group
        use consumer = startConsumerFromConfig log config onlyConsumeFirstBatchHandler
        let monitor = mkMonitor log
        use _ = monitor.OnStatus.Subscribe(noopObserver)
        use _ = monitor.Start(consumer.Inner, group)
        do! startTimeout().AwaitTimeoutOr(fun () -> consumer.Inner.Assignment.Count = testPartitionCount)

        // consumer should have all partitions assigned to it
        testPartitionCount =! consumer.Inner.Assignment.Count

        let acc = AdminClientConfig(config.Inner)
        let ac = AdminClientBuilder(acc).Build()

        // should be one member in group
        1 =! ac.ListGroup(group, TimeSpan.FromSeconds 30.).Members.Count
    }
