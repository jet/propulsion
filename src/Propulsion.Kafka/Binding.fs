namespace Propulsion.Kafka

open Confluent.Kafka
open FsKafka
open Serilog
open System
open System.Collections.Generic

module Binding =
    let mapConsumeResult (x : ConsumeResult<string,string>) = KeyValuePair(x.Message.Key,x.Message.Value)
    let makeTopicPartition (topic : string) (partition : int) = TopicPartition(topic, Partition partition)
    let createConsumer log config : IConsumer<string,string> * (unit -> unit) =
        let consumer = ConsumerBuilder.WithLogging(log, config)
        consumer, consumer.Close
    let inline storeOffset (log : ILogger) (consumer : IConsumer<_,_>) (highWaterMark : ConsumeResult<string,string>) =
        try consumer.StoreOffset(highWaterMark)
        with e -> log.Error(e, "Consuming... storing offsets failed")
    let inline tryConsume (log : ILogger) (consumer : IConsumer<_,_>) (intervalRemainder : TimeSpan) ingest =
        try match consumer.Consume(intervalRemainder) with
            | null -> ()
            | message -> ingest message
        with| :? System.OperationCanceledException -> log.Warning("Consuming... cancelled")
            | :? ConsumeException as e -> log.Warning(e, "Consuming... exception")
