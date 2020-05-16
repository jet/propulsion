namespace Propulsion.Kafka

open Confluent.Kafka
open FsKafka
open Serilog
open System
open System.Collections.Generic

module Binding =
    let mapConsumeResult (x : ConsumeResult<string,string>) = KeyValuePair(x.Key,x.Value)
    let inline makeTopicPartition (topic : string) (partition : int) = TopicPartition(topic, partition)
    let createConsumer log config : IConsumer<string,string> * (unit -> unit) =
        ConsumerBuilder.WithLogging(log, config)
    let inline storeOffset (log : ILogger) (consumer : IConsumer<_,_>) (highWaterMark : ConsumeResult<string,string>) =
        try let e = consumer.StoreOffset(highWaterMark)
            if e.Error.HasError then log.Error("Consuming... storing offsets failed {@e}", e.Error)
        with e -> log.Error(e, "Consuming... storing offsets failed")
    let inline tryConsume (log : ILogger) (consumer : IConsumer<_,_>) (intervalRemainder : TimeSpan) ingest =
        try let mutable message = null
            if consumer.Consume(&message, intervalRemainder) then
                if message.Error.HasError then log.Warning("Consuming... error {e}", message.Error)
                else ingest message
        with| :? System.OperationCanceledException -> log.Warning("Consuming... cancelled")
