﻿namespace Propulsion.Kafka

open Confluent.Kafka
open FsKafka
open Serilog
open System
open System.Collections.Generic

type IConsumer<'K,'V> = Consumer<'K,'V>
type ConsumeResult<'K,'V> = Message<'K,'V>

module Bindings =
    let mapMessage : ConsumeResult<_,_> -> Message<_,_> = id
    let mapConsumeResult (x : ConsumeResult<string,string>) = KeyValuePair(x.Key,x.Value)
    let inline partitionId (x : ConsumeResult<_,_>) = x.Partition
    let inline topicPartition (topic : string) (partition : int) = TopicPartition(topic, partition)
    let inline tp (x : ConsumeResult<_,_>) = x.TopicPartition
    let partitionValue = id
    let offsetUnset = Offset.Invalid
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
