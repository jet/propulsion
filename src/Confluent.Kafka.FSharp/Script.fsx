#r "bin/Debug/net45/System.Runtime.CompilerServices.Unsafe.dll"
#r "bin/Debug/net45/Confluent.Kafka.dll"
#r "bin/Debug/net45/FSharp.Control.AsyncSeq.dll"
#r "bin/Debug/net45/Confluent.Kafka.FSharp.dll"

open System
open FSharp.Control
open Confluent.Kafka

let envOr env deflt =
    Environment.GetEnvironmentVariable env
    |> String.IsNullOrEmpty
    |> function
        | true -> deflt
        | false -> Environment.GetEnvironmentVariable env
let host = envOr "CK_HOST" "localhost:9092"

let topic = envOr "CK_TOPIC" "test-topic"

let groupId = envOr "CK_GROUP_ID" "test-group"


//
// Create consumer configuration
//
let consumer = 
    Config.Consumer.safe
    |> Config.bootstrapServers host
    |> Config.Consumer.groupId groupId
    |> Config.clientId "test-client"
    |> Config.Consumer.Topic.autoOffsetReset Config.Consumer.Topic.Beginning
    |> Config.debug [Config.DebugFlags.Consumer; Config.DebugFlags.Cgrp]
    |> Consumer.create
    |> Consumer.onLog(fun logger -> 
        printfn "level: %d [%s] [%s]: %s" logger.Level logger.Facility logger.Name logger.Message
    )
//
// Set offset for every partition to "10"
//
let setOffsets =
    let customOffsets = 
        consumer.GetMetadata(true).Topics
        |> Seq.filter(fun m -> m.Topic = topic)
        |> Seq.collect(fun m -> m.Partitions)
        |> Seq.map(fun p -> 
            let myOffset = 10L 
            new TopicPartitionOffset(topic, p.PartitionId, Offset(myOffset))
        )
      
    async {
        let! offsets = 
            consumer.CommitAsync(customOffsets)
            |> Async.AwaitTask
        match offsets.Error.HasError with        
            | true -> printfn "CommitAsync error %s" offsets.Error.Reason
            | false -> 
                offsets.Offsets 
                |> Seq.map(fun o -> sprintf "%d:%d" o.Partition o.Offset.Value)
                |> String.concat ", "
                |> printfn "CommitAsync %s"
    }
    |> Async.RunSynchronously

//
// Consume single message
//
let consume = 
    consumer.Subscribe topic
    async {
        let! first =
            Consumer.stream consumer 10000
            |> AsyncSeq.tryFirst
        printfn "Got message partition: %d @offset:%d" first.Value.Partition first.Value.Offset.Value
    }
    |> Async.RunSynchronously

do ()