namespace FsKafka

open System
open System.Collections.Generic

[<AutoOpen>]
module Types =
    [<RequireQualifiedAccess; Struct>]
    type CompressionType = None | GZip | Snappy | Lz4

    [<RequireQualifiedAccess; Struct>]
    type Acks = Zero | Leader | All

    [<RequireQualifiedAccess; Struct>]
    type Partitioner = Random | Consistent | ConsistentRandom

    [<RequireQualifiedAccess; Struct>]
    type AutoOffsetReset = Earliest | Latest | None

// Stand-ins for stuff presented in Confluent.Kafka v1
namespace Propulsion.Kafka0.Confluent.Kafka

open FsKafka
open System
open System.Collections.Generic

[<RequireQualifiedAccess>]
module Config =

    [<NoComparison; NoEquality>]
    type ConfigKey<'T> = Key of id : string * render : ('T -> obj) with
        member k.Id = let (Key(id,_)) = k in id

        static member (==>) (Key(id, render) : ConfigKey<'T>, value : 'T) =
            match render value with
            | null -> nullArg id
            | :? string as str when String.IsNullOrWhiteSpace str -> nullArg id
            | obj -> KeyValuePair(id, obj)

    let private mkKey id render = Key(id, render >> box)

    (* shared keys applying to producers and consumers alike *)

    let bootstrapServers    = mkKey "bootstrap.servers" id<string>
    let clientId            = mkKey "client.id" id<string>
    let logConnectionClose  = mkKey "log.connection.close" id<bool>
    let maxInFlight         = mkKey "max.in.flight.requests.per.connection" id<int>
    let retryBackoff        = mkKey "retry.backoff.ms" id<int>
    let socketKeepAlive     = mkKey "socket.keepalive.enable" id<bool>
    let statisticsInterval  = mkKey "statistics.interval.ms" id<int>

    /// Config keys applying to Producers
    module Producer =
        let acks                = mkKey "acks" (function Acks.Zero -> 0 | Acks.Leader -> 1 | Acks.All -> -1)
        // NOTE CK 0.11.4 adds a "compression.type" alias - we use "compression.codec" as 0.11.3 will otherwise throw
        let compression         = mkKey "compression.codec" (function CompressionType.None -> "none" | CompressionType.GZip -> "gzip" | CompressionType.Snappy -> "snappy" | CompressionType.Lz4 -> "lz4")
        let linger              = mkKey "linger.ms" id<int>
        let messageSendRetries  = mkKey "message.send.max.retries" id<int>
        let partitioner         = mkKey "partitioner" (function Partitioner.Random -> "random" | Partitioner.Consistent -> "consistent" | Partitioner.ConsistentRandom -> "consistent_random")

     /// Config keys applying to Consumers
    module Consumer =
        let autoCommitInterval  = mkKey "auto.commit.interval.ms" id<int>
        let autoOffsetReset     = mkKey "auto.offset.reset" (function AutoOffsetReset.Earliest -> "earliest" | AutoOffsetReset.Latest -> "latest" | AutoOffsetReset.None -> "none")
        let enableAutoCommit    = mkKey "enable.auto.commit" id<bool>
        let enableAutoOffsetStore = mkKey "enable.auto.offset.store" id<bool>
        let groupId             = mkKey "group.id" id<string>
        let fetchMaxBytes       = mkKey "fetch.message.max.bytes" id<int>
        let fetchMinBytes       = mkKey "fetch.min.bytes" id<int>

[<AutoOpen>]
module private NullableHelpers =
    let (|Null|HasValue|) (x:Nullable<'T>) =
        if x.HasValue then HasValue x.Value
        else Null

type ProducerConfig() =
    let vals = Dictionary<string,obj>()
    let set key value = vals.[key] <- box value

    member __.Set(key, value) = set key value

    member val ClientId = null with get, set
    member val BootstrapServers = null with get, set
    member val RetryBackoffMs = Nullable() with get, set
    member val MessageSendMaxRetries = Nullable() with get, set
    member val Acks = Nullable() with get, set
    member val SocketKeepaliveEnable = Nullable() with get, set
    member val LogConnectionClose = Nullable() with get, set
    member val MaxInFlight = Nullable() with get, set
    member val LingerMs = Nullable() with get, set
    member val Partitioner = Nullable() with get, set
    member val CompressionType = Nullable() with get, set
    member val StatisticsIntervalMs = Nullable() with get, set

    member __.Render() : KeyValuePair<string,obj>[] =
        [|  match __.ClientId               with null -> () | v ->          yield Config.clientId ==> v
            match __.BootstrapServers       with null -> () | v ->          yield Config.bootstrapServers ==> v
            match __.RetryBackoffMs         with Null -> () | HasValue v -> yield Config.retryBackoff ==> v
            match __.MessageSendMaxRetries  with Null -> () | HasValue v -> yield Config.Producer.messageSendRetries ==> v
            match __.Acks                   with Null -> () | HasValue v -> yield Config.Producer.acks ==> v
            match __.SocketKeepaliveEnable  with Null -> () | HasValue v -> yield Config.socketKeepAlive ==> v
            match __.LogConnectionClose     with Null -> () | HasValue v -> yield Config.logConnectionClose ==> v
            match __.MaxInFlight            with Null -> () | HasValue v -> yield Config.maxInFlight ==> v
            match __.LingerMs               with Null -> () | HasValue v -> yield Config.Producer.linger ==> v
            match __.Partitioner            with Null -> () | HasValue v -> yield Config.Producer.partitioner ==> v
            match __.CompressionType        with Null -> () | HasValue v -> yield Config.Producer.compression ==> v
            match __.StatisticsIntervalMs   with Null -> () | HasValue v -> yield Config.statisticsInterval ==> v
            yield! vals |]

type ConsumerConfig() =
    let vals = Dictionary<string,obj>()
    let set key value = vals.[key] <- box value

    member __.Set(key, value) = set key value

    member val ClientId = null with get, set
    member val BootstrapServers = null with get, set
    member val GroupId = null with get, set
    member val AutoOffsetReset = Nullable() with get, set
    member val FetchMaxBytes = Nullable() with get, set
    member val EnableAutoCommit = Nullable() with get, set
    member val EnableAutoOffsetStore = Nullable() with get, set
    member val LogConnectionClose = Nullable() with get, set
    member val FetchMinBytes = Nullable() with get, set
    member val StatisticsIntervalMs = Nullable() with get, set
    member val AutoCommitIntervalMs = Nullable() with get, set

    member __.Render() : KeyValuePair<string,obj>[] =
        [|  match __.ClientId               with null -> () | v ->          yield Config.clientId ==> v
            match __.BootstrapServers       with null -> () | v ->          yield Config.bootstrapServers ==> v
            match __.GroupId                with null -> () | v ->          yield Config.Consumer.groupId ==> v
            match __.AutoOffsetReset        with Null -> () | HasValue v -> yield Config.Consumer.autoOffsetReset ==> v
            match __.FetchMaxBytes          with Null -> () | HasValue v -> yield Config.Consumer.fetchMaxBytes ==> v
            match __.LogConnectionClose     with Null -> () | HasValue v -> yield Config.logConnectionClose ==> v
            match __.EnableAutoCommit       with Null -> () | HasValue v -> yield Config.Consumer.enableAutoCommit ==> v
            match __.EnableAutoOffsetStore  with Null -> () | HasValue v -> yield Config.Consumer.enableAutoOffsetStore ==> v
            match __.FetchMinBytes          with Null -> () | HasValue v -> yield Config.Consumer.fetchMinBytes ==> v
            match __.AutoCommitIntervalMs   with Null -> () | HasValue v -> yield Config.Consumer.autoCommitInterval ==> v
            match __.StatisticsIntervalMs   with Null -> () | HasValue v -> yield Config.statisticsInterval ==> v
            yield! vals |]
