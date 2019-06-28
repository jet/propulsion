// Implements a watchdog that can be used to have a service self-detect stalled consumers and/or consistently growing lags
// Adapted from https://github.com/linkedin/Burrow by @jgardella
namespace Propulsion.Kafka

open Confluent.Kafka
open Jet.ConfluentKafka.FSharp
open Serilog
open System
open System.Diagnostics

type PartitionResult =
    | OkReachedZero // check 1
    | WarningLagIncreasing // check 3
    | ErrorPartitionStalled of lag: int64 // check 2
    | Healthy

module MonitorImpl =
#if NET461
    module Array =
        let head xs = Seq.head xs
        let last xs = Seq.last xs
#endif
    module private Map =
        let mergeChoice (f:'a -> Choice<'b * 'c, 'b, 'c> -> 'd) (map1:Map<'a, 'b>) (map2:Map<'a, 'c>) : Map<'a, 'd> =
          Set.union (map1 |> Seq.map (fun k -> k.Key) |> set) (map2 |> Seq.map (fun k -> k.Key) |> set)
          |> Seq.map (fun k ->
            match Map.tryFind k map1, Map.tryFind k map2 with
            | Some b, Some c -> k, f k (Choice1Of3 (b,c))
            | Some b, None   -> k, f k (Choice2Of3 b)
            | None,   Some c -> k, f k (Choice3Of3 c)
            | None,   None   -> failwith "invalid state")
          |> Map.ofSeq

    /// Progress information for a consumer in a group.
    type [<NoComparison>] private ConsumerProgressInfo =
        {   /// The consumer group id.
            group : string

            /// The name of the kafka topic.
            topic : string

            /// Progress info for each partition.
            partitions : ConsumerPartitionProgressInfo[]

            /// The total lag across all partitions.
            totalLag : int64

            /// The minimum lead across all partitions.
            minLead : int64 }
    /// Progress information for a consumer in a group, for a specific topic-partition.
    and [<NoComparison>] private ConsumerPartitionProgressInfo =
        {   /// The partition id within the topic.
            partition : int

            /// The consumer's current offset.
            consumerOffset : Offset

            /// The offset at the current start of the topic.
            earliestOffset : Offset

            /// The offset at the current end of the topic.
            highWatermarkOffset : Offset

            /// The distance between the high watermark offset and the consumer offset.
            lag : int64

            /// The distance between the consumer offset and the earliest offset.
            lead : int64

            /// The number of messages in the partition.
            messageCount : int64 }

    /// Operations for providing consumer progress information.
    module private ConsumerInfo =
                
        /// Returns consumer progress information.
        /// Note that this does not join the group as a consumer instance
        let progress (timeout : TimeSpan) (consumer:IConsumer<'k,'v>) (topic:string) (ps:int[]) = async {
            let topicPartitions = ps |> Seq.map (Bindings.topicPartition topic)

            let sw = System.Diagnostics.Stopwatch.StartNew()
            let committedOffsets =
                consumer.Committed(topicPartitions, timeout)
                |> Seq.sortBy(fun e -> Bindings.partitionValue e.Partition)
                |> Seq.map(fun e -> Bindings.partitionValue e.Partition, e)
                |> Map.ofSeq
            
            let timeout = let elapsed = sw.Elapsed in if elapsed > timeout then TimeSpan.Zero else timeout - elapsed
            let! watermarkOffsets =
                topicPartitions
                |> Seq.map(fun tp -> async {
                    return Bindings.partitionValue tp.Partition, consumer.QueryWatermarkOffsets(tp, timeout)} )
                |> Async.Parallel
            let watermarkOffsets = watermarkOffsets |> Map.ofArray

            let partitions =
                (watermarkOffsets, committedOffsets)
                ||> Map.mergeChoice (fun p -> function
                    | Choice1Of3 (hwo,cOffset) ->
                        let e,l,o = (let v = hwo.Low in v.Value),(let v = hwo.High in v.Value),let v = cOffset.Offset in v.Value
                        // Consumer offset of (Invalid Offset -1001) indicates that no consumer offset is present.  In this case, we should calculate lag as the high water mark minus earliest offset
                        let lag, lead =
                          match o with
                          | offset when offset = let v = Bindings.offsetUnset in v.Value -> l - e, 0L
                          | _ -> l - o, o - e
                        { partition = p ; consumerOffset = cOffset.Offset ; earliestOffset = hwo.Low ; highWatermarkOffset = hwo.High ; lag = lag ; lead = lead ; messageCount = l - e }
                    | Choice2Of3 hwo ->
                        // in the event there is no consumer offset present, lag should be calculated as high watermark minus earliest
                        // this prevents artifically high lags for partitions with no consumer offsets
                        let e,l = (let v = hwo.Low in v.Value),let v = hwo.High in v.Value
                        { partition = p ; consumerOffset = Bindings.offsetUnset; earliestOffset = hwo.Low ; highWatermarkOffset = hwo.High ; lag = l - e ; lead = 0L ; messageCount = l - e }
                        //failwithf "unable to find consumer offset for topic=%s partition=%i" topic p
                    | Choice3Of3 o ->
                        let invalid = Bindings.offsetUnset
                        { partition = p ; consumerOffset = o.Offset ; earliestOffset = invalid ; highWatermarkOffset = invalid ; lag = invalid.Value ; lead = invalid.Value ; messageCount = -1L })
                |> Seq.map (fun kvp -> kvp.Value)
                |> Seq.toArray

            return {
                topic = topic ; group = consumer.Name ; partitions = partitions
                totalLag = partitions |> Seq.sumBy (fun p -> p.lag)
                minLead =
                    if partitions.Length > 0 then
                        partitions |> Seq.map (fun p -> p.lead) |> Seq.min
                    else let v = Bindings.offsetUnset in v.Value } }
    
    type PartitionInfo =
        {   partition : int
            consumerOffset : OffsetValue
            earliestOffset : OffsetValue
            highWatermarkOffset : OffsetValue
            lag : int64 }

    [<NoComparison>]
    type Window = Window of PartitionInfo []

    let private toPartitionInfo (info : ConsumerPartitionProgressInfo) = {
        partition = info.partition
        consumerOffset = OffsetValue.ofOffset info.consumerOffset
        earliestOffset = OffsetValue.ofOffset info.earliestOffset
        highWatermarkOffset = OffsetValue.ofOffset info.highWatermarkOffset
        lag = info.lag }
    let private createPartitionInfoList (info : ConsumerProgressInfo) =
        Window (Array.map toPartitionInfo info.partitions)

    // Naive insert and copy out buffer
    type private RingBuffer<'A> (capacity : int) =
        let buffer : 'A [] = Array.zeroCreate capacity
        let mutable head,tail,size = 0,-1,0

        member __.TryCopyFull() =
            if size <> capacity then None
            else
                let arr = Array.zeroCreate size
                let mutable i = head
                for x = 0 to size - 1 do
                    arr.[x] <- buffer.[i % capacity]
                    i <- i + 1
                Some arr

        member __.Add(x : 'A) =
            tail <- (tail + 1) % capacity
            buffer.[tail] <- x
            if (size < capacity) then
                size <- size + 1
            else
                head <- (head + 1) % capacity

        member __.Clear() =
            head <- 0
            tail <- -1
            size <- 0

    module Rules =

        // Rules taken from https://github.com/linkedin/Burrow
        // Rule 1:  If over the stored period, the lag is ever zero for the partition, the period is OK
        // Rule 2:  If the consumer offset does not change, and the lag is non-zero, it's an error (partition is stalled)
        // Rule 3:  If the consumer offsets are moving, but the lag is consistently increasing, it's a warning (consumer is slow)

        // The following rules are not implementable given our poll based implementation - they should also not be needed
        // Rule 4:  If the difference between now and the lastPartition offset timestamp is greater than the difference between the lastPartition and firstPartition offset timestamps, the
        //          consumer has stopped committing offsets for that partition (error), unless
        // Rule 5:  If the lag is -1, this is a special value that means there is no broker offset yet. Consider it good (will get caught in the next refresh of topics)

        // If lag is ever zero in the window, no other checks needed
        let checkRule1 (partitionInfoWindow : PartitionInfo []) =
            partitionInfoWindow |> Array.exists (fun i -> i.lag = 0L)

        // If there is lag, the offsets should be progressing in window
        let checkRule2 (partitionInfoWindow : PartitionInfo []) =
            let offsetsIndicateLag (firstConsumerOffset : OffsetValue) (lastConsumerOffset : OffsetValue) =
                match (firstConsumerOffset, lastConsumerOffset) with
                | Valid validFirst, Valid validLast ->
                    validLast - validFirst <= 0L
                | Unset, Valid _ ->
                    // Partition got its initial offset value this window, check again next window.
                    false
                | Valid _, Unset ->
                    // Partition somehow lost its offset in this window, something's probably wrong.
                    true
                | Unset, Unset ->
                    // Partition has invalid offsets for the entire window, there may be lag.
                    true

            let firstWindowPartitions = partitionInfoWindow |> Array.head
            let lastWindowPartitions = partitionInfoWindow |> Array.last

            let checkPartitionForLag (firstWindowPartition : PartitionInfo) (lastWindowPartition : PartitionInfo)  =
                match lastWindowPartition.lag with
                | 0L -> None
                | lastPartitionLag when offsetsIndicateLag firstWindowPartition.consumerOffset lastWindowPartition.consumerOffset ->
                    if lastWindowPartition.partition <> firstWindowPartition.partition then failwithf "Partitions did not match in rule2"
                    Some lastPartitionLag
                | _ -> None

            checkPartitionForLag firstWindowPartitions lastWindowPartitions

        // Has the lag reduced between steps in the window
        let checkRule3 (partitionInfoWindow : PartitionInfo []) =
            let lagDecreasing =
                partitionInfoWindow
                |> Seq.pairwise
                |> Seq.exists (fun (prev, curr) -> curr.lag < prev.lag)

            not lagDecreasing

        let checkRulesForPartition (partitionInfoWindow : PartitionInfo []) =
            if checkRule1 partitionInfoWindow then OkReachedZero else

            match checkRule2 partitionInfoWindow with
            | Some lag ->
                ErrorPartitionStalled lag
            | None when checkRule3 partitionInfoWindow ->
                WarningLagIncreasing
            | _ ->
                Healthy

        let checkRulesForAllPartitions (windows : Window []) =
            windows
            |> Seq.collect (fun (Window partitionInfo) -> partitionInfo)
            |> Seq.groupBy (fun p -> p.partition)
            |> Seq.map (fun (p, info) -> p, checkRulesForPartition (Array.ofSeq info))

    let private queryConsumerProgress intervalMs  (consumer : IConsumer<'k,'v>) (topic : string) = async {
        let partitionIds = [| for t in consumer.Assignment do if t.Topic = topic then yield Bindings.partitionValue t.Partition |] 
        let! r = ConsumerInfo.progress intervalMs consumer topic partitionIds
        return createPartitionInfoList r }

    let run (consumer : IConsumer<'k,'v> ) (interval,windowSize,failResetCount) (topic : string) (group : string) (onQuery,onCheckFailed,onStatus) =
        let getAssignedPartitions () = seq { for x in consumer.Assignment do if x.Topic = topic then yield Bindings.partitionValue x.Partition }
        let buffer = new RingBuffer<_>(windowSize)
        let validateAssignments =
            let mutable assignments = getAssignedPartitions() |> set
            fun () ->
                let current = getAssignedPartitions() |> set
                if current <> assignments then
                    buffer.Clear()
                    assignments <- current
                assignments.Count <> 0

        let checkConsumerProgress () = async {
            let! res = queryConsumerProgress interval consumer topic
            onQuery res
            buffer.Add res
            match buffer.TryCopyFull() with
            | None -> ()
            | Some ci ->
                let states = Rules.checkRulesForAllPartitions ci |> List.ofSeq
                onStatus topic group states }

        let rec loop failCount = async {
            let sw = Stopwatch.StartNew()
            let! failCount = async {
                try if validateAssignments () then
                        do! checkConsumerProgress()
                    return 0
                with exn ->
                    let count' = failCount + 1
                    // If it's been too long since we've successfully obtained a reading, discard preceding values to avoid false positives e.g. re stalled consumers
                    if count' = failResetCount then
                        buffer.Clear()
                    onCheckFailed count' exn
                    return count'
            }
            match sw.Elapsed with
            | e when e < interval ->
                let rem = interval-e
                do! Async.Sleep (int rem.TotalMilliseconds)
            | _ -> ()
            return! loop failCount }
        loop 0

    module Logging =

        let logResults (log : ILogger) topic group (partitionResults : (int * PartitionResult) seq) =
            let cat = function
                | OkReachedZero | Healthy -> Choice1Of3 ()
                | ErrorPartitionStalled _lag -> Choice2Of3 ()
                | WarningLagIncreasing -> Choice3Of3 ()
            match partitionResults |> Seq.groupBy (snd >> cat) |> List.ofSeq with
            | [ Choice1Of3 (), _ ] -> log.Information("Monitoring... {topic}/{group} Healthy", topic, group)
            | errs ->
                for res in errs do
                    match res with
                    | Choice1Of3 (), _ -> ()
                    | Choice2Of3 (), errs ->
                        let lag = function (partitionId, ErrorPartitionStalled lag) -> Some (partitionId,lag) | x -> failwithf "mismapped %A" x
                        log.Error("Monitoring... {topic}/{group} Stalled with backlogs on {@stalled} [(partition,lag)]", topic, group, errs |> Seq.choose lag)
                    | Choice3Of3 (), warns -> 
                        log.Warning("Monitoring... {topic}/{group} Growing lags on {@partitionIds}", topic, group, warns |> Seq.map fst)

        let logLatest (logger : ILogger) (topic : string) (consumerGroup : string) (Window partitionInfos) =
            let partitionOffsets =
                partitionInfos
                |> Seq.sortBy (fun p -> p.partition)
                |> Seq.map (fun p -> p.partition, p.highWatermarkOffset, p.consumerOffset)

            let aggregateLag = partitionInfos |> Seq.sumBy (fun p -> p.lag)

            logger.Information("Monitoring... {topic}/{consumerGroup} lag {lag} offsets {offsets}",
                topic, consumerGroup, aggregateLag, partitionOffsets)

        let logFailure (log : ILogger) (topic : string) (group : string) failCount exn =
            log.Warning(exn, "Monitoring... {topic}/{group} Exception # {failCount}", topic, group, failCount)

/// Used to manage a set of bacground tasks that perdically (based on `interval`) grab the broker's recorded high/low watermarks
/// and then map that to a per-partition status for each partition that the consumer being observed has been assigned
type KafkaMonitor<'k,'v>
    (   log : ILogger,
        /// Interval between checks of high/low watermarks. Default 30s
        ?interval,
        /// Number if readings per partition to use in order to make inferences. Default 10 (at default interval of 30s, implies a 5m window).
        ?windowSize,
        /// Number of failed calls to broker that should trigger discarding of buffered readings in order to avoid false positives. Default 3.
        ?failResetCount) =
    let failResetCount = defaultArg failResetCount 3
    let interval = defaultArg interval (TimeSpan.FromSeconds 30.)
    let windowSize = defaultArg windowSize 10
    let onStatus, onCheckFailed = new Event<string*(int *PartitionResult) list>(), new Event<string*int*exn>()

    /// Periodically supplies the status for all assigned partitions (whenever we've gathered `windowSize` of readings)
    /// Subscriber can e.g. use this to force a consumer restart if no progress is being made
    [<CLIEvent>] member __.OnStatus = onStatus.Publish

    /// Raised whenever call to broker to ascertain watermarks has failed
    /// Subscriber can e.g. raise an alert if enough consecutive failures have occurred
    [<CLIEvent>] member __.OnCheckFailed = onCheckFailed.Publish

    // One of these runs per topic
    member private __.Pump(consumer, topic, group) =
        let onQuery res = 
            MonitorImpl.Logging.logLatest log topic group res
        let onStatus topic group xs =
            MonitorImpl.Logging.logResults log topic group xs
            onStatus.Trigger(topic, xs)
        let onCheckFailed count exn =
            MonitorImpl.Logging.logFailure log topic group count exn
            onCheckFailed.Trigger(topic, count, exn)
        MonitorImpl.run consumer (interval,windowSize,failResetCount) topic group (onQuery,onCheckFailed,onStatus)

    /// Commences a child task per subscribed topic that will ob
    member __.StartAsChild(target : IConsumer<'k,'v>, group) = async {
        for topic in target.Subscription do
            Async.Start(__.Pump(target, topic, group)) }