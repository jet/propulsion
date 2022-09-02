// Manages efficiently and continuously reading from the Confluent.Kafka consumer, offloading the pushing of those batches onward to the Submitter
// Responsible for ensuring we don't over-read, which would cause the rdkafka buffers to overload the system in terms of memory usage
namespace Propulsion.Kafka

open Confluent.Kafka
open FsCodec
open FsKafka
open Propulsion
open Propulsion.Internal // intervalCheck, AwaitTaskCorrect
open Propulsion.Streams
open Serilog
open System
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks

[<AutoOpen>]
module private Impl =

    /// Retains the messages we've accumulated for a given Partition
    [<NoComparison>]
    type PartitionBuffer<'M> =
        {   mutable reservation : int64 // accumulate reserved in flight bytes so we can reverse the reservation when it completes
            mutable highWaterMark : ConsumeResult<string, string> // hang on to it so we can generate a checkpointing lambda
            messages : ResizeArray<'M> }
        member x.Enqueue(sz, message, mapMessage) =
            x.highWaterMark <- message
            x.reservation <- x.reservation + sz // size we need to unreserve upon completion
            x.messages.Add(mapMessage message)
        static member Create(sz, message, mapMessage) =
            let x = { reservation = 0L; highWaterMark = null; messages = ResizeArray(256) }
            x.Enqueue(sz, message, mapMessage)
            x

    /// guesstimate approximate message size in bytes
    let approximateMessageBytes (m : Message<string, string>) =
        let inline len (x : string) = match x with null -> 0 | x -> sizeof<char> * x.Length
        16 + len m.Key + len m.Value |> int64

module private Binding =

    let mapConsumeResult (result : ConsumeResult<string,string>) =
        let m = Binding.message result
        if m = null then invalidOp "Cannot dereference null message"
        KeyValuePair(m.Key, m.Value)

/// Continuously polls across the assigned partitions, building spans; periodically (at intervals of `emitInterval`), `submit`s accumulated messages as
///   checkpointable Batches
/// Pauses if in-flight upper threshold is breached until such time as it drops below that the lower limit
type KafkaIngestionEngine<'Info>
    (   log : ILogger, counter : Core.InFlightMessageCounter, consumer : IConsumer<_, _>, closeConsumer,
        mapMessage : ConsumeResult<_, _> -> 'Info, emit : Submission.Batch<TopicPartition, 'Info>[] -> unit,
        maxBatchSize, emitInterval, statsInterval) =
    let acc = Dictionary<TopicPartition, _>()
    let ingestionWindow = IntervalTimer emitInterval
    let mutable intervalMsgs, intervalChars, totalMessages, totalChars = 0L, 0L, 0L, 0L
    let dumpStats () =
        totalMessages <- totalMessages + intervalMsgs; totalChars <- totalChars + intervalChars
        log.Information("Ingested {msgs:n0}m, {chars:n0}c In-flight ~{inflightMb:n1}MB Σ {totalMessages:n0} messages, {totalChars:n0} chars",
            intervalMsgs, intervalChars, counter.InFlightMb, totalMessages, totalChars)
        intervalMsgs <- 0L; intervalChars <- 0L
    let maybeLogStats =
        let interval = IntervalTimer statsInterval
        fun () -> if interval.IfDueRestart() then dumpStats ()
    let mkSubmission topicPartition span : Submission.Batch<'S, 'M> =
        let checkpoint () =
            counter.Delta(-span.reservation) // counterbalance Delta(+) per ingest, below
            try consumer.StoreOffset(span.highWaterMark)
            with e -> log.Error(e, "Consuming... storing offsets failed")
        { source = topicPartition; onCompletion = checkpoint; messages = span.messages.ToArray() }
    let ingest (result : ConsumeResult<string, string>) =
        let m = result.Message
        if m = null then invalidOp "Cannot dereference null message"
        let sz = approximateMessageBytes m
        counter.Delta(+sz) // counterbalanced by Delta(-) in checkpoint(), below
        intervalMsgs <- intervalMsgs + 1L
        let inline stringLen (s : string) = match s with null -> 0 | x -> x.Length
        intervalChars <- intervalChars + int64 (stringLen m.Key + stringLen m.Value)
        let tp = result.TopicPartition
        let span =
            match acc.TryGetValue tp with
            | false, _ -> let span = PartitionBuffer<'Info>.Create(sz, result, mapMessage) in acc[tp] <- span; span
            | true, span -> span.Enqueue(sz, result, mapMessage); span
        if span.messages.Count = maxBatchSize then
            acc.Remove tp |> ignore
            emit [| mkSubmission tp span |]
    let submit () =
        match acc.Count with
        | 0 -> ()
        | topicPartitionsWithMessagesThisInterval ->
            let tmp = ResizeArray<Submission.Batch<_, 'Info>>(topicPartitionsWithMessagesThisInterval)
            for KeyValue(tp, span) in acc do
                tmp.Add(mkSubmission tp span)
            acc.Clear()
            emit <| tmp.ToArray()
    member _.Pump(ct : CancellationToken) = task {
        use _ = consumer // Dispose it at the end (NB but one has to Close first or risk AccessViolations etc)
        try while not ct.IsCancellationRequested do
                if counter.IsOverLimitNow() then
                    let busyWork () =
                        submit()
                        maybeLogStats()
                    counter.AwaitThreshold(ct, consumer, busyWork)
                elif ingestionWindow.IfDueRestart() then
                    submit ()
                    maybeLogStats ()
                else
                    try match consumer.Consume(millisecondsTimeout = ingestionWindow.RemainingMs) with
                        | null -> ()
                        | message -> ingest message
                    with  :? OperationCanceledException -> log.Warning("Consuming... cancelled")
                        | :? ConsumeException as e -> log.Warning(e, "Consuming... exception")
        finally
            submit () // We don't want to leak our reservations against the counter and want to pass of messages we ingested
            dumpStats () // Unconditional logging when completing
            closeConsumer () (* Orderly Close() before Dispose() is critical *) }

/// Consumes according to the `config` supplied to `Start`, until `Stop()` is requested or `handle` yields a fault.
/// Conclusion of processing can be awaited by via `AwaitShutdown` or `AwaitWithStopOnCancellation`.
type ConsumerPipeline private (inner : IConsumer<string, string>, task : Task<unit>, triggerStop) =
    inherit Pipeline(task, triggerStop)

    /// Provides access to the Confluent.Kafka interface directly
    member _.Inner = inner

    /// Builds a processing pipeline per the `config` running up to `dop` instances of `handle` concurrently to maximize global throughput across partitions.
    /// Processor pumps until `handle` yields a `Choice2Of2` or `Stop()` is requested.
    static member Start(log : ILogger, config : KafkaConsumerConfig, mapResult, submit, pumpSubmitter, pumpScheduler, pumpDispatcher, statsInterval) =
        let maxDelay, maxItems = config.Buffering.maxBatchDelay, config.Buffering.maxBatchSize
        log.Information("Consuming... {bootstrapServers} {topics} {groupId} autoOffsetReset {autoOffsetReset} fetchMaxBytes={fetchMaxB} maxInFlight={maxInFlightGB:n1}GB maxBatchDelay={maxBatchDelay}s maxBatchSize={maxBatchSize}",
            config.Inner.BootstrapServers, config.Topics, config.Inner.GroupId, (let x = config.Inner.AutoOffsetReset in x.Value), config.Inner.FetchMaxBytes,
            Log.miB config.Buffering.maxInFlightBytes / 1024., maxDelay.TotalSeconds, maxItems)
        let limiterLog = log.ForContext(Serilog.Core.Constants.SourceContextPropertyName, Core.Constants.messageCounterSourceContext)
        let limiter = Core.InFlightMessageCounter(limiterLog, config.Buffering.minInFlightBytes, config.Buffering.maxInFlightBytes)
        let consumer = ConsumerBuilder.WithLogging(log, config.Inner) // teardown is managed by ingester.Pump
        consumer.Subscribe config.Topics
        let ingester = KafkaIngestionEngine<'M>(log, limiter, consumer, consumer.Close, mapResult, submit, maxItems, maxDelay, statsInterval=statsInterval)
        let cts = new CancellationTokenSource()
        let triggerStop () =
            let level = if cts.IsCancellationRequested then Events.LogEventLevel.Debug else Events.LogEventLevel.Information
            log.Write(level, "Consuming... Stopping {name}", consumer.Name)
            cts.Cancel()
        let ct = cts.Token
        let tcs = TaskCompletionSource<unit>()
        // if scheduler encounters a faulted handler, we propagate that as the consumer's Result
        let abend (exns : AggregateException) =
            if tcs.TrySetException(exns) then log.Warning(exns, "Cancelling processing due to {count} faulted handlers", exns.InnerExceptions.Count)
            else log.Information("Failed setting {count} exceptions", exns.InnerExceptions.Count)
            // NB cancel needs to be after TSE or the Register(TSE) will win
            cts.Cancel()
        let start (name : string) (f : CancellationToken -> Task<unit>) =
            let wrap () = task {
                try do! f ct
                    log.Information("Exiting pipeline component {name}", name)
                with e ->
                    log.Fatal(e, "Abend from pipeline component {name}", name)
                    triggerStop () }
            Task.start wrap

        let supervise () = task {
            // external cancellation should yield a success result
            use _ = ct.Register(fun _ -> tcs.TrySetResult () |> ignore)

            start "dispatcher" pumpDispatcher
            // ... fault results from dispatched tasks result in the `machine` concluding with an exception
            start "scheduler" (pumpScheduler abend)
            start "submitter" pumpSubmitter
            start "ingester" ingester.Pump

            // await for either handler-driven abend or external cancellation via Stop()
            return! tcs.Task
        }
        let task = Task.Run<unit>(supervise)
        new ConsumerPipeline(consumer, task, triggerStop)

type ParallelConsumer private () =

    /// Builds a processing pipeline per the `config` running up to `dop` instances of `handle` concurrently to maximize global throughput across partitions.
    /// Processor pumps until `handle` yields a `Choice2Of2` or `Stop()` is requested.
    static member Start<'Msg>
        (   log : ILogger, config : KafkaConsumerConfig, maxDop,
            mapResult : ConsumeResult<string, string> -> 'Msg,
            handle : 'Msg -> Async<Choice<unit, exn>>,
            // Default 5
            ?maxSubmissionsPerPartition,
            // Default 5m
            ?statsInterval, ?logExternalStats) =
        let statsInterval = defaultArg statsInterval (TimeSpan.FromMinutes 5.)
        let maxSubmissionsPerPartition = defaultArg maxSubmissionsPerPartition 5

        let dispatcher = Parallel.Scheduling.Dispatcher maxDop
        let scheduler = Parallel.Scheduling.PartitionedSchedulingEngine<_, 'Msg>(log, handle, dispatcher.TryAdd, statsInterval, ?logExternalStats=logExternalStats)
        let mapBatch onCompletion (x : Submission.Batch<_, _>) : Parallel.Scheduling.Batch<_, 'Msg> =
            let onCompletion' () = x.onCompletion(); onCompletion()
            { source = x.source; messages = x.messages; onCompletion = onCompletion'; }
        let submitBatch (x : Parallel.Scheduling.Batch<_, _>) : int =
            scheduler.Submit x
            x.messages.Length
        let submitter = Submission.SubmissionEngine(log, maxSubmissionsPerPartition, mapBatch, submitBatch, statsInterval)
        ConsumerPipeline.Start(log, config, mapResult, submitter.Ingest, submitter.Pump, (fun abend ct -> scheduler.Pump(abend, ct)), dispatcher.Pump, statsInterval)

    /// Builds a processing pipeline per the `config` running up to `dop` instances of `handle` concurrently to maximize global throughput across partitions.
    /// Processor pumps until `handle` yields a `Choice2Of2` or `Stop()` is requested.
    static member Start
        (   log : ILogger, config : KafkaConsumerConfig, maxDop, handle : KeyValuePair<string, string> -> Async<unit>,
            // Default 5
            ?maxSubmissionsPerPartition,
            // Default 5m
            ?statsInterval, ?logExternalStats) =
        ParallelConsumer.Start<KeyValuePair<string, string>>(log, config, maxDop, Binding.mapConsumeResult, handle >> Async.Catch,
            ?maxSubmissionsPerPartition=maxSubmissionsPerPartition, ?statsInterval=statsInterval, ?logExternalStats=logExternalStats)

/// APIs only required for advanced scenarios (specifically the integration tests)
/// APIs within are not part of the stable API and are subject to unlimited change
module Core =

    type StreamsConsumer =

        static member Start<'Info, 'Outcome>
            (   log : ILogger, config : KafkaConsumerConfig, resultToInfo, infoToStreamEvents,
                prepare, handle, maxDop,
                stats : Scheduling.Stats<struct (StreamSpan.Metrics * 'Outcome), struct (StreamSpan.Metrics * exn)>, statsInterval,
                ?maxSubmissionsPerPartition, ?logExternalState,
                ?maxBatches, ?purgeInterval, ?wakeForResults, ?idleDelay,
                ?maximizeOffsetWriting) =
            let maxSubmissionsPerPartition = defaultArg maxSubmissionsPerPartition 5
            let dispatcher = Dispatch.ItemDispatcher<_, _> maxDop
            let dumpStreams logStreamStates log =
                logExternalState |> Option.iter (fun f -> f log)
                logStreamStates Default.eventSize
            let streamsScheduler =
                Scheduling.StreamSchedulingEngine.Create<_, _, _, _>(
                    dispatcher, stats, prepare, handle, SpanResult.toIndex, dumpStreams,
                    ?maxBatches = maxBatches, ?purgeInterval = purgeInterval, ?wakeForResults = wakeForResults, ?idleDelay = idleDelay)
            let mapConsumedMessagesToStreamsBatch onCompletion (x : Submission.Batch<TopicPartition, 'Info>) : Buffer.Batch<_> =
                let onCompletion () = x.onCompletion(); onCompletion()
                Buffer.Batch.Create(onCompletion, Seq.collect infoToStreamEvents x.messages) |> ValueTuple.fst
            let submitter =
                Projector.StreamsSubmitter.Create
                    (   log, maxSubmissionsPerPartition, mapConsumedMessagesToStreamsBatch,
                        streamsScheduler.Submit, statsInterval,
                        ?disableCompaction=maximizeOffsetWriting)
            ConsumerPipeline.Start(log, config, resultToInfo, submitter.Ingest, submitter.Pump, (fun _abend -> streamsScheduler.Pump), dispatcher.Pump, statsInterval)

        static member Start<'Info, 'Outcome>
            (   log : ILogger, config : KafkaConsumerConfig, consumeResultToInfo, infoToStreamEvents,
                handle : struct (StreamName * StreamSpan<_>) -> Async<struct (Streams.SpanResult * 'Outcome)>, maxDop,
                stats : Scheduling.Stats<struct (StreamSpan.Metrics * 'Outcome), struct (StreamSpan.Metrics * exn)>, statsInterval,
                ?maxSubmissionsPerPartition, ?logExternalState,
                ?maxBatches, ?purgeInterval, ?wakeForResults, ?idleDelay,
                ?maximizeOffsetWriting) =
            let prepare struct (streamName, span) =
                let metrics = StreamSpan.metrics Default.eventSize span
                struct (metrics, struct (streamName, span))
            StreamsConsumer.Start<'Info, 'Outcome>(
                log, config, consumeResultToInfo, infoToStreamEvents, prepare, handle, maxDop,
                stats, statsInterval,
                ?maxSubmissionsPerPartition = maxSubmissionsPerPartition,
                ?logExternalState = logExternalState,
                ?maxBatches = maxBatches, ?purgeInterval = purgeInterval, ?wakeForResults = wakeForResults, ?idleDelay = idleDelay,
                ?maximizeOffsetWriting = maximizeOffsetWriting)

        (* KeyValuePair optimized mappings (these were the original implementation); retained as:
            - the default mapping overloads in Propulsion.Kafka.StreamsConsumer pass the ConsumeResult to parser functions,
              which can potentially induce avoidable memory consumption before the ingestion handler runs the mapping process
            - for symmetry with ParallelConsumer signatures, we provide an out of the box API mapping from the relevant Kafka underlying type to a neutral one *)

        /// Starts a Kafka Consumer running spans of events per stream through the `handle` function to `maxDop` concurrently
        static member Start<'Outcome>
            (   log : ILogger, config : KafkaConsumerConfig,
                // often implemented via <c>StreamNameSequenceGenerator.KeyValueToStreamEvent</c>
                keyValueToStreamEvents,
                prepare, handle : struct (StreamName * StreamSpan<_>) -> Async<struct (Streams.SpanResult * 'Outcome)>,
                maxDop,
                stats : Scheduling.Stats<struct (StreamSpan.Metrics * 'Outcome), struct (StreamSpan.Metrics * exn)>, statsInterval,
                ?maxSubmissionsPerPartition, ?logExternalState,
                ?maxBatches, ?purgeInterval, ?wakeForResults, ?idleDelay,
                ?maximizeOffsetWriting) =
            StreamsConsumer.Start<KeyValuePair<string, string>, 'Outcome>(
                log, config, Binding.mapConsumeResult, keyValueToStreamEvents, prepare, handle, maxDop,
                stats, statsInterval = statsInterval,
                ?maxSubmissionsPerPartition = maxSubmissionsPerPartition, ?logExternalState = logExternalState,
                ?maxBatches = maxBatches, ?purgeInterval = purgeInterval, ?wakeForResults = wakeForResults, ?idleDelay = idleDelay,
                ?maximizeOffsetWriting = maximizeOffsetWriting)

        /// Starts a Kafka Consumer running spans of events per stream through the `handle` function to `maxDop` concurrently
        static member Start<'Outcome>
            (   log : ILogger, config : KafkaConsumerConfig,
                // often implemented via <c>StreamNameSequenceGenerator.KeyValueToStreamEvent</c>
                keyValueToStreamEvents : KeyValuePair<string, string> -> StreamEvent<_> seq,
                handle : struct (StreamName * StreamSpan<_>) -> Async<struct (Streams.SpanResult * 'Outcome)>, maxDop,
                stats : Scheduling.Stats<struct (StreamSpan.Metrics * 'Outcome), struct (StreamSpan.Metrics * exn)>, statsInterval,
                ?maxSubmissionsPerPartition, ?logExternalState,
                ?maxBatches, ?purgeInterval, ?wakeForResults, ?idleDelay,
                ?maximizeOffsetWriting) =
            StreamsConsumer.Start<KeyValuePair<string, string>, 'Outcome>(
                log, config, Binding.mapConsumeResult, keyValueToStreamEvents, handle, maxDop,
                stats, statsInterval,
                ?maxSubmissionsPerPartition = maxSubmissionsPerPartition, ?logExternalState = logExternalState,
                ?maxBatches = maxBatches, ?purgeInterval = purgeInterval, ?wakeForResults = wakeForResults, ?idleDelay = idleDelay,
                ?maximizeOffsetWriting = maximizeOffsetWriting)

    // Maps a (potentially `null`) message key to a valid {Category}-{StreamId} StreamName for routing and/or propagation through StreamsProjector
    let parseMessageKey defaultCategory = function
        | null -> FsCodec.StreamName.create defaultCategory ""
        | key -> StreamName.parseWithDefaultCategory defaultCategory key
    let toTimelineEvent toDataAndContext (result : ConsumeResult<string, string>, index) =
        let data, context = toDataAndContext result
        FsCodec.Core.TimelineEvent.Create(index, String.Empty, data, context = context)

    let toStreamName defaultCategory (result : ConsumeResult<string, string>) =
        let m = result.Message
        if m = null then invalidOp "Cannot dereference null message"
        parseMessageKey defaultCategory m.Key

    // This type is not a formal part of the API; the intent is to provide context as to the origin of the event for insertion into a parser error message
    // Arguably it should be an anonymous record...
    type ConsumeResultContext = { topic : string; partition : int; offset : int64 }
    let toDataAndContext (result : ConsumeResult<string, string>) : Default.EventBody * obj =
        let m = Binding.message result
        if m = null then invalidOp "Cannot dereference null message"
        let data = System.Text.Encoding.UTF8.GetBytes m.Value
        let context = { topic = result.Topic; partition = Binding.partitionValue result.Partition; offset = Binding.offsetValue result.Offset }
        (ReadOnlyMemory data, box context)

/// StreamsProjector buffers and deduplicates messages from a contiguous stream with each event bearing a monotonically incrementing `index`.
/// Where the messages we consume don't have such characteristics, we need to maintain a fake `index` by keeping an int per stream in a dictionary
/// Does not need to be thread-safe as the <c>Propulsion.Submission.SubmissionEngine</c> does not unpack input messages/documents in parallel.
type StreamNameSequenceGenerator() =

    // Last-used index per streamName
    let indices = Dictionary()

    /// Generates an index for the specified StreamName. Sequence starts at 0, incrementing per call.
    member _.GenerateIndex(streamName : StreamName) =
        let streamName = FsCodec.StreamName.toString streamName
        match indices.TryGetValue streamName with
        | true, v -> let x = v + 1L in indices[streamName] <- x; x
        | false, _ -> let x = 0L in indices[streamName] <- x; x

    /// Provides a generic mapping from a ConsumeResult to a <c>StreamName</c> and <c>ITimelineEvent</c>
    member x.ConsumeResultToStreamEvent
        (   toStreamName : ConsumeResult<_, _> -> StreamName,
            toTimelineEvent : ConsumeResult<_, _> * int64 -> ITimelineEvent<_>)
        : ConsumeResult<_, _> -> Default.StreamEvent seq =
        fun consumeResult ->
            let sn = toStreamName consumeResult
            let e = toTimelineEvent (consumeResult, x.GenerateIndex sn)
            Seq.singleton (sn, e)

    /// Enables customizing of mapping from ConsumeResult to<br/>
    /// 1) The <c>StreamName</c><br/>
    /// 2) The <c>ITimelineEvent.Data : byte[]</c>, which bears the (potentially transformed in <c>toDataAndContext</c>) UTF-8 payload<br/>
    /// 3) The <c>ITimelineEvent.Context : obj</c>, which can be used to include any metadata
    member x.ConsumeResultToStreamEvent
        (    toStreamName : ConsumeResult<_, _> -> StreamName,
             toDataAndContext : ConsumeResult<_, _> -> Default.EventBody * obj)
        : ConsumeResult<string, string> -> Default.StreamEvent seq =
        x.ConsumeResultToStreamEvent(toStreamName, Core.toTimelineEvent toDataAndContext)

    /// Enables customizing of mapping from ConsumeResult to<br/>
    /// 1) The <c>ITimelineEvent.Data : byte[]</c>, which bears the (potentially transformed in <c>toDataAndContext</c>) UTF-8 payload<br/>
    /// 2) The <c>ITimelineEvent.Context : obj</c>, which can be used to include any metadata
    member x.ConsumeResultToStreamEvent(toDataAndContext : ConsumeResult<_, _> -> Default.EventBody * obj, ?defaultCategory)
        : ConsumeResult<string, string> -> Default.StreamEvent seq =
        let defaultCategory = defaultArg defaultCategory ""
        x.ConsumeResultToStreamEvent(Core.toStreamName defaultCategory, Core.toTimelineEvent toDataAndContext)

    /// Enables customizing of mapping from ConsumeResult to the StreamName<br/>
    /// The body of the message is passed as the <c>ITimelineEvent.Data</c><br/>
    /// Stores the topic, partition and offset as a <c>ConsumeResultContext</c> in the <c>ITimelineEvent.Context</c>
    member x.ConsumeResultToStreamEvent(toStreamName : ConsumeResult<_, _> -> StreamName)
        : ConsumeResult<string, string> -> Default.StreamEvent seq =
        x.ConsumeResultToStreamEvent(toStreamName, Core.toDataAndContext)

    /// Default Mapping: <br/>
    /// - Treats <c>null</c> keys as having <c>streamId</c> of <c>""</c><br/>
    /// - Replaces missing categories within keys with the (optional) <c>defaultCategory</c> (or <c>""</c>)<br/>
    /// - Stores the topic, partition and offset as a <c>ConsumeResultContext</c> in the <c>ITimelineEvent.Context</c>
    member x.ConsumeResultToStreamEvent(
            // Placeholder category to use for StreamName where key is null and/or does not adhere to standard {category}-{streamId} form
            ?defaultCategory) : ConsumeResult<string, string> -> StreamEvent<Default.EventBody> seq =
        let defaultCategory = defaultArg defaultCategory ""
        x.ConsumeResultToStreamEvent(Core.toStreamName defaultCategory)

    /// Takes the key and value as extracted from the ConsumeResult, mapping them respectively to the StreamName and ITimelineEvent.Data
    member x.KeyValueToStreamEvent(KeyValue (k, v : string), ?eventType, ?defaultCategory) : StreamEvent<Default.EventBody> seq =
        let sn = Core.parseMessageKey (defaultArg defaultCategory String.Empty) k
        let e = FsCodec.Core.TimelineEvent.Create(x.GenerateIndex sn, defaultArg eventType String.Empty, System.Text.Encoding.UTF8.GetBytes v |> ReadOnlyMemory)
        Seq.singleton (sn, e)

type StreamsConsumer =

    /// Starts a Kafka Consumer per the supplied <c>config</c>, which defines the source topic(s).<br/>
    /// The Consumer feeds <c>StreamEvent</c>s (mapped from the Kafka <c>ConsumeResult</c> by <c>consumeResultToStreamEvents</c>) to an (internal) Sink.<br/>
    /// The Sink runs up to <c>maxDop</c> instances of <c>handler</c> concurrently.<br/>
    /// Each <c>handler</c> invocation processes the pending span of events for a <i>single</i> given stream.<br/>
    /// There is a strong guarantee that there is only a single <c>handler</c> in flight for any given stream at any time.<br>
    /// Processor <c>'Outcome<c/>s are passed to be accumulated into the <c>stats</c> for periodic emission.<br/>
    /// Processor will run perpetually in a background until `Stop()` is requested.
    static member Start<'Outcome>
        (   log : ILogger, config : KafkaConsumerConfig,
            // often implemented via <c>StreamNameSequenceGenerator.ConsumeResultToStreamEvent</c> where the incoming message does not have an embedded sequence number
            consumeResultToStreamEvents : ConsumeResult<_, _> -> StreamEvent<_> seq,
            // Handler responses:
            // - first component: Index at which next processing will proceed (which can trigger discarding of earlier items on that stream)
            // - second component: Outcome (can be simply <c>unit</c>), to pass to the <c>stats</c> processor
            // - throwing marks the processing of a stream as having faulted (the stream's pending events and/or
            //   new ones that arrived while the handler was processing are then eligible for retry purposes in the next dispatch cycle)
            handle : struct (StreamName * StreamSpan<_>) -> Async<struct (Streams.SpanResult * 'Outcome)>,
            // The maximum number of instances of <c>handle</c> that are permitted to be dispatched at any point in time.
            // The scheduler seeks to maximise the in-flight <c>handle</c>rs at any point in time.
            // The scheduler guarantees to never schedule two concurrent <c>handler<c> invocations for the same stream.
            maxDop,
            // The <c>'Outcome</c> from each handler invocation is passed to the Statistics processor by the scheduler for periodic emission
            stats : Scheduling.Stats<struct (StreamSpan.Metrics * 'Outcome), struct (StreamSpan.Metrics * exn)>, statsInterval,
            ?maxSubmissionsPerPartition, ?logExternalState,
            ?maxBatches, ?purgeInterval, ?wakeForResults, ?idleDelay,
            // Prevent batches being consolidated prior to scheduling in order to maximize granularity of consumer offset updates
            ?maximizeOffsetWriting) =
        Core.StreamsConsumer.Start<ConsumeResult<_, _>, 'Outcome>(
            log, config, id, consumeResultToStreamEvents, handle, maxDop,
            stats, statsInterval,
            ?maxSubmissionsPerPartition = maxSubmissionsPerPartition, ?logExternalState = logExternalState,
            ?maxBatches = maxBatches, ?purgeInterval = purgeInterval, ?wakeForResults = wakeForResults, ?idleDelay = idleDelay,
            ?maximizeOffsetWriting=maximizeOffsetWriting)

type BatchesConsumer =

    /// Starts a Kafka Consumer per the supplied <c>config</c>, which defines the source topic(s).<br/>
    /// The Consumer feeds <c>StreamEvent</c>s (mapped from the Kafka <c>ConsumeResult</c> by <c>consumeResultToInfo</c> and <c>infoToStreamEvents</c>) to an (internal) Sink.<br/>
    /// The Sink loops continually:-<br/>
    /// 1. supplying all pending items the scheduler has ingested thus far (controlled via <c>schedulerIngestionBatchCount</c>
    ///    and incoming batch sizes) to <c>select</c> to determine streams to process in this iteration.<br/>
    /// 2. (if any items <c>select</c>ed) passing them to the <c>handle</c>r for processing.<br/>
    /// 3. The <c>handler</c> results are passed to the <c>stats</c> for periodic emission.<br/>
    /// Processor <c>'Outcome<c/>s are passed to be accumulated into the <c>stats</c> for periodic emission.<br/>
    /// Processor will run perpetually in a background until `Stop()` is requested.
    static member Start<'Info>
        (   log : ILogger, config : KafkaConsumerConfig, consumeResultToInfo, infoToStreamEvents,
            select,
            // Handler responses:
            // - the result seq is expected to match the ordering of the input <c>DispatchItem</c>s
            // - Choice1Of2: Index at which next processing will proceed (which can trigger discarding of earlier items on that stream)
            // - Choice2Of2: Records the processing of the stream in question as having faulted (the stream's pending events and/or
            //   new ones that arrived while the handler was processing are then eligible for retry purposes in the next dispatch cycle)
            handle : Dispatch.Item<_>[] -> Async<seq<Choice<int64, exn>>>,
            // The responses from each <c>handle</c> invocation are passed to <c>stats</c> for periodic emission
            stats : Scheduling.Stats<struct (StreamSpan.Metrics * unit), struct (StreamSpan.Metrics * exn)>, statsInterval,
            // Maximum number of batches to ingest for scheduling at any one time (Default: 24.)
            // NOTE Stream-wise consumption defaults to taking 5 batches each time replenishment is required
            ?schedulerIngestionBatchCount,
            // Limits number of batches passed to the scheduler.
            // Holding items back makes scheduler processing more efficient as less state needs to be traversed.
            // Default 5
            ?maxSubmissionsPerPartition,
            ?purgeInterval, ?wakeForResults, ?idleDelay,
            ?logExternalState) =
        let maxBatches = defaultArg schedulerIngestionBatchCount 24
        let maxSubmissionsPerPartition = defaultArg maxSubmissionsPerPartition 5
        let dumpStreams logStreamStates log =
            logExternalState |> Option.iter (fun f -> f log)
            logStreamStates Default.eventSize
        let handle (items : Dispatch.Item<Default.EventBody>[]) ct
            : Task<struct (TimeSpan * StreamName * bool * Choice<struct (int64 * struct (StreamSpan.Metrics * unit)), struct (StreamSpan.Metrics * exn)>)[]> = task {
            let sw = Stopwatch.start ()
            let avgElapsed () =
                let tot = float sw.ElapsedMilliseconds
                TimeSpan.FromMilliseconds(tot / float items.Length)
            try let! results = handle items |> fun f -> Async.StartAsTask(f, cancellationToken = ct)
                let ae = avgElapsed ()
                return
                    [| for x in Seq.zip items results ->
                        match x with
                        | item, Choice1Of2 index' ->
                            let used : StreamSpan<_> = item.span |> Seq.takeWhile (fun e -> e.Index <> index' ) |> Array.ofSeq
                            let metrics = StreamSpan.metrics Default.eventSize used
                            struct (ae, item.stream, true, Choice1Of2 struct (index', struct (metrics, ())))
                        | item, Choice2Of2 exn ->
                            let metrics = StreamSpan.metrics Default.jsonSize item.span
                            ae, item.stream, false, Choice2Of2 struct (metrics, exn) |]
            with e ->
                let ae = avgElapsed ()
                return
                    [| for x in items ->
                        let metrics = StreamSpan.metrics Default.jsonSize x.span
                        ae, x.stream, false, Choice2Of2 struct (metrics, e) |] }
        let dispatcher = Scheduling.Dispatcher.BatchedDispatcher(select, handle, stats, dumpStreams)
        let streamsScheduler = Scheduling.StreamSchedulingEngine.Create(
            dispatcher,
            maxBatches = maxBatches,
            ?purgeInterval = purgeInterval, ?wakeForResults = wakeForResults, ?idleDelay = idleDelay)
        let mapConsumedMessagesToStreamsBatch onCompletion (x : Submission.Batch<TopicPartition, 'Info>) : Buffer.Batch<_> =
            let onCompletion () = x.onCompletion(); onCompletion()
            Buffer.Batch.Create(onCompletion, Seq.collect infoToStreamEvents x.messages) |> ValueTuple.fst
        let submitter = Projector.StreamsSubmitter.Create(log, maxSubmissionsPerPartition, mapConsumedMessagesToStreamsBatch, streamsScheduler.Submit, statsInterval)
        ConsumerPipeline.Start(log, config, consumeResultToInfo, submitter.Ingest, submitter.Pump, (fun abend -> streamsScheduler.Pump), dispatcher.Pump, statsInterval)
