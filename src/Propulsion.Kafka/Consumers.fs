/// Manages efficiently and continuously reading from the Confluent.Kafka consumer, offloading the pushing of those batches onward to the Submitter
/// Responsible for ensuring we don't over-read, which would cause the rdkafka buffers to overload the system in terms of memory usage
namespace Propulsion.Kafka

open Confluent.Kafka
open FsCodec
open FsKafka
open Propulsion
open Propulsion.Internal // intervalCheck
open Propulsion.Kafka.Internal // AwaitTaskCorrect
open Serilog
open System
open System.Collections.Generic
open System.Diagnostics
open System.Threading
open System.Threading.Tasks

[<AutoOpen>]
module private Impl =

    /// Maintains a Stopwatch used to drive a periodic loop, computing the remaining portion of the period per invocation
    /// - `Some remainder` if the interval has time remaining
    /// - `None` if the interval has expired (and triggers restarting the timer)
    let intervalTimer (period : TimeSpan) =
        let timer = Stopwatch.StartNew()
        fun () ->
            match period - timer.Elapsed with
            | remainder when remainder.Ticks > 0L -> Some remainder
            | _ -> timer.Restart(); None

    /// Retains the messages we've accumulated for a given Partition
    [<NoComparison>]
    type PartitionBuffer<'M> =
        {   mutable reservation : int64 // accumulate reserved in flight bytes so we can reverse the reservation when it completes
            mutable highWaterMark : ConsumeResult<string, string> // hang on to it so we can generate a checkpointing lambda
            messages : ResizeArray<'M> }
        member __.Enqueue(sz, message, mapMessage) =
            __.highWaterMark <- message
            __.reservation <- __.reservation + sz // size we need to unreserve upon completion
            __.messages.Add(mapMessage message)
        static member Create(sz, message, mapMessage) =
            let x = { reservation = 0L; highWaterMark = null; messages = ResizeArray(256) }
            x.Enqueue(sz, message, mapMessage)
            x

    /// guesstimate approximate message size in bytes
    let approximateMessageBytes (m : Message<string, string>) =
        let inline len (x : string) = match x with null -> 0 | x -> sizeof<char> * x.Length
        16 + len m.Key + len m.Value |> int64
    let inline mb x = float x / 1024. / 1024.

#if !KAFKA0 // TODO push overload down into FsKafka
#nowarn "0040"

[<AutoOpen>]
module internal Shims =

    type FsKafka.Core.InFlightMessageCounter with
        member __.AwaitThreshold(ct : CancellationToken, consumer : IConsumer<_,_>, ?busyWork) =
            // Avoid having our assignments revoked due to MAXPOLL (exceeding max.poll.interval.ms between calls to .Consume)
            let showConsumerWeAreStillAlive () =
                let tps = consumer.Assignment
                consumer.Pause(tps)
                match busyWork with Some f -> f () | None -> ()
                let _ = consumer.Consume(1)
                consumer.Resume(tps)
            __.AwaitThreshold(ct, showConsumerWeAreStillAlive)
#endif

/// Continuously polls across the assigned partitions, building spans; periodically (at intervals of `emitInterval`), `submit`s accumulated messages as
///   checkpointable Batches
/// Pauses if in-flight upper threshold is breached until such time as it drops below that the lower limit
type KafkaIngestionEngine<'Info>
    (   log : ILogger, counter : Core.InFlightMessageCounter, consumer : IConsumer<_, _>, closeConsumer,
        mapMessage : ConsumeResult<_, _> -> 'Info, emit : Submission.SubmissionBatch<TopicPartition, 'Info>[] -> unit,
        maxBatchSize, emitInterval, statsInterval) =
    let acc = Dictionary<TopicPartition, _>()
    let remainingIngestionWindow = intervalTimer emitInterval
    let mutable intervalMsgs, intervalChars, totalMessages, totalChars = 0L, 0L, 0L, 0L
    let dumpStats () =
        totalMessages <- totalMessages + intervalMsgs; totalChars <- totalChars + intervalChars
        log.Information("Ingested {msgs:n0}m, {chars:n0}c In-flight ~{inflightMb:n1}MB Σ {totalMessages:n0} messages, {totalChars:n0} chars",
            intervalMsgs, intervalChars, counter.InFlightMb, totalMessages, totalChars)
        intervalMsgs <- 0L; intervalChars <- 0L
    let maybeLogStats =
        let due = intervalCheck statsInterval
        fun () -> if due () then dumpStats ()
    let mkSubmission topicPartition span : Submission.SubmissionBatch<'S, 'M> =
        let checkpoint () =
            counter.Delta(-span.reservation) // counterbalance Delta(+) per ingest, below
            Binding.storeOffset log consumer span.highWaterMark
        { source = topicPartition; onCompletion = checkpoint; messages = span.messages.ToArray() }
    let ingest result =
        let m = FsKafka.Binding.message result
        if m = null then invalidOp "Cannot dereference null message"
        let sz = approximateMessageBytes m
        counter.Delta(+sz) // counterbalanced by Delta(-) in checkpoint(), below
        intervalMsgs <- intervalMsgs + 1L
        let inline stringLen (s : string) = match s with null -> 0 | x -> x.Length
        intervalChars <- intervalChars + int64 (stringLen m.Key + stringLen m.Value)
        let tp = result.TopicPartition
        let span =
            match acc.TryGetValue tp with
            | false, _ -> let span = PartitionBuffer<'Info>.Create(sz, result, mapMessage) in acc.[tp] <- span; span
            | true, span -> span.Enqueue(sz, result, mapMessage); span
        if span.messages.Count = maxBatchSize then
            acc.Remove tp |> ignore
            emit [| mkSubmission tp span |]
    let submit () =
        match acc.Count with
        | 0 -> ()
        | topicPartitionsWithMessagesThisInterval ->
            let tmp = ResizeArray<Submission.SubmissionBatch<_, 'Info>>(topicPartitionsWithMessagesThisInterval)
            for KeyValue(tp, span) in acc do
                tmp.Add(mkSubmission tp span)
            acc.Clear()
            emit <| tmp.ToArray()
    member __.Pump() = async {
        let! ct = Async.CancellationToken
        use _ = consumer // Dispose it at the end (NB but one has to Close first or risk AccessViolations etc)
        try while not ct.IsCancellationRequested do
                match counter.IsOverLimitNow(), remainingIngestionWindow () with
                | true, _ ->
#if KAFKA0
                    let busyWork () =
                        submit()
                        maybeLogStats()
                        Thread.Sleep 1
                    counter.AwaitThreshold(ct, busyWork)
#else
                    let busyWork () =
                        submit()
                        maybeLogStats()
                    counter.AwaitThreshold(ct, consumer, busyWork)
#endif
                | false, None ->
                    submit()
                    maybeLogStats()
                | false, Some intervalRemainder ->
                    Binding.tryConsume log consumer intervalRemainder ingest
        finally
            submit () // We don't want to leak our reservations against the counter and want to pass of messages we ingested
            dumpStats () // Unconditional logging when completing
            closeConsumer() (* Orderly Close() before Dispose() is critical *) }

/// Consumes according to the `config` supplied to `Start`, until `Stop()` is requested or `handle` yields a fault.
/// Conclusion of processing can be awaited by via `AwaitCompletion()`.
type ConsumerPipeline private (inner : IConsumer<string, string>, task : Task<unit>, triggerStop) =
    inherit Pipeline(task, triggerStop)

    /// Provides access to the Confluent.Kafka interface directly
    member __.Inner = inner

    /// Builds a processing pipeline per the `config` running up to `dop` instances of `handle` concurrently to maximize global throughput across partitions.
    /// Processor pumps until `handle` yields a `Choice2Of2` or `Stop()` is requested.
    static member Start(log : ILogger, config : KafkaConsumerConfig, mapResult, submit, pumpSubmitter, pumpScheduler, pumpDispatcher, statsInterval) =
        let maxDelay, maxItems = config.Buffering.maxBatchDelay, config.Buffering.maxBatchSize
        log.Information("Consuming... {bootstrapServers} {topics} {groupId} autoOffsetReset {autoOffsetReset} fetchMaxBytes={fetchMaxB} maxInFlight={maxInFlightGB:n1}GB maxBatchDelay={maxBatchDelay}s maxBatchSize={maxBatchSize}",
            config.Inner.BootstrapServers, config.Topics, config.Inner.GroupId, (let x = config.Inner.AutoOffsetReset in x.Value), config.Inner.FetchMaxBytes,
            float config.Buffering.maxInFlightBytes / 1024. / 1024. / 1024., maxDelay.TotalSeconds, maxItems)
        let limiterLog = log.ForContext(Serilog.Core.Constants.SourceContextPropertyName, Core.Constants.messageCounterSourceContext)
        let limiter = Core.InFlightMessageCounter(limiterLog, config.Buffering.minInFlightBytes, config.Buffering.maxInFlightBytes)
        let consumer, closeConsumer = Binding.createConsumer log config.Inner // teardown is managed by ingester.Pump()
        consumer.Subscribe config.Topics
        let ingester = KafkaIngestionEngine<'M>(log, limiter, consumer, closeConsumer, mapResult, submit, maxItems, maxDelay, statsInterval=statsInterval)
        let cts = new CancellationTokenSource()
        let ct = cts.Token
        let tcs = TaskCompletionSource<unit>()
        let triggerStop () =
            let level = if cts.IsCancellationRequested then Events.LogEventLevel.Debug else Events.LogEventLevel.Information
            log.Write(level, "Consuming... Stopping {name}", consumer.Name)
            cts.Cancel()
        let start name f =
            let wrap (name : string) computation = async {
                try do! computation
                    log.Information("Exiting pipeline component {name}", name)
                with e ->
                    log.Fatal(e, "Abend from pipeline component {name}", name)
                    triggerStop () }
            Async.Start(wrap name f, ct)
        // if scheduler encounters a faulted handler, we propagate that as the consumer's Result
        let abend (exns : AggregateException) =
            if tcs.TrySetException(exns) then log.Warning(exns, "Cancelling processing due to {count} faulted handlers", exns.InnerExceptions.Count)
            else log.Information("Failed setting {count} exceptions", exns.InnerExceptions.Count)
            // NB cancel needs to be after TSE or the Register(TSE) will win
            cts.Cancel()

        let machine = async {
            // external cancellation should yield a success result
            use _ = ct.Register(fun _ -> tcs.TrySetResult () |> ignore)
            start "dispatcher" <| pumpDispatcher
            // ... fault results from dispatched tasks result in the `machine` concluding with an exception
            start "scheduler" <| pumpScheduler abend
            start "submitter" <| pumpSubmitter
            start "ingester" <| ingester.Pump()

            // await for either handler-driven abend or external cancellation via Stop()
            do! Async.AwaitTaskCorrect tcs.Task
        }
        let task = Async.StartAsTask machine
        new ConsumerPipeline(consumer, task, triggerStop)

type ParallelConsumer private () =

    /// Builds a processing pipeline per the `config` running up to `dop` instances of `handle` concurrently to maximize global throughput across partitions.
    /// Processor pumps until `handle` yields a `Choice2Of2` or `Stop()` is requested.
    static member Start<'Msg>
        (   log : ILogger, config : KafkaConsumerConfig, maxDop,
            mapResult : ConsumeResult<string, string> -> 'Msg,
            handle : 'Msg -> Async<Choice<unit, exn>>,
            /// Default 5
            ?maxSubmissionsPerPartition, ?pumpInterval,
            /// Default 5m
            ?statsInterval, ?logExternalStats) =
        let statsInterval = defaultArg statsInterval (TimeSpan.FromMinutes 5.)

        let dispatcher = Parallel.Scheduling.Dispatcher maxDop
        let scheduler = Parallel.Scheduling.PartitionedSchedulingEngine<_, 'Msg>(log, handle, dispatcher.TryAdd, statsInterval, ?logExternalStats=logExternalStats)
        let maxSubmissionsPerPartition = defaultArg maxSubmissionsPerPartition 5
        let mapBatch onCompletion (x : Submission.SubmissionBatch<_, _>) : Parallel.Scheduling.Batch<_, 'Msg> =
            let onCompletion' () = x.onCompletion(); onCompletion()
            { source = x.source; messages = x.messages; onCompletion = onCompletion'; }
        let submitBatch (x : Parallel.Scheduling.Batch<_, _>) : int =
            scheduler.Submit x
            x.messages.Length
        let submitter = Submission.SubmissionEngine(log, maxSubmissionsPerPartition, mapBatch, submitBatch, statsInterval, ?pumpInterval=pumpInterval)
        ConsumerPipeline.Start(log, config, mapResult, submitter.Ingest, submitter.Pump(), scheduler.Pump, dispatcher.Pump(), statsInterval)

    /// Builds a processing pipeline per the `config` running up to `dop` instances of `handle` concurrently to maximize global throughput across partitions.
    /// Processor pumps until `handle` yields a `Choice2Of2` or `Stop()` is requested.
    static member Start
        (   log : ILogger, config : KafkaConsumerConfig, maxDop, handle : KeyValuePair<string, string> -> Async<unit>,
            /// Default 5
            ?maxSubmissionsPerPartition, ?pumpInterval,
            /// Default 5m
            ?statsInterval, ?logExternalStats) =
        ParallelConsumer.Start<KeyValuePair<string, string>>(log, config, maxDop, Binding.mapConsumeResult, handle >> Async.Catch,
            ?maxSubmissionsPerPartition=maxSubmissionsPerPartition, ?pumpInterval=pumpInterval, ?statsInterval=statsInterval, ?logExternalStats=logExternalStats)

type EventMetrics = Streams.EventMetrics

[<AbstractClass>]
type StreamsConsumerStats<'Outcome>(log : ILogger, statsInterval, stateInterval) =
    inherit Streams.Scheduling.Stats<EventMetrics * 'Outcome, EventMetrics * exn>(log, statsInterval, stateInterval)
    let okStreams, failStreams = HashSet(), HashSet()
    let mutable okEvents, okBytes, exnEvents, exnBytes = 0, 0L, 0, 0L

    override __.DumpStats() =
        if okStreams.Count <> 0 && failStreams.Count <> 0 then
            log.Information("Completed {okMb:n0}MB {okStreams:n0}s {okEvents:n0}e Exceptions {exnMb:n0}MB {exnStreams:n0}s {exnEvents:n0}e",
                mb okBytes, okStreams.Count, okEvents, mb exnBytes, failStreams.Count, exnEvents)
        okStreams.Clear(); okEvents <- 0; okBytes <- 0L

    override __.Handle message =
        let inline adds x (set:HashSet<_>) = set.Add x |> ignore
        base.Handle message
        match message with
        | Propulsion.Streams.Scheduling.InternalMessage.Added _ -> () // Processed by standard logging already; we have nothing to add
        | Propulsion.Streams.Scheduling.InternalMessage.Result (_duration, (stream, Choice1Of2 ((es, bs), res))) ->
            adds stream okStreams
            okEvents <- okEvents + es
            okBytes <- okBytes + int64 bs
            __.HandleOk res
        | Propulsion.Streams.Scheduling.InternalMessage.Result (_duration, (stream, Choice2Of2 ((es, bs), exn))) ->
            adds stream failStreams
            exnEvents <- exnEvents + es
            exnBytes <- exnBytes + int64 bs
            __.HandleExn exn

    abstract member HandleOk : outcome : 'Outcome -> unit
    abstract member HandleExn : exn : exn -> unit

/// APIs only required for advanced scenarios (specifically the integration tests)
/// APIs within are not part of the stable API and are subject to unlimited change
module Core =

    type StreamsConsumer =

        static member Start<'Info, 'Outcome>
            (   log : ILogger, config : KafkaConsumerConfig, resultToInfo, infoToStreamEvents,
                prepare, handle, maxDop,
                stats : Streams.Scheduling.Stats<EventMetrics * 'Outcome, EventMetrics * exn>, statsInterval,
                ?maxSubmissionsPerPartition, ?pumpInterval, ?logExternalState, ?idleDelay, ?purgeInterval, ?maxBatches, ?maximizeOffsetWriting) =
            let dispatcher = Streams.Scheduling.ItemDispatcher<_> maxDop
            let dumpStreams (streams : Streams.Scheduling.StreamStates<_>) log =
                logExternalState |> Option.iter (fun f -> f log)
                streams.Dump(log, Streams.Buffering.StreamState.eventsSize)
            let streamsScheduler =
                Streams.Scheduling.StreamSchedulingEngine.Create<_, _, _, _>
                    (   dispatcher, stats, prepare, handle, Streams.SpanResult.toIndex, dumpStreams,
                        ?idleDelay=idleDelay, ?purgeInterval=purgeInterval, ?maxBatches=maxBatches)
            let mapConsumedMessagesToStreamsBatch onCompletion (x : Submission.SubmissionBatch<TopicPartition, 'Info>) : Streams.Scheduling.StreamsBatch<_> =
                let onCompletion () = x.onCompletion(); onCompletion()
                Streams.Scheduling.StreamsBatch.Create(onCompletion, Seq.collect infoToStreamEvents x.messages) |> fst
            let submitter =
                Streams.Projector.StreamsSubmitter.Create
                    (   log, mapConsumedMessagesToStreamsBatch,
                        streamsScheduler.Submit, statsInterval,
                        ?maxSubmissionsPerPartition=maxSubmissionsPerPartition, ?pumpInterval=pumpInterval,
                        ?disableCompaction=maximizeOffsetWriting)
            ConsumerPipeline.Start(log, config, resultToInfo, submitter.Ingest, submitter.Pump(), streamsScheduler.Pump, dispatcher.Pump(), statsInterval)

        static member Start<'Info, 'Outcome>
            (   log : ILogger, config : KafkaConsumerConfig, consumeResultToInfo, infoToStreamEvents,
                handle : StreamName * Streams.StreamSpan<_> -> Async<Streams.SpanResult * 'Outcome>, maxDop,
                stats : Streams.Scheduling.Stats<EventMetrics * 'Outcome, EventMetrics * exn>, statsInterval,
                ?maxSubmissionsPerPartition, ?pumpInterval, ?logExternalState, ?idleDelay, ?purgeInterval, ?maxBatches, ?maximizeOffsetWriting) =
            let prepare (streamName, span) =
                let stats = Streams.Buffering.StreamSpan.stats span
                stats, (streamName, span)
            StreamsConsumer.Start<'Info, 'Outcome>(
                log, config, consumeResultToInfo, infoToStreamEvents, prepare, handle, maxDop,
                stats, statsInterval,
                ?maxSubmissionsPerPartition=maxSubmissionsPerPartition,
                ?pumpInterval=pumpInterval,
                ?logExternalState=logExternalState,
                ?idleDelay=idleDelay, ?purgeInterval=purgeInterval,
                ?maxBatches=maxBatches,
                ?maximizeOffsetWriting=maximizeOffsetWriting)

        (* KeyValuePair optimized mappings (these were the original implementation); retained as:
            - the default mapping overloads in Propulsion.Kafka.StreamsConsumer pass the ConsumeResult to parser functions,
              which can potentially induce avoidable memory consumption before the ingestion handler runs the mapping process
            - for symmetry with ParallelConsumer signatures, we provide an out of the box API mapping from the relevant Kafka underlying type to a neutral one *)

        /// Starts a Kafka Consumer running spans of events per stream through the `handle` function to `maxDop` concurrently
        static member Start<'Outcome>
            (   log : ILogger, config : KafkaConsumerConfig,
                /// often implemented via <c>StreamNameSequenceGenerator.KeyValueToStreamEvent</c>
                keyValueToStreamEvents,
                prepare, handle : StreamName * Streams.StreamSpan<_> -> Async<Streams.SpanResult * 'Outcome>,
                maxDop,
                stats : Streams.Scheduling.Stats<EventMetrics * 'Outcome, EventMetrics * exn>, statsInterval,
                ?maximizeOffsetWriting, ?maxSubmissionsPerPartition, ?pumpInterval, ?logExternalState, ?idleDelay, ?purgeInterval)=
            StreamsConsumer.Start<KeyValuePair<string, string>, 'Outcome>(
                log, config, Binding.mapConsumeResult, keyValueToStreamEvents, prepare, handle, maxDop,
                stats, statsInterval=statsInterval,
                ?maxSubmissionsPerPartition=maxSubmissionsPerPartition,
                ?pumpInterval=pumpInterval,
                ?logExternalState=logExternalState,
                ?idleDelay=idleDelay, ?purgeInterval=purgeInterval,
                ?maximizeOffsetWriting=maximizeOffsetWriting)

        /// Starts a Kafka Consumer running spans of events per stream through the `handle` function to `maxDop` concurrently
        static member Start<'Outcome>
            (   log : ILogger, config : KafkaConsumerConfig,
                /// often implemented via <c>StreamNameSequenceGenerator.KeyValueToStreamEvent</c>
                keyValueToStreamEvents : KeyValuePair<string, string> -> Propulsion.Streams.StreamEvent<_> seq,
                handle : StreamName * Streams.StreamSpan<_> -> Async<Streams.SpanResult * 'Outcome>, maxDop,
                stats : Streams.Scheduling.Stats<EventMetrics * 'Outcome, EventMetrics * exn>, statsInterval,
                ?maximizeOffsetWriting, ?maxSubmissionsPerPartition, ?pumpInterval, ?logExternalState, ?idleDelay, ?purgeInterval, ?maxBatches) =
            StreamsConsumer.Start<KeyValuePair<string, string>, 'Outcome>(
                log, config, Binding.mapConsumeResult, keyValueToStreamEvents, handle, maxDop,
                stats, statsInterval,
                ?maxSubmissionsPerPartition=maxSubmissionsPerPartition,
                ?pumpInterval=pumpInterval,
                ?logExternalState=logExternalState,
                ?idleDelay=idleDelay, ?purgeInterval=purgeInterval,
                ?maxBatches=maxBatches,
                ?maximizeOffsetWriting=maximizeOffsetWriting)

    // Maps a (potentially `null`) message key to a valid {Category}-{StreamId} StreamName for routing and/or propagation through StreamsProjector
    let parseMessageKey defaultCategory = function
        | null -> FsCodec.StreamName.create defaultCategory ""
        | key -> Propulsion.Streams.StreamName.parseWithDefaultCategory defaultCategory key
    let toTimelineEvent toDataAndContext (result : ConsumeResult<string, string>, index) =
        let data, context = toDataAndContext result
        FsCodec.Core.TimelineEvent.Create(index, String.Empty, data, context = context)

    let toStreamName defaultCategory (result : ConsumeResult<string, string>) =
        let m = Binding.message result
        if m = null then invalidOp "Cannot dereference null message"
        parseMessageKey defaultCategory m.Key

    // This type is not a formal part of the API; the intent is to provide context as to the origin of the event for insertion into a parser error message
    // Arguably it should be an anonymous record...
    type ConsumeResultContext = { topic : string; partition : int; offset : int64 }
    let toDataAndContext (result : ConsumeResult<string, string>) : byte[] * obj =
        let m = Binding.message result
        if m = null then invalidOp "Cannot dereference null message"
        let data = System.Text.Encoding.UTF8.GetBytes m.Value
        let context = { topic = result.Topic; partition = Binding.partitionValue result.Partition; offset = Binding.offsetValue result.Offset }
        (data, box context)

/// StreamsProjector buffers and deduplicates messages from a contiguous stream with each event bearing a monotonically incrementing `index`.
/// Where the messages we consume don't have such characteristics, we need to maintain a fake `index` by keeping an int per stream in a dictionary
/// Does not need to be thread-safe as the <c>Propulsion.Submission.SubmissionEngine</c> does not unpack input messages/documents in parallel.
type StreamNameSequenceGenerator() =

    // Last-used index per streamName
    let indices = System.Collections.Generic.Dictionary()

    /// Generates an index for the specified StreamName. Sequence starts at 0, incrementing per call.
    member __.GenerateIndex(streamName : StreamName) =
        let streamName = FsCodec.StreamName.toString streamName
        match indices.TryGetValue streamName with
        | true, v -> let x = v + 1L in indices.[streamName] <- x; x
        | false, _ -> let x = 0L in indices.[streamName] <- x; x

    /// Provides a generic mapping from a ConsumeResult to a <c>StreamName</c> and <c>ITimelineEvent</c>
    member __.ConsumeResultToStreamEvent
        (   toStreamName : ConsumeResult<_, _> -> StreamName,
            toTimelineEvent : ConsumeResult<_, _> * int64 -> ITimelineEvent<_>)
        : ConsumeResult<_, _> -> Propulsion.Streams.StreamEvent<byte[]> seq =
        fun consumeResult ->
            let sn = toStreamName consumeResult
            let e = toTimelineEvent (consumeResult, __.GenerateIndex sn)
            Seq.singleton { stream = sn; event = e }

    /// Enables customizing of mapping from ConsumeResult to<br/>
    /// 1) The <c>StreamName</c><br/>
    /// 2) The <c>ITimelineEvent.Data : byte[]</c>, which bears the (potentially transformed in <c>toDataAndContext</c>) UTF-8 payload<br/>
    /// 3) The <c>ITimelineEvent.Context : obj</c>, which can be used to include any metadata
    member __.ConsumeResultToStreamEvent
        (    toStreamName : ConsumeResult<_, _> -> StreamName,
             toDataAndContext : ConsumeResult<_, _> -> byte[] * obj)
        : ConsumeResult<string, string> -> Propulsion.Streams.StreamEvent<byte[]> seq =
        __.ConsumeResultToStreamEvent(toStreamName, Core.toTimelineEvent toDataAndContext)

    /// Enables customizing of mapping from ConsumeResult to<br/>
    /// 1) The <c>ITimelineEvent.Data : byte[]</c>, which bears the (potentially transformed in <c>toDataAndContext</c>) UTF-8 payload<br/>
    /// 2) The <c>ITimelineEvent.Context : obj</c>, which can be used to include any metadata
    member __.ConsumeResultToStreamEvent(toDataAndContext : ConsumeResult<_, _> -> byte[] * obj, ?defaultCategory)
        : ConsumeResult<string, string> -> Propulsion.Streams.StreamEvent<byte[]> seq =
        let defaultCategory = defaultArg defaultCategory ""
        __.ConsumeResultToStreamEvent(Core.toStreamName defaultCategory, Core.toTimelineEvent toDataAndContext)

    /// Enables customizing of mapping from ConsumeResult to the StreamName<br/>
    /// The body of the message is passed as the <c>ITimelineEvent.Data</c><br/>
    /// Stores the topic, partition and offset as a <c>ConsumeResultContext</c> in the <c>ITimelineEvent.Context</c>
    member __.ConsumeResultToStreamEvent(toStreamName : ConsumeResult<_, _> -> StreamName)
        : ConsumeResult<string, string> -> Propulsion.Streams.StreamEvent<byte[]> seq =
        __.ConsumeResultToStreamEvent(toStreamName, Core.toDataAndContext)

    /// Default Mapping: <br/>
    /// - Treats <c>null</c> keys as having <c>streamId</c> of <c>""</c><br/>
    /// - Replaces missing categories within keys with the (optional) <c>defaultCategory</c> (or <c>""</c>)<br/>
    /// - Stores the topic, partition and offset as a <c>ConsumeResultContext</c> in the <c>ITimelineEvent.Context</c>
    member __.ConsumeResultToStreamEvent
        (   /// Placeholder category to use for StreamName where key is null and/or does not adhere to standard {category}-{streamId} form
            ?defaultCategory) : ConsumeResult<string, string> -> Propulsion.Streams.StreamEvent<byte[]> seq =
        let defaultCategory = defaultArg defaultCategory ""
        __.ConsumeResultToStreamEvent(Core.toStreamName defaultCategory)

    /// Takes the key and value as extracted from the ConsumeResult, mapping them respectively to the StreamName and ITimelineEvent.Data
    member __.KeyValueToStreamEvent(KeyValue (k, v : string), ?eventType, ?defaultCategory) : Propulsion.Streams.StreamEvent<byte[]> seq =
        let sn = Core.parseMessageKey (defaultArg defaultCategory String.Empty) k
        let e = FsCodec.Core.TimelineEvent.Create(__.GenerateIndex sn, defaultArg eventType String.Empty, System.Text.Encoding.UTF8.GetBytes v)
        Seq.singleton { stream = sn; event = e }

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
            /// often implemented via <c>StreamNameSequenceGenerator.ConsumeResultToStreamEvent</c> where the incoming message does not have an embedded sequence number
            consumeResultToStreamEvents : ConsumeResult<_, _> -> Propulsion.Streams.StreamEvent<_> seq,
            /// Handler responses:
            /// - first component: Index at which next processing will proceed (which can trigger discarding of earlier items on that stream)
            /// - second component: Outcome (can be simply <c>unit</c>), to pass to the <c>stats</c> processor
            /// - throwing marks the processing of a stream as having faulted (the stream's pending events and/or
            ///   new ones that arrived while the handler was processing are then eligible for retry purposes in the next dispatch cycle)
            handle : StreamName * Streams.StreamSpan<_> -> Async<Streams.SpanResult * 'Outcome>,
            /// The maximum number of instances of <c>handle</c> that are permitted to be dispatched at any point in time.
            /// The scheduler seeks to maximise the in-flight <c>handle</c>rs at any point in time.
            /// The scheduler guarantees to never schedule two concurrent <c>handler<c> invocations for the same stream.
            maxDop,
            /// The <c>'Outcome</c> from each handler invocation is passed to the Statistics processor by the scheduler for periodic emission
            stats : Streams.Scheduling.Stats<EventMetrics * 'Outcome, EventMetrics * exn>, statsInterval,
            /// Prevent batches being consolidated prior to scheduling in order to maximize granularity of consumer offset updates
            ?maximizeOffsetWriting,
            ?maxSubmissionsPerPartition, ?pumpInterval, ?logExternalState,
            /// Tune the sleep time when there are no items to schedule or responses to process. Default 1ms.
            ?idleDelay,
            /// Frequency with which to jettison Write Position information for inactive streams in order to limit memory consumption
            /// NOTE: Can impair performance and/or increase costs of writes as it inhibits the ability of the ingester to discard redundant inputs
            ?purgeInterval,
            ?maxBatches) =
        Core.StreamsConsumer.Start<ConsumeResult<_, _>, 'Outcome>(
            log, config, id, consumeResultToStreamEvents, handle, maxDop,
            stats, statsInterval,
            ?maxSubmissionsPerPartition=maxSubmissionsPerPartition,
            ?pumpInterval=pumpInterval,
            ?logExternalState=logExternalState,
            ?idleDelay=idleDelay, ?purgeInterval=purgeInterval,
            ?maxBatches=maxBatches,
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
            /// Handler responses:
            /// - the result seq is expected to match the ordering of the input <c>DispatchItem</c>s
            /// - Choice1Of2: Index at which next processing will proceed (which can trigger discarding of earlier items on that stream)
            /// - Choice2Of2: Records the processing of the stream in question as having faulted (the stream's pending events and/or
            ///   new ones that arrived while the handler was processing are then eligible for retry purposes in the next dispatch cycle)
            handle : Streams.Scheduling.DispatchItem<_>[] -> Async<seq<Choice<int64, exn>>>,
            /// The responses from each <c>handle</c> invocation are passed to <c>stats</c> for periodic emission
            stats : Streams.Scheduling.Stats<EventMetrics * unit, EventMetrics * exn>, statsInterval,
            /// Maximum number of batches to ingest for scheduling at any one time (Default: 24.)
            /// NOTE Stream-wise consumption defaults to taking 5 batches each time replenishment is required
            ?schedulerIngestionBatchCount, ?maxSubmissionsPerPartition,
            /// Default 5ms
            ?pumpInterval,
            ?logExternalState,
            /// Tune the sleep time when there are no items to schedule or responses to process. Default 1ms.
            ?idleDelay,
            /// Frequency with which to jettison Write Position information for inactive streams in order to limit memory consumption
            /// NOTE: Can impair performance and/or increase costs of writes as it inhibits the ability of the ingester to discard redundant inputs
            ?purgeInterval) =
        let maxBatches = defaultArg schedulerIngestionBatchCount 24
        let dumpStreams (streams : Streams.Scheduling.StreamStates<_>) log =
            logExternalState |> Option.iter (fun f -> f log)
            streams.Dump(log, Streams.Buffering.StreamState.eventsSize)
        let handle (items : Streams.Scheduling.DispatchItem<byte[]>[])
            : Async<(StreamName * Choice<int64 * (EventMetrics * unit), EventMetrics * exn>)[]> = async {
            try let! results = handle items
                return
                    [| for x in Seq.zip items results ->
                        match x with
                        | item, Choice1Of2 index' ->
                            let used : Streams.StreamSpan<_> = { item.span with events = item.span.events |> Seq.takeWhile (fun e -> e.Index <> index' ) |> Array.ofSeq }
                            let s = Streams.Buffering.StreamSpan.stats used
                            item.stream, Choice1Of2 (index', (s, ()))
                        | item, Choice2Of2 exn ->
                            let s = Streams.Buffering.StreamSpan.stats item.span
                            item.stream, Choice2Of2 (s, exn) |]
            with e ->
                return
                    [| for x in items ->
                        let s = Streams.Buffering.StreamSpan.stats x.span
                        x.stream, Choice2Of2 (s, e) |] }
        let dispatcher = Streams.Scheduling.BatchedDispatcher(select, handle, stats, dumpStreams)
        let streamsScheduler = Streams.Scheduling.StreamSchedulingEngine.Create(dispatcher, ?idleDelay=idleDelay, ?purgeInterval=purgeInterval, maxBatches=maxBatches)
        let mapConsumedMessagesToStreamsBatch onCompletion (x : Submission.SubmissionBatch<TopicPartition, 'Info>) : Streams.Scheduling.StreamsBatch<_> =
            let onCompletion () = x.onCompletion(); onCompletion()
            Streams.Scheduling.StreamsBatch.Create(onCompletion, Seq.collect infoToStreamEvents x.messages) |> fst
        let submitter =
            Streams.Projector.StreamsSubmitter.Create
                (   log, mapConsumedMessagesToStreamsBatch, streamsScheduler.Submit, statsInterval,
                    ?maxSubmissionsPerPartition=maxSubmissionsPerPartition, ?pumpInterval=pumpInterval)
        ConsumerPipeline.Start(log, config, consumeResultToInfo, submitter.Ingest, submitter.Pump(), streamsScheduler.Pump, dispatcher.Pump(), statsInterval)
