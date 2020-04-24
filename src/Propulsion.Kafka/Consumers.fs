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
    let approximateMessageBytes (message : Message<string, string>) =
        let inline len (x : string) = match x with null -> 0 | x -> sizeof<char> * x.Length
        16 + len message.Key + len message.Value |> int64
    let inline mb x = float x / 1024. / 1024.

/// Continuously polls across the assigned partitions, building spans; periodically (at intervals of `emitInterval`), `submit`s accumulated messages as
///   checkpointable Batches
/// Pauses if in-flight upper threshold is breached until such time as it drops below that the lower limit
type KafkaIngestionEngine<'Info>
    (   log : ILogger, counter : Core.InFlightMessageCounter, consumer : IConsumer<_, _>, closeConsumer,
        mapMessage : ConsumeResult<_, _> -> 'Info, emit : Submission.SubmissionBatch<'Info>[] -> unit,
        maxBatchSize, emitInterval, statsInterval) =
    let acc = Dictionary<int, _>()
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
    let mkSubmission partitionId span : Submission.SubmissionBatch<'M> =
        let checkpoint () =
            counter.Delta(-span.reservation) // counterbalance Delta(+) per ingest, below
            Bindings.storeOffset log consumer span.highWaterMark
        { partitionId = partitionId; onCompletion = checkpoint; messages = span.messages.ToArray() }
    let ingest result =
        let message = Bindings.mapMessage result
        let sz = approximateMessageBytes message
        counter.Delta(+sz) // counterbalanced by Delta(-) in checkpoint(), below
        intervalMsgs <- intervalMsgs + 1L
        let inline stringLen (s : string) = match s with null -> 0 | x -> x.Length
        intervalChars <- intervalChars + int64 (stringLen message.Key + stringLen message.Value)
        let partitionId = Bindings.partitionId result
        let span =
            match acc.TryGetValue partitionId with
            | false, _ -> let span = PartitionBuffer<'Info>.Create(sz, result, mapMessage) in acc.[partitionId] <- span; span
            | true, span -> span.Enqueue(sz, result, mapMessage); span
        if span.messages.Count = maxBatchSize then
            acc.Remove partitionId |> ignore
            emit [| mkSubmission partitionId span |]
    let submit () =
        match acc.Count with
        | 0 -> ()
        | partitionsWithMessagesThisInterval ->
            let tmp = ResizeArray<Submission.SubmissionBatch<'Info>>(partitionsWithMessagesThisInterval)
            for KeyValue(partitionIndex, span) in acc do
                tmp.Add(mkSubmission partitionIndex span)
            acc.Clear()
            emit <| tmp.ToArray()
    member __.Pump() = async {
        let! ct = Async.CancellationToken
        use _ = consumer // Dispose it at the end (NB but one has to Close first or risk AccessViolations etc)
        try while not ct.IsCancellationRequested do
                match counter.IsOverLimitNow(), remainingIngestionWindow () with
                | true, _ ->
                    let busyWork () =
                        submit()
                        maybeLogStats()
                        Thread.Sleep 1
                    counter.AwaitThreshold busyWork
                | false, None ->
                    submit()
                    maybeLogStats()
                | false, Some intervalRemainder ->
                    Bindings.tryConsume log consumer intervalRemainder ingest
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
        let consumer, closeConsumer = Bindings.createConsumer log config.Inner // teardown is managed by ingester.Pump()
        consumer.Subscribe config.Topics
        let ingester = KafkaIngestionEngine<'M>(log, limiter, consumer, closeConsumer, mapResult, submit, maxItems, maxDelay, statsInterval = statsInterval)
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
            ?maxSubmissionsPerPartition, ?pumpInterval, ?statsInterval, ?logExternalStats) =
        let statsInterval = defaultArg statsInterval (TimeSpan.FromMinutes 5.)
        let pumpInterval = defaultArg pumpInterval (TimeSpan.FromMilliseconds 5.)

        let dispatcher = Parallel.Scheduling.Dispatcher maxDop
        let scheduler = Parallel.Scheduling.PartitionedSchedulingEngine<'Msg>(log, handle, dispatcher.TryAdd, statsInterval, ?logExternalStats=logExternalStats)
        let maxSubmissionsPerPartition = defaultArg maxSubmissionsPerPartition 5
        let mapBatch onCompletion (x : Submission.SubmissionBatch<_>) : Parallel.Scheduling.Batch<'Msg> =
            let onCompletion' () = x.onCompletion(); onCompletion()
            { partitionId = x.partitionId; messages = x.messages; onCompletion = onCompletion'; }
        let submitBatch (x : Parallel.Scheduling.Batch<_>) : int =
            scheduler.Submit x
            x.messages.Length
        let submitter = Submission.SubmissionEngine(log, maxSubmissionsPerPartition, mapBatch, submitBatch, statsInterval, pumpInterval)
        ConsumerPipeline.Start(log, config, mapResult, submitter.Ingest, submitter.Pump(), scheduler.Pump, dispatcher.Pump(), statsInterval)

    /// Builds a processing pipeline per the `config` running up to `dop` instances of `handle` concurrently to maximize global throughput across partitions.
    /// Processor pumps until `handle` yields a `Choice2Of2` or `Stop()` is requested.
    static member Start
        (   log : ILogger, config : KafkaConsumerConfig, maxDop, handle : KeyValuePair<string, string> -> Async<unit>,
            ?maxSubmissionsPerPartition, ?pumpInterval, ?statsInterval, ?logExternalStats) =
        ParallelConsumer.Start<KeyValuePair<string, string>>(log, config, maxDop, Bindings.mapConsumeResult, handle >> Async.Catch,
            ?maxSubmissionsPerPartition=maxSubmissionsPerPartition, ?pumpInterval=pumpInterval, ?statsInterval=statsInterval, ?logExternalStats=logExternalStats)

type EventStats = Streams.EventStats

[<AbstractClass>]
type StreamsConsumerStats<'Outcome>(log : ILogger, statsInterval, stateInterval) =
    inherit Streams.Scheduling.StreamSchedulerStats<EventStats * 'Outcome, EventStats * exn>(log, statsInterval, stateInterval)
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
        | Propulsion.Streams.Scheduling.InternalMessage.Result (_duration, (stream, Choice2Of2 ((es, bs), _exn))) ->
            adds stream failStreams
            exnEvents <- exnEvents + es
            exnBytes <- exnBytes + int64 bs

    abstract member HandleOk : outcome : 'Outcome -> unit

/// APIs only required for advanced scenarios (specifically the integration tests)
/// APIs within are not part of the stable API and are subject to unlimited change
module Core =

    type StreamsConsumer =

        static member Start<'Info, 'Outcome>
            (   log : ILogger, config : KafkaConsumerConfig, resultToInfo, infoToStreamEvents,
                prepare, handle, maxDop, stats : Streams.Scheduling.StreamSchedulerStats<int64 * EventStats * 'Outcome, EventStats * exn>,
                ?pipelineStatsInterval, ?maxSubmissionsPerPartition, ?pumpInterval, ?logExternalState, ?idleDelay, ?maxBatches, ?maximizeOffsetWriting) =
            let pipelineStatsInterval = defaultArg pipelineStatsInterval (TimeSpan.FromMinutes 10.)
            let dispatcher = Streams.Scheduling.ItemDispatcher<_> maxDop
            let dumpStreams (streams : Streams.Scheduling.StreamStates<_>) log =
                logExternalState |> Option.iter (fun f -> f log)
                streams.Dump(log, Streams.Buffering.StreamState.eventsSize)
            let streamsScheduler = Streams.Scheduling.StreamSchedulingEngine.Create<_, _, _>(dispatcher, stats, prepare, handle, dumpStreams, ?idleDelay=idleDelay, ?maxBatches=maxBatches)
            let mapConsumedMessagesToStreamsBatch onCompletion (x : Submission.SubmissionBatch<'Info>) : Streams.Scheduling.StreamsBatch<_> =
                let onCompletion () = x.onCompletion(); onCompletion()
                Streams.Scheduling.StreamsBatch.Create(onCompletion, Seq.collect infoToStreamEvents x.messages) |> fst
            let submitter =
                Streams.Projector.StreamsSubmitter.Create
                    (   log, mapConsumedMessagesToStreamsBatch,
                        streamsScheduler.Submit, pipelineStatsInterval,
                        ?maxSubmissionsPerPartition=maxSubmissionsPerPartition, ?pumpInterval=pumpInterval,
                        ?disableCompaction=maximizeOffsetWriting)
            ConsumerPipeline.Start(log, config, resultToInfo, submitter.Ingest, submitter.Pump(), streamsScheduler.Pump, dispatcher.Pump(), pipelineStatsInterval)

        static member Start<'Info, 'Outcome>
            (   log : ILogger, config : KafkaConsumerConfig, consumeResultToInfo, infoToStreamEvents,
                handle : StreamName * Streams.StreamSpan<_> -> Async<int64 * 'Outcome>, maxDop,
                stats : Streams.Scheduling.StreamSchedulerStats<int64 * EventStats * 'Outcome, EventStats * exn>,
                ?pipelineStatsInterval, ?maxSubmissionsPerPartition, ?pumpInterval, ?logExternalState, ?idleDelay, ?maxBatches, ?maximizeOffsetWriting) =
            let prepare (streamName, span) =
                let stats = Streams.Buffering.StreamSpan.stats span
                stats, (streamName, span)
            StreamsConsumer.Start<'Info, 'Outcome>(
                log, config, consumeResultToInfo, infoToStreamEvents, prepare, handle, maxDop, stats,
                ?pipelineStatsInterval = pipelineStatsInterval,
                ?maxSubmissionsPerPartition = maxSubmissionsPerPartition,
                ?pumpInterval = pumpInterval,
                ?logExternalState = logExternalState,
                ?idleDelay = idleDelay,
                ?maxBatches = maxBatches,
                ?maximizeOffsetWriting = maximizeOffsetWriting)

        (* KeyValuePair optimized mappings (these were the original implementation); retained as:
            - the default mapping overloads in Propulsion.Kafka.StreamsConsumer pass the ConsumeResult to parser functions,
              which can potentially induce avoidable memory consumption before the ingestion handler runs the mapping process
            - for symmetry with ParallelConsumer signatures, we provide an out of the box API mapping from the relevant Kafka underlying type to a neutral one *)

        /// Starts a Kafka Consumer running spans of events per stream through the `handle` function to `maxDop` concurrently
        static member Start<'Outcome>
            (   log : ILogger, config : KafkaConsumerConfig,
                /// often implemented via <c>StreamNameSequenceGenerator.KeyValueToStreamEvent</c>
                keyValueToStreamEvents,
                prepare, handle : StreamName * Streams.StreamSpan<_> -> Async<int64 * 'Outcome>,
                maxDop, stats : Streams.Scheduling.StreamSchedulerStats<int64 * EventStats * 'Outcome, EventStats * exn>,
                ?maximizeOffsetWriting, ?pipelineStatsInterval, ?maxSubmissionsPerPartition, ?pumpInterval, ?logExternalState, ?idleDelay)=
            StreamsConsumer.Start<KeyValuePair<string, string>, 'Outcome>(
                log, config, Bindings.mapConsumeResult, keyValueToStreamEvents, prepare, handle, maxDop, stats,
                ?pipelineStatsInterval = pipelineStatsInterval,
                ?maxSubmissionsPerPartition = maxSubmissionsPerPartition,
                ?pumpInterval = pumpInterval,
                ?logExternalState = logExternalState,
                ?idleDelay = idleDelay,
                ?maximizeOffsetWriting = maximizeOffsetWriting)

        /// Starts a Kafka Consumer running spans of events per stream through the `handle` function to `maxDop` concurrently
        static member Start<'Outcome>
            (   log : ILogger, config : KafkaConsumerConfig,
                /// often implemented via <c>StreamNameSequenceGenerator.KeyValueToStreamEvent</c>
                keyValueToStreamEvents : KeyValuePair<string, string> -> Propulsion.Streams.StreamEvent<_> seq,
                handle : StreamName * Streams.StreamSpan<_> -> Async<int64 * 'Outcome>, maxDop,
                stats : Streams.Scheduling.StreamSchedulerStats<int64 * EventStats * 'Outcome, EventStats * exn>,
                ?maximizeOffsetWriting, ?pipelineStatsInterval, ?maxSubmissionsPerPartition, ?pumpInterval, ?logExternalState, ?idleDelay, ?maxBatches) =
            StreamsConsumer.Start<KeyValuePair<string, string>, 'Outcome>(
                log, config, Bindings.mapConsumeResult, keyValueToStreamEvents, handle, maxDop, stats,
                ?pipelineStatsInterval = pipelineStatsInterval,
                ?maxSubmissionsPerPartition = maxSubmissionsPerPartition,
                ?pumpInterval = pumpInterval,
                ?logExternalState = logExternalState,
                ?idleDelay = idleDelay,
                ?maxBatches = maxBatches,
                ?maximizeOffsetWriting = maximizeOffsetWriting)

    // Maps a (potentially `null`) message key to a valid {Category}-{StreamId} StreamName for routing and/or propagation through StreamsProjector
    let parseMessageKey defaultCategory = function
        | null -> FsCodec.StreamName.create defaultCategory ""
        | key -> Propulsion.Streams.StreamName.parseWithDefaultCategory defaultCategory key

/// StreamsConsumer buffers and deduplicates messages from a contiguous stream with each message bearing an `index`.
/// Where the messages we consume don't have such characteristics, we need to maintain a fake `index` by keeping an int per stream in a dictionary
type StreamNameSequenceGenerator() =

    // we synthesize a monotonically increasing index to render the deduplication facility inert
    let indices = System.Collections.Generic.Dictionary()

    /// Generates an index for the specified StreamName
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

    /// Enables customizing of mapping from ConsumeResult to the StreamName<br/>
    /// The body of the message is passed as the <c>ITimelineEvent.Data</c>
    member __.ConsumeResultToStreamEvent(toStreamName : ConsumeResult<_, _> -> StreamName)
        : ConsumeResult<string, string> -> Propulsion.Streams.StreamEvent<byte[]> seq =
        let toDataAndContext (result : ConsumeResult<string, string>) =
            let message = Bindings.mapMessage result
            System.Text.Encoding.UTF8.GetBytes message.Value, null
        __.ConsumeResultToStreamEvent(toStreamName, toDataAndContext)

    /// Enables customizing of mapping from ConsumeResult to
    /// 1) The <c>StreamName</c>
    /// 2) The <c>ITimelineEvent.Data : byte[]</c>, which bears the (potentially transformed in <c>toDataAndContext</c>) UTF-8 payload
    /// 3) The <c>ITimelineEvent.Context : obj</c>, which can be used to include any metadata
    member __.ConsumeResultToStreamEvent
        (    toStreamName : ConsumeResult<_, _> -> StreamName,
             toDataAndContext : ConsumeResult<_, _> -> byte[] * obj)
        : ConsumeResult<string, string> -> Propulsion.Streams.StreamEvent<byte[]> seq =
        let toTimelineEvent (result : ConsumeResult<string, string>, index) =
            let data, context = toDataAndContext result
            FsCodec.Core.TimelineEvent.Create(index, String.Empty, data, context = context)
        __.ConsumeResultToStreamEvent(toStreamName, toTimelineEvent)

    /// Enables customizing of mapping from ConsumeResult to
    /// 1) The <c>ITimelineEvent.Data : byte[]</c>, which bears the (potentially transformed in <c>toDataAndContext</c>) UTF-8 payload
    /// 2) The <c>ITimelineEvent.Context : obj</c>, which can be used to include any metadata
    member __.ConsumeResultToStreamEvent(toDataAndContext : ConsumeResult<_, _> -> byte[] * obj, ?defaultCategory)
        : ConsumeResult<string, string> -> Propulsion.Streams.StreamEvent<byte[]> seq =
        let toStreamName (result : ConsumeResult<string, string>) =
            let message = Bindings.mapMessage result
            Core.parseMessageKey (defaultArg defaultCategory "") message.Key
        let toTimelineEvent (result : ConsumeResult<string, string>, index) =
            let data, context = toDataAndContext result
            FsCodec.Core.TimelineEvent.Create(index, String.Empty, data, context = context)
        __.ConsumeResultToStreamEvent(toStreamName, toTimelineEvent)

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
            handle : StreamName * Streams.StreamSpan<_> -> Async<int64 * 'Outcome>,
            /// The maximum number of instances of <c>handle</c> that are permitted to be dispatched at any point in time.
            /// The scheduler seeks to maximise the in-flight <c>handle</c>rs at any point in time.
            /// The scheduler guarantees to never schedule two concurrent <c>handler<c> invocations for the same stream.
            maxDop,
            /// The <c>'Outcome</c> from each handler invocation is passed to the Statistics processor by the scheduler for periodic emission
            stats : Streams.Scheduling.StreamSchedulerStats<int64 *EventStats * 'Outcome, EventStats * exn>,
            /// Prevent batches being consolidated prior to scheduling in order to maximize granularity of consumer offset updates
            ?maximizeOffsetWriting,
            ?pipelineStatsInterval, ?maxSubmissionsPerPartition, ?pumpInterval, ?logExternalState, ?idleDelay, ?maxBatches) =
        Core.StreamsConsumer.Start<ConsumeResult<_, _>, 'Outcome>(
            log, config, id, consumeResultToStreamEvents, handle, maxDop, stats,
            ?pipelineStatsInterval = pipelineStatsInterval,
            ?maxSubmissionsPerPartition = maxSubmissionsPerPartition,
            ?pumpInterval = pumpInterval,
            ?logExternalState = logExternalState,
            ?idleDelay = idleDelay,
            ?maxBatches = maxBatches,
            ?maximizeOffsetWriting = maximizeOffsetWriting)

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
            stats : Streams.Scheduling.StreamSchedulerStats<int64 * EventStats * unit, EventStats * exn>,
            /// Maximum number of batches to ingest for scheduling at any one time (Default: 24.)
            /// NOTE Stream-wise consumption defaults to taking 5 batches each time replenishment is required
            ?schedulerIngestionBatchCount, ?pipelineStatsInterval, ?maxSubmissionsPerPartition, ?pumpInterval, ?logExternalState, ?idleDelay) =
        let maxBatches = defaultArg schedulerIngestionBatchCount 24
        let pipelineStatsInterval = defaultArg pipelineStatsInterval (TimeSpan.FromMinutes 10.)
        let dumpStreams (streams : Streams.Scheduling.StreamStates<_>) log =
            logExternalState |> Option.iter (fun f -> f log)
            streams.Dump(log, Streams.Buffering.StreamState.eventsSize)
        let handle (items : Streams.Scheduling.DispatchItem<byte[]>[])
            : Async<(StreamName * Choice<int64 * EventStats * unit, EventStats * exn>)[]> = async {
            try let! results = handle items
                return
                    [| for x in Seq.zip items results ->
                        match x with
                        | item, Choice1Of2 index' ->
                            let used : Streams.StreamSpan<_> = { item.span with events = item.span.events |> Seq.takeWhile (fun e -> e.Index <> index' ) |> Array.ofSeq }
                            let s = Streams.Buffering.StreamSpan.stats used
                            item.stream, Choice1Of2 (index', s, ())
                        | item, Choice2Of2 exn ->
                            let s = Streams.Buffering.StreamSpan.stats item.span
                            item.stream, Choice2Of2 (s, exn) |]
            with e ->
                return
                    [| for x in items ->
                        let s = Streams.Buffering.StreamSpan.stats x.span
                        x.stream, Choice2Of2 (s, e) |] }
        let dispatcher = Streams.Scheduling.BatchedDispatcher(select, handle, stats, dumpStreams)
        let streamsScheduler = Streams.Scheduling.StreamSchedulingEngine.Create(dispatcher, ?idleDelay=idleDelay, maxBatches=maxBatches)
        let mapConsumedMessagesToStreamsBatch onCompletion (x : Submission.SubmissionBatch<'Info>) : Streams.Scheduling.StreamsBatch<_> =
            let onCompletion () = x.onCompletion(); onCompletion()
            Streams.Scheduling.StreamsBatch.Create(onCompletion, Seq.collect infoToStreamEvents x.messages) |> fst
        let submitter = Streams.Projector.StreamsSubmitter.Create(log, mapConsumedMessagesToStreamsBatch, streamsScheduler.Submit, pipelineStatsInterval, ?maxSubmissionsPerPartition=maxSubmissionsPerPartition, ?pumpInterval=pumpInterval)
        ConsumerPipeline.Start(log, config, consumeResultToInfo, submitter.Ingest, submitter.Pump(), streamsScheduler.Pump, dispatcher.Pump(), pipelineStatsInterval)
