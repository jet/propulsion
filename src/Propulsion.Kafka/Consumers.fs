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
            mutable highWaterMark : ConsumeResult<string,string> // hang on to it so we can generate a checkpointing lambda
            messages : ResizeArray<'M> }
        member __.Enqueue(sz, message, mapMessage) =
            __.highWaterMark <- message 
            __.reservation <- __.reservation + sz // size we need to unreserve upon completion
            __.messages.Add(mapMessage message)
        static member Create(sz,message,mapMessage) =
            let x = { reservation = 0L; highWaterMark = null; messages = ResizeArray(256) }
            x.Enqueue(sz, message, mapMessage)
            x

    /// guesstimate approximate message size in bytes
    let approximateMessageBytes (message : ConsumeResult<string, string>) =
        let inline len (x:string) = match x with null -> 0 | x -> sizeof<char> * x.Length
        16 + len message.Key + len message.Value |> int64
    let inline mb x = float x / 1024. / 1024.

/// Continuously polls across the assigned partitions, building spans; periodically (at intervals of `emitInterval`), `submit`s accumulated messages as
///   checkpointable Batches
/// Pauses if in-flight upper threshold is breached until such time as it drops below that the lower limit
type KafkaIngestionEngine<'M>
    (   log : ILogger, counter : Core.InFlightMessageCounter, consumer : IConsumer<_,_>, closeConsumer,
        mapMessage : ConsumeResult<_,_> -> 'M, emit : Submission.SubmissionBatch<'M>[] -> unit,
        maxBatchSize, emitInterval, statsInterval) =
    let acc = Dictionary<int,_>()
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
    let ingest message =
        let sz = approximateMessageBytes message
        counter.Delta(+sz) // counterbalanced by Delta(-) in checkpoint(), below
        intervalMsgs <- intervalMsgs + 1L
        let inline stringLen (s : string) = match s with null -> 0 | x -> x.Length
        intervalChars <- intervalChars + int64 (stringLen message.Key + stringLen message.Value)
        let partitionId = Bindings.partitionId message
        let span =
            match acc.TryGetValue partitionId with
            | false, _ -> let span = PartitionBuffer<'M>.Create(sz,message,mapMessage) in acc.[partitionId] <- span; span
            | true, span -> span.Enqueue(sz,message,mapMessage); span
        if span.messages.Count = maxBatchSize then
            acc.Remove partitionId |> ignore
            emit [| mkSubmission partitionId span |]
    let submit () =
        match acc.Count with
        | 0 -> ()
        | partitionsWithMessagesThisInterval ->
            let tmp = ResizeArray<Submission.SubmissionBatch<'M>>(partitionsWithMessagesThisInterval)
            for KeyValue(partitionIndex,span) in acc do
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
        log.Information("Consuming... {broker} {topics} {groupId} autoOffsetReset {autoOffsetReset} fetchMaxBytes={fetchMaxB} maxInFlight={maxInFlightGB:n1}GB maxBatchDelay={maxBatchDelay}s maxBatchSize={maxBatchSize}",
            config.Inner.BootstrapServers, config.Topics, config.Inner.GroupId, (let x = config.Inner.AutoOffsetReset in x.Value), config.Inner.FetchMaxBytes,
            float config.Buffering.maxInFlightBytes / 1024. / 1024. / 1024., maxDelay.TotalSeconds, maxItems)
        let limiterLog = log.ForContext(Serilog.Core.Constants.SourceContextPropertyName, Core.Constants.messageCounterSourceContext)
        let limiter = new Core.InFlightMessageCounter(limiterLog, config.Buffering.minInFlightBytes, config.Buffering.maxInFlightBytes)
        let consumer, closeConsumer = Bindings.createConsumer log config.Inner // teardown is managed by ingester.Pump()
        consumer.Subscribe config.Topics
        let ingester = KafkaIngestionEngine<'M>(log, limiter, consumer, closeConsumer, mapResult, submit, maxItems, maxDelay, statsInterval = statsInterval)
        let cts = new CancellationTokenSource()
        let ct = cts.Token
        let tcs = new TaskCompletionSource<unit>()
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
    static member Start<'M>
        (   log : ILogger, config : KafkaConsumerConfig, maxDop, mapResult : (ConsumeResult<string,string> -> 'M), handle : ('M -> Async<Choice<unit,exn>>),
            ?maxSubmissionsPerPartition, ?pumpInterval, ?statsInterval, ?logExternalStats) =
        let statsInterval = defaultArg statsInterval (TimeSpan.FromMinutes 5.)
        let pumpInterval = defaultArg pumpInterval (TimeSpan.FromMilliseconds 5.)

        let dispatcher = Parallel.Scheduling.Dispatcher maxDop
        let scheduler = Parallel.Scheduling.PartitionedSchedulingEngine<'M>(log, handle, dispatcher.TryAdd, statsInterval, ?logExternalStats=logExternalStats)
        let maxSubmissionsPerPartition = defaultArg maxSubmissionsPerPartition 5
        let mapBatch onCompletion (x : Submission.SubmissionBatch<_>) : Parallel.Scheduling.Batch<'M> =
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
        (   log : ILogger, config : KafkaConsumerConfig, maxDop, handle : KeyValuePair<string,string> -> Async<unit>,
            ?maxSubmissionsPerPartition, ?pumpInterval, ?statsInterval, ?logExternalStats) =
        ParallelConsumer.Start<KeyValuePair<string,string>>(log, config, maxDop, Bindings.mapConsumeResult, handle >> Async.Catch,
            ?maxSubmissionsPerPartition=maxSubmissionsPerPartition, ?pumpInterval=pumpInterval, ?statsInterval=statsInterval, ?logExternalStats=logExternalStats)

type OkResult<'R> = int64*(int*int)*'R
type FailResult = (int*int) * exn

[<AbstractClass>]
type StreamsConsumerStats<'R>(log : ILogger, statsInterval, stateInterval) =
    inherit Streams.Scheduling.StreamSchedulerStats<OkResult<'R>,FailResult>(log, statsInterval, stateInterval)
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
        | Propulsion.Streams.Scheduling.InternalMessage.Result (_duration, (stream, Choice1Of2 (_,(es,bs),res))) ->
            adds stream okStreams
            okEvents <- okEvents + es
            okBytes <- okBytes + int64 bs
            __.HandleOk res
        | Propulsion.Streams.Scheduling.InternalMessage.Result (_duration, (stream, Choice2Of2 ((es,bs),_exn))) ->
            adds stream failStreams
            exnEvents <- exnEvents + es
            exnBytes <- exnBytes + int64 bs

    abstract member HandleOk : outcome : 'R -> unit

/// APIs only required for advanced scenarios (specifically the integration tests)
/// APIs within are not part of the stable API and are subject to unlimited change
module Core =

    type StreamsConsumer =

        static member Start<'M,'Req,'Res>
            (   log : ILogger, config : KafkaConsumerConfig, mapConsumeResult, parseStreamEvents,
                prepare, handle, maxDop, stats : Streams.Scheduling.StreamSchedulerStats<OkResult<'Res>,FailResult>,
                ?pipelineStatsInterval, ?maxSubmissionsPerPartition, ?pumpInterval, ?logExternalState, ?idleDelay, ?maxBatches, ?maximizeOffsetWriting) =
            let pipelineStatsInterval = defaultArg pipelineStatsInterval (TimeSpan.FromMinutes 10.)
            let dispatcher = Streams.Scheduling.ItemDispatcher<_> maxDop
            let dumpStreams (streams : Streams.Scheduling.StreamStates<_>) log =
                logExternalState |> Option.iter (fun f -> f log)
                streams.Dump(log, Streams.Buffering.StreamState.eventsSize)
            let streamsScheduler = Streams.Scheduling.StreamSchedulingEngine.Create<_,_,_>(dispatcher, stats, prepare, handle, dumpStreams, ?idleDelay=idleDelay, ?maxBatches=maxBatches)
            let mapConsumedMessagesToStreamsBatch onCompletion (x : Submission.SubmissionBatch<KeyValuePair<string,string>>) : Streams.Scheduling.StreamsBatch<_> =
                let onCompletion () = x.onCompletion(); onCompletion()
                Streams.Scheduling.StreamsBatch.Create(onCompletion, Seq.collect parseStreamEvents x.messages) |> fst
            let submitter =
                Streams.Projector.StreamsSubmitter.Create
                    (   log, mapConsumedMessagesToStreamsBatch,
                        streamsScheduler.Submit, pipelineStatsInterval,
                        ?maxSubmissionsPerPartition=maxSubmissionsPerPartition, ?pumpInterval=pumpInterval,
                        ?disableCompaction=maximizeOffsetWriting)
            ConsumerPipeline.Start(log, config, mapConsumeResult, submitter.Ingest, submitter.Pump(), streamsScheduler.Pump, dispatcher.Pump(), pipelineStatsInterval)

        static member Start<'M,'Res>
            (   log : ILogger, config : KafkaConsumerConfig, mapConsumeResult, parseStreamEvents,
                handle : StreamName * Streams.StreamSpan<_> -> Async<'Res>, maxDop,
                stats : Streams.Scheduling.StreamSchedulerStats<OkResult<'Res>,FailResult>,
                ?pipelineStatsInterval, ?maxSubmissionsPerPartition, ?pumpInterval, ?logExternalState, ?idleDelay, ?maxBatches, ?maximizeOffsetWriting) =
            let prepare (streamName,span) =
                let stats = Streams.Buffering.StreamSpan.stats span
                stats,(streamName,span)
            let handle (streamName,span : Streams.StreamSpan<_>) = async {
                let! res = handle (streamName,span)
                return span.events.Length,res }
            StreamsConsumer.Start<'M,(StreamName*Propulsion.Streams.StreamSpan<_>),'Res>(
                log, config, mapConsumeResult, parseStreamEvents, prepare, handle, maxDop, stats,
                ?pipelineStatsInterval = pipelineStatsInterval,
                ?maxSubmissionsPerPartition = maxSubmissionsPerPartition,
                ?pumpInterval = pumpInterval,
                ?logExternalState = logExternalState,
                ?idleDelay = idleDelay,
                ?maxBatches = maxBatches,
                ?maximizeOffsetWriting = maximizeOffsetWriting)

    /// StreamsConsumer buffers and deduplicates messages from a contiguous stream with each message bearing an `index`.
    /// Where the messages we consume don't have such characteristics, we need to maintain a fake `index` by keeping an int per stream in a dictionary
    type StreamKeyEventSequencer() =
        // we synthesize a monotonically increasing index to render the deduplication facility inert
        let indices = System.Collections.Generic.Dictionary()
        let genIndex streamName =
            let streamName = match streamName with null -> "" | x -> x // Kafka keys can be null, Dictionary ones cannot
            match indices.TryGetValue streamName with
            | true, v -> let x = v + 1 in indices.[streamName] <- x; int64 x
            | false, _ -> let x = 0 in indices.[streamName] <- x; int64 x

        // Stuff the full content of the message into an Event record - we'll parse it when it comes out the other end in a span
        member __.ToStreamEvent(KeyValue (k,v : string), ?eventType) : Propulsion.Streams.StreamEvent<byte[]> seq =
            let e = FsCodec.Core.TimelineEvent.Create(genIndex k,defaultArg eventType String.Empty,System.Text.Encoding.UTF8.GetBytes v)
            Seq.singleton { stream = Propulsion.Streams.StreamName.internalParseSafe k; event=e }

type StreamsConsumer =

    /// Starts a Kafka Consumer processing pipeline per the `config` running up to `maxDop` instances of `handle` concurrently to maximize global throughput across partitions.
    /// Processor pumps until `handle` yields a `Choice2Of2` or `Stop()` is requested.
    static member Start<'M,'Res>
        (   log : ILogger, config : KafkaConsumerConfig, parseStreamEvents,
            prepare, handle, maxDop, stats : Streams.Scheduling.StreamSchedulerStats<OkResult<'Res>,FailResult>,
            /// Prevent batches being consolidated prior to scheduling in order to maximize granularity of consumer offset updates
            ?maximizeOffsetWriting,
            ?pipelineStatsInterval, ?maxSubmissionsPerPartition, ?pumpInterval, ?logExternalState, ?idleDelay)=
        Core.StreamsConsumer.Start<'M,(string*Propulsion.Streams.StreamSpan<_>),'Res>(
            log, config, Bindings.mapConsumeResult, parseStreamEvents, prepare, handle, maxDop, stats,
            ?pipelineStatsInterval = pipelineStatsInterval,
            ?maxSubmissionsPerPartition = maxSubmissionsPerPartition,
            ?pumpInterval = pumpInterval,
            ?logExternalState = logExternalState,
            ?idleDelay = idleDelay,
            ?maximizeOffsetWriting = maximizeOffsetWriting)

    /// Starts a Kafka Consumer running spans of events per stream through the `handle` function to `maxDop` concurrently
    /// Processor statistics are accumulated serially into the supplied `stats` buffer
    /// Processor pumps until `Stop()` is requested.
    /// Handler `Choice1Of2` result must indicate Index at which next processing will proceed (which can trigger discarding of earlier items on that stream)
    /// Handler `Choice2Of2` result marks the processing of a stream failed (which will then be offered again for retry purposes on the next cycle)
    static member Start<'M,'Res>
        (   log : ILogger, config : KafkaConsumerConfig, parseStreamEvents,
            handle : StreamName * Streams.StreamSpan<_> -> Async<'Res>, maxDop,
            stats : Streams.Scheduling.StreamSchedulerStats<OkResult<'Res>,FailResult>,
            /// Prevent batches being consolidated prior to scheduling in order to maximize granularity of consumer offset updates
            ?maximizeOffsetWriting,
            ?pipelineStatsInterval, ?maxSubmissionsPerPartition, ?pumpInterval, ?logExternalState, ?idleDelay, ?maxBatches) =
        Core.StreamsConsumer.Start<'M,'Res>(
            log, config, Bindings.mapConsumeResult, parseStreamEvents, handle, maxDop, stats,
            ?pipelineStatsInterval = pipelineStatsInterval,
            ?maxSubmissionsPerPartition = maxSubmissionsPerPartition,
            ?pumpInterval = pumpInterval,
            ?logExternalState = logExternalState,
            ?idleDelay = idleDelay,
            ?maxBatches = maxBatches,
            ?maximizeOffsetWriting = maximizeOffsetWriting)

type BatchesConsumer =

    /// Starts a Kafka Consumer processing pipeline per the `config` that loops continuously
    /// 1. providing all pending items the scheduler has ingested (controlled via `maxBatches` and incoming batch sizes) to `select` to determine streams to process
    /// 2. (and, if any `select`ed) passing them to the `handle`r for processing
    /// Processor pumps until `Stop()` is requested.
    /// Handler `Choice1Of2` result must indicate Index at which next processing will proceed (which can trigger discarding of earlier items on that stream)
    /// Handler `Choice2Of2` result marks the processing of a stream failed (which will then be offered again for retry purposes on the next cycle)
    static member Start<'M>
        (   log : ILogger, config : KafkaConsumerConfig, mapConsumeResult, parseStreamEvents,
            select, handle, stats : Streams.Scheduling.StreamSchedulerStats<OkResult<unit>,FailResult>,
            /// Maximum number of batches to ingest for scheduling at any one time (Default: 24.)
            /// NOTE Stream-wise consumption defaults to taking 5 batches each time replenishment is required
            ?schedulerIngestionBatchCount, ?pipelineStatsInterval, ?maxSubmissionsPerPartition, ?pumpInterval, ?logExternalState, ?idleDelay) =
        let maxBatches = defaultArg schedulerIngestionBatchCount 24
        let pipelineStatsInterval = defaultArg pipelineStatsInterval (TimeSpan.FromMinutes 10.)
        let dumpStreams (streams : Streams.Scheduling.StreamStates<_>) log =
            logExternalState |> Option.iter (fun f -> f log)
            streams.Dump(log, Streams.Buffering.StreamState.eventsSize)
        let handle (items : Streams.Scheduling.DispatchItem<byte[]>[]) : Async<(StreamName*Choice<int64*(int*int)*unit,(int*int)*exn>)[]> = async {
            try let! results = handle items
                return
                    [| for x in Seq.zip items results ->
                        match x with
                        | item, Choice1Of2 index' ->
                            let used : Streams.StreamSpan<_> = { item.span with events = item.span.events |> Seq.takeWhile (fun e -> e.Index <> index' ) |> Array.ofSeq }
                            let s = Streams.Buffering.StreamSpan.stats used
                            item.stream,Choice1Of2 (index',s,())
                        | item, Choice2Of2 exn ->
                            let s = Streams.Buffering.StreamSpan.stats item.span
                            item.stream,Choice2Of2 (s,exn) |]
            with e ->
                return
                    [| for x in items ->
                        let s = Streams.Buffering.StreamSpan.stats x.span
                        x.stream,Choice2Of2 (s,e) |] }
        let dispatcher = Streams.Scheduling.BatchedDispatcher(select, handle, stats, dumpStreams)
        let streamsScheduler = Streams.Scheduling.StreamSchedulingEngine.Create(dispatcher, ?idleDelay=idleDelay, maxBatches=maxBatches)
        let mapConsumedMessagesToStreamsBatch onCompletion (x : Submission.SubmissionBatch<'M>) : Streams.Scheduling.StreamsBatch<_> =
            let onCompletion () = x.onCompletion(); onCompletion()
            Streams.Scheduling.StreamsBatch.Create(onCompletion, Seq.collect parseStreamEvents x.messages) |> fst
        let submitter = Streams.Projector.StreamsSubmitter.Create(log, mapConsumedMessagesToStreamsBatch, streamsScheduler.Submit, pipelineStatsInterval, ?maxSubmissionsPerPartition=maxSubmissionsPerPartition, ?pumpInterval=pumpInterval)
        ConsumerPipeline.Start(log, config, mapConsumeResult, submitter.Ingest, submitter.Pump(), streamsScheduler.Pump, dispatcher.Pump(), pipelineStatsInterval)