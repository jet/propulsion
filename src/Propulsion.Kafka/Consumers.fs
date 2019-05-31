namespace Propulsion.Kafka

open Confluent.Kafka
open Jet.ConfluentKafka.FSharp
open Propulsion
open Serilog
open System
open System.Collections.Generic
open System.Diagnostics
open System.Threading
open System.Threading.Tasks

[<AutoOpen>]
module private Helpers =

    /// Maintains a Stopwatch used to drive a periodic loop, computing the remaining portion of the period per invocation
    /// - `Some remainder` if the interval has time remaining
    /// - `None` if the interval has expired (and triggers restarting the timer)
    let intervalTimer (period : TimeSpan) =
        let timer = Stopwatch.StartNew()
        fun () ->
            match period - timer.Elapsed with
            | remainder when remainder.Ticks > 0L -> Some remainder
            | _ -> timer.Restart(); None

/// Manages efficiently and continuously reading from the Confluent.Kafka consumer, offloading the pushing of those batches onward to the Submitter
/// Responsible for ensuring we over-read, which would cause the rdkafka buffers to overload the system in terms of memory usage
[<AutoOpen>]
module KafkaIngestion =

    /// Retains the messages we've accumulated for a given Partition
    [<NoComparison>]
    type PartitionSpan<'M> =
        {   mutable reservation : int64 // accumulate reserved in flight bytes so we can reverse the reservation when it completes
            mutable highWaterMark : ConsumeResult<string,string> // hang on to it so we can generate a checkpointing lambda
            messages : ResizeArray<'M> }
        member __.Append(sz, message, mapMessage) =
            __.highWaterMark <- message 
            __.reservation <- __.reservation + sz // size we need to unreserve upon completion
            __.messages.Add(mapMessage message)
        static member Create(sz,message,mapMessage) =
            let x = { reservation = 0L; highWaterMark = null; messages = ResizeArray(256) }
            x.Append(sz, message, mapMessage)
            x

    /// guesstimate approximate message size in bytes
    let approximateMessageBytes (message : ConsumeResult<string, string>) =
        let inline len (x:string) = match x with null -> 0 | x -> sizeof<char> * x.Length
        16 + len message.Key + len message.Value |> int64

    /// Continuously polls across the assigned partitions, building spans; periodically (at intervals of `emitInterval`), `submit`s accummulated messages as
    ///   checkpointable Batches
    /// Pauses if in-flight upper threshold is breached until such time as it drops below that the lower limit
    type KafkaIngestionEngine<'M>
        (   log : ILogger, counter : Core.InFlightMessageCounter, consumer : IConsumer<_,_>, mapMessage : ConsumeResult<_,_> -> 'M, emit : Submission.SubmissionBatch<'M>[] -> unit,
            emitInterval, statsInterval) =
        let acc = Dictionary()
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
        let ingest message =
            let sz = approximateMessageBytes message
            counter.Delta(+sz) // counterbalanced by Delta(-) in checkpoint(), below
            intervalMsgs <- intervalMsgs + 1L
            intervalChars <- intervalChars + int64 (message.Key.Length + message.Value.Length)
            let partitionId = let p = message.Partition in p.Value
            match acc.TryGetValue partitionId with
            | false, _ -> acc.[partitionId] <- PartitionSpan<'M>.Create(sz,message,mapMessage)
            | true, span -> span.Append(sz,message,mapMessage)
        let submit () =
            match acc.Count with
            | 0 -> ()
            | partitionsWithMessagesThisInterval ->
                let tmp = ResizeArray<Submission.SubmissionBatch<'M>>(partitionsWithMessagesThisInterval)
                for KeyValue(partitionIndex,span) in acc do
                    let checkpoint () =
                        counter.Delta(-span.reservation) // counterbalance Delta(+) per ingest, above
                        try consumer.StoreOffset(span.highWaterMark)
                        with e -> log.Error(e, "Consuming... storing offsets failed")
                    tmp.Add { partitionId = partitionIndex; onCompletion = checkpoint; messages = span.messages.ToArray() }
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
                        try match consumer.Consume(intervalRemainder) with
                            | null -> ()
                            | message -> ingest message
                        with| :? System.OperationCanceledException -> log.Warning("Consuming... cancelled")
                            | :? ConsumeException as e -> log.Warning(e, "Consuming... exception")
            finally
                submit () // We don't want to leak our reservations against the counter and want to pass of messages we ingested
                dumpStats () // Unconditional logging when completing
                consumer.Close() (* Orderly Close() before Dispose() is critical *) }

/// Consumption pipeline that attempts to maximize concurrency of `handle` invocations (up to `dop` concurrently).
/// Consumes according to the `config` supplied to `Start`, until `Stop()` is requested or `handle` yields a fault.
/// Conclusion of processing can be awaited by via `AwaitCompletion()`.
type ConsumerPipeline private (inner : IConsumer<string, string>, task : Task<unit>, triggerStop) =

    interface IDisposable with member __.Dispose() = __.Stop()

    /// Provides access to the Confluent.Kafka interface directly
    member __.Inner = inner
    /// Inspects current status of processing task
    member __.Status = task.Status
    /// After AwaitCompletion, can be used to infer whether exit was clean
    member __.RanToCompletion = task.Status = TaskStatus.RanToCompletion 

    /// Request cancellation of processing
    member __.Stop() = triggerStop ()

    /// Asynchronously awaits until consumer stops or a `handle` invocation yields a fault
    member __.AwaitCompletion() = Async.AwaitTaskCorrect task

    /// Builds a processing pipeline per the `config` running up to `dop` instances of `handle` concurrently to maximize global throughput across partitions.
    /// Processor pumps until `handle` yields a `Choice2Of2` or `Stop()` is requested.
    static member Start(log : ILogger, config : KafkaConsumerConfig, mapResult, submit, pumpSubmitter, pumpScheduler, pumpDispatcher, statsInterval) =
        log.Information("Consuming... {broker} {topics} {groupId} autoOffsetReset {autoOffsetReset} fetchMaxBytes={fetchMaxB} maxInFlight={maxInFlightGB:n1}GB maxBatchDelay={maxBatchDelay}s",
            config.Inner.BootstrapServers, config.Topics, config.Inner.GroupId, (let x = config.Inner.AutoOffsetReset in x.Value), config.Inner.FetchMaxBytes,
            float config.Buffering.maxInFlightBytes / 1024. / 1024. / 1024., (let t = config.Buffering.maxBatchDelay in t.TotalSeconds))
        let limiterLog = log.ForContext(Serilog.Core.Constants.SourceContextPropertyName, Core.Constants.messageCounterSourceContext)
        let limiter = new Core.InFlightMessageCounter(limiterLog, config.Buffering.minInFlightBytes, config.Buffering.maxInFlightBytes)
        let consumer = ConsumerBuilder.WithLogging(log, config) // teardown is managed by ingester.Pump()
        let ingester = KafkaIngestionEngine<'M>(log, limiter, consumer, mapResult, submit, emitInterval = config.Buffering.maxBatchDelay, statsInterval = statsInterval)
        let cts = new CancellationTokenSource()
        let ct = cts.Token
        let tcs = new TaskCompletionSource<unit>()
        let triggerStop () =
            log.Information("Consuming ... Stopping {name}", consumer.Name)
            cts.Cancel();  
        let start name f =
            let wrap (name : string) computation = async {
                try do! computation
                    log.Information("Exiting pipeline component {name}", name)
                with e ->
                    log.Fatal(e, "Abend from pipeline component {name}", name)
                    triggerStop() }
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

[<AbstractClass; Sealed>]
type ParallelConsumer private () =
    /// Builds a processing pipeline per the `config` running up to `dop` instances of `handle` concurrently to maximize global throughput across partitions.
    /// Processor pumps until `handle` yields a `Choice2Of2` or `Stop()` is requested.
    static member Start<'M>
        (   log : ILogger, config : KafkaConsumerConfig, maxDop, mapResult : (ConsumeResult<string,string> -> 'M), handle : ('M -> Async<Choice<unit,exn>>),
            ?maxSubmissionsPerPartition, ?pumpInterval, ?statsInterval, ?logExternalStats) =
        let statsInterval = defaultArg statsInterval (TimeSpan.FromMinutes 5.)
        let pumpInterval = defaultArg pumpInterval (TimeSpan.FromMilliseconds 5.)
        let maxSubmissionsPerPartition = defaultArg maxSubmissionsPerPartition 5

        let dispatcher = Parallel.Scheduling.Dispatcher maxDop
        let scheduler = Parallel.Scheduling.PartitionedSchedulingEngine<'M>(log, handle, dispatcher.TryAdd, statsInterval, ?logExternalStats=logExternalStats)
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
        let mapConsumeResult (x : ConsumeResult<string,string>) = KeyValuePair(x.Key, x.Value)
        ParallelConsumer.Start<KeyValuePair<string,string>>(log, config, maxDop, mapConsumeResult, handle >> Async.Catch,
            ?maxSubmissionsPerPartition=maxSubmissionsPerPartition, ?pumpInterval=pumpInterval, ?statsInterval=statsInterval, ?logExternalStats=logExternalStats)