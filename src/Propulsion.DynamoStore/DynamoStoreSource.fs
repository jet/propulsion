namespace Propulsion.DynamoStore

open Equinox.DynamoStore
open FSharp.Control
open Propulsion.Internal
open System
open System.Threading
open System.Threading.Tasks

module private Impl =

    let renderPos (Checkpoint.Parse (epochId, offset)) = sprintf"%s@%d" (AppendsEpochId.toString epochId) offset

    let readPartitions storeLog context =
        let index = AppendsIndex.Reader.create storeLog context
        index.ReadKnownPartitions()

    let readTailPositionForPartition log context (AppendsPartitionId.Parse partitionId) ct = task {
        let index = AppendsIndex.Reader.create log context
        let! epochId = index.ReadIngestionEpochId(partitionId) |> Async.startImmediateAsTask ct
        let epochs = AppendsEpoch.Reader.Config.create log context
        let! version = epochs.ReadVersion(partitionId, epochId) |> Async.startImmediateAsTask ct
        return Checkpoint.positionOfEpochAndOffset epochId version }

    let logReadFailure (storeLog : Serilog.ILogger) =
        let force = storeLog.IsEnabled Serilog.Events.LogEventLevel.Verbose
        function
        | Exceptions.ProvisionedThroughputExceeded when not force -> ()
        | e -> storeLog.Warning(e, "DynamoStoreSource read failure")

    let logCommitFailure (storeLog : Serilog.ILogger) =
        let force = storeLog.IsEnabled Serilog.Events.LogEventLevel.Verbose
        function
        | Exceptions.ProvisionedThroughputExceeded when not force -> ()
        | e -> storeLog.Warning(e, "DynamoStoreSource commit failure")

    let mkBatch position isTail items : Propulsion.Feed.Core.Batch<Propulsion.Streams.Default.EventBody> =
        { items = items; checkpoint = position; isTail = isTail }
    let sliceBatch epochId offset items =
        mkBatch (Checkpoint.positionOfEpochAndOffset epochId offset) false items
    let finalBatch epochId (version, state : AppendsEpoch.Reader.State) items =
        mkBatch (Checkpoint.positionOfEpochClosedAndVersion epochId state.closed version) (not state.closed) items

    // Includes optional hydrating of events with event bodies and/or metadata (controlled via hydrating/maybeLoad args)
    let materializeIndexEpochAsBatchesOfStreamEvents
            (log : Serilog.ILogger, sourceId, storeLog) (hydrating, maybeLoad : _  -> _ -> (CancellationToken -> Task<_>) voption, loadDop) batchCutoff (context : DynamoStoreContext)
            (AppendsPartitionId.Parse pid, Checkpoint.Parse (epochId, offset), ct) = taskSeq {
        let epochs = AppendsEpoch.Reader.Config.create storeLog context
        let sw = Stopwatch.start ()
        let! _maybeSize, version, state = epochs.Read(pid, epochId, offset) |> Async.startImmediateAsTask ct
        let totalChanges = state.changes.Length
        sw.Stop()
        let totalStreams, chosenEvents, totalEvents, streamEvents =
            let all = state.changes |> Seq.collect (fun struct (_i, xs) -> xs) |> AppendsEpoch.flatten |> Array.ofSeq
            let totalEvents = all |> Array.sumBy (fun x -> x.c.Length)
            let mutable chosenEvents = 0
            let chooseStream (span : AppendsEpoch.Events.StreamSpan) =
                match maybeLoad (IndexStreamId.toStreamName span.p) (span.i, span.c) with
                | ValueSome f ->
                    chosenEvents <- chosenEvents + span.c.Length
                    ValueSome (span.p, f)
                | ValueNone -> ValueNone
            let streamEvents = all |> Seq.chooseV chooseStream |> dict
            all.Length, chosenEvents, totalEvents, streamEvents
        let largeEnoughToLog = streamEvents.Count > batchCutoff
        if largeEnoughToLog then
            log.Information("DynamoStoreSource {source}/{partition}/{epochId}@{offset} {mode:l} {totalChanges} changes {loadingS}/{totalS} streams {loadingE}/{totalE} events",
                            sourceId, string pid, string epochId, offset, (if hydrating then "Hydrating" else "Feeding"), totalChanges, streamEvents.Count, totalStreams, chosenEvents, totalEvents)

        let buffer, cache = ResizeArray<AppendsEpoch.Events.StreamSpan>(), System.Collections.Concurrent.ConcurrentDictionary()
        // For each batch we produce, we load any streams we have not already loaded at this time
        let materializeSpans ct = task {
            let loadsRequired =
                [| let streamsToLoad = seq { for span in buffer do if not (cache.ContainsKey(span.p)) then span.p }
                   for p in Seq.distinct streamsToLoad -> fun ct -> task {
                        let! items = streamEvents[p] ct
                        cache.TryAdd(p, items) |> ignore } |]
            if loadsRequired.Length <> 0 then
                sw.Start()
                do! loadsRequired |> Task.parallelLimit loadDop ct |> Task.ignore<unit[]>
                sw.Stop()
            return [|
                for span in buffer do
                    match cache.TryGetValue span.p with
                    | false, _ -> ()
                    | true, (items : FsCodec.ITimelineEvent<_> array) ->
                        // NOTE this could throw if a span has been indexed, but the stream read is from a replica that does not yet have it
                        //      the exception in that case will trigger a safe re-read from the last saved read position that a consumer has forwarded
                        // TOCONSIDER revise logic to share session key etc to rule this out
                        let events = Array.sub items (span.i - items[0].Index |> int) span.c.Length
                        for e in events -> struct (IndexStreamId.toStreamName span.p, e) |] }
        let mutable prevLoaded, batchIndex = 0L, 0
        let report (i : int option) len =
            if largeEnoughToLog && hydrating then
                match cache.Count with
                | loadedNow when prevLoaded <> loadedNow ->
                    prevLoaded <- loadedNow
                    let eventsLoaded = cache.Values |> Seq.sumBy Array.length
                    log.Information("DynamoStoreSource {source}/{partition}/{epochId}@{offset}/{totalChanges} {result} {batch} {events}e Loaded {loadedS}/{loadingS}s {loadedE}/{loadingE}e",
                                    sourceId, string pid, string epochId, Option.toNullable i, version, "Hydrated", batchIndex, len, cache.Count, streamEvents.Count, eventsLoaded, chosenEvents)
                | _ -> ()
            batchIndex <- batchIndex + 1
        for i, spans in state.changes do
            let pending = spans |> Array.filter (fun (span : AppendsEpoch.Events.StreamSpan) -> streamEvents.ContainsKey(span.p))
            if buffer.Count <> 0 && buffer.Count + pending.Length > batchCutoff then
                let! hydrated = materializeSpans ct
                report (Some i) hydrated.Length
                yield struct (sw.Elapsed, sliceBatch epochId i hydrated) // not i + 1 as the batch does not include these changes
                sw.Reset()
                buffer.Clear()
            buffer.AddRange(pending)
        let! hydrated = materializeSpans ct
        report None hydrated.Length
        yield struct (sw.Elapsed, finalBatch epochId (version, state) hydrated) }

/// Defines the strategy to use for hydrating the events prior to routing them to the Handler
[<NoComparison; NoEquality>]
type EventLoadMode =
    /// Skip loading of Data/Meta for events; this is the most efficient mode as it means the Source only needs to read from the Index Table
    | IndexOnly
    /// Populates the Data/Meta fields for events matching the categories and/or categoryFilter
    /// Requires a roundtrip per stream to the Store Table (constrained by streamParallelismLimit)
    | WithData of /// Maximum concurrency for stream reads from the Store Table
                  streamParallelismLimit : int
                  * /// Defines the Context to use when loading the Event Data/Meta
                  storeContext : DynamoStoreContext
module internal EventLoadMode =
    let private mapTimelineEvent = FsCodec.Core.TimelineEvent.Map FsCodec.Deflate.EncodedToUtf8
    let private withData (eventsContext : Equinox.DynamoStore.Core.EventsContext) categoryFilter =
        fun sn (i, cs : string array) ->
            if categoryFilter (FsCodec.StreamName.category sn) then
                ValueSome (fun ct -> task {
                               let! _pos, events = eventsContext.Read(FsCodec.StreamName.toString sn, ct, i, maxCount = cs.Length)
                               return events |> Array.map mapTimelineEvent })
            else ValueNone
    let private withoutBodies categoryFilter =
        fun sn (i, cs) ->
            let renderEvent offset c = FsCodec.Core.TimelineEvent.Create(i + int64 offset, eventType = c, data = Unchecked.defaultof<_>)
            if categoryFilter (FsCodec.StreamName.category sn) then ValueSome (fun _ct -> task { return cs |> Array.mapi renderEvent }) else ValueNone
    let map categoryFilter storeLog : EventLoadMode -> _ = function
        | IndexOnly -> false, withoutBodies categoryFilter, 1
        | WithData (dop, storeContext) ->
            let eventsContext = Equinox.DynamoStore.Core.EventsContext(storeContext, storeLog)
            true, withData eventsContext categoryFilter, dop

type DynamoStoreSource
    (   log : Serilog.ILogger, statsInterval,
        indexClient : DynamoStoreClient, batchSizeCutoff, tailSleepInterval,
        checkpoints : Propulsion.Feed.IFeedCheckpointStore, sink : Propulsion.Streams.Default.Sink,
        // If the Handler does not utilize the Data/Meta of the events, we can avoid having to read from the Store Table
        mode : EventLoadMode,
        // The whitelist of Categories to use
        ?categories,
        // Predicate to filter Categories to use
        ?categoryFilter : string -> bool,
        // Override default start position to be at the tail of the index. Default: Replay all events.
        ?startFromTail,
        // Separated log for DynamoStore calls in order to facilitate filtering and/or gathering metrics
        ?storeLog,
        ?readFailureSleepInterval,
        ?sourceId,
        ?trancheIds) =
    inherit Propulsion.Feed.Core.TailingFeedSource
        (   log, statsInterval, defaultArg sourceId FeedSourceId.wellKnownId, tailSleepInterval,
            checkpoints,
            (   if startFromTail <> Some true then None
                else Some (Impl.readTailPositionForPartition (defaultArg storeLog log) (DynamoStoreContext indexClient))),
            sink,
            Impl.materializeIndexEpochAsBatchesOfStreamEvents
                (log, defaultArg sourceId FeedSourceId.wellKnownId, defaultArg storeLog log)
                (EventLoadMode.map (Propulsion.Feed.Core.Categories.mapFilters categories categoryFilter) (defaultArg storeLog log) mode)
                batchSizeCutoff (DynamoStoreContext indexClient),
            Impl.renderPos,
            Impl.logReadFailure (defaultArg storeLog log),
            defaultArg readFailureSleepInterval (tailSleepInterval * 2.),
            Impl.logCommitFailure (defaultArg storeLog log))

    abstract member ListTranches : ct : CancellationToken -> Task<Propulsion.Feed.TrancheId array>
    default _.ListTranches(ct) = task {
        match trancheIds with
        | Some ids -> return ids
        | None ->
            let context = DynamoStoreContext(indexClient)
            let storeLog = defaultArg storeLog log
            let! res = Impl.readPartitions storeLog context |> Async.startImmediateAsTask ct
            let appendsPartitionIds = match res with [||] -> [| AppendsPartitionId.wellKnownId |] | ids -> ids
            return appendsPartitionIds |> Array.map AppendsPartitionId.toTrancheId }

    abstract member Pump : ct : CancellationToken -> Task<unit>
    default x.Pump(ct) = base.Pump(x.ListTranches, ct)

    abstract member Start : unit -> Propulsion.SourcePipeline<Propulsion.Feed.Core.FeedMonitor>
    default x.Start() = base.Start(x.Pump)

    /// Pumps to the Sink until either the specified timeout has been reached, or all items in the Source have been fully consumed
    member x.RunUntilCaughtUp(timeout : TimeSpan, statsInterval : IntervalTimer) = task {
        let sw = Stopwatch.start ()
        // Kick off reading from the source (Disposal will Stop it if we're exiting due to a timeout; we'll spin up a fresh one when re-triggered)
        use pipeline = x.Start()

        try // In the case of sustained activity and/or catch-up scenarios, proactively trigger an orderly shutdown of the Source
            // in advance of the Lambda being killed (no point starting new work or incurring DynamoDB CU consumption that won't finish)
            Task.Delay(timeout).ContinueWith(fun _ -> pipeline.Stop()) |> ignore

            // If for some reason we're not provisioned well enough to read something within 1m, no point for paying for a full lambda timeout
            let initialReaderTimeout = TimeSpan.FromMinutes 1.
            do! pipeline.Monitor.AwaitCompletion(initialReaderTimeout, awaitFullyCaughtUp = true, logInterval = TimeSpan.FromSeconds 30)
            // Shut down all processing (we create a fresh Source per Lambda invocation)
            pipeline.Stop()

            if sw.ElapsedSeconds > 2 then statsInterval.Trigger()
            // force a final attempt to flush anything not already checkpointed (normally checkpointing is at 5s intervals)
            return! x.Checkpoint()
        finally statsInterval.SleepUntilTriggerCleared() }
