﻿namespace Propulsion.DynamoStore

open Equinox.DynamoStore
open FSharp.Control
open Propulsion.Internal
open System.Collections.Concurrent

module private Impl =

    type [<Measure>] checkpoint
    type Checkpoint = int64<checkpoint>
    module internal Checkpoint =

        open FSharp.UMX

        let private maxItemsPerEpoch = int64 AppendsEpoch.MaxItemsPerEpoch
        let private ofPosition : Propulsion.Feed.Position -> Checkpoint = Propulsion.Feed.Position.toInt64 >> UMX.tag
        let toPosition : Checkpoint -> Propulsion.Feed.Position = UMX.untag >> Propulsion.Feed.Position.parse

        let ofEpochAndOffset (epoch : AppendsEpochId) offset : Checkpoint =
            int64 (AppendsEpochId.value epoch) * maxItemsPerEpoch + int64 offset |> UMX.tag

        let ofEpochClosedAndVersion (epoch : AppendsEpochId) isClosed version : Checkpoint =
            let epoch, offset =
                if isClosed then AppendsEpochId.next epoch, 0L
                else epoch, version
            ofEpochAndOffset epoch offset

        let private toEpochAndOffset (value : Checkpoint) : AppendsEpochId * int =
            let d, r = System.Math.DivRem(%value, maxItemsPerEpoch)
            (%int %d : AppendsEpochId), int r

        let (|Parse|) : Propulsion.Feed.Position -> AppendsEpochId * int = ofPosition >> toEpochAndOffset

    let renderPos (Checkpoint.Parse (epochId, offset)) = sprintf"%s@%d" (AppendsEpochId.toString epochId) offset

    let readTranches storeLog context =
        let index = AppendsIndex.Reader.create storeLog context
        index.ReadKnownTranches()

    let readTailPositionForTranche log context (AppendsTrancheId.Parse trancheId) = async {
        let index = AppendsIndex.Reader.create log context
        let! epochId = index.ReadIngestionEpochId(trancheId)
        let epochs = AppendsEpoch.Reader.Config.create log context
        let! version = epochs.ReadVersion(trancheId, epochId)
        return Checkpoint.ofEpochAndOffset epochId version |> Checkpoint.toPosition }

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

    let mkBatch checkpoint isTail items : Propulsion.Feed.Core.Batch<_> =
        { items = items; checkpoint = Checkpoint.toPosition checkpoint; isTail = isTail }
    let sliceBatch epochId offset items =
        mkBatch (Checkpoint.ofEpochAndOffset epochId offset) false items
    let finalBatch epochId (version, state : AppendsEpoch.Reader.State) items : Propulsion.Feed.Core.Batch<_> =
        mkBatch (Checkpoint.ofEpochClosedAndVersion epochId state.closed version) (not state.closed) items

    // Includes optional hydrating of events with event bodies and/or metadata (controlled via hydrating/maybeLoad args)
    let materializeIndexEpochAsBatchesOfStreamEvents
            (log : Serilog.ILogger, sourceId, storeLog) (hydrating, maybeLoad, loadDop) batchCutoff (context : DynamoStoreContext)
            (AppendsTrancheId.Parse tid, Checkpoint.Parse (epochId, offset))
        : AsyncSeq<struct (System.TimeSpan * Propulsion.Feed.Core.Batch<_>)> = asyncSeq {
        let epochs = AppendsEpoch.Reader.Config.create storeLog context
        let sw = Stopwatch.start ()
        let! _maybeSize, version, state = epochs.Read(tid, epochId, offset)
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
        let largeEnough = streamEvents.Count > batchCutoff
        if largeEnough then
            log.Information("DynamoStoreSource {sourceId}/{trancheId}/{epochId}@{offset} {mode:l} {totalChanges} changes {loadingS}/{totalS} streams {loadingE}/{totalE} events",
                            sourceId, string tid, string epochId, offset, (if hydrating then "Hydrating" else "Feeding"), totalChanges, streamEvents.Count, totalStreams, chosenEvents, totalEvents)
        let buffer, cache = ResizeArray<AppendsEpoch.Events.StreamSpan>(), ConcurrentDictionary()
        // For each batch we produce, we load any streams we have not already loaded at this time
        let materializeSpans : Async<Propulsion.Streams.Default.StreamEvent array> = async {
            let loadsRequired =
                buffer
                |> Seq.distinctBy (fun x -> x.p)
                |> Seq.filter (fun x -> not (cache.ContainsKey x.p))
                |> Seq.map (fun x -> x.p, streamEvents[x.p])
                |> Array.ofSeq
            if loadsRequired.Length <> 0 then
                sw.Start()
                do! seq { for sn, load in loadsRequired -> async { let! items = load in cache.TryAdd(sn, items) |> ignore } }
                    |> Async.parallelThrottled loadDop
                    |> Async.Ignore<unit[]>
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
                        for e in events -> IndexStreamId.toStreamName span.p, e |] }
        let mutable prevLoaded, batchIndex = 0L, 0
        let report (i : int option) len =
            if largeEnough && hydrating then
                match cache.Count with
                | loadedNow when prevLoaded <> loadedNow ->
                    prevLoaded <- loadedNow
                    let eventsLoaded = cache.Values |> Seq.sumBy Array.length
                    log.Information("DynamoStoreSource {sourceId}/{trancheId}/{epochId}@{offset}/{totalChanges} {result} {batch} {events}e Loaded {loadedS}/{loadingS}s {loadedE}/{loadingE}e",
                                    sourceId, string tid, string epochId, Option.toNullable i, version, "Hydrated", batchIndex, len, cache.Count, streamEvents.Count, eventsLoaded, chosenEvents)
                | _ -> ()
            batchIndex <- batchIndex + 1
        for i, spans in state.changes do
            let pending = spans |> Array.filter (fun (span : AppendsEpoch.Events.StreamSpan) -> streamEvents.ContainsKey(span.p))
            if buffer.Count <> 0 && buffer.Count + pending.Length > batchCutoff then
                let! hydrated = materializeSpans
                report (Some i) hydrated.Length
                yield struct (sw.Elapsed, sliceBatch epochId i hydrated) // not i + 1 as the batch does not include these changes
                sw.Reset()
                buffer.Clear()
            buffer.AddRange(pending)
        let! hydrated = materializeSpans
        report None hydrated.Length
        yield sw.Elapsed, finalBatch epochId (version, state) hydrated }

[<NoComparison; NoEquality>]
type LoadMode =
    /// Skip loading of Data/Meta for events; this is the most efficient mode as it means the Source only needs to read from the index
    | WithoutEventBodies of categoryFilter : (string -> bool)
    /// Populates the Data/Meta fields for events; necessitates loads of all individual streams that pass the categoryFilter before they can be handled
    | Hydrated of categoryFilter : (string -> bool)
                  * degreeOfParallelism : int
                  * /// Defines the Context to use when loading the Event Data/Meta
                    storeContext : DynamoStoreContext
module internal LoadMode =
    let private mapTimelineEvent = FsCodec.Core.TimelineEvent.Map FsCodec.Deflate.EncodedToUtf8
    let private withBodies (eventsContext : Equinox.DynamoStore.Core.EventsContext) categoryFilter =
        fun sn (i, cs : string array) ->
            if categoryFilter (FsCodec.StreamName.category sn) then
                ValueSome (async { let! _pos, events = eventsContext.Read(FsCodec.StreamName.toString sn, i, maxCount = cs.Length)
                                   return events |> Array.map mapTimelineEvent })
            else ValueNone
    let private withoutBodies categoryFilter =
        fun sn (i, cs) ->
            let renderEvent offset c = FsCodec.Core.TimelineEvent.Create(i + int64 offset, eventType = c, data = Unchecked.defaultof<_>)
            if categoryFilter (FsCodec.StreamName.category sn) then ValueSome (async { return cs |> Array.mapi renderEvent }) else ValueNone
    let map storeLog : LoadMode -> _ = function
        | WithoutEventBodies categoryFilter -> false, withoutBodies categoryFilter, 1
        | Hydrated (categoryFilter, dop, storeContext) ->
            let eventsContext = Equinox.DynamoStore.Core.EventsContext(storeContext, storeLog)
            true, withBodies eventsContext categoryFilter, dop

type DynamoStoreSource
    (   log : Serilog.ILogger, statsInterval,
        indexClient : DynamoStoreClient, batchSizeCutoff, tailSleepInterval,
        checkpoints : Propulsion.Feed.IFeedCheckpointStore, sink : Propulsion.Streams.Default.Sink,
        // If the Handler does not utilize the Data/Meta of the events, we can avoid loading them from the Store
        loadMode : LoadMode,
        // Override default start position to be at the tail of the index. Default: Replay all events.
        ?startFromTail,
        // Separated log for DynamoStore calls in order to facilitate filtering and/or gathering metrics
        ?storeLog,
        ?readFailureSleepInterval,
        ?sourceId,
        ?trancheIds) =
    inherit Propulsion.Feed.Core.TailingFeedSource
        (   log, statsInterval, defaultArg sourceId FeedSourceId.wellKnownId, tailSleepInterval,
            Impl.materializeIndexEpochAsBatchesOfStreamEvents
                (log, defaultArg sourceId FeedSourceId.wellKnownId, defaultArg storeLog log)
                (LoadMode.map (defaultArg storeLog log) loadMode) batchSizeCutoff (DynamoStoreContext indexClient),
            checkpoints,
            (   if startFromTail <> Some true then None
                else Some (Impl.readTailPositionForTranche (defaultArg storeLog log) (DynamoStoreContext indexClient))),
            sink,
            Impl.renderPos,
            Impl.logReadFailure (defaultArg storeLog log),
            defaultArg readFailureSleepInterval (tailSleepInterval * 2.),
            Impl.logCommitFailure (defaultArg storeLog log))

    abstract member ListTranches : unit -> Async<Propulsion.Feed.TrancheId array>
    default _.ListTranches() = async {
        match trancheIds with
        | Some ids -> return ids
        | None ->
            let context = DynamoStoreContext(indexClient)
            let storeLog = defaultArg storeLog log
            let! res = Impl.readTranches storeLog context
            let appendsTrancheIds = match res with [||] -> [| AppendsTrancheId.wellKnownId |] | ids -> ids
            return appendsTrancheIds |> Array.map AppendsTrancheId.toTrancheId }

    abstract member Pump : unit -> Async<unit>
    default x.Pump() = base.Pump(x.ListTranches)

    abstract member Start : unit -> Propulsion.Pipeline
    default x.Start() = base.Start(x.Pump())
