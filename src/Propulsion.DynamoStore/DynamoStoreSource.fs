namespace Propulsion.DynamoStore

type StreamEvent = Propulsion.Streams.StreamEvent<byte[]>

open Equinox.DynamoStore
open FSharp.Control
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

    let readTranches log context = async {
        let index = AppendsIndex.Reader.create log context
        let! res = index.ReadKnownTranches()
        // TODO remove this hard-coding if/when FeedSourceBase.Pump starts to periodically pick up new tranches
        let res = [||]
        let appendsTrancheIds = match res with [||] -> [| AppendsTrancheId.wellKnownId |] | ids -> ids
        return appendsTrancheIds |> Array.map AppendsTrancheId.toTrancheId }

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

    let mkBatch checkpoint isTail items : Propulsion.Feed.Internal.Batch<_> =
        { items = items; checkpoint = Checkpoint.toPosition checkpoint; isTail = isTail }
    let sliceBatch epochId offset items =
        mkBatch (Checkpoint.ofEpochAndOffset epochId offset) false items
    let finalBatch epochId (version, state : AppendsEpoch.Reader.State) items : Propulsion.Feed.Internal.Batch<_> =
        mkBatch (Checkpoint.ofEpochClosedAndVersion epochId state.closed version) (not state.closed) items

    // Includes optional hydrating of events with event bodies and/or metadata (controlled via hydrating/maybeLoad args)
    let materializeIndexEpochAsBatchesOfStreamEvents
            (log : Serilog.ILogger, sourceId, storeLog) (hydrating, maybeLoad, loadDop) batchCutoff (context : DynamoStoreContext)
            (AppendsTrancheId.Parse tid, Checkpoint.Parse (epochId, offset))
        : AsyncSeq<System.TimeSpan * Propulsion.Feed.Internal.Batch<byte[]>> = asyncSeq {
        let epochs = AppendsEpoch.Reader.Config.create storeLog context
        let sw = System.Diagnostics.Stopwatch.StartNew()
        let! _maybeSize, version, state = epochs.Read(tid, epochId, offset)
        let totalChanges = state.changes.Length
        sw.Stop()
        let totalStreams, chosenEvents, totalEvents, streamEvents =
            let all = state.changes |> Seq.collect (fun struct (_i, xs) -> xs) |> AppendsEpoch.flatten |> Array.ofSeq
            let totalEvents = all |> Array.sumBy (fun x -> x.c.Length)
            let mutable chosenEvents = 0
            let chooseStream (span : AppendsEpoch.Events.StreamSpan) =
                match maybeLoad (IndexStreamId.toStreamName span.p) (span.i, span.c) with
                | Some f ->
                    chosenEvents <- chosenEvents + span.c.Length
                    Some (span.p, f)
                | None -> None
            let streamEvents = all |> Seq.choose chooseStream |> dict
            all.Length, chosenEvents, totalEvents, streamEvents
        let largeEnough = streamEvents.Count > batchCutoff
        if largeEnough then
            log.Information("DynamoStoreSource {sourceId}/{trancheId}/{epochId}@{offset} {mode:l} {totalChanges} changes {loadingS}/{totalS} streams {loadingE}/{totalE} events",
                            sourceId, string tid, string epochId, offset, (if hydrating then "Hydrating" else "Feeding"), totalChanges, streamEvents.Count, totalStreams, chosenEvents, totalEvents)
        let buffer, cache = ResizeArray<AppendsEpoch.Events.StreamSpan>(), ConcurrentDictionary()
        // For each batch we produce, we load any streams we have not already loaded at this time
        let materializeSpans : Async<StreamEvent array> = async {
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
                    | true, (items : FsCodec.ITimelineEvent<_>[]) ->
                        // NOTE this could throw if a span has been indexed, but the stream read is from a replica that does not yet have it
                        //      the exception in that case will trigger a safe re-read from the last saved read position that a consumer has forwarded
                        // TOCONSIDER revise logic to share session key etc to rule this out
                        let events = Array.sub items (span.i - items[0].Index |> int) span.c.Length
                        for e in events do ({ stream = IndexStreamId.toStreamName span.p; event = e } : StreamEvent) |] }
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
                yield sw.Elapsed, sliceBatch epochId i hydrated // not i + 1 as the batch does not include these changes
                sw.Reset()
                buffer.Clear()
            buffer.AddRange(pending)
        let! hydrated = materializeSpans
        report None hydrated.Length
        yield sw.Elapsed, finalBatch epochId (version, state) hydrated }

[<NoComparison; NoEquality>]
type LoadMode =
    | All
    | Filtered of filter : (FsCodec.StreamName -> bool)
    | Hydrated of filter : (FsCodec.StreamName -> bool)
                  * degreeOfParallelism : int
                  * /// Defines the Context to use when loading the bodies
                    storeContext : DynamoStoreContext
module internal LoadMode =
    let private mapTimelineEvent =
        let mapBodyToBytes = (fun (x : System.ReadOnlyMemory<byte>) -> x.ToArray())
        FsCodec.Core.TimelineEvent.Map (FsCodec.Deflate.EncodedToUtf8 >> mapBodyToBytes) // TODO replace with FsCodec.Deflate.EncodedToByteArray
    let private withBodies (eventsContext : Equinox.DynamoStore.Core.EventsContext) filter =
        fun sn (i, cs : string array) ->
            if filter sn then Some (async { let! _pos, events = eventsContext.Read(FsCodec.StreamName.toString sn, i, maxCount = cs.Length)
                                            return events |> Array.map mapTimelineEvent })
            else None
    let private withoutBodies filter =
        fun sn (i, cs) ->
            let render = async { return cs |> Array.mapi (fun offset c -> FsCodec.Core.TimelineEvent.Create(i + int64 offset, eventType = c, data = Unchecked.defaultof<_>)) }
            if filter sn then Some render else None
    let map storeLog : LoadMode -> _ = function
        | All -> false, withoutBodies (fun _ -> true), 1
        | Filtered filter -> false, withoutBodies filter, 1
        | Hydrated (filter, dop, storeContext) ->
            let eventsContext = Equinox.DynamoStore.Core.EventsContext(storeContext, storeLog)
            true, withBodies eventsContext filter, dop

type DynamoStoreSource
    (   log : Serilog.ILogger, statsInterval,
        indexClient : DynamoStoreClient, batchSizeCutoff, tailSleepInterval,
        checkpoints : Propulsion.Feed.IFeedCheckpointStore,
        sink : Propulsion.ProjectorPipeline<Propulsion.Ingestion.Ingester<seq<StreamEvent>, Propulsion.Submission.SubmissionBatch<int, StreamEvent>>>,
        // If the Handler does not utilize the bodies of the events, we can avoid shipping them from the Store in the first instance.
        loadMode,
        // Override default start position to be at the tail of the index (Default: Always replay all events)
        ?fromTail,
        // Separated log for DynamoStore calls in order to facilitate filtering and/or gathering metrics
        ?storeLog,
        ?readFailureSleepInterval,
        ?sourceId) =
    inherit Propulsion.Feed.Internal.TailingFeedSource
        (   log, statsInterval, defaultArg sourceId FeedSourceId.wellKnownId, tailSleepInterval,
            Impl.materializeIndexEpochAsBatchesOfStreamEvents (log, defaultArg sourceId FeedSourceId.wellKnownId, defaultArg storeLog log)
                                                (LoadMode.map (defaultArg storeLog log) loadMode) batchSizeCutoff (DynamoStoreContext indexClient),
            checkpoints,
            (if fromTail = Some true then Some (Impl.readTailPositionForTranche (defaultArg storeLog log) (DynamoStoreContext indexClient)) else None),
            sink,
            Impl.renderPos,
            Impl.logReadFailure (defaultArg storeLog log),
            (defaultArg readFailureSleepInterval (tailSleepInterval * 2.)),
            Impl.logCommitFailure (defaultArg storeLog log))

    member _.Pump() =
        let context = DynamoStoreContext(indexClient)
        base.Pump(fun () -> Impl.readTranches (defaultArg storeLog log) context)

    member x.Start() = base.Start(x.Pump())
