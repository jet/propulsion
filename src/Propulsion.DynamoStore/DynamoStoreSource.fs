namespace Propulsion.DynamoStore

open Equinox.DynamoStore
open FSharp.Control
open Propulsion.Infrastructure // AwaitTaskCorrect
open System.Collections.Concurrent

type StreamEvent = Propulsion.Streams.StreamEvent<byte[]>

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

    let readTranches log context = async {
        let index = AppendsIndex.Reader.create log context
        let! res = index.ReadKnownTranches()
        return res |> Array.map AppendsTrancheId.toTrancheId }

    let mkBatch checkpoint isTail items : Propulsion.Feed.Internal.Batch<_> =
        { items = items; checkpoint = Checkpoint.toPosition checkpoint; isTail = isTail }
    let sliceBatch epochId offset items =
        mkBatch (Checkpoint.ofEpochAndOffset epochId offset) false items
    let finalBatch epochId (version, state : AppendsEpoch.Reader.State) items : Propulsion.Feed.Internal.Batch<_> =
        mkBatch (Checkpoint.ofEpochClosedAndVersion epochId state.closed version) (not state.closed) items

    let readIndexedSpansAsStreamEvents log (maybeLoad, loadDop) batchCutoff (context : DynamoStoreContext) (AppendsTrancheId.Parse tid, Checkpoint.Parse (epochId, offset))
        : AsyncSeq<System.TimeSpan * Propulsion.Feed.Internal.Batch<_>> = asyncSeq {
        let epochs = AppendsEpoch.Reader.Config.create log context
        let sw = System.Diagnostics.Stopwatch.StartNew()
        let! version, state = epochs.Read(tid, epochId, offset)
        log.Debug("Loaded {c} ingestion records from {tid} {eid} {off}", state.changes.Length, tid, epochId, offset)
        sw.Stop()
        let streamEvents =
            let all = state.changes |> Seq.collect (fun struct (_i, xs) -> xs) |> AppendsEpoch.flatten
            all |> Seq.choose (fun span -> maybeLoad (IndexStreamId.toStreamName span.p) (span.i, span.c) |> Option.map (fun load -> span.p, load)) |> dict
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
        for i, spans in state.changes do
            let pending = spans |> Array.filter (fun (span : AppendsEpoch.Events.StreamSpan) -> streamEvents.ContainsKey(span.p))
            if buffer.Count <> 0 && buffer.Count + pending.Length > batchCutoff then
                let! hydrated = materializeSpans
                yield sw.Elapsed, sliceBatch epochId i hydrated // not i + 1 as the batch does not include these changes
                sw.Reset()
                buffer.Clear()
            buffer.AddRange(pending)
        let! hydrated = materializeSpans
        yield sw.Elapsed, finalBatch epochId (version, state) hydrated }

[<NoComparison; NoEquality>]
type LoadMode =
    | All
    | Filtered of filter : (FsCodec.StreamName -> bool)
    | WithBodies of degreeOfParallelism : int * filter : (FsCodec.StreamName -> bool)
module internal LoadMode =
    let private withBodies (eventsContext : Equinox.DynamoStore.Core.EventsContext) filter =
        fun sn (i, cs : string []) ->
            if filter sn then Some (async { let! _pos, events = eventsContext.Read(FsCodec.StreamName.toString sn, i, maxCount = cs.Length) in return events })
            else None
    let private withoutBodies filter =
        fun sn (i, cs) ->
            let render = async { return cs |> Array.mapi (fun offset c -> FsCodec.Core.TimelineEvent.Create(i + int64 offset, eventType = c, data = null)) }
            if filter sn then Some render else None
    let map (storeLog, storeClient) : LoadMode -> _ = function
        | All -> withoutBodies (fun _ -> true), 1
        | Filtered filter -> withoutBodies filter, 1
        | WithBodies (dop, filter) ->
            let context = DynamoStoreContext storeClient
            let eventsContext = Equinox.DynamoStore.Core.EventsContext(context, storeLog)
            withBodies eventsContext filter, dop

type DynamoStoreSource
    (   log : Serilog.ILogger, statsInterval,
        storeClient : DynamoStoreClient, sourceId, eventBatchLimit, tailSleepInterval,
        checkpoints : Propulsion.Feed.IFeedCheckpointStore,
        sink : Propulsion.ProjectorPipeline<Propulsion.Ingestion.Ingester<seq<StreamEvent>, Propulsion.Submission.SubmissionBatch<int, StreamEvent>>>,
        // If the Handler does not utilize the bodies of the events, we can avoid shipping them from the Store in the first instance.
        loadMode,
        // Separated log for DynamoStore calls in order to facilitate filtering and/or gathering metrics
        ?storeLog) =
    inherit Propulsion.Feed.Internal.TailingFeedSource(log, statsInterval, sourceId, tailSleepInterval,
                                                       Impl.readIndexedSpansAsStreamEvents
                                                            (defaultArg storeLog log)
                                                            (LoadMode.map (defaultArg storeLog log, storeClient) loadMode)
                                                            eventBatchLimit
                                                            (DynamoStoreContext storeClient),
                                                       checkpoints, sink)

    member internal _.Pump() =
        let context = DynamoStoreContext(storeClient)
        base.Pump(fun () -> Impl.readTranches (defaultArg storeLog log) context)

    member x.Start() =
        let cts = new System.Threading.CancellationTokenSource()
        let ct = cts.Token
        let tcs = System.Threading.Tasks.TaskCompletionSource<unit>()

        let machine = async {
            // external cancellation should yield a success result
            use _ = ct.Register(fun _ -> tcs.TrySetResult () |> ignore)

            do! x.Pump()

            // aka base.AwaitShutdown()
            do! Async.AwaitTaskCorrect tcs.Task }

        let task = Async.StartAsTask machine

        new Propulsion.Pipeline(task, cts.Cancel)
