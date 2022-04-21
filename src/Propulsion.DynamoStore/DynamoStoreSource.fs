﻿namespace Propulsion.DynamoStore

open Equinox.DynamoStore
open FSharp.Control
open Propulsion.Infrastructure // AwaitTaskCorrect

type StreamEvent = Propulsion.Streams.StreamEvent<byte[]>

module private Impl =

    type [<Measure>] checkpoint
    type Checkpoint = int64<checkpoint>
    module internal Checkpoint =

        open FSharp.UMX

        //let initial : Checkpoint = %0L
        let [<Literal>] private maxItemsPerEpoch = 1_000_000L

        let private ofPosition : Propulsion.Feed.Position -> Checkpoint = Propulsion.Feed.Position.toInt64 >> UMX.tag
        let toPosition : Checkpoint -> Propulsion.Feed.Position = UMX.untag >> Propulsion.Feed.Position.parse

        let ofEpochAndOffset (epoch : AppendsEpochId) offset : Checkpoint =
            int64 (AppendsEpochId.value epoch) * maxItemsPerEpoch + int64 offset |> UMX.tag

        let ofEpochContent (epoch : AppendsEpochId) isClosed count : Checkpoint =
            let epoch, offset =
                if isClosed then AppendsEpochId.next epoch, 0
                else epoch, count
            ofEpochAndOffset epoch offset

        let toEpochAndOffset (value : Checkpoint) : AppendsEpochId * int =
            let d, r = System.Math.DivRem(%value, maxItemsPerEpoch)
            (%int %d : AppendsEpochId), int r

        let (|Parse|) : Propulsion.Feed.Position -> AppendsEpochId * int = ofPosition >> toEpochAndOffset

    let readTranches context = async {
        let index = AppendsIndex.Reader.create context
        let! res = index.ReadKnownTranches()
        return res |> Array.map AppendsTrancheId.toTrancheId }

    let mkBatch checkpoint isTail items : Propulsion.Feed.Internal.Batch<_> =
        { items = items; checkpoint = Checkpoint.toPosition checkpoint; isTail = isTail }
    let sliceBatch epochId offset items =
        mkBatch (Checkpoint.ofEpochAndOffset epochId offset) false items
    let finalBatch epochId (state : AppendsEpoch.Reader.State) items : Propulsion.Feed.Internal.Batch<_> =
        mkBatch (Checkpoint.ofEpochContent epochId state.closed state.changes.Length) (not state.closed) items

    let spansToStreamEvents filter includeBodies batchCutoff (context : DynamoStoreContext) (AppendsTrancheId.Parse tid, Checkpoint.Parse (eid, offset)) : AsyncSeq<Propulsion.Feed.Internal.Batch<_>> = asyncSeq {
        let epochs = AppendsEpoch.Reader.Config.create context
        let! state = epochs.Read(tid, eid, offset)
        if not includeBodies then
            let generateStubs (span : AppendsEpoch.Events.StreamSpan) : StreamEvent seq =
                let sn = IndexStreamId.toStreamName span.p
                let events = span.c |> Array.mapi (fun offset c -> FsCodec.Core.TimelineEvent.Create(span.i + int64 offset, eventType = c, data = null))
                seq { for e in events -> { stream = sn; event = e } }
            let buffer = ResizeArray()
            for i, spans in state.changes do
                let pending = spans |> Seq.collect (generateStubs >> filter) |> Seq.toArray
                if buffer.Count <> 0 && buffer.Count + pending.Length > batchCutoff then
                    yield sliceBatch eid i (buffer.ToArray())
                    buffer.Clear()
                buffer.AddRange(pending)
            yield finalBatch eid state (buffer.ToArray())
         else
            // let all = state.changes |> Seq.collect (fun struct (_i, xs) -> xs) |> AppendsEpoch.flatten |> Seq.map (fun x -> x.p, x) |> dict
            // TODO coalesce spans for reading (within reason) if reading bodies/event types
            yield failwith "E_NOTIMPL"
    }

type DynamoStoreSource
    (   log : Serilog.ILogger, statsInterval,
        storeClient : DynamoStoreClient, sourceId, eventBatchLimit, tailSleepInterval,
        checkpoints : Propulsion.Feed.IFeedCheckpointStore,
        sink : Propulsion.ProjectorPipeline<Propulsion.Ingestion.Ingester<seq<StreamEvent>, Propulsion.Submission.SubmissionBatch<int, StreamEvent>>>,
        filter : StreamEvent seq -> StreamEvent seq,
        // If the Handler does not utilize the bodies of the events, we can avoid shipping them from the Store in the first instance. Default false.
        ?includeBodies) =
    inherit Propulsion.Feed.Internal.TailingFeedSource(log, statsInterval, sourceId, tailSleepInterval,
                                                       Impl.spansToStreamEvents filter (includeBodies = Some true) eventBatchLimit (DynamoStoreContext storeClient),
                                                       checkpoints, sink)

    member internal _.Pump() =
        let context = DynamoStoreContext(storeClient)
        base.Pump(fun () -> Impl.readTranches context)

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
