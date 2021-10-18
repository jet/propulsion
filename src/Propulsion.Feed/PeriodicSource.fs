/// Provides for periodic crawling of a source that can be represented as events grouped into streams
/// Checkpointing is based on the time of the traversal, rather than intrinsic properties of the underlying data
/// Each run traverses the entire data set, which is obviously not ideal, if it can be avoided
/// i.e. this is for sources that do/can not provide a mechanism that one might use to checkpoint within a given traversal
namespace Propulsion.Feed

open FSharp.Control
open Propulsion
open Propulsion.Streams
open System

/// Int64.MaxValue = 9223372036854775807
/// ([datetimeoffset]::FromUnixTimeSeconds(9223372036854775807 / 1000000000)) is in 2262
module private DateTimeOffsetPosition =

    let factor = 1_000_000_000L
    let getDateTimeOffset (x : Position) =
        let datepart = Position.toInt64 x / factor
        DateTimeOffset.FromUnixTimeSeconds datepart
    let ofDateTimeOffset (x : DateTimeOffset) =
        let epochTime = x.ToUnixTimeSeconds()
        epochTime * factor |> Position.parse

module private TimelineEvent =

    let ofBasePositionIndexAndEventData<'t> (basePosition : Position) =
        let baseIndex = Position.toInt64 basePosition
        fun (i, x : FsCodec.IEventData<_>, context : obj) ->
            if i > DateTimeOffsetPosition.factor then invalidArg "i" (sprintf "Index may not exceed %d" DateTimeOffsetPosition.factor)
            FsCodec.Core.TimelineEvent.Create(
                baseIndex + i, x.EventType, x.Data, x.Meta, x.EventId, x.CorrelationId, x.CausationId, x.Timestamp, isUnfold = true, context = context)

[<Struct; NoComparison; NoEquality>]
type SourceItem = { streamName : FsCodec.StreamName; eventData : FsCodec.IEventData<byte[]>; context : obj }

/// Drives reading and checkpointing for a custom source which does not have a way to incrementally query the data within as a change feed. <br/>
/// Reads the supplied `source` at `pollInterval` intervals, offsetting the `Index` of the events read based on the start time of the traversal
///   in order to ensure that the Index of each event propagated to the Sink is monotonically increasing as required. <br/>
/// Processing concludes if <c>readTranches</c> and <c>readPage</c> throw, in which case the <c>Pump</c> loop terminates, propagating the exception.
type PeriodicSource
    (   log : Serilog.ILogger, statsInterval : TimeSpan, sourceId,
        checkpoints : IFeedCheckpointStore, defaultCheckpointEventInterval : TimeSpan,
        /// The <c>AsyncSeq</c> is expected to manage its own resilience strategy (retries etc). <br/>
        /// Yielding an exception will result in the <c>Pump<c/> loop terminating, tearing down the source pipeline
        crawl : unit -> AsyncSeq<SourceItem array>, refreshInterval : TimeSpan,
        sink : ProjectorPipeline<Ingestion.Ingester<seq<StreamEvent<byte[]>>, Submission.SubmissionBatch<int,StreamEvent<byte[]>>>>) =
    inherit Internal.FeedSourceBase(log, statsInterval, sourceId, checkpoints, defaultCheckpointEventInterval, sink)

    // We could conceivably expose multi-tranche support; can't think of a use case at present
    let readTranches () = async { return [| TrancheId.parse "0" |] }

    // We don't want to checkpoint for real until we know the scheduler has handled the full set of pages in the crawl.
    let crawl _wasLast (_trancheId, position) : AsyncSeq<Internal.Batch<_>> = asyncSeq {
        let startDate = DateTimeOffsetPosition.getDateTimeOffset position
        let dueDate = startDate + refreshInterval
        match dueDate - DateTimeOffset.UtcNow with
        | waitTime when waitTime.Ticks > 0L -> do! Async.Sleep waitTime
        | _ -> ()

        let basePosition = DateTimeOffset.UtcNow |> DateTimeOffsetPosition.ofDateTimeOffset
        let mkTimelineEvent = TimelineEvent.ofBasePositionIndexAndEventData basePosition
        // wrap the source AsyncSeq, holding back one an item to go into a final
        // guaranteed (assuming the source contains at least one item, that is) non-empty batch
        let buffer = ResizeArray()
        let mutable index = 0L
        for xs in crawl () do
            let streamEvents = seq {
                for si in xs ->
                    let i = index
                    index <- index + 1L
                    { StreamEvent.stream = si.streamName; event = mkTimelineEvent (i, si.eventData, si.context) }
            }
            buffer.AddRange(streamEvents)
            match buffer.Count - 1 with
            | ready when ready > 0 ->
                let items = Array.zeroCreate ready
                buffer.CopyTo(0, items, 0, ready)
                buffer.RemoveRange(0, ready)
                yield ({ items = items; checkpoint = position; isTail = false } : Internal.Batch<_> )
            | _ -> ()
        let items, checkpoint =
            match buffer.ToArray() with
            | [||] as noItems -> noItems, basePosition
            | finalItem -> finalItem, (Array.last finalItem).event |> Internal.TimelineEvent.toCheckpointPosition
        yield ({ items = items; checkpoint = checkpoint; isTail = true } : Internal.Batch<_>) }

    /// Drives the continual loop of reading and checkpointing until the <c>crawl</c> <c>AsyncSeq</c> reports a fault (by throwing).
    member _.Pump() =
        base.Pump(readTranches, crawl)
