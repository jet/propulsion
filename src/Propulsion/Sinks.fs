namespace Propulsion.Sinks

open Propulsion
open Propulsion.Internal
open System
open System.Threading
open System.Threading.Tasks

/// Canonical Data/Meta type supplied by the majority of Sources
type EventBody = ReadOnlyMemory<byte>

/// Timeline Event with Data/Meta in the default format
type Event = FsCodec.ITimelineEvent<EventBody>

/// Helpers for use with spans of events as supplied to a handler
module Events =

    /// The Index of the next event ordinarily expected on the next handler invocation (assuming this invocation handles all successfully)
    let nextIndex : Event[] -> int64 = Streams.StreamSpan.ver
    /// The Index of the first event as supplied to this handler
    let index : Event[] -> int64 = Streams.StreamSpan.idx

/// Represents progress attained during the processing of the supplied Events for a given <c>StreamName</c>.
/// This will be reflected in adjustments to the Write Position for the stream in question.
/// Incoming <c>StreamEvent</c>s with <c>Index</c>es prior to the Write Position implied by the result are proactively
/// dropped from incoming buffers, yielding increased throughput due to reduction of redundant processing.
type StreamResult =
   /// Indicates no events where processed.
   /// Handler should be supplied the same events (plus any that arrived in the interim) in the next scheduling cycle.
   | NoneProcessed
   /// Indicates all <c>Event</c>s supplied have been processed.
   /// Write Position should move beyond the last event supplied.
   | AllProcessed
   /// Indicates only a subset of the presented events have been processed;
   /// Write Position should remove <c>count</c> items from the <c>Event</c>s supplied.
   | PartiallyProcessed of count : int
   /// Apply an externally observed Version determined by the handler during processing.
   /// If the Version of the stream is running ahead or behind the current input StreamSpan, this enables one to have
   /// events that have already been handled be dropped from the scheduler's buffers and/or as they arrive.
   | OverrideWritePosition of version : int64

module StreamResult =

    let toIndex<'F> (span : FsCodec.ITimelineEvent<'F>[]) = function
        | NoneProcessed -> span[0].Index
        | AllProcessed -> span[0].Index + span.LongLength
        | PartiallyProcessed count -> span[0].Index + int64 count
        | OverrideWritePosition index -> index

/// Internal helpers used to compute buffer sizes for stats
module Event =

    let storedSize (x : Event) = x.Size
    let renderedSize (x : Event) = storedSize x + 80

/// Canonical Sink type that the bulk of Sources are configured to feed into
type Sink = Propulsion.Sink<Ingestion.Ingester<StreamEvent seq>>
/// A Single Event from an Ordered stream ready to be fed into a Sink's Ingester, using the Canonical Data/Meta type
and StreamEvent = Propulsion.Streams.StreamEvent<EventBody>

/// Stream State as provided to the <c>select</c> function for a <c>StartBatched</c>
type StreamState = Propulsion.Streams.Scheduling.Item<EventBody>

type Factory private () =

    /// Project Events using up to <c>maxConcurrentStreams</c> <code>handle</code> functions that yield a StreamResult and an Outcome to be fed to the Stats
    static member StartConcurrentAsync<'Outcome>
        (   log, maxReadAhead,
            maxConcurrentStreams, handle : Func<FsCodec.StreamName, Event[], CancellationToken, Task<struct (StreamResult * 'Outcome)>>,
            stats,
            [<O; D null>] ?pendingBufferSize,
            [<O; D null>] ?purgeInterval,
            [<O; D null>] ?wakeForResults,
            [<O; D null>] ?idleDelay,
            [<O; D null>] ?ingesterStatsInterval,
            [<O; D null>] ?requireCompleteStreams)
        : Sink =
        Streams.Concurrent.Start<'Outcome, EventBody, StreamResult>(
            log, maxReadAhead, maxConcurrentStreams, handle, StreamResult.toIndex, Event.storedSize, stats,
            ?pendingBufferSize = pendingBufferSize, ?purgeInterval = purgeInterval,
            ?wakeForResults = wakeForResults, ?idleDelay = idleDelay, ?ingesterStatsInterval = ingesterStatsInterval,
            ?requireCompleteStreams = requireCompleteStreams)

    /// Project Events sequentially via a <code>handle</code> function that yields a StreamResult per <c>select</c>ed Item
    static member StartBatchedAsync<'Outcome>
        (   log, maxReadAhead,
            select : Func<StreamState seq, StreamState[]>,
            handle : Func<StreamState[], CancellationToken, Task<seq<Result<StreamResult, exn>>>>,
            stats,
            [<O; D null>] ?pendingBufferSize, [<O; D null>] ?purgeInterval, [<O; D null>] ?wakeForResults, [<O; D null>] ?idleDelay,
            [<O; D null>] ?ingesterStatsInterval, [<O; D null>] ?requireCompleteStreams) =
        let handle items ct = task {
            let! res = handle.Invoke(items, ct)
            return seq { for i, r in Seq.zip items res -> Result.map (StreamResult.toIndex i.span) r } }
        Streams.Batched.Start(log, maxReadAhead, select, handle, Event.storedSize, stats,
            ?pendingBufferSize = pendingBufferSize, ?purgeInterval = purgeInterval, ?wakeForResults = wakeForResults, ?idleDelay = idleDelay,
            ?ingesterStatsInterval = ingesterStatsInterval, ?requireCompleteStreams = requireCompleteStreams)

    /// Project Events using up to <c>maxConcurrentStreams</c> concurrent instances of a <code>handle</code> function
    /// Each dispatched handle invocation yields a StreamResult conveying progress, together with an Outcome to be fed to the Stats
    static member StartConcurrent<'Outcome>
        (   log, maxReadAhead,
            maxConcurrentStreams, handle : FsCodec.StreamName -> Event[] -> Async<StreamResult * 'Outcome>,
            stats,
            // Configure max number of batches to buffer within the scheduler; Default: Same as maxReadAhead
            [<O; D null>] ?pendingBufferSize, [<O; D null>] ?purgeInterval, [<O; D null>] ?wakeForResults, [<O; D null>] ?idleDelay,
            [<O; D null>] ?ingesterStatsInterval, [<O; D null>] ?requireCompleteStreams) =
        let handle' stream events ct = task {
            let! res, outcome = handle stream events |> Async.startImmediateAsTask ct
            return struct (res, outcome) }
        Factory.StartConcurrentAsync(log, maxReadAhead, maxConcurrentStreams, handle', stats,
            ?pendingBufferSize = pendingBufferSize, ?purgeInterval = purgeInterval, ?wakeForResults = wakeForResults, ?idleDelay = idleDelay,
            ?ingesterStatsInterval = ingesterStatsInterval, ?requireCompleteStreams = requireCompleteStreams)

    /// Project Events by continually <c>select</c>ing and then dispatching a batch of streams to a <code>handle</code> function
    /// Per handled stream, the result can be either a StreamResult conveying progress, or an exception
    static member StartBatched<'Outcome>
        (   log, maxReadAhead,
            select : StreamState seq -> StreamState[],
            handle : StreamState[] -> Async<seq<Result<StreamResult, exn>>>,
            stats,
            // Configure max number of batches to buffer within the scheduler; Default: Same as maxReadAhead
            [<O; D null>] ?pendingBufferSize, [<O; D null>] ?purgeInterval, [<O; D null>] ?wakeForResults, [<O; D null>] ?idleDelay,
            [<O; D null>] ?ingesterStatsInterval, [<O; D null>] ?requireCompleteStreams) =
        let handle items ct = Async.startImmediateAsTask ct (handle items)
        Factory.StartBatchedAsync(log, maxReadAhead, select, handle, stats,
            ?pendingBufferSize = pendingBufferSize, ?purgeInterval = purgeInterval, ?wakeForResults = wakeForResults, ?idleDelay = idleDelay,
            ?ingesterStatsInterval = ingesterStatsInterval, ?requireCompleteStreams = requireCompleteStreams)
