namespace Propulsion.Sinks

open System
open System.Threading
open System.Threading.Tasks
open Propulsion
open Propulsion.Internal

/// Canonical Data/Meta type supplied by the majority of Sources
type EventBody = ReadOnlyMemory<byte>

/// Timeline Event with Data/Meta in the default format
type Event = FsCodec.ITimelineEvent<EventBody>

/// Internal helpers used to compute buffer sizes for stats
module Event =

    let storedSize (x : Event) = x.Size
    let renderedSize (x : Event) = storedSize x + 80

/// A Single Event from an Ordered stream, using the Canonical Data/Meta type
type StreamEvent = Propulsion.Streams.StreamEvent<EventBody>

/// Canonical Sink type that the bulk of Sources are configured to feed into
type Sink = Propulsion.Sink<Ingestion.Ingester<StreamEvent seq>>

/// Stream State as provided to the <c>select</c> function for a <c>BatchesSink</c>
type SchedulingItem = Propulsion.Streams.Scheduling.Item<EventBody>

type Factory private () =

    /// Project Events using up to <c>maxConcurrentStreams</c> <code>handle</code> function that yields a SpanResult and an Outcome to be fed to the Stats
    static member StartConcurrentAsync<'Outcome>
        (   log, maxReadAhead,
            maxConcurrentStreams, handle : Func<FsCodec.StreamName, Event[], CancellationToken, Task<struct (Streams.SpanResult * 'Outcome)>>,
            stats, statsInterval,
            [<O; D null>] ?pendingBufferSize,
            [<O; D null>] ?purgeInterval,
            [<O; D null>] ?wakeForResults,
            [<O; D null>] ?idleDelay,
            [<O; D null>] ?ingesterStatsInterval,
            [<O; D null>] ?requireCompleteStreams)
        : Sink =
        Streams.Concurrent.Start<'Outcome, EventBody>(
            log, maxReadAhead, maxConcurrentStreams, handle, stats, statsInterval, Event.storedSize,
            ?pendingBufferSize = pendingBufferSize, ?purgeInterval = purgeInterval,
            ?wakeForResults = wakeForResults, ?idleDelay = idleDelay, ?ingesterStatsInterval = ingesterStatsInterval,
            ?requireCompleteStreams = requireCompleteStreams)

    /// Project Events using a <code>handle</code> function that yields a SpanResult per <c>select</c>ed Item
    static member StartBatchedAsync<'Outcome>
        (   log, maxReadAhead,
            select : SchedulingItem seq -> SchedulingItem[],
            handle : Func<SchedulingItem[], CancellationToken, Task<seq<Choice<Streams.SpanResult, exn>>>>,
            stats, statsInterval,
            [<O; D null>] ?pendingBufferSize, [<O; D null>] ?purgeInterval, [<O; D null>] ?wakeForResults, [<O; D null>] ?idleDelay,
            [<O; D null>] ?ingesterStatsInterval, [<O; D null>] ?requireCompleteStreams) =
        let handle items ct = task {
            let! res = handle.Invoke(items, ct)
            let spanResultToStreamPos span = function Choice1Of2 sr -> Choice1Of2 (Streams.SpanResult.toIndex span sr) | Choice2Of2 ex -> Choice2Of2 ex
            return seq { for i, r in Seq.zip items res -> spanResultToStreamPos i.span r } }
        Streams.Batched.Start(log, maxReadAhead, select, handle, stats, statsInterval, Event.storedSize,
            ?pendingBufferSize = pendingBufferSize, ?purgeInterval = purgeInterval, ?wakeForResults = wakeForResults, ?idleDelay = idleDelay,
            ?ingesterStatsInterval = ingesterStatsInterval, ?requireCompleteStreams = requireCompleteStreams)

    /// Project Events using up to <c>maxConcurrentStreams</c> instances of a <code>handle</code> function
    /// Each dispatched handle invocation yields a SpanResult conveying progress, together with an Outcome to be fed to the Stats
    static member StartConcurrent<'Outcome>
        (   log, maxReadAhead,
            maxConcurrentStreams, handle : FsCodec.StreamName -> Event[] -> Async<struct (Streams.SpanResult * 'Outcome)>,
            stats, statsInterval,
            // Configure max number of batches to buffer within the scheduler; Default: Same as maxReadAhead
            [<O; D null>] ?pendingBufferSize, [<O; D null>] ?purgeInterval, [<O; D null>] ?wakeForResults, [<O; D null>] ?idleDelay,
            [<O; D null>] ?ingesterStatsInterval, [<O; D null>] ?requireCompleteStreams) =
        let handle' stream events ct = Async.startImmediateAsTask ct (handle stream events)
        Factory.StartConcurrentAsync(log, maxReadAhead, maxConcurrentStreams, handle', stats, statsInterval,
            ?pendingBufferSize = pendingBufferSize, ?purgeInterval = purgeInterval, ?wakeForResults = wakeForResults, ?idleDelay = idleDelay,
            ?ingesterStatsInterval = ingesterStatsInterval, ?requireCompleteStreams = requireCompleteStreams)

    /// Project Events by continually <c>select</c>ing and then dispatching a batch of streams to a <code>handle</code> function
    /// Per handled stream, the result can be either a SpanResult conveying progress, or an exception
    static member StartBatched<'Outcome>
        (   log, maxReadAhead,
            select : SchedulingItem seq -> SchedulingItem[],
            handle : SchedulingItem[] -> Async<seq<Choice<Streams.SpanResult, exn>>>,
            stats, statsInterval,
            // Configure max number of batches to buffer within the scheduler; Default: Same as maxReadAhead
            [<O; D null>] ?pendingBufferSize, [<O; D null>] ?purgeInterval, [<O; D null>] ?wakeForResults, [<O; D null>] ?idleDelay,
            [<O; D null>] ?ingesterStatsInterval, [<O; D null>] ?requireCompleteStreams) =
        let handle items ct = Async.startImmediateAsTask ct (handle items)
        Factory.StartBatchedAsync(log, maxReadAhead, select, handle, stats, statsInterval,
            ?pendingBufferSize = pendingBufferSize, ?purgeInterval = purgeInterval, ?wakeForResults = wakeForResults, ?idleDelay = idleDelay,
            ?ingesterStatsInterval = ingesterStatsInterval, ?requireCompleteStreams = requireCompleteStreams)
