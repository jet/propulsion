namespace Propulsion.Sinks

open Propulsion
open Propulsion.Internal
open System

/// Canonical Data/Meta type supplied by the majority of Sources
type EventBody = FsCodec.Encoded

/// Timeline Event with Data/Meta in the default format
type Event = FsCodec.ITimelineEvent<EventBody>

/// Codec compatible with canonical <c>Event</c> type
type Codec<'E> = FsCodec.IEventCodec<'E, EventBody, unit>

/// Helpers for use with spans of events as supplied to a handler
module Events =

    /// The Index of the next event ordinarily expected on the next handler invocation (assuming this invocation handles all successfully)
    let next: Event[] -> int64 = Streams.StreamSpan.next
    /// The Index of the first event as supplied to this handler
    let index: Event[] -> int64 = Streams.StreamSpan.index

/// Internal helpers used to compute buffer sizes for stats
module Event =

    let storedSize (x: Event) = x.Size
    let renderedSize (x: Event) = storedSize x + 80

/// Canonical Sink type that the bulk of Sources are configured to feed into
type SinkPipeline = SinkPipeline<Ingestion.Ingester<StreamEvent seq>>
/// A Single Event from an Ordered stream ready to be fed into a Sink's Ingester, using the Canonical Data/Meta type
and StreamEvent = Propulsion.Streams.StreamEvent<EventBody>

/// Stream State as provided to the <c>select</c> function for a <c>StartBatched</c>
type StreamState = Propulsion.Streams.Scheduling.Item<EventBody>

[<AbstractClass; Sealed>]
type Factory private () =

    /// Project Events using up to <c>maxConcurrentStreams</c> concurrent instances of a
    /// <code>handle</code> function that yields an Outcome to be fed to the Stats, and an updated Stream Position
    static member StartConcurrentAsync<'Outcome>
        (   log, maxReadAhead,
            maxConcurrentStreams, handle: Func<FsCodec.StreamName, Event[], CancellationToken, Task<struct ('Outcome * int64)>>,
            stats,
            [<O; D null>] ?pendingBufferSize,
            [<O; D null>] ?purgeInterval,
            [<O; D null>] ?wakeForResults, [<O; D null>] ?idleDelay, [<O; D null>] ?requireAll,
            [<O; D null>] ?commitInterval, [<O; D null>] ?ingesterStateInterval) =
        Streams.Concurrent.Start<'Outcome, EventBody>(
            log, maxReadAhead, maxConcurrentStreams, handle, Event.storedSize, stats,
            ?pendingBufferSize = pendingBufferSize, ?purgeInterval = purgeInterval,
            ?wakeForResults = wakeForResults, ?idleDelay = idleDelay, ?requireAll = requireAll,
            ?ingesterStateInterval = ingesterStateInterval, ?commitInterval = commitInterval)

    /// Project Events sequentially via a <code>handle</code> function that yields an updated Stream Position and latency per <c>select</c>ed Item
    static member StartBatchedAsync<'Outcome>
        (   log, maxReadAhead,
            select: Func<StreamState seq, StreamState[]>,
            handle: Func<StreamState[], CancellationToken, Task<seq<struct (Result<int64, exn> * TimeSpan)>>>,
            stats,
            [<O; D null>] ?pendingBufferSize, [<O; D null>] ?purgeInterval,
            [<O; D null>] ?wakeForResults, [<O; D null>] ?idleDelay, [<O; D null>] ?requireAll,
            [<O; D null>] ?commitInterval, [<O; D null>] ?ingesterStateInterval) =
        Streams.Batched.Start(log, maxReadAhead, select, handle, Event.storedSize, stats,
            ?pendingBufferSize = pendingBufferSize, ?purgeInterval = purgeInterval,
            ?wakeForResults = wakeForResults, ?idleDelay = idleDelay, ?requireAll = requireAll,
            ?ingesterStateInterval = ingesterStateInterval, ?commitInterval = commitInterval)

    /// Project Events using up to <c>maxConcurrentStreams</c> concurrent instances of a
    /// <code>handle</code> function that yields an Outcome to be fed to the Stats, and an updated Stream Position
    static member StartConcurrent<'Outcome>
        (   log, maxReadAhead,
            maxConcurrentStreams, handle: FsCodec.StreamName -> Event[] -> Async<'Outcome * int64>,
            stats,
            // Configure max number of batches to buffer within the scheduler; Default: Same as maxReadAhead
            [<O; D null>] ?pendingBufferSize, [<O; D null>] ?purgeInterval,
            [<O; D null>] ?wakeForResults, [<O; D null>] ?idleDelay, [<O; D null>] ?requireAll,
            [<O; D null>] ?commitInterval, [<O; D null>] ?ingesterStateInterval) =
        let handle' stream events ct = task {
            let! outcome, pos' = handle stream events |> Async.executeAsTask ct
            return struct (outcome, pos') }
        Factory.StartConcurrentAsync(log, maxReadAhead, maxConcurrentStreams, handle', stats,
            ?pendingBufferSize = pendingBufferSize, ?purgeInterval = purgeInterval,
            ?wakeForResults = wakeForResults, ?idleDelay = idleDelay, ?requireAll = requireAll,
            ?ingesterStateInterval = ingesterStateInterval, ?commitInterval = commitInterval)

    /// Project Events using up to <c>maxConcurrentStreams</c> concurrent instances of a
    /// <code>handle</code> function that yields an Outcome to be fed to the Stats, and an updated Stream Position
    /// Like StartConcurrent, but the events supplied to the Handler are constrained by <c>maxBytes</c> and <c>maxEvents</c>
    static member StartConcurrentChunked<'Outcome>
        (   log, maxReadAhead,
            maxConcurrentStreams, handle: FsCodec.StreamName -> Event[] -> Async<'Outcome * int64>,
            stats: Sync.Stats<'Outcome>,
            // Default 1 ms
            ?idleDelay,
            // Default 1 MiB
            ?maxBytes,
            // Default 16384
            ?maxEvents,
            // Hook to wire in external stats
            ?dumpExternalStats,
            // Frequency of jettisoning Write Position state of inactive streams (held by the scheduler for deduplication purposes) to limit memory consumption
            // NOTE: Purging can impair performance, increase write costs or result in duplicate event emissions due to redundant inputs not being deduplicated
            ?purgeInterval) =
        let handle' s xs ct = task { let! o, pos' = handle s xs |> Async.executeAsTask ct in return struct (o, pos') }
        Sync.Factory.StartAsync(log, maxReadAhead, maxConcurrentStreams, handle', stats, Event.renderedSize, Event.storedSize,
                                ?dumpExternalStats = dumpExternalStats, ?idleDelay = idleDelay, ?maxBytes = maxBytes, ?maxEvents = maxEvents, ?purgeInterval = purgeInterval)

    /// Project Events by continually <c>select</c>ing and then dispatching a batch of streams to a <code>handle</code> function
    /// Per handled stream, the result can be either an updated Stream Position, or an exception
    static member StartBatched<'Outcome>
        (   log, maxReadAhead,
            select: StreamState seq -> StreamState[],
            handle: StreamState[] -> Async<seq<struct (Result<int64, exn> * TimeSpan)>>,
            stats,
            // Configure max number of batches to buffer within the scheduler; Default: Same as maxReadAhead
            [<O; D null>] ?pendingBufferSize, [<O; D null>] ?purgeInterval,
            [<O; D null>] ?wakeForResults, [<O; D null>] ?idleDelay, [<O; D null>] ?requireAll,
            [<O; D null>] ?commitInterval, [<O; D null>] ?ingesterStateInterval) =
        let handle items ct = handle items |> Async.executeAsTask ct
        Factory.StartBatchedAsync(log, maxReadAhead, select, handle, stats,
            ?pendingBufferSize = pendingBufferSize, ?purgeInterval = purgeInterval,
            ?wakeForResults = wakeForResults, ?idleDelay = idleDelay, ?requireAll = requireAll,
            ?ingesterStateInterval = ingesterStateInterval, ?commitInterval = commitInterval)
