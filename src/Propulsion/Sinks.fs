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
module Event =

    let eventSize (x : Event) = x.Size
    let jsonSize (x : Event) = eventSize x + 80

/// A Single Event from an Ordered stream, using the Canonical Data/Meta type
type StreamEvent = Propulsion.Streams.StreamEvent<EventBody>

type Sink = Propulsion.Sink<Ingestion.Ingester<StreamEvent seq>>

/// Stream State as provided to the <c>select</c> function for a <c>BatchesSink</c>
type SchedulingItem = Propulsion.Streams.Scheduling.Item<EventBody>

type Core private () =

    /// Project Events using a C#/Task-friendly <code>handle</code> function that yields a SpanResult and an Outcome to be fed to the Stats
    static member Start<'Outcome>
        (   log, maxReadAhead, maxConcurrentStreams,
            handle : Func<FsCodec.StreamName, Event[], CancellationToken, Task<struct (Streams.SpanResult * 'Outcome)>>,
            stats, statsInterval,
            // Configure max number of batches to buffer within the scheduler; Default: Same as maxReadAhead
            [<O; D null>] ?pendingBufferSize,
            [<O; D null>] ?purgeInterval,
            [<O; D null>] ?wakeForResults,
            [<O; D null>] ?idleDelay,
            [<O; D null>] ?ingesterStatsInterval,
            [<O; D null>] ?requireCompleteStreams)
        : Sink =
        Streams.StreamsSink.Start<'Outcome, EventBody>(
            log, maxReadAhead, maxConcurrentStreams, handle, stats, statsInterval, Event.eventSize,
            ?pendingBufferSize = pendingBufferSize, ?purgeInterval = purgeInterval,
            ?wakeForResults = wakeForResults, ?idleDelay = idleDelay, ?ingesterStatsInterval = ingesterStatsInterval,
            ?requireCompleteStreams = requireCompleteStreams)

type Config private () =

    /// Project Events using an F# Async <code>handle</code> function that yields a SpanResult and an Outcome to be fed to the Stats
    /// See also StartEx
    static member Start<'Outcome>
        (   log, maxReadAhead, maxConcurrentStreams,
            handle : FsCodec.StreamName -> Event[] -> Async<struct (Streams.SpanResult * 'Outcome)>,
            stats, statsInterval,
            [<O; D null>] ?pendingBufferSize, [<O; D null>] ?purgeInterval, [<O; D null>] ?wakeForResults, [<O; D null>] ?idleDelay,
            [<O; D null>] ?ingesterStatsInterval, [<O; D null>] ?requireCompleteStreams) =
        Core.Start(log, maxReadAhead, maxConcurrentStreams,
            (fun stream events ct -> Async.startImmediateAsTask ct (handle stream events)),
            stats, statsInterval,
            ?pendingBufferSize = pendingBufferSize, ?purgeInterval = purgeInterval, ?wakeForResults = wakeForResults, ?idleDelay = idleDelay,
            ?ingesterStatsInterval = ingesterStatsInterval, ?requireCompleteStreams = requireCompleteStreams)

    static member StartBatched<'Outcome>
        (   log, maxReadAhead,
            select : SchedulingItem seq -> SchedulingItem[],
            handle : Func<SchedulingItem[], CancellationToken, Task<seq<Choice<Streams.SpanResult, exn>>>>,
            stats, statsInterval,
            [<O; D null>] ?pendingBufferSize, [<O; D null>] ?purgeInterval, [<O; D null>] ?wakeForResults, [<O; D null>] ?idleDelay,
            [<O; D null>] ?ingesterStatsInterval, [<O; D null>] ?requireCompleteStreams) =
        let handle items ct = task {
            let! res = handle.Invoke(items, ct)
            return seq { for i, r in Seq.zip items res ->
                            match r with
                            | Choice1Of2 sr -> Choice1Of2 (Streams.SpanResult.toIndex i.span sr)
                            | Choice2Of2 ex -> Choice2Of2 ex }
        }
        Streams.BatchesSink.Start(log, maxReadAhead, select, handle, stats, statsInterval, Event.eventSize,
            ?pendingBufferSize = pendingBufferSize, ?purgeInterval = purgeInterval, ?wakeForResults = wakeForResults, ?idleDelay = idleDelay,
            ?ingesterStatsInterval = ingesterStatsInterval, ?requireCompleteStreams = requireCompleteStreams)
