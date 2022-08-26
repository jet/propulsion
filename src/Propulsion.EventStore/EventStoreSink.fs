#if EVENTSTORE_LEGACY
namespace Propulsion.EventStore

open Equinox.EventStore
#else
namespace Propulsion.EventStoreDb

open Equinox.EventStoreDb
#endif

open Propulsion
open Propulsion.Internal
open Propulsion.Streams
open Serilog
open System.Collections.Generic
open System
open System.Threading

module private StreamSpan =

#if EVENTSTORE_LEGACY
    let private nativeToDefault_ = FsCodec.Core.TimelineEvent.Map (fun (xs : byte array) -> ReadOnlyMemory xs)
    let inline nativeToDefault span = Array.map nativeToDefault_ span
    let defaultToNative_ = FsCodec.Core.TimelineEvent.Map (fun (xs : ReadOnlyMemory<byte>) -> xs.ToArray())
    let inline defaultToNative span = Array.map defaultToNative_ span
#else
    let nativeToDefault = id
    let defaultToNative_ = id
    let defaultToNative = id
#endif

module Internal =

    [<AutoOpen>]
    module Writer =
        type [<NoComparison; NoEquality>] Result =
            | Ok of updatedPos : int64
            | Duplicate of updatedPos : int64
            | PartialDuplicate of overage : Default.StreamSpan
            | PrefixMissing of batch : Default.StreamSpan * writePos : int64

        let logTo (log : ILogger) (res : FsCodec.StreamName * Choice<struct (StreamSpan.Metrics * Result), struct (StreamSpan.Metrics * exn)>) =
            match res with
            | stream, Choice1Of2 (_, Ok pos) ->
                log.Information("Wrote     {stream} up to {pos}", stream, pos)
            | stream, Choice1Of2 (_, Duplicate updatedPos) ->
                log.Information("Ignored   {stream} (synced up to {pos})", stream, updatedPos)
            | stream, Choice1Of2 (_, PartialDuplicate overage) ->
                log.Information("Requeuing {stream} {pos} ({count} events)", stream, overage[0].Index, overage.Length)
            | stream, Choice1Of2 (_, PrefixMissing (batch, pos)) ->
                log.Information("Waiting   {stream} missing {gap} events ({count} events @ {pos})",
                                stream, batch[0].Index - pos, batch.Length, batch[0].Index)
            | stream, Choice2Of2 (_, exn) ->
                log.Warning(exn,"Writing   {stream} failed, retrying", stream)

        let write (log : ILogger) (context : EventStoreContext) stream (span : Default.StreamSpan) = async {
            log.Debug("Writing {s}@{i}x{n}", stream, span[0].Index, span.Length)
            let! res = context.Sync(log, stream, span[0].Index - 1L, (span |> Array.map (fun span -> StreamSpan.defaultToNative_ span :> _)))
            let ress =
                match res with
                | GatewaySyncResult.Written (Token.Unpack pos') ->
#if EVENTSTORE_LEGACY
                    Ok (pos'.pos.streamVersion + 1L)
#else
                    Ok (pos'.streamVersion + 1L)
#endif
                | GatewaySyncResult.ConflictUnknown (Token.Unpack pos) ->
#if EVENTSTORE_LEGACY
                    match pos.pos.streamVersion + 1L with
#else
                    match pos.streamVersion + 1L with
#endif
                    | actual when actual < span[0].Index -> PrefixMissing (span, actual)
                    | actual when actual >= span[0].Index + span.LongLength -> Duplicate actual
                    | actual -> PartialDuplicate (span |> Array.skip (actual - span[0].Index |> int))
            log.Debug("Result: {res}", ress)
            return ress }

        type [<RequireQualifiedAccess>] ResultKind = TimedOut | Other

        let classify e =
            match box e with
            | :? TimeoutException -> ResultKind.TimedOut
            | _ -> ResultKind.Other

    type Stats(log : ILogger, statsInterval, stateInterval) =
        inherit Scheduling.Stats<struct (StreamSpan.Metrics * Writer.Result), struct (StreamSpan.Metrics * exn)>(log, statsInterval, stateInterval)
        let mutable okStreams, badCats, failStreams, toStreams, oStreams = HashSet(), Stats.CatStats(), HashSet(), HashSet(), HashSet()
        let mutable resultOk, resultDup, resultPartialDup, resultPrefix, resultExnOther, timedOut = 0, 0, 0, 0, 0, 0
        let mutable okEvents, okBytes, exnEvents, exnBytes = 0, 0L, 0, 0L

        override _.DumpStats() =
            let results = resultOk + resultDup + resultPartialDup + resultPrefix
            log.Information("Completed {mb:n0}MB {completed:n0}r {streams:n0}s {events:n0}e ({ok:n0} ok {dup:n0} redundant {partial:n0} partial {prefix:n0} waiting)",
                Log.miB okBytes, results, okStreams.Count, okEvents, resultOk, resultDup, resultPartialDup, resultPrefix)
            okStreams.Clear(); resultOk <- 0; resultDup <- 0; resultPartialDup <- 0; resultPrefix <- 0; okEvents <- 0; okBytes <- 0L
            if timedOut <> 0 || badCats.Any then
                let fails = timedOut + resultExnOther
                log.Warning("Exceptions {mb:n0}MB {fails:n0}r {streams:n0}s {events:n0}e Timed out {toCount:n0}r {toStreams:n0}s",
                    Log.miB exnBytes, fails, failStreams.Count, exnEvents, timedOut, toStreams.Count)
                timedOut <- 0; resultExnOther <- 0; failStreams.Clear(); toStreams.Clear(); exnBytes <- 0L; exnEvents <- 0
            if badCats.Any then
                log.Warning(" Affected cats {@badCats} Other {other:n0}r {@oStreams}",
                    badCats.StatsDescending |> Seq.truncate 50, resultExnOther, oStreams |> Seq.truncate 100)
                badCats.Clear(); resultExnOther <- 0; oStreams.Clear()
            Log.InternalMetrics.dump log

        override this.Handle message =
            let inline adds x (set : HashSet<_>) = set.Add x |> ignore
            let inline bads streamName (set : HashSet<_>) = badCats.Ingest(StreamName.categorize streamName); adds streamName set
            base.Handle message
            match message with
            | Scheduling.InternalMessage.Added _ -> () // Processed by standard logging already; we have nothing to add
            | Scheduling.InternalMessage.Result (_duration, stream, _progressed, Choice1Of2 ((es, bs), res)) ->
                adds stream okStreams
                okEvents <- okEvents + es
                okBytes <- okBytes + int64 bs
                match res with
                | Writer.Result.Ok _ -> resultOk <- resultOk + 1
                | Writer.Result.Duplicate _ -> resultDup <- resultDup + 1
                | Writer.Result.PartialDuplicate _ -> resultPartialDup <- resultPartialDup + 1
                | Writer.Result.PrefixMissing _ -> resultPrefix <- resultPrefix + 1
                this.HandleOk res
            | Scheduling.InternalMessage.Result (_duration, stream, _progressed, Choice2Of2 ((es, bs), exn)) ->
                adds stream failStreams
                exnEvents <- exnEvents + es
                exnBytes <- exnBytes + int64 bs
                match Writer.classify exn with
                | ResultKind.TimedOut -> adds stream toStreams; timedOut <- timedOut + 1
                | ResultKind.Other -> bads stream oStreams; resultExnOther <- resultExnOther + 1
                this.HandleExn(log.ForContext("stream", stream).ForContext("events", es), exn)
        abstract member HandleOk : Result -> unit
        default _.HandleOk _ : unit = ()
        abstract member HandleExn : log : ILogger * exn : exn -> unit
        default _.HandleExn(_, _) : unit = ()

    type EventStoreSchedulingEngine =
        static member Create(log : ILogger, storeLog, connections : _ [], itemDispatcher, stats : Stats, dumpStreams, ?maxBatches, ?idleDelay, ?purgeInterval)
            : Scheduling.StreamSchedulingEngine<_, _, _, _> =
            let writerResultLog = log.ForContext<Writer.Result>()
            let mutable robin = 0

            let attemptWrite struct (stream, span) ct = task {
                let index = Interlocked.Increment(&robin) % connections.Length
                let selectedConnection = connections[index]
                let maxEvents, maxBytes = 65536, 4 * 1024 * 1024 - (*fudge*)4096
                let struct (met, span') = StreamSpan.slice Default.jsonSize (maxEvents, maxBytes) span
                try let! res = Writer.write storeLog selectedConnection (FsCodec.StreamName.toString stream) span'
                               |> fun f -> Async.StartAsTask(f, cancellationToken = ct)
                    return struct (span'.Length > 0, Choice1Of2 struct (met, res))
                with e -> return false, Choice2Of2 struct (met, e) }

            let interpretWriteResultProgress (streams : Scheduling.StreamStates<_>) stream res =
                let applyResultToStreamState = function
                    | Choice1Of2 struct (_stats, Writer.Ok pos) ->                streams.RecordWriteProgress(stream, pos, null)
                    | Choice1Of2 (_stats, Writer.Duplicate pos) ->                streams.RecordWriteProgress(stream, pos, null)
                    | Choice1Of2 (_stats, Writer.PartialDuplicate overage) ->     streams.RecordWriteProgress(stream, overage[0].Index, [| overage |])
                    | Choice1Of2 (_stats, Writer.PrefixMissing (overage, pos)) -> streams.RecordWriteProgress(stream, pos, [| overage |])
                    | Choice2Of2 struct (_stats, _exn) ->                         streams.SetMalformed(stream, false)
                let ss = applyResultToStreamState res
                Writer.logTo writerResultLog (stream, res)
                struct (ss.WritePos, res)

            let dispatcher = Scheduling.Dispatcher.MultiDispatcher<_, _, _, _>.Create(itemDispatcher, attemptWrite, interpretWriteResultProgress, stats, dumpStreams)
            Scheduling.StreamSchedulingEngine(dispatcher, enableSlipstreaming = true, ?maxBatches = maxBatches, ?idleDelay = idleDelay, ?purgeInterval = purgeInterval)

type EventStoreSink =

    /// Starts a <c>Sink</c> that ingests all submitted events into the supplied <c>connections</c>
    static member Start
        (   log : ILogger, storeLog, maxReadAhead, connections, maxConcurrentStreams,
            // Default 5m
            ?statsInterval,
            // Default 5m
            ?stateInterval,
            ?maxSubmissionsPerPartition,
            // Tune the sleep time when there are no items to schedule or responses to process. Default 1ms.
            ?idleDelay,
            // Frequency with which to jettison Write Position information for inactive streams in order to limit memory consumption
            // NOTE: Can impair performance and/or increase costs of writes as it inhibits the ability of the ingester to discard redundant inputs
            ?purgeInterval,
            ?ingesterStatsInterval)
        : Default.Sink =
        let statsInterval, stateInterval = defaultArg statsInterval (TimeSpan.FromMinutes 5.), defaultArg stateInterval (TimeSpan.FromMinutes 5.)
        let stats = Internal.Stats(log.ForContext<Internal.Stats>(), statsInterval, stateInterval)
        let dispatcher = Dispatch.ItemDispatcher<_, _>(maxConcurrentStreams)
        let dumpStreams logStreamStates _log = logStreamStates Default.eventSize
        let streamScheduler = Internal.EventStoreSchedulingEngine.Create(log, storeLog, connections, dispatcher, stats, dumpStreams, ?idleDelay = idleDelay, ?purgeInterval = purgeInterval)
        Projector.Pipeline.Start(
            log, dispatcher.Pump, (fun _abend -> streamScheduler.Pump), maxReadAhead, streamScheduler.Submit, statsInterval,
            ?maxSubmissionsPerPartition = maxSubmissionsPerPartition,
            ?ingesterStatsInterval = ingesterStatsInterval)
