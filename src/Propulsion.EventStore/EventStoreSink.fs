﻿#if EVENTSTORE_LEGACY
namespace Propulsion.EventStore

open Equinox.EventStore
#else
namespace Propulsion.EventStoreDb

open Equinox.EventStoreDb
#endif

open Propulsion.Internal
open Propulsion.Sinks
open Propulsion.Streams
open Serilog
open System.Collections.Generic

module Internal =

    [<AutoOpen>]
    module Writer =
        type [<NoComparison; NoEquality; RequireQualifiedAccess>] Result =
            | Ok of updatedPos: int64
            | Duplicate of updatedPos: int64
            | PartialDuplicate of overage: Event[]
            | PrefixMissing of batch: Event[] * writePos: int64

        let logTo (log: ILogger) (res: FsCodec.StreamName * Result<struct (StreamSpan.Metrics * Result), struct (StreamSpan.Metrics * exn)>) =
            match res with
            | stream, Ok (_, Result.Ok pos) ->
                log.Information("Wrote     {stream} up to {pos}", stream, pos)
            | stream, Ok (_, Result.Duplicate updatedPos) ->
                log.Information("Ignored   {stream} (synced up to {pos})", stream, updatedPos)
            | stream, Ok (_, Result.PartialDuplicate overage) ->
                log.Information("Requeuing {stream} {pos} ({count} events)", stream, overage[0].Index, overage.Length)
            | stream, Ok (_, Result.PrefixMissing (batch, pos)) ->
                log.Information("Waiting   {stream} missing {gap} events ({count} events @ {pos})",
                                stream, batch[0].Index - pos, batch.Length, batch[0].Index)
            | stream, Error (_, exn) ->
                log.Warning(exn,"Writing   {stream} failed, retrying", stream)

        let write (log: ILogger) (context: EventStoreContext) stream (span: Event[]) ct = task {
            log.Debug("Writing {s}@{i}x{n}", stream, span[0].Index, span.Length)
#if EVENTSTORE_LEGACY
            let! res = context.Sync(log, stream, span[0].Index - 1L, span |> Array.map (fun span -> span :> _))
#else
            let! res = context.Sync(log, stream, span[0].Index - 1L, span |> Array.map (fun span -> span :> _), ct)
#endif
            let ress =
                match res with
                | GatewaySyncResult.Written (Token.Unpack pos') ->
                    Result.Ok (pos'.streamVersion + 1L)
                | GatewaySyncResult.ConflictUnknown (Token.Unpack pos) ->
                    match pos.streamVersion + 1L with
                    | actual when actual < span[0].Index -> Result.PrefixMissing (span, actual)
                    | actual when actual >= span[0].Index + span.LongLength -> Result.Duplicate actual
                    | actual -> Result.PartialDuplicate (span |> Array.skip (actual - span[0].Index |> int))
            log.Debug("Result: {res}", ress)
            return ress }

        type [<RequireQualifiedAccess>] ResultKind = TimedOut | Other

    type Dispatcher =

        static member Create(log: ILogger, storeLog, connections: _[], maxDop) =
            let writerResultLog = log.ForContext<Writer.Result>()
            let mutable robin = 0

            let attemptWrite stream span ct = task {
                let index = System.Threading.Interlocked.Increment(&robin) % connections.Length
                let selectedConnection = connections[index]
                let maxEvents, maxBytes = 65536, 4 * 1024 * 1024 - (*fudge*)4096
                let struct (met, span') = StreamSpan.slice Event.renderedSize (maxEvents, maxBytes) span
                try let! res = Writer.write storeLog selectedConnection (FsCodec.StreamName.toString stream) span' ct
                    return Ok struct (met, res)
                with e -> return Error struct (met, e) }
            let interpretProgress (streams: Scheduling.StreamStates<_>) stream res =
                let applyResultToStreamState = function
                    | Ok struct (_stats, Writer.Result.Ok pos) ->               streams.RecordWriteProgress(stream, pos, null)
                    | Ok (_stats, Writer.Result.Duplicate pos) ->               streams.RecordWriteProgress(stream, pos, null)
                    | Ok (_stats, Writer.Result.PartialDuplicate overage) ->    streams.RecordWriteProgress(stream, overage[0].Index, [| overage |])
                    | Ok (_stats, Writer.Result.PrefixMissing (overage, pos)) -> streams.RecordWriteProgress(stream, pos, [| overage |])
                    | Error struct (_stats, _exn) ->                            streams.SetMalformed(stream, false)
                let ss = applyResultToStreamState res
                Writer.logTo writerResultLog (stream, res)
                struct (ss.WritePos, res)
            Dispatcher.Concurrent<_, _, _, _>.Create(maxDop, attemptWrite, interpretProgress)

type WriterResult = Internal.Writer.Result

type EventStoreSinkStats(log: ILogger, statsInterval, stateInterval, [<O; D null>] ?failThreshold, [<O; D null>] ?logExternalStats) =
    inherit Scheduling.Stats<struct (StreamSpan.Metrics * WriterResult), struct (StreamSpan.Metrics * exn)>(
        log, statsInterval, stateInterval, ?failThreshold = failThreshold,
        logExternalStats = defaultArg logExternalStats Log.InternalMetrics.dump)
    let mutable okStreams, okEvents, okBytes, exnCats, exnStreams, exnEvents, exnBytes = HashSet(), 0, 0L, Stats.Counters(), HashSet(), 0 , 0L
    let mutable resultOk, resultDup, resultPartialDup, resultPrefix, resultExn = 0, 0, 0, 0, 0
    override _.Handle message =
        match message with
        | { stream = stream; result = Ok ((es, bs), res) } ->
            okStreams.Add stream |> ignore
            okEvents <- okEvents + es
            okBytes <- okBytes + int64 bs
            match res with
            | WriterResult.Ok _ -> resultOk <- resultOk + 1
            | WriterResult.Duplicate _ -> resultDup <- resultDup + 1
            | WriterResult.PartialDuplicate _ -> resultPartialDup <- resultPartialDup + 1
            | WriterResult.PrefixMissing _ -> resultPrefix <- resultPrefix + 1
            base.RecordOk(message)
        | { stream = stream; result = Error ((es, bs), Exception.Inner exn) } ->
            exnCats.Ingest(StreamName.categorize stream)
            exnStreams.Add stream |> ignore
            exnEvents <- exnEvents + es
            exnBytes <- exnBytes + int64 bs
            resultExn <- resultExn + 1
            base.RecordExn(message, OutcomeKind.classify exn, log.ForContext("stream", stream).ForContext("events", es), exn)
    override _.DumpStats() =
        let results = resultOk + resultDup + resultPartialDup + resultPrefix
        log.Information("Completed {mb:n0}MB {completed:n0}r {streams:n0}s {events:n0}e ({ok:n0} ok {dup:n0} redundant {partial:n0} partial {prefix:n0} waiting)",
            Log.miB okBytes, results, okStreams.Count, okEvents, resultOk, resultDup, resultPartialDup, resultPrefix)
        okStreams.Clear(); resultOk <- 0; resultDup <- 0; resultPartialDup <- 0; resultPrefix <- 0; okEvents <- 0; okBytes <- 0L
        if exnCats.Any then
            log.Warning(" Exceptions {mb:n0}MB {fails:n0}r {streams:n0}s {events:n0}e",
                Log.miB exnBytes, resultExn, exnStreams.Count, exnEvents)
            resultExn <- 0; exnBytes <- 0L; exnEvents <- 0
            log.Warning("  Affected cats {@exnCats} Streams {@exnStreams}",
                exnCats.StatsDescending |> Seq.truncate 50, exnStreams |> Seq.truncate 100)
            exnCats.Clear(); exnStreams.Clear()

    override _.HandleExn(log, exn) = log.Warning(exn, "Unhandled")

type EventStoreSink =

    /// Starts a <c>Sink</c> that ingests all submitted events into the supplied <c>connections</c>
    static member Start
        (   log: ILogger, storeLog, maxReadAhead, connections, maxConcurrentStreams, stats: EventStoreSinkStats,
            // Frequency with which to jettison Write Position information for inactive streams in order to limit memory consumption
            // NOTE: Can impair performance and/or increase costs of writes as it inhibits the ability of the ingester to discard redundant inputs
            ?purgeInterval,
            // Tune the sleep time when there are no items to schedule or responses to process. Default 1ms.
            ?idleDelay,
            ?ingesterStateInterval, ?commitInterval): SinkPipeline =
        let dispatcher = Internal.Dispatcher.Create(log, storeLog, connections, maxConcurrentStreams)
        let scheduler =
            let dumpStreams logStreamStates _log = logStreamStates Event.storedSize
            Scheduling.Engine(dispatcher, stats, dumpStreams, pendingBufferSize = 5, ?purgeInterval = purgeInterval, ?idleDelay = idleDelay)
        Factory.Start(log, scheduler.Pump, maxReadAhead, scheduler,
                      ingesterStateInterval = defaultArg ingesterStateInterval stats.StateInterval.Period, ?commitInterval = commitInterval)
