#if EVENTSTORE_LEGACY
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
            | PartialDuplicate of updatedPos: int64
            | PrefixMissing of gap: int * actualPos: int64

        let logTo (log: ILogger) (stream: FsCodec.StreamName): Result<Dispatcher.ResProgressAndMetrics<Result>, Dispatcher.ExnAndMetrics> -> unit = function
            | Ok (Result.Ok pos, _, _) ->
                log.Information("Wrote     {stream} up to {pos}", stream, pos)
            | Ok (Result.Duplicate updatedPos, _, _) ->
                log.Information("Ignored   {stream} (synced up to {pos})", stream, updatedPos)
            | Ok (Result.PartialDuplicate updatedPos, _, _) ->
                log.Information("Requeuing {stream} {pos}", stream, updatedPos)
            | Ok (Result.PrefixMissing (gap, pos), _, _) ->
                log.Information("Waiting   {stream} missing {gap} events before {pos}", stream, gap, pos)
            | Error (exn, _, _) ->
                log.Warning(exn,"Writing   {stream} failed, retrying", stream)

        let write (log: ILogger) (context: EventStoreContext) stream (span: Event[]) ct = task {
            let i = StreamSpan.index span
            log.Debug("Writing {s}@{i}x{n}", stream, i, span.Length)
#if EVENTSTORE_LEGACY
            let! res = context.Sync(log, stream, i - 1L, span |> Array.map (FsCodec.Core.EventData.mapBodies FsCodec.Encoding.ToBlob))
#else
            let! res = context.Sync(log, stream, i - 1L, span |> Array.map (FsCodec.Core.EventData.mapBodies FsCodec.Encoding.ToBlob), ct)
#endif
            let res' =
                match res with
                | GatewaySyncResult.Written (Token.Unpack pos') ->
                    Result.Ok (pos'.streamVersion + 1L)
                | GatewaySyncResult.ConflictUnknown (Token.Unpack pos) ->
                    match pos.streamVersion + 1L with
                    | actual when actual < i -> Result.PrefixMissing (i - actual |> int, actual)
                    | actual when actual >= i + span.LongLength -> Result.Duplicate actual
                    | actual -> Result.PartialDuplicate actual
            log.Debug("Result: {res}", res')
            return res' }

        type [<RequireQualifiedAccess>] ResultKind = TimedOut | Other

    type Dispatcher =

        static member Create(storeLog, connections: _[], maxDop) =
            let mutable robin = 0

            let attemptWrite stream span _revision ct = task {
                let index = System.Threading.Interlocked.Increment(&robin) % connections.Length
                let selectedConnection = connections[index]
                let maxEvents, maxBytes = 65536, 4 * 1024 * 1024 - (*fudge*)4096
                let struct (span, met) = StreamSpan.slice Event.renderedSize (maxEvents, maxBytes) span
                try let! res = Writer.write storeLog selectedConnection (FsCodec.StreamName.toString stream) span ct
                    let hp = res |> function
                        | Writer.Result.Ok pos' | Writer.Result.Duplicate pos' | Writer.Result.PartialDuplicate pos' -> Buffer.HandlerProgress.ofPos pos' |> ValueSome
                        | Writer.Result.PrefixMissing _ -> ValueNone
                    return Ok struct (res, hp, met)
                with e -> return Error struct (e, false, met) }
            let interpretProgress = function
                | Ok struct (_res, hp, _met) as res -> struct (res, hp, false)
                | Error struct (_exn, malformed, _met) as res -> res, ValueNone, malformed
            Dispatcher.Concurrent<_, _, _, _>.Create(maxDop, project = attemptWrite, interpretProgress = interpretProgress)

type WriterResult = Internal.Writer.Result

type EventStoreSinkStats(log: ILogger, statsInterval, stateInterval, [<O; D null>] ?failThreshold, [<O; D null>] ?logExternalStats) =
    inherit Scheduling.Stats<Dispatcher.ResProgressAndMetrics<WriterResult>, Dispatcher.ExnAndMetrics>(
        log, statsInterval, stateInterval, ?failThreshold = failThreshold,
        logExternalStats = defaultArg logExternalStats Log.InternalMetrics.dump)
    let writerResultLog = log.ForContext<WriterResult>()
    let mutable okStreams, okEvents, okBytes, exnCats, exnStreams, exnEvents, exnBytes = HashSet(), 0, 0L, Stats.Counters(), HashSet(), 0 , 0L
    let mutable resultOk, resultDup, resultPartialDup, resultPrefix, resultExn = 0, 0, 0, 0, 0
    override _.Handle message =
        match message with
        | { stream = stream; result = Ok (res, _hp, (es, us, bs)) as r } ->
            okStreams.Add stream |> ignore
            okEvents <- okEvents + es
            okBytes <- okBytes + int64 bs
            match res with
            | WriterResult.Ok _ -> resultOk <- resultOk + 1
            | WriterResult.Duplicate _ -> resultDup <- resultDup + 1
            | WriterResult.PartialDuplicate _ -> resultPartialDup <- resultPartialDup + 1
            | WriterResult.PrefixMissing _ -> resultPrefix <- resultPrefix + 1
            Internal.Writer.logTo writerResultLog stream r
            base.RecordOk(message, us <> 0)
        | { stream = stream; result = Error (Exception.Inner exn, _malformed, (es, _us, bs)) as r } ->
            exnCats.Ingest(StreamName.categorize stream)
            exnStreams.Add stream |> ignore
            exnEvents <- exnEvents + es
            exnBytes <- exnBytes + int64 bs
            resultExn <- resultExn + 1
            Internal.Writer.logTo writerResultLog stream r
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
        let dispatcher = Internal.Dispatcher.Create(storeLog, connections, maxConcurrentStreams)
        let scheduler =
            let dumpStreams logStreamStates _log = logStreamStates Event.storedSize
            Scheduling.Engine(dispatcher, stats, dumpStreams, pendingBufferSize = 5, ?purgeInterval = purgeInterval, ?idleDelay = idleDelay)
        Factory.Start(log, scheduler.Pump, maxReadAhead, scheduler,
                      ingesterStateInterval = defaultArg ingesterStateInterval stats.StateInterval.Period, ?commitInterval = commitInterval)
