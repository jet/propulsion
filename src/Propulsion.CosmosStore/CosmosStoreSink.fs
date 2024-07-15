namespace Propulsion.CosmosStore

open Equinox.CosmosStore.Core
open FsCodec
open Propulsion.Internal
open Propulsion.Sinks
open Propulsion.Streams
open Serilog
open System.Collections.Generic

[<AutoOpen>]
module private Impl =

#if COSMOSV3
    type EventBody = byte[] // V4 defines one directly, here we shim it
    module StreamSpan =

        let toNativeEventBody (xs: Propulsion.Sinks.EventBody): byte[] = xs.ToArray()
    // Trimmed edition of what V4 exposes
    module internal Equinox =
        module CosmosStore =
            module Exceptions =
                open Microsoft.Azure.Cosmos
                let [<return: Struct>] (|CosmosStatus|_|) (x: exn) = match x with :? CosmosException as ce -> ValueSome ce.StatusCode | _ -> ValueNone
                let (|RateLimited|RequestTimeout|CosmosStatusCode|Other|) = function
                    | CosmosStatus System.Net.HttpStatusCode.TooManyRequests ->     RateLimited
                    | CosmosStatus System.Net.HttpStatusCode.RequestTimeout ->      RequestTimeout
                    | CosmosStatus s ->                                             CosmosStatusCode s
                    | _ ->                                                          Other

#else
    module StreamSpan =

        // v4 and later use JsonElement, but Propulsion is using ReadOnlyMemory<byte> rather than assuming and/or offering optimization for JSON bodies
        open System.Text.Json
        let toNativeEventBody (x: EventBody): JsonElement =
            if x.IsEmpty then JsonElement()
            else JsonSerializer.Deserialize(x.Span)
#endif

module Internal =

    [<AutoOpen>]
    module Writer =
        type [<RequireQualifiedAccess>] ResultKind = TimedOut | RateLimited | TooLarge | Malformed | Other

        type [<NoComparison; NoEquality; RequireQualifiedAccess>] Result =
            | Ok of updatedPos: int64
            | Duplicate of updatedPos: int64
            | PartialDuplicate of updatedPos: int64
            | PrefixMissing of gap: int * actualPos: int64
        let logTo (log: ILogger) malformed (res: StreamName * Result<struct (Result * StreamSpan.Metrics), struct (exn * StreamSpan.Metrics)>) =
            match res with
            | stream, Ok (Result.Ok pos, _) ->
                log.Information("Wrote     {stream} up to {pos}", stream, pos)
            | stream, Ok (Result.Duplicate updatedPos, _) ->
                log.Information("Ignored   {stream} (synced up to {pos})", stream, updatedPos)
            | stream, Ok (Result.PartialDuplicate updatedPos, _) ->
                log.Information("Requeuing {stream} {pos}", stream, updatedPos)
            | stream, Ok (Result.PrefixMissing (gap, pos), _) ->
                log.Information("Waiting   {stream} missing {gap} events before {pos}", stream, gap, pos)
            | stream, Error (exn, _) ->
                let level = if malformed then LogEventLevel.Warning else Events.LogEventLevel.Information
                log.Write(level, exn, "Writing   {stream} failed, retrying", stream)

        let write (log: ILogger) (ctx: EventsContext) stream (span: Event[]) ct = task {
            let i = StreamSpan.index span
            let n = StreamSpan.nextIndex span
            span |> Seq.iter (fun x -> if x.IsUnfold then invalidOp "CosmosStore3 does not [yet] support ingesting unfolds")
            log.Debug("Writing {s}@{i}x{n}", stream, i, span.Length)
            let mapData = FsCodec.Core.EventData.Map StreamSpan.toNativeEventBody
#if COSMOSV3
            let! res = ctx.Sync(stream, { index = i; etag = None }, span |> Array.map mapData)
                       |> Async.executeAsTask ct
#else
            let! res = ctx.Sync(stream, { index = i; etag = None }, span |> Array.map mapData, ct)
#endif
            let res' =
                match res with
                | AppendResult.Ok pos -> Result.Ok pos.index
                | AppendResult.Conflict (pos, _) | AppendResult.ConflictUnknown pos ->
                    match pos.index with
                    | actual when actual < i -> Result.PrefixMissing (actual - i |> int, actual)
                    | actual when actual >= n -> Result.Duplicate actual
                    | actual -> Result.PartialDuplicate actual
            log.Debug("Result: {res}", res')
            return res' }
        let containsMalformedMessage e =
            let m = string e
            m.Contains "SyntaxError: JSON.parse Error: Unexpected input at position"
            || m.Contains "SyntaxError: JSON.parse Error: Invalid character at position"
        let classify = function
            | Equinox.CosmosStore.Exceptions.RateLimited ->                                                      ResultKind.RateLimited
            | Equinox.CosmosStore.Exceptions.RequestTimeout ->                                                   ResultKind.TimedOut
            | Equinox.CosmosStore.Exceptions.CosmosStatusCode System.Net.HttpStatusCode.RequestEntityTooLarge -> ResultKind.TooLarge
            | e when containsMalformedMessage e ->                                                               ResultKind.Malformed
            | _ ->                                                                                               ResultKind.Other
        let isMalformed = function
            | ResultKind.RateLimited | ResultKind.TimedOut | ResultKind.Other -> false
            | ResultKind.TooLarge    | ResultKind.Malformed -> true

    type Dispatcher =

        static member Create(log: ILogger, eventsContext, itemDispatcher, ?maxEvents, ?maxBytes) =
            let maxEvents, maxBytes = defaultArg maxEvents 16384, defaultArg maxBytes (256 * 1024)
            let writerResultLog = log.ForContext<Writer.Result>()
            let attemptWrite stream span ct = task {
                let struct (span, met) = StreamSpan.slice Event.renderedSize (maxEvents, maxBytes) span
#if COSMOSV3
                try let! res = Writer.write log eventsContext (StreamName.toString stream) span ct
#else
                try let! res = Writer.write log eventsContext stream span ct
#endif
                    return Ok struct (res, met)
                with e -> return Error struct (e, met) }
            let interpretProgress (streams: Scheduling.StreamStates<_>) stream res =
                let applyResultToStreamState = function
                    | Ok struct ((Writer.Result.Ok pos' | Writer.Result.Duplicate pos' | Writer.Result.PartialDuplicate pos'), _stats) ->
                        let ss = streams.RecordWriteProgress(stream, pos', null)
                        struct (ss.WritePos, false)
                    | Ok (Writer.Result.PrefixMissing _, _stats) ->
                        streams.WritePos(stream), false
                    | Error struct (exn, _stats) ->
                        let malformed = Writer.classify exn |> Writer.isMalformed
                        let ss = streams.SetMalformed(stream, malformed)
                        ss.WritePos, malformed
                let struct (writePos, malformed) = applyResultToStreamState res
                Writer.logTo writerResultLog malformed (stream, res)
                struct (res, writePos)
            Dispatcher.Concurrent<_, _, _, _>.Create(itemDispatcher, attemptWrite, interpretProgress)

type WriterResult = Internal.Writer.Result

type CosmosStoreSinkStats(log: ILogger, statsInterval, stateInterval, [<O; D null>] ?failThreshold, [<O; D null>] ?logExternalStats) =
    inherit Scheduling.Stats<struct (WriterResult * StreamSpan.Metrics), struct (exn * StreamSpan.Metrics)>(
        log, statsInterval, stateInterval, ?failThreshold = failThreshold,
        logExternalStats = defaultArg logExternalStats Equinox.CosmosStore.Core.Log.InternalMetrics.dump)
    let mutable okStreams, okEvents, okBytes = HashSet(), 0, 0L
    let mutable exnCats, exnStreams, exnEvents, exnBytes = Stats.Counters(), HashSet(), 0, 0L
    let mutable resultOk, resultDup, resultPartialDup, resultPrefix, resultExn = 0, 0, 0, 0, 0
    override _.Handle message =
        match message with
        | { stream = stream; result = Ok (res, (es, bs)) } ->
            okStreams.Add stream |> ignore
            okEvents <- okEvents + es
            okBytes <- okBytes + int64 bs
            match res with
            | WriterResult.Ok _ -> resultOk <- resultOk + 1
            | WriterResult.Duplicate _ -> resultDup <- resultDup + 1
            | WriterResult.PartialDuplicate _ -> resultPartialDup <- resultPartialDup + 1
            | WriterResult.PrefixMissing _ -> resultPrefix <- resultPrefix + 1
            base.RecordOk(message)
        | { stream = stream; result = Error (Exception.Inner exn, (es, bs)) } ->
            exnCats.Ingest(StreamName.categorize stream)
            exnStreams.Add stream |> ignore
            exnEvents <- exnEvents + es
            exnBytes <- exnBytes + int64 bs
            resultExn <- resultExn + 1
            let kind =
                match Internal.Writer.classify exn with
                | Internal.Writer.ResultKind.RateLimited -> OutcomeKind.RateLimited
                | Internal.Writer.ResultKind.TimedOut ->    OutcomeKind.Timeout
                | Internal.Writer.ResultKind.TooLarge ->    OutcomeKind.Tagged "tooLarge"
                | Internal.Writer.ResultKind.Malformed ->   OutcomeKind.Tagged "malformed"
                | Internal.Writer.ResultKind.Other ->       OutcomeKind.Exn
            base.RecordExn(message, kind, log.ForContext("stream", stream).ForContext("events", es), exn)
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

type CosmosStoreSink =

    /// Starts a <c>Sink</c> that ingests all submitted events into the supplied <c>context</c>
    static member Start
        (   log: ILogger, maxReadAhead, eventsContext, maxConcurrentStreams, stats: CosmosStoreSinkStats,
            ?purgeInterval, ?wakeForResults, ?idleDelay, ?requireAll,
            // Default: 16384
            ?maxEvents,
            // Default: 256KB (limited by maximum size of a CosmosDB stored procedure invocation)
            ?maxBytes,
            ?ingesterStateInterval, ?commitInterval): SinkPipeline =
        let dispatcher = Internal.Dispatcher.Create(log, eventsContext, maxConcurrentStreams, ?maxEvents = maxEvents, ?maxBytes = maxBytes)
        let scheduler =
            let dumpStreams logStreamStates _log = logStreamStates Event.storedSize
            Scheduling.Engine(dispatcher, stats, dumpStreams, pendingBufferSize = 5, prioritizeStreamsBy = Event.storedSize,
                              ?purgeInterval = purgeInterval, ?wakeForResults = wakeForResults, ?idleDelay = idleDelay, ?requireAll = requireAll)
        Factory.Start(log, scheduler.Pump, maxReadAhead, scheduler,
                      ingesterStateInterval = defaultArg ingesterStateInterval stats.StateInterval.Period, ?commitInterval = commitInterval)
