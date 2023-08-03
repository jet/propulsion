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

        let private toNativeEventBody (xs : Propulsion.Sinks.EventBody) : byte[] = xs.ToArray()
        let defaultToNative_ = FsCodec.Core.TimelineEvent.Map toNativeEventBody
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
        let private toNativeEventBody (x : EventBody) : JsonElement =
            if x.IsEmpty then JsonElement()
            else JsonSerializer.Deserialize(x.Span)
        let defaultToNative_ = FsCodec.Core.TimelineEvent.Map toNativeEventBody
#endif

module Internal =

    [<AutoOpen>]
    module Writer =
        type [<RequireQualifiedAccess>] ResultKind = TimedOut | RateLimited | TooLarge | Malformed | Other

        type [<NoComparison; NoEquality; RequireQualifiedAccess>] Result =
            | Ok of updatedPos : int64
            | Duplicate of updatedPos : int64
            | PartialDuplicate of overage : Event[]
            | PrefixMissing of batch : Event[] * writePos : int64
        let logTo (log : ILogger) malformed (res : StreamName * Result<struct (StreamSpan.Metrics * Result), struct (StreamSpan.Metrics * exn)>) =
            match res with
            | stream, Ok (_, Result.Ok pos) ->
                log.Information("Wrote     {stream} up to {pos}", stream, pos)
            | stream, Ok (_, Result.Duplicate updatedPos) ->
                log.Information("Ignored   {stream} (synced up to {pos})", stream, updatedPos)
            | stream, Ok (_, Result.PartialDuplicate overage) ->
                log.Information("Requeuing {stream} {pos} ({count} events)", stream, overage[0].Index, overage.Length)
            | stream, Ok (_, Result.PrefixMissing (batch, pos)) ->
                log.Information("Waiting   {stream} missing {gap} events ({count} events @ {pos})", stream, batch[0].Index - pos, batch.Length, batch[0].Index)
            | stream, Error (_, exn) ->
                let level = if malformed then LogEventLevel.Warning else Events.LogEventLevel.Information
                log.Write(level, exn, "Writing   {stream} failed, retrying", stream)

        let write (log : ILogger) (ctx : EventsContext) stream (span : Event[]) ct = task {
            log.Debug("Writing {s}@{i}x{n}", stream, span[0].Index, span.Length)
#if COSMOSV3
            let! res = ctx.Sync(stream, { index = span[0].Index; etag = None }, span |> Array.map (fun x -> StreamSpan.defaultToNative_ x :> _))
                       |> Async.executeAsTask ct
#else
            let! res = ctx.Sync(stream, { index = span[0].Index; etag = None }, span |> Array.map (fun x -> StreamSpan.defaultToNative_ x :> _), ct)
#endif
            let res' =
                match res with
                | AppendResult.Ok pos -> Result.Ok pos.index
                | AppendResult.Conflict (pos, _) | AppendResult.ConflictUnknown pos ->
                    match pos.index with
                    | actual when actual < span[0].Index -> Result.PrefixMissing (span, actual)
                    | actual when actual >= span[0].Index + span.LongLength -> Result.Duplicate actual
                    | actual -> Result.PartialDuplicate (span |> Array.skip (actual - span[0].Index |> int))
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

        static member Create(log : ILogger, eventsContext, itemDispatcher, ?maxEvents, ?maxBytes) =
            let maxEvents, maxBytes = defaultArg maxEvents 16384, defaultArg maxBytes (256 * 1024)
            let writerResultLog = log.ForContext<Writer.Result>()
            let attemptWrite stream span ct = task {
                let struct (met, span') = StreamSpan.slice Event.renderedSize (maxEvents, maxBytes) span
                try let! res = Writer.write log eventsContext (StreamName.toString stream) span' ct
                    return struct (span'.Length > 0, Ok struct (met, res))
                with e -> return struct (false, Error struct (met, e)) }
            let interpretWriteResultProgress (streams: Scheduling.StreamStates<_>) stream res =
                let applyResultToStreamState = function
                    | Ok struct (_stats, Writer.Result.Ok pos) ->               struct (streams.RecordWriteProgress(stream, pos, null), false)
                    | Ok (_stats, Writer.Result.Duplicate pos) ->               streams.RecordWriteProgress(stream, pos, null), false
                    | Ok (_stats, Writer.Result.PartialDuplicate overage) ->    streams.RecordWriteProgress(stream, overage[0].Index, [| overage |]), false
                    | Ok (_stats, Writer.Result.PrefixMissing (overage, pos)) -> streams.RecordWriteProgress(stream, pos, [|overage|]), false
                    | Error struct (_stats, exn) ->
                        let malformed = Writer.classify exn |> Writer.isMalformed
                        streams.SetMalformed(stream, malformed), malformed
                let struct (ss, malformed) = applyResultToStreamState res
                Writer.logTo writerResultLog malformed (stream, res)
                struct (ss.WritePos, res)
            Dispatcher.Concurrent<_, _, _, _>.Create(itemDispatcher, attemptWrite, interpretWriteResultProgress)

type WriterResult = Internal.Writer.Result

type CosmosStoreSinkStats(log : ILogger, statsInterval, stateInterval) =
    inherit Scheduling.Stats<struct (StreamSpan.Metrics * WriterResult), struct (StreamSpan.Metrics * exn)>(log, statsInterval, stateInterval)
    let mutable okStreams, resultOk, resultDup, resultPartialDup, resultPrefix, resultExnOther = HashSet(), 0, 0, 0, 0, 0
    let mutable badCats, failStreams, rateLimited, timedOut, tooLarge, malformed = Stats.CatStats(), HashSet(), 0, 0, 0, 0
    let rlStreams, toStreams, tlStreams, mfStreams, oStreams = HashSet(), HashSet(), HashSet(), HashSet(), HashSet()
    let mutable okEvents, okBytes, exnEvents, exnBytes = 0, 0L, 0, 0L
    override _.Handle message =
        let inline adds x (set:HashSet<_>) = set.Add x |> ignore
        let inline bads x (set:HashSet<_>) = badCats.Ingest(StreamName.categorize x); adds x set
        match message with
        | { stream = stream; result = Ok ((es, bs), res) } ->
            adds stream okStreams
            okEvents <- okEvents + es
            okBytes <- okBytes + int64 bs
            match res with
            | WriterResult.Ok _ -> resultOk <- resultOk + 1
            | WriterResult.Duplicate _ -> resultDup <- resultDup + 1
            | WriterResult.PartialDuplicate _ -> resultPartialDup <- resultPartialDup + 1
            | WriterResult.PrefixMissing _ -> resultPrefix <- resultPrefix + 1
            base.RecordOk(message)
        | { stream = stream; result = Error ((es, bs), Exception.Inner exn) } ->
            adds stream failStreams
            exnEvents <- exnEvents + es
            exnBytes <- exnBytes + int64 bs
            let kind =
                match Internal.Writer.classify exn with
                | Internal.Writer.ResultKind.RateLimited -> adds stream rlStreams; rateLimited <- rateLimited + 1; OutcomeKind.RateLimited
                | Internal.Writer.ResultKind.TimedOut ->    adds stream toStreams; timedOut <- timedOut + 1;       OutcomeKind.Timeout
                | Internal.Writer.ResultKind.TooLarge ->    bads stream tlStreams; tooLarge <- tooLarge + 1;       OutcomeKind.Failed
                | Internal.Writer.ResultKind.Malformed ->   bads stream mfStreams; malformed <- malformed + 1;     OutcomeKind.Failed
                | Internal.Writer.ResultKind.Other ->       adds stream toStreams; timedOut <- timedOut + 1;       OutcomeKind.Exception
            base.RecordExn(message, kind, log.ForContext("stream", stream).ForContext("events", es), exn)
    override _.DumpStats() =
        let results = resultOk + resultDup + resultPartialDup + resultPrefix
        log.Information("Completed {mb:n0}MB {completed:n0}r {streams:n0}s {events:n0}e ({ok:n0} ok {dup:n0} redundant {partial:n0} partial {prefix:n0} waiting)",
            Log.miB okBytes, results, okStreams.Count, okEvents, resultOk, resultDup, resultPartialDup, resultPrefix)
        okStreams.Clear(); resultOk <- 0; resultDup <- 0; resultPartialDup <- 0; resultPrefix <- 0; okEvents <- 0; okBytes <- 0L
        if rateLimited <> 0 || timedOut <> 0 || tooLarge <> 0 || malformed <> 0 || badCats.Any then
            let fails = rateLimited + timedOut + tooLarge + malformed + resultExnOther
            log.Warning(" Exceptions {mb:n0}MB {fails:n0}r {streams:n0}s {events:n0}e Rate-limited {rateLimited:n0}r {rlStreams:n0}s Timed out {toCount:n0}r {toStreams:n0}s",
                Log.miB exnBytes, fails, failStreams.Count, exnEvents, rateLimited, rlStreams.Count, timedOut, toStreams.Count)
            rateLimited <- 0; timedOut <- 0; resultExnOther <- 0; failStreams.Clear(); rlStreams.Clear(); toStreams.Clear(); exnBytes <- 0L; exnEvents <- 0
        if badCats.Any then
            log.Warning("  Affected cats {@badCats} Too large {tooLarge:n0}r {@tlStreams} Malformed {malformed:n0}r {@mfStreams} Other {other:n0}r {@oStreams}",
                badCats.StatsDescending |> Seq.truncate 50, tooLarge, tlStreams |> Seq.truncate 100, malformed, mfStreams |> Seq.truncate 100, resultExnOther, oStreams |> Seq.truncate 100)
            badCats.Clear(); tooLarge <- 0; malformed <- 0;  resultExnOther <- 0; tlStreams.Clear(); mfStreams.Clear(); oStreams.Clear()
        Equinox.CosmosStore.Core.Log.InternalMetrics.dump log

    override _.HandleExn(log, exn) = log.Warning(exn, "Unhandled")

type CosmosStoreSink =

    /// Starts a <c>Sink</c> that ingests all submitted events into the supplied <c>context</c>
    static member Start
        (   log : ILogger, maxReadAhead, eventsContext, maxConcurrentStreams, stats: CosmosStoreSinkStats,
            ?purgeInterval, ?wakeForResults, ?idleDelay,
            // Default: 16384
            ?maxEvents,
            // Default: 256KB (limited by maximum size of a CosmosDB stored procedure invocation)
            ?maxBytes,
            ?ingesterStatsInterval)
        : Sink =
        let dispatcher = Internal.Dispatcher.Create(log, eventsContext, maxConcurrentStreams, ?maxEvents = maxEvents, ?maxBytes = maxBytes)
        let scheduler =
            let dumpStreams logStreamStates _log = logStreamStates Event.storedSize
            Scheduling.Engine(dispatcher, stats, dumpStreams, pendingBufferSize = 5, prioritizeStreamsBy = Event.storedSize,
                              ?purgeInterval = purgeInterval, ?wakeForResults = wakeForResults, ?idleDelay = idleDelay)
        Projector.Pipeline.Start(log, scheduler.Pump, maxReadAhead, scheduler, ingesterStatsInterval = defaultArg ingesterStatsInterval stats.StatsInterval.Period)
