namespace Propulsion.CosmosStore

open Equinox.CosmosStore.Core
open FsCodec
open Propulsion.Streams
open Propulsion.Streams.Internal // Helpers
open Serilog
open System
open System.Collections.Generic

[<AutoOpen>]
module private Impl =

#if COSMOSV3
    type EventBody = byte array // V4 defines one directly, here we shim it
    module StreamSpan =

        let private toNativeEventBody (xs : Default.EventBody) : byte array = xs.ToArray()
        let defaultToNative_ = FsCodec.Core.TimelineEvent.Map toNativeEventBody
#else
    module StreamSpan =

        // v4 and later use JsonElement, but Propulsion is sticking with byte arrays until 3.x (at which point it'll probably shift to ReadOnlyMemory<byte> rather than assuming and/or offering optimization for JSON bodies)
        open System.Text.Json
        let private toNativeEventBody (x : Default.EventBody) : JsonElement =
            if x.IsEmpty then JsonElement()
            else JsonSerializer.Deserialize(x.Span)
        let defaultToNative_ = FsCodec.Core.TimelineEvent.Map toNativeEventBody
#endif

module Internal =

    [<AutoOpen>]
    module Writer =
        type [<RequireQualifiedAccess>] ResultKind = TimedOut | RateLimited | TooLarge | Malformed | Other

        type [<NoComparison;NoEquality>] Result =
            | Ok of updatedPos : int64
            | Duplicate of updatedPos : int64
            | PartialDuplicate of overage : StreamSpan<Default.EventBody>
            | PrefixMissing of batch : StreamSpan<Default.EventBody> * writePos : int64
        let logTo (log : ILogger) malformed (res : StreamName * Choice<StreamSpan.Metrics * Result, StreamSpan.Metrics * exn>) =
            match res with
            | stream, Choice1Of2 (_, Ok pos) ->
                log.Information("Wrote     {stream} up to {pos}", stream, pos)
            | stream, Choice1Of2 (_, Duplicate updatedPos) ->
                log.Information("Ignored   {stream} (synced up to {pos})", stream, updatedPos)
            | stream, Choice1Of2 (_, PartialDuplicate overage) ->
                log.Information("Requeuing {stream} {pos} ({count} events)", stream, overage[0].Index, overage.Length)
            | stream, Choice1Of2 (_, PrefixMissing (batch, pos)) ->
                log.Information("Waiting   {stream} missing {gap} events ({count} events @ {pos})", stream, batch[0].Index - pos, batch.Length, batch[0].Index)
            | stream, Choice2Of2 (_, exn) ->
                let level = if malformed then Events.LogEventLevel.Warning else Events.LogEventLevel.Information
                log.Write(level, exn, "Writing   {stream} failed, retrying", stream)

        let write (log : ILogger) (ctx : EventsContext) stream (span : Default.StreamSpan) = async {
            log.Debug("Writing {s}@{i}x{n}", stream, span[0].Index, span.Length)
            let! res = ctx.Sync(stream, { index = span[0].Index; etag = None }, span |> Array.map (fun x -> StreamSpan.defaultToNative_ x :> _))
            let res' =
                match res with
                | AppendResult.Ok pos -> Ok pos.index
                | AppendResult.Conflict (pos, _) | AppendResult.ConflictUnknown pos ->
                    match pos.index with
                    | actual when actual < span[0].Index -> PrefixMissing (span, actual)
                    | actual when actual >= span[0].Index + span.LongLength -> Duplicate actual
                    | actual -> PartialDuplicate (span |> Array.skip (actual - span[0].Index |> int))
            log.Debug("Result: {res}", res')
            return res' }
        let (|TimedOutMessage|RateLimitedMessage|TooLargeMessage|MalformedMessage|Other|) (e : exn) =
            let isMalformed e =
                let m = string e
                m.Contains "SyntaxError: JSON.parse Error: Unexpected input at position"
                 || m.Contains "SyntaxError: JSON.parse Error: Invalid character at position"
            match e with
            | :? Microsoft.Azure.Cosmos.CosmosException as ce when ce.StatusCode = System.Net.HttpStatusCode.TooManyRequests -> RateLimitedMessage
            | :? Microsoft.Azure.Cosmos.CosmosException as ce when ce.StatusCode = System.Net.HttpStatusCode.RequestEntityTooLarge -> TooLargeMessage
            | e when e.GetType().FullName = "Microsoft.Azure.Documents.RequestTimeoutException" -> TimedOutMessage
            | e when isMalformed e -> MalformedMessage
            | _ -> Other

        let classify = function
            | RateLimitedMessage -> ResultKind.RateLimited
            | TimedOutMessage -> ResultKind.TimedOut
            | TooLargeMessage -> ResultKind.TooLarge
            | MalformedMessage -> ResultKind.Malformed
            | Other -> ResultKind.Other
        let isMalformed = function
            | ResultKind.RateLimited | ResultKind.TimedOut | ResultKind.Other -> false
            | ResultKind.TooLarge | ResultKind.Malformed -> true

    type Stats(log : ILogger, statsInterval, stateInterval) =
        inherit Scheduling.Stats<StreamSpan.Metrics * Writer.Result, StreamSpan.Metrics * exn>(log, statsInterval, stateInterval)
        let mutable okStreams, resultOk, resultDup, resultPartialDup, resultPrefix, resultExnOther = HashSet(), 0, 0, 0, 0, 0
        let mutable badCats, failStreams, rateLimited, timedOut, tooLarge, malformed = CatStats(), HashSet(), 0, 0, 0, 0
        let rlStreams, toStreams, tlStreams, mfStreams, oStreams = HashSet(), HashSet(), HashSet(), HashSet(), HashSet()
        let mutable okEvents, okBytes, exnEvents, exnBytes = 0, 0L, 0, 0L

        override _.DumpStats() =
            let results = resultOk + resultDup + resultPartialDup + resultPrefix
            log.Information("Completed {mb:n0}MB {completed:n0}r {streams:n0}s {events:n0}e ({ok:n0} ok {dup:n0} redundant {partial:n0} partial {prefix:n0} waiting)",
                mb okBytes, results, okStreams.Count, okEvents, resultOk, resultDup, resultPartialDup, resultPrefix)
            okStreams.Clear(); resultOk <- 0; resultDup <- 0; resultPartialDup <- 0; resultPrefix <- 0; okEvents <- 0; okBytes <- 0L
            if rateLimited <> 0 || timedOut <> 0 || tooLarge <> 0 || malformed <> 0 || badCats.Any then
                let fails = rateLimited + timedOut + tooLarge + malformed + resultExnOther
                log.Warning("Exceptions {mb:n0}MB {fails:n0}r {streams:n0}s {events:n0}e Rate-limited {rateLimited:n0}r {rlStreams:n0}s Timed out {toCount:n0}r {toStreams:n0}s",
                    mb exnBytes, fails, failStreams.Count, exnEvents, rateLimited, rlStreams.Count, timedOut, toStreams.Count)
                rateLimited <- 0; timedOut <- 0; resultExnOther <- 0; failStreams.Clear(); rlStreams.Clear(); toStreams.Clear(); exnBytes <- 0L; exnEvents <- 0
            if badCats.Any then
                log.Warning(" Affected cats {@badCats} Too large {tooLarge:n0}r {@tlStreams} Malformed {malformed:n0}r {@mfStreams} Other {other:n0}r {@oStreams}",
                    badCats.StatsDescending |> Seq.truncate 50, tooLarge, tlStreams |> Seq.truncate 100, malformed, mfStreams |> Seq.truncate 100, resultExnOther, oStreams |> Seq.truncate 100)
                badCats.Clear(); tooLarge <- 0; malformed <- 0;  resultExnOther <- 0; tlStreams.Clear(); mfStreams.Clear(); oStreams.Clear()
            Equinox.CosmosStore.Core.Log.InternalMetrics.dump log

        override this.Handle message =
            let inline adds x (set:HashSet<_>) = set.Add x |> ignore
            let inline bads x (set:HashSet<_>) = badCats.Ingest(StreamName.categorize x); adds x set
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
                | ResultKind.RateLimited -> adds stream rlStreams; rateLimited <- rateLimited + 1
                | ResultKind.TimedOut -> adds stream toStreams; timedOut <- timedOut + 1
                | ResultKind.TooLarge -> bads stream tlStreams; tooLarge <- tooLarge + 1
                | ResultKind.Malformed -> bads stream mfStreams; malformed <- malformed + 1
                | ResultKind.Other -> bads stream oStreams; resultExnOther <- resultExnOther + 1
                this.HandleExn(log.ForContext("stream", stream).ForContext("events", es), exn)
        abstract member HandleOk : Result -> unit
        default _.HandleOk _ : unit = ()
        abstract member HandleExn : log : ILogger * exn : exn -> unit
        default _.HandleExn(_, _) : unit = ()

    type StreamSchedulingEngine =

        static member Create(
                log : ILogger, eventsContext, itemDispatcher, stats : Stats, dumpStreams,
                ?maxBatches, ?purgeInterval, ?wakeForResults, ?idleDelay, ?maxEvents, ?maxBytes, ?prioritizeStreamsBy)
            : Scheduling.StreamSchedulingEngine<_, _, _, _> =
            let maxEvents, maxBytes = defaultArg maxEvents 16384, defaultArg maxBytes (1024 * 1024 - (*fudge*)4096)
            let writerResultLog = log.ForContext<Writer.Result>()
            let attemptWrite (stream, span) ct = task {
                let met, span' = StreamSpan.slice Default.jsonSize (maxEvents, maxBytes) span
                try let! res = Writer.write log eventsContext (StreamName.toString stream) span' |> fun f -> Async.StartAsTask(f, cancellationToken = ct)
                    return span'.Length > 0, Choice1Of2 (met, res)
                with e -> return false, Choice2Of2 (met, e) }
            let interpretWriteResultProgress (streams: Scheduling.StreamStates<_>) stream res =
                let applyResultToStreamState = function
                    | Choice1Of2 (_stats, Writer.Ok pos) ->                       struct (streams.RecordWriteProgress(stream, pos, null), false)
                    | Choice1Of2 (_stats, Writer.Duplicate pos) ->                streams.RecordWriteProgress(stream, pos, null), false
                    | Choice1Of2 (_stats, Writer.PartialDuplicate overage) ->     streams.RecordWriteProgress(stream, overage[0].Index, [| overage |]), false
                    | Choice1Of2 (_stats, Writer.PrefixMissing (overage, pos)) -> streams.RecordWriteProgress(stream, pos, [|overage|]), false
                    | Choice2Of2 (_stats, exn) ->
                        let malformed = Writer.classify exn |> Writer.isMalformed
                        streams.SetMalformed(stream, malformed), malformed
                let struct (ss, malformed) = applyResultToStreamState res
                Writer.logTo writerResultLog malformed (stream, res)
                struct (ss.WritePos, res)
            let dispatcher = Scheduling.MultiDispatcher<_, _, _, _>.Create(itemDispatcher, attemptWrite, interpretWriteResultProgress, stats, dumpStreams)
            Scheduling.StreamSchedulingEngine(
                 dispatcher,
                 ?maxBatches = maxBatches, ?purgeInterval = purgeInterval, ?wakeForResults = wakeForResults, ?idleDelay = idleDelay,
                 enableSlipstreaming = true, ?prioritizeStreamsBy = prioritizeStreamsBy)

type CosmosStoreSink =

    /// Starts a <c>Sink</c> that ingests all submitted events into the supplied <c>context</c>
    static member Start
        (   log : ILogger, maxReadAhead, eventsContext, maxConcurrentStreams,
            // Default 5m
            ?statsInterval,
            // Default 5m
            ?stateInterval,
            ?maxSubmissionsPerPartition,
            ?maxBatches, ?purgeInterval, ?wakeForResults, ?idleDelay,
            // Default: 16384
            ?maxEvents,
            // Default: 1MB (limited by maximum size of a CosmosDB stored procedure invocation)
            ?maxBytes,
            ?ingesterStatsInterval)
        : Default.Sink =
        let statsInterval, stateInterval = defaultArg statsInterval (TimeSpan.FromMinutes 5.), defaultArg stateInterval (TimeSpan.FromMinutes 5.)
        let stats = Internal.Stats(log.ForContext<Internal.Stats>(), statsInterval, stateInterval)
        let dispatcher = Scheduling.ItemDispatcher<_, _>(maxConcurrentStreams)
        let dumpStreams struct (s : Scheduling.StreamStates<_>, totalPurged) logger =
            s.Dump(logger, totalPurged, Buffering.StreamState.storedSize Default.eventSize)
        let streamScheduler =
            Internal.StreamSchedulingEngine.Create(
                log, eventsContext, dispatcher, stats, dumpStreams,
                ?maxBatches = maxBatches, ?purgeInterval = purgeInterval, ?wakeForResults = wakeForResults, ?idleDelay = idleDelay,
                ?maxEvents=maxEvents, ?maxBytes=maxBytes, prioritizeStreamsBy = Default.eventDataSize)
        Projector.Pipeline.Start(
            log, dispatcher.Pump, streamScheduler.Pump, maxReadAhead, streamScheduler.Submit, statsInterval,
            ?maxSubmissionsPerPartition = maxSubmissionsPerPartition,
            ?ingesterStatsInterval = ingesterStatsInterval)
