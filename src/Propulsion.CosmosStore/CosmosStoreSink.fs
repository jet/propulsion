namespace Propulsion.CosmosStore

open Equinox.CosmosStore.Core
open FsCodec
open Propulsion
open Propulsion.Streams
open Propulsion.Streams.Internal // Helpers
open Serilog
open System.Collections.Generic
open System
open System.Threading

[<AutoOpen>]
module private Impl =
    let inline mb x = float x / 1024. / 1024.

module Internal =

    [<AutoOpen>]
    module Writer =
        type [<RequireQualifiedAccess>] ResultKind = TimedOut | RateLimited | TooLarge | Malformed | Other

        type [<NoComparison;NoEquality>] Result =
            | Ok of updatedPos : int64
            | Duplicate of updatedPos : int64
            | PartialDuplicate of overage : StreamSpan<byte[]>
            | PrefixMissing of batch : StreamSpan<byte[]> * writePos : int64
        let logTo (log : ILogger) malformed (res : StreamName * Choice<EventMetrics * Result, EventMetrics * exn>) =
            match res with
            | stream, (Choice1Of2 (_, Ok pos)) ->
                log.Information("Wrote     {stream} up to {pos}", stream, pos)
            | stream, (Choice1Of2 (_, Duplicate updatedPos)) ->
                log.Information("Ignored   {stream} (synced up to {pos})", stream, updatedPos)
            | stream, (Choice1Of2 (_, PartialDuplicate overage)) ->
                log.Information("Requeuing {stream} {pos} ({count} events)", stream, overage.index, overage.events.Length)
            | stream, (Choice1Of2 (_, PrefixMissing (batch, pos))) ->
                log.Information("Waiting   {stream} missing {gap} events ({count} events @ {pos})", stream, batch.index-pos, batch.events.Length, batch.index)
            | stream, (Choice2Of2 (_, exn)) ->
                let level = if malformed then Events.LogEventLevel.Warning else Events.LogEventLevel.Information
                log.Write(level, exn, "Writing   {stream} failed, retrying", stream)

        let write (log : ILogger) (ctx : EventsContext) stream span = async {
            log.Debug("Writing {s}@{i}x{n}", stream, span.index, span.events.Length)
            let! res = ctx.Sync(stream, { index = span.index; etag = None }, span.events |> Array.map (fun x -> x :> _))
            let res' =
                match res with
                | AppendResult.Ok pos -> Ok pos.index
                | AppendResult.Conflict (pos, _) | AppendResult.ConflictUnknown pos ->
                    match pos.index with
                    | actual when actual < span.index -> PrefixMissing (span, actual)
                    | actual when actual >= span.index + span.events.LongLength -> Duplicate actual
#if NET461
                    | actual -> PartialDuplicate { index = actual; events = span.events |> Seq.skip (actual-span.index |> int) |> Array.ofSeq }
#else
                    | actual -> PartialDuplicate { index = actual; events = span.events |> Array.skip (actual-span.index |> int) }
#endif
            log.Debug("Result: {res}", res')
            return res' }
        let (|TimedOutMessage|RateLimitedMessage|TooLargeMessage|MalformedMessage|Other|) (e : exn) =
            let isMalformed e =
                let m = string e
                m.Contains "SyntaxError: JSON.parse Error: Unexpected input at position"
                 || m.Contains "SyntaxError: JSON.parse Error: Invalid character at position"
            match e with
            | (:? Microsoft.Azure.Cosmos.CosmosException as ce) when ce.StatusCode = System.Net.HttpStatusCode.TooManyRequests -> RateLimitedMessage
            | (:? Microsoft.Azure.Cosmos.CosmosException as ce) when ce.StatusCode = System.Net.HttpStatusCode.RequestEntityTooLarge -> TooLargeMessage
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
        inherit Scheduling.Stats<EventMetrics * Writer.Result, EventMetrics * exn>(log, statsInterval, stateInterval)
        let okStreams, resultOk, resultDup, resultPartialDup, resultPrefix, resultExnOther = HashSet(), ref 0, ref 0, ref 0, ref 0, ref 0
        let badCats, failStreams, rateLimited, timedOut, tooLarge, malformed = CatStats(), HashSet(), ref 0, ref 0, ref 0, ref 0
        let rlStreams, toStreams, tlStreams, mfStreams, oStreams = HashSet(), HashSet(), HashSet(), HashSet(), HashSet()
        let mutable okEvents, okBytes, exnEvents, exnBytes = 0, 0L, 0, 0L

        override __.DumpStats() =
            let results = !resultOk + !resultDup + !resultPartialDup + !resultPrefix
            log.Information("Completed {mb:n0}MB {completed:n0}r {streams:n0}s {events:n0}e ({ok:n0} ok {dup:n0} redundant {partial:n0} partial {prefix:n0} waiting)",
                mb okBytes, results, okStreams.Count, okEvents, !resultOk, !resultDup, !resultPartialDup, !resultPrefix)
            okStreams.Clear(); resultOk := 0; resultDup := 0; resultPartialDup := 0; resultPrefix := 0; okEvents <- 0; okBytes <- 0L
            if !rateLimited <> 0 || !timedOut <> 0 || !tooLarge <> 0 || !malformed <> 0 || badCats.Any then
                let fails = !rateLimited + !timedOut + !tooLarge + !malformed + !resultExnOther
                log.Warning("Exceptions {mb:n0}MB {fails:n0}r {streams:n0}s {events:n0}e Rate-limited {rateLimited:n0}r {rlStreams:n0}s Timed out {toCount:n0}r {toStreams:n0}s",
                    mb exnBytes, fails, failStreams.Count, exnEvents, !rateLimited, rlStreams.Count, !timedOut, toStreams.Count)
                rateLimited := 0; timedOut := 0; resultExnOther := 0; failStreams.Clear(); rlStreams.Clear(); toStreams.Clear(); exnBytes <- 0L; exnEvents <- 0
            if badCats.Any then
                log.Warning("Affected cats {@badCats} Too large {tooLarge:n0}r {@tlStreams} Malformed {malformed:n0}r {@mfStreams} Other {other:n0}r {@oStreams}",
                    badCats.StatsDescending |> Seq.truncate 50, !tooLarge, tlStreams |> Seq.truncate 100, !malformed, mfStreams |> Seq.truncate 100, !resultExnOther, oStreams |> Seq.truncate 100)
                badCats.Clear(); tooLarge := 0; malformed := 0;  resultExnOther := 0; tlStreams.Clear(); mfStreams.Clear(); oStreams.Clear()
            Equinox.CosmosStore.Core.Log.InternalMetrics.dump log

        override __.Handle message =
            let inline adds x (set:HashSet<_>) = set.Add x |> ignore
            let inline bads x (set:HashSet<_>) = badCats.Ingest(StreamName.categorize x); adds x set
            base.Handle message
            match message with
            | Scheduling.InternalMessage.Added _ -> () // Processed by standard logging already; we have nothing to add
            | Scheduling.InternalMessage.Result (_duration, (stream, Choice1Of2 ((es, bs), res))) ->
                adds stream okStreams
                okEvents <- okEvents + es
                okBytes <- okBytes + int64 bs
                match res with
                | Writer.Result.Ok _ -> incr resultOk
                | Writer.Result.Duplicate _ -> incr resultDup
                | Writer.Result.PartialDuplicate _ -> incr resultPartialDup
                | Writer.Result.PrefixMissing _ -> incr resultPrefix
                __.HandleOk res
            | Scheduling.InternalMessage.Result (_duration, (stream, Choice2Of2 ((es, bs), exn))) ->
                adds stream failStreams
                exnEvents <- exnEvents + es
                exnBytes <- exnBytes + int64 bs
                match Writer.classify exn with
                | ResultKind.RateLimited -> adds stream rlStreams; incr rateLimited
                | ResultKind.TimedOut -> adds stream toStreams; incr timedOut
                | ResultKind.TooLarge -> bads stream tlStreams; incr tooLarge
                | ResultKind.Malformed -> bads stream mfStreams; incr malformed
                | ResultKind.Other -> bads stream oStreams; incr resultExnOther
                __.HandleExn(log.ForContext("stream", stream).ForContext("events", es), exn)
        abstract member HandleOk : Result -> unit
        default __.HandleOk(_) : unit = ()
        abstract member HandleExn : log : ILogger * exn : exn -> unit
        default __.HandleExn(_, _) : unit = ()

    type StreamSchedulingEngine =

        static member Create(log : ILogger, eventsContext, itemDispatcher, stats : Stats, dumpStreams, ?maxBatches, ?idleDelay, ?maxEvents, ?maxBytes)
            : Scheduling.StreamSchedulingEngine<_, _, _> =
            let maxEvents, maxBytes = defaultArg maxEvents 16384, defaultArg maxBytes (1024 * 1024 - (*fudge*)4096)
            let writerResultLog = log.ForContext<Writer.Result>()
            let attemptWrite (item : Scheduling.DispatchItem<_>) = async {
                let stats, span = Buffering.StreamSpan.slice (maxEvents, maxBytes) item.span
                try let! res = Writer.write log eventsContext (StreamName.toString item.stream) span
                    return Choice1Of2 (stats, res)
                with e -> return Choice2Of2 (stats, e) }
            let interpretWriteResultProgress (streams: Scheduling.StreamStates<_>) stream res =
                let applyResultToStreamState = function
                    | Choice1Of2 (_stats, Writer.Ok pos) ->                       streams.InternalUpdate stream pos null, false
                    | Choice1Of2 (_stats, Writer.Duplicate pos) ->                streams.InternalUpdate stream pos null, false
                    | Choice1Of2 (_stats, Writer.PartialDuplicate overage) ->     streams.InternalUpdate stream overage.index [|overage|], false
                    | Choice1Of2 (_stats, Writer.PrefixMissing (overage, pos)) -> streams.InternalUpdate stream pos [|overage|], false
                    | Choice2Of2 (_stats, exn) ->
                        let malformed = Writer.classify exn |> Writer.isMalformed
                        streams.SetMalformed(stream, malformed), malformed
                let (_stream, ss), malformed = applyResultToStreamState res
                Writer.logTo writerResultLog malformed (stream, res)
                ss.write, res
            let dispatcher = Scheduling.MultiDispatcher<_, _, _>(itemDispatcher, attemptWrite, interpretWriteResultProgress, stats, dumpStreams)
            Scheduling.StreamSchedulingEngine(dispatcher, enableSlipstreaming=true, ?maxBatches=maxBatches, ?idleDelay=idleDelay)

type CosmosStoreSink =

    /// Starts a <c>StreamsProjectorPipeline</c> that ingests all submitted events into the supplied <c>context</c>
    static member Start
        (   log : ILogger, maxReadAhead, eventsContext, maxConcurrentStreams,
            /// Default 5m
            ?statsInterval,
            /// Default 5m
            ?stateInterval,
            ?ingesterStatsInterval, ?maxSubmissionsPerPartition, ?pumpInterval,
            /// Tune the sleep time when there are no items to schedule or responses to process. Default 1ms.
            ?idleDelay,
            /// Default: 16384
            ?maxEvents,
            /// Default: 1MB (limited by maximum size of a CosmosDB stored procedure invocation)
            ?maxBytes)
        : Propulsion.ProjectorPipeline<_> =
        let statsInterval, stateInterval = defaultArg statsInterval (TimeSpan.FromMinutes 5.), defaultArg stateInterval (TimeSpan.FromMinutes 5.)
        let stats = Internal.Stats(log.ForContext<Internal.Stats>(), statsInterval, stateInterval)
        let dispatcher = Propulsion.Streams.Scheduling.ItemDispatcher<_>(maxConcurrentStreams)
        let dumpStreams (s : Scheduling.StreamStates<_>) l = s.Dump(l, Propulsion.Streams.Buffering.StreamState.eventsSize)
        let streamScheduler = Internal.StreamSchedulingEngine.Create(log, eventsContext, dispatcher, stats, dumpStreams, ?idleDelay=idleDelay, ?maxEvents=maxEvents, ?maxBytes=maxBytes)
        Propulsion.Streams.Projector.StreamsProjectorPipeline.Start(
            log, dispatcher.Pump(), streamScheduler.Pump, maxReadAhead, streamScheduler.Submit, statsInterval,
            ?ingesterStatsInterval=ingesterStatsInterval, ?maxSubmissionsPerPartition=maxSubmissionsPerPartition, ?pumpInterval=pumpInterval)
