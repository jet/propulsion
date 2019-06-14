namespace Propulsion.Cosmos

open Equinox.Cosmos.Core
open Equinox.Cosmos.Store
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
            | Ok of updatedPos: int64
            | Duplicate of updatedPos: int64
            | PartialDuplicate of overage: StreamSpan<byte[]>
            | PrefixMissing of batch: StreamSpan<byte[]> * writePos: int64
        let logTo (log: ILogger) (res : string * Choice<(int*int)*Result,(int*int)*exn>) =
            match res with
            | stream, (Choice1Of2 (_, Ok pos)) ->
                log.Information("Wrote     {stream} up to {pos}", stream, pos)
            | stream, (Choice1Of2 (_, Duplicate updatedPos)) ->
                log.Information("Ignored   {stream} (synced up to {pos})", stream, updatedPos)
            | stream, (Choice1Of2 (_, PartialDuplicate overage)) ->
                log.Information("Requeing  {stream} {pos} ({count} events)", stream, overage.index, overage.events.Length)
            | stream, (Choice1Of2 (_, PrefixMissing (batch,pos))) ->
                log.Information("Waiting   {stream} missing {gap} events ({count} events @ {pos})", stream, batch.index-pos, batch.events.Length, batch.index)
            | stream, (Choice2Of2 (_, exn)) ->
                log.Warning(exn,"Writing   {stream} failed, retrying", stream)

        module EquinoxEvent =
            let ofStreamEvent (x: Propulsion.Streams.IEvent<_>) =
                { new Equinox.Codec.IEvent<_> with
                    member __.EventType = x.EventType
                    member __.Data = x.Data
                    member __.Meta = x.Meta
                    member __.Timestamp = x.Timestamp }
        let write (log : ILogger) (ctx : Context) stream span = async {
            let stream = ctx.CreateStream stream
            log.Debug("Writing {s}@{i}x{n}",stream,span.index,span.events.Length)
            let! res = ctx.Sync(stream, { index = span.index; etag = None }, span.events |> Array.map EquinoxEvent.ofStreamEvent)
            let ress =
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
            log.Debug("Result: {res}",ress)
            return ress }
        let (|TimedOutMessage|RateLimitedMessage|TooLargeMessage|MalformedMessage|Other|) (e: exn) =
            let isMalformed () =
                let m = string e
                m.Contains "SyntaxError: JSON.parse Error: Unexpected input at position"
                 || m.Contains "SyntaxError: JSON.parse Error: Invalid character at position"
            match e.GetType().Name with
            | "Microsoft.Azure.Documents.RequestTimeoutException" -> TimedOutMessage
            | "Microsoft.Azure.Documents.RequestRateTooLargeException" -> RateLimitedMessage
            | "Microsoft.Azure.Documents.RequestEntityTooLargeException" -> TooLargeMessage
            | _ when isMalformed () -> MalformedMessage
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

    type OkResult = (int*int)*Writer.Result
    type FailResult = (int*int) * exn

    type CosmosStats(log : ILogger, categorize, statsInterval, stateInterval) =
        inherit Scheduling.StreamSchedulerStats<OkResult,FailResult>(log, statsInterval, stateInterval)
        let okStreams, resultOk, resultDup, resultPartialDup, resultPrefix, resultExnOther = HashSet(), ref 0, ref 0, ref 0, ref 0, ref 0
        let badCats, failStreams, rateLimited, timedOut, tooLarge, malformed = CatStats(), HashSet(), ref 0, ref 0, ref 0, ref 0
        let rlStreams, toStreams, tlStreams, mfStreams, oStreams = HashSet(), HashSet(), HashSet(), HashSet(), HashSet()
        let mutable okEvents, okBytes, exnEvents, exnBytes = 0, 0L, 0, 0L

        override __.DumpExtraStats() =
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
                log.Warning("Malformed cats {@badCats} Too large {tooLarge:n0}r {@tlStreams} Malformed {malformed:n0}r {@mfStreams} Other {other:n0}r {@oStreams}",
                    badCats.StatsDescending |> Seq.truncate 50, !tooLarge, tlStreams |> Seq.truncate 100, !malformed, mfStreams |> Seq.truncate 100, !resultExnOther, oStreams |> Seq.truncate 100)
                badCats.Clear(); tooLarge := 0; malformed := 0;  resultExnOther := 0; tlStreams.Clear(); mfStreams.Clear(); oStreams.Clear()
            Equinox.Cosmos.Store.Log.InternalMetrics.dump log

        override __.Handle message =
            let inline adds x (set:HashSet<_>) = set.Add x |> ignore
            let inline bads x (set:HashSet<_>) = badCats.Ingest(categorize x); adds x set
            base.Handle message
            match message with
            | Scheduling.InternalMessage.Added _ -> () // Processed by standard logging already; we have nothing to add
            | Scheduling.InternalMessage.Result (_duration, (stream, Choice1Of2 ((es,bs),r))) ->
                adds stream okStreams
                okEvents <- okEvents + es
                okBytes <- okBytes + int64 bs
                match r with
                | Writer.Result.Ok _ -> incr resultOk
                | Writer.Result.Duplicate _ -> incr resultDup
                | Writer.Result.PartialDuplicate _ -> incr resultPartialDup
                | Writer.Result.PrefixMissing _ -> incr resultPrefix
            | Scheduling.InternalMessage.Result (_duration, (stream, Choice2Of2 ((es,bs),exn))) ->
                adds stream failStreams
                exnEvents <- exnEvents + es
                exnBytes <- exnBytes + int64 bs
                match Writer.classify exn with
                | ResultKind.RateLimited -> adds stream rlStreams; incr rateLimited
                | ResultKind.TimedOut -> adds stream toStreams; incr timedOut
                | ResultKind.TooLarge -> bads stream tlStreams; incr tooLarge
                | ResultKind.Malformed -> bads stream mfStreams; incr malformed
                | ResultKind.Other -> bads stream oStreams; incr resultExnOther

    type CosmosSchedulingEngine =
        static member Create(log : ILogger, cosmosContexts : _ [], dispatcher, stats : CosmosStats, dumpStreams, ?maxBatches)
            : Scheduling.StreamSchedulingEngine<_,_> =
            let writerResultLog = log.ForContext<Writer.Result>()
            let mutable robin = 0
            let attemptWrite (_maybeWritePos,stream,span) = async {
                let index = Interlocked.Increment(&robin) % cosmosContexts.Length
                let selectedConnection = cosmosContexts.[index]
                let maxEvents, maxBytes = 16384, 1024 * 1024 - (*fudge*)4096
                let stats, span' = Buffering.StreamSpan.slice (maxEvents,maxBytes) span
                try let! res = Writer.write log selectedConnection stream span'
                    return Choice1Of2 (stats,res)
                with e -> return Choice2Of2 (stats,e) }
            let interpretWriteResultProgress (streams: Scheduling.StreamStates<_>) stream res =
                let applyResultToStreamState = function
                    | Choice1Of2 (_stats, Writer.Ok pos) ->                       streams.InternalUpdate stream pos null
                    | Choice1Of2 (_stats, Writer.Duplicate pos) ->                streams.InternalUpdate stream pos null
                    | Choice1Of2 (_stats, Writer.PartialDuplicate overage) ->     streams.InternalUpdate stream overage.index [|overage|]
                    | Choice1Of2 (_stats, Writer.PrefixMissing (overage,pos)) ->  streams.InternalUpdate stream pos [|overage|]
                    | Choice2Of2 (_stats, exn) ->
                        let malformed = Writer.classify exn |> Writer.isMalformed
                        streams.SetMalformed(stream,malformed)
                let _stream, ss = applyResultToStreamState res
                Writer.logTo writerResultLog (stream,res)
                ss.write
            Scheduling.StreamSchedulingEngine(dispatcher, stats, attemptWrite, interpretWriteResultProgress, dumpStreams, enableSlipstreaming=true, ?maxBatches = maxBatches)

type CosmosSink =
    static member Start(log : ILogger, maxReadAhead, cosmosContexts, maxConcurrentStreams, categorize, ?statsInterval, ?stateInterval, ?maxSubmissionsPerPartition)
            : Propulsion.ProjectorPipeline<_> =
        let statsInterval, stateInterval = defaultArg statsInterval (TimeSpan.FromMinutes 5.), defaultArg stateInterval (TimeSpan.FromMinutes 5.)
        let projectionStats = Internal.CosmosStats(log.ForContext<Internal.CosmosStats>(), categorize, statsInterval, stateInterval)
        let dispatcher = Propulsion.Streams.Scheduling.Dispatcher<_>(maxConcurrentStreams)
        let dumpStreams (s : Scheduling.StreamStates<_>) l = s.Dump(l, Propulsion.Streams.Buffering.StreamState.eventsSize, categorize)
        let streamScheduler = Internal.CosmosSchedulingEngine.Create(log, cosmosContexts, dispatcher, projectionStats, dumpStreams)
        let mapBatch onCompletion (x : Submission.SubmissionBatch<StreamEvent<_>>) : Scheduling.StreamsBatch<_> =
            let onCompletion () = x.onCompletion(); onCompletion()
            Scheduling.StreamsBatch.Create(onCompletion, x.messages) |> fst
        let submitBatch (x : Scheduling.StreamsBatch<_>) : int =
            streamScheduler.Submit x
            x.RemainingStreamsCount
        let tryCompactQueue (queue : Queue<Scheduling.StreamsBatch<_>>) =
            let mutable acc, worked = None, false
            for x in queue do
                match acc with
                | None -> acc <- Some x
                | Some a -> if a.TryMerge x then worked <- true
            worked
        let maxSubmissionsPerPartition = defaultArg maxSubmissionsPerPartition 5
        let submitter = Submission.SubmissionEngine<_,_>(log, maxSubmissionsPerPartition, mapBatch, submitBatch, statsInterval, tryCompactQueue=tryCompactQueue)
        let startIngester (rangeLog, projectionId) = Projector.StreamsIngester.Start(rangeLog, projectionId, maxReadAhead, submitter.Ingest)
        ProjectorPipeline.Start(log, dispatcher.Pump(), streamScheduler.Pump, submitter.Pump(), startIngester)