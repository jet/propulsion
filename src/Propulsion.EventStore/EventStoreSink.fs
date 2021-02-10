namespace Propulsion.EventStore

open Equinox.EventStore
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
        type EventStoreContext() = class end

        type [<NoComparison; NoEquality>] Result =
            | Ok of updatedPos : int64
            | Duplicate of updatedPos : int64
            | PartialDuplicate of overage : StreamSpan<byte[]>
            | PrefixMissing of batch : StreamSpan<byte[]> * writePos : int64

        let logTo (log : ILogger) (res : FsCodec.StreamName * Choice<EventMetrics * Result, EventMetrics * exn>) =
            match res with
            | stream, (Choice1Of2 (_, Ok pos)) ->
                log.Information("Wrote     {stream} up to {pos}", stream, pos)
            | stream, (Choice1Of2 (_, Duplicate updatedPos)) ->
                log.Information("Ignored   {stream} (synced up to {pos})", stream, updatedPos)
            | stream, (Choice1Of2 (_, PartialDuplicate overage)) ->
                log.Information("Requeuing {stream} {pos} ({count} events)", stream, overage.index, overage.events.Length)
            | stream, (Choice1Of2 (_, PrefixMissing (batch, pos))) ->
                log.Information("Waiting   {stream} missing {gap} events ({count} events @ {pos})", stream, batch.index - pos, batch.events.Length, batch.index)
            | stream, (Choice2Of2 (_, exn)) ->
                log.Warning(exn,"Writing   {stream} failed, retrying", stream)

        let write (log : ILogger) (context : Context) stream span = async {
            log.Debug("Writing {s}@{i}x{n}", stream, span.index, span.events.Length)
            let! res = context.Sync(log, stream, span.index - 1L, span.events |> Array.map (fun x -> x :> _))
            let ress =
                match res with
                | GatewaySyncResult.Written (Token.Unpack pos') ->
                    Ok (pos'.pos.streamVersion + 1L)
                | GatewaySyncResult.ConflictUnknown (Token.Unpack pos) ->
                    match pos.pos.streamVersion + 1L with
                    | actual when actual < span.index -> PrefixMissing (span, actual)
                    | actual when actual >= span.index + span.events.LongLength -> Duplicate actual
#if NET461
                    | actual -> PartialDuplicate { index = actual; events = span.events |> Seq.skip (actual - span.index |> int) |> Array.ofSeq }
#else
                    | actual -> PartialDuplicate { index = actual; events = span.events |> Array.skip (actual - span.index |> int) }
#endif
            log.Debug("Result: {res}", ress)
            return ress }

        type [<RequireQualifiedAccess>] ResultKind = TimedOut | Other

        let classify e =
            match box e with
            | :? System.TimeoutException -> ResultKind.TimedOut
            | _ -> ResultKind.Other

    type Stats(log : ILogger, statsInterval, stateInterval) =
        inherit Scheduling.Stats<EventMetrics * Writer.Result, EventMetrics * exn>(log, statsInterval, stateInterval)
        let okStreams, resultOk, resultDup, resultPartialDup, resultPrefix, resultExnOther = HashSet(), ref 0, ref 0, ref 0, ref 0, ref 0
        let badCats, failStreams, timedOut = CatStats(), HashSet(), ref 0
        let toStreams, oStreams = HashSet(), HashSet()
        let mutable okEvents, okBytes, exnEvents, exnBytes = 0, 0L, 0, 0L

        override __.DumpStats() =
            let results = !resultOk + !resultDup + !resultPartialDup + !resultPrefix
            log.Information("Completed {mb:n0}MB {completed:n0}r {streams:n0}s {events:n0}e ({ok:n0} ok {dup:n0} redundant {partial:n0} partial {prefix:n0} waiting)",
                mb okBytes, results, okStreams.Count, okEvents, !resultOk, !resultDup, !resultPartialDup, !resultPrefix)
            okStreams.Clear(); resultOk := 0; resultDup := 0; resultPartialDup := 0; resultPrefix := 0; okEvents <- 0; okBytes <- 0L
            if !timedOut <> 0 || badCats.Any then
                let fails = !timedOut + !resultExnOther
                log.Warning("Exceptions {mb:n0}MB {fails:n0}r {streams:n0}s {events:n0}e Timed out {toCount:n0}r {toStreams:n0}s",
                    mb exnBytes, fails, failStreams.Count, exnEvents, !timedOut, toStreams.Count)
                timedOut := 0; resultExnOther := 0; failStreams.Clear(); toStreams.Clear(); exnBytes <- 0L; exnEvents <- 0
            if badCats.Any then
                log.Warning("Affected cats {@badCats} Other {other:n0}r {@oStreams}",
                    badCats.StatsDescending |> Seq.truncate 50, !resultExnOther, oStreams |> Seq.truncate 100)
                badCats.Clear(); resultExnOther := 0; oStreams.Clear()
            Equinox.EventStore.Log.InternalMetrics.dump log

        override __.Handle message =
            let inline adds x (set : HashSet<_>) = set.Add x |> ignore
            let inline bads streamName (set : HashSet<_>) = badCats.Ingest(StreamName.categorize streamName); adds streamName set
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
                | ResultKind.TimedOut -> adds stream toStreams; incr timedOut
                | ResultKind.Other -> bads stream oStreams; incr resultExnOther
                __.HandleExn exn
        abstract member HandleOk : outcome : 'Outcome -> unit
        default __.HandleOk(_ : 'Outcome) : unit = ()
        abstract member HandleExn : exn : exn -> unit
        default __.HandleExn(_ : exn) : unit = ()

    type EventStoreSchedulingEngine =
        static member Create(log : ILogger, storeLog, connections : _ [], itemDispatcher, stats : Stats, dumpStreams, ?maxBatches, ?idleDelay, ?purgeInterval)
            : Scheduling.StreamSchedulingEngine<_, _, _> =
            let writerResultLog = log.ForContext<Writer.Result>()
            let mutable robin = 0

            let attemptWrite (item : Scheduling.DispatchItem<_>) = async {
                let index = Interlocked.Increment(&robin) % connections.Length
                let selectedConnection = connections.[index]
                let maxEvents, maxBytes = 65536, 4 * 1024 * 1024 - (*fudge*)4096
                let stats, span' = Buffering.StreamSpan.slice (maxEvents, maxBytes) item.span
                try let! res = Writer.write storeLog selectedConnection (FsCodec.StreamName.toString item.stream) span'
                    return Choice1Of2 (stats, res)
                with e -> return Choice2Of2 (stats, e) }

            let interpretWriteResultProgress (streams : Scheduling.StreamStates<_>) stream res =
                let applyResultToStreamState = function
                    | Choice1Of2 (_stats, Writer.Ok pos) ->                       streams.InternalUpdate stream pos null
                    | Choice1Of2 (_stats, Writer.Duplicate pos) ->                streams.InternalUpdate stream pos null
                    | Choice1Of2 (_stats, Writer.PartialDuplicate overage) ->     streams.InternalUpdate stream overage.index [|overage|]
                    | Choice1Of2 (_stats, Writer.PrefixMissing (overage, pos)) -> streams.InternalUpdate stream pos [|overage|]
                    | Choice2Of2 (_stats, _exn) -> streams.SetMalformed(stream, false)
                let _stream, ss = applyResultToStreamState res
                Writer.logTo writerResultLog (stream, res)
                ss.Write, res

            let dispatcher = Scheduling.MultiDispatcher<_, _, _>(itemDispatcher, attemptWrite, interpretWriteResultProgress, stats, dumpStreams)
            Scheduling.StreamSchedulingEngine(dispatcher, enableSlipstreaming=true, ?maxBatches=maxBatches, ?idleDelay=idleDelay, ?purgeInterval=purgeInterval)

type EventStoreSink =

    /// Starts a <c>StreamsProjectorPipeline</c> that ingests all submitted events into the supplied <c>connections</c>
    static member Start
        (   log : ILogger, storeLog, maxReadAhead, connections, maxConcurrentStreams,
            /// Default 5m
            ?statsInterval,
            /// Default 5m
            ?stateInterval, ?ingesterStatsInterval, ?maxSubmissionsPerPartition, ?pumpInterval,
            /// Tune the sleep time when there are no items to schedule or responses to process. Default 1ms.
            ?idleDelay,
            /// Frequency with which to jettison Write Position information for inactive streams in order to limit memory consumption
            /// NOTE: Can impair performance and/or increase costs of writes as it inhibits the ability of the ingester to discard redundant inputs
            ?purgeInterval)
        : Propulsion.ProjectorPipeline<_> =
        let statsInterval, stateInterval = defaultArg statsInterval (TimeSpan.FromMinutes 5.), defaultArg stateInterval (TimeSpan.FromMinutes 5.)
        let stats = Internal.Stats(log.ForContext<Internal.Stats>(), statsInterval, stateInterval)
        let dispatcher = Propulsion.Streams.Scheduling.ItemDispatcher<_>(maxConcurrentStreams)
        let dumpStats (s : Scheduling.StreamStates<_>) l = s.Dump(l, Propulsion.Streams.Buffering.StreamState.eventsSize)
        let streamScheduler = Internal.EventStoreSchedulingEngine.Create(log, storeLog, connections, dispatcher, stats, dumpStats, ?idleDelay=idleDelay, ?purgeInterval=purgeInterval)
        Propulsion.Streams.Projector.StreamsProjectorPipeline.Start(
            log, dispatcher.Pump(), streamScheduler.Pump, maxReadAhead, streamScheduler.Submit, statsInterval,
            ?ingesterStatsInterval=ingesterStatsInterval, ?maxSubmissionsPerPartition=maxSubmissionsPerPartition, ?pumpInterval=pumpInterval)
