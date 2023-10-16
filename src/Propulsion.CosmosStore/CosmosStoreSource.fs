namespace Propulsion.CosmosStore

open Microsoft.Azure.Cosmos
open Propulsion.Internal
open Serilog
open System

type internal TrancheChangeFeedObserver<'Items>(stats: Log.Stats, trancheIngester: Propulsion.Ingestion.Ingester<'Items>, mapContent: IReadOnlyCollection<_> -> 'Items) =

    let sw = Stopwatch.start () // we'll end up reporting the warmup/connect time on the first batch, but that's ok
    let lastWait = System.Diagnostics.Stopwatch()

    interface IChangeFeedObserver with
        member _.Ingest(ctx: ChangeFeedObserverContext, checkpoint, docs: IReadOnlyCollection<_>, _ct) = task {
            sw.Stop() // Stop the clock after ChangeFeedProcessor hands off to us
            let batch: Propulsion.Ingestion.Batch<_> = { epoch = ctx.epoch; checkpoint = checkpoint; items = mapContent docs; onCompletion = ignore; isTail = false }
            let struct (cur, max) = trancheIngester.Ingest batch
            stats.ReportRead(int ctx.rangeId, lastWait.Elapsed, ctx.epoch, ctx.requestCharge, ctx.timestamp, sw.Elapsed, docs.Count, cur, max)
            lastWait.Restart()
            let! struct (cur, max) = trancheIngester.AwaitCapacity()
            lastWait.Stop()
            stats.ReportWait(int ctx.rangeId, lastWait.Elapsed, cur, max)
            sw.Restart() } // restart the clock as we handoff back to the ChangeFeedProcessor to fetch and pass that back to us
    interface IDisposable with
        member _.Dispose() =
            trancheIngester.Stop()

type internal ChangeFeedObservers<'Items>(stats, startIngester: ILogger * int -> Propulsion.Ingestion.Ingester<'Items>, mapContent: IReadOnlyCollection<_> -> 'Items) =

    // Its important we don't risk >1 instance https://andrewlock.net/making-getoradd-on-concurrentdictionary-thread-safe-using-lazy/
    // while it would be safe, there would be a risk of incurring the cost of multiple initialization loops
    let forTranche = System.Collections.Concurrent.ConcurrentDictionary<int, Lazy<IChangeFeedObserver>>()
    let build trancheId = lazy (new TrancheChangeFeedObserver<'Items>(stats, startIngester (stats.Log, trancheId), mapContent) :> IChangeFeedObserver)
    let getOrAddForTranche trancheId = forTranche.GetOrAdd(trancheId, build).Value

    interface IChangeFeedObserver with
        member _.Ingest(context, checkpoint, docs, ct) =
            let trancheObserver = getOrAddForTranche context.rangeId
            trancheObserver.Ingest(context, checkpoint, docs, ct)
    interface IDisposable with
        member _.Dispose() =
            for x in forTranche.Values do
                x.Value.Dispose()

type CosmosStoreSource
    (   log: ILogger,
        monitored: Container, leases: Container, processorName, parseFeedDoc, sink: Propulsion.Sinks.Sink,
        [<O; D null>] ?maxItems,
        [<O; D null>] ?tailSleepInterval,
        [<O; D null>] ?startFromTail,
        [<O; D null>] ?lagReportFreq: TimeSpan,
        [<O; D null>] ?notifyError,
        [<O; D null>] ?customize) =
    let stats = Log.Stats(log, monitored.Database.Id, monitored.Id, processorName, ?lagReportFreq = lagReportFreq)
    let observer = new ChangeFeedObservers<_>(stats, sink.StartIngester, Seq.collect parseFeedDoc)
    member _.Flush() = (observer: IDisposable).Dispose()
    member _.Start() =
        ChangeFeedProcessor.Start(
            monitored, leases, processorName, observer, stats, ?notifyError = notifyError, ?customize = customize,
            ?maxItems = maxItems, ?feedPollDelay = tailSleepInterval,
            startFromTail = defaultArg startFromTail false,
            leaseAcquireInterval = TimeSpan.seconds 5., leaseRenewInterval = TimeSpan.seconds 5., leaseTtl = TimeSpan.seconds 10.)
