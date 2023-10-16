namespace Propulsion.CosmosStore

open Microsoft.Azure.Cosmos
open Propulsion.Internal
open Serilog
open System

module internal CosmosStoreSource =

    let createTrancheObserver<'Items>
        (   log: ILogger,
            trancheIngester: Propulsion.Ingestion.Ingester<'Items>,
            mapContent: IReadOnlyCollection<_> -> 'Items): IChangeFeedObserver =

        let sw = Stopwatch.start () // we'll end up reporting the warmup/connect time on the first batch, but that's ok
        let ingest (ctx: ChangeFeedObserverContext) (checkpoint, docs: IReadOnlyCollection<_>, _ct) = task {
            sw.Stop() // Stop the clock after ChangeFeedProcessor hands off to us
            let readElapsed, age = sw.Elapsed, DateTime.UtcNow - ctx.timestamp
            let pt = Stopwatch.start ()
            let! struct (cur, max) = trancheIngester.Ingest { epoch = ctx.epoch; checkpoint = checkpoint; items = mapContent docs; onCompletion = ignore; isTail = false }
            let m = Log.Metric.Read {
                database = ctx.source.Database.Id; container = ctx.source.Id; group = ctx.group; rangeId = int ctx.rangeId
                token = ctx.epoch; latency = readElapsed; rc = ctx.requestCharge; age = age; docs = docs.Count
                ingestLatency = pt.Elapsed; ingestQueued = cur }
            (log |> Log.withMetric m).Information("Reader {partition} {token,9} age {age:dddd\.hh\:mm\:ss} {count,4} docs {requestCharge,6:f1}RU {l,5:f1}s Wait {pausedS:f3}s Ahead {cur}/{max}",
                                                  ctx.rangeId, ctx.epoch, age, docs.Count, ctx.requestCharge, readElapsed.TotalSeconds, pt.ElapsedSeconds, cur, max)
            sw.Restart() } // restart the clock as we handoff back to the ChangeFeedProcessor to fetch and pass that back to us

        { new IChangeFeedObserver with
#if COSMOSV3
            member _.Ingest(context, checkpoint, docs, ct) = ingest context (checkpoint, docs, ct)
#else
            member _.Ingest(context, checkpoint, docs, ct) = ingest context (checkpoint, docs, ct)
#endif
          interface IDisposable with
            member _.Dispose() = trancheIngester.Stop() }

type internal CosmosStoreObserver<'Items>
    (   log: ILogger,
        startIngester: ILogger * int -> Propulsion.Ingestion.Ingester<'Items>,
        mapContent: IReadOnlyCollection<_> -> 'Items) =

    // Its important we don't risk >1 instance https://andrewlock.net/making-getoradd-on-concurrentdictionary-thread-safe-using-lazy/
    // while it would be safe, there would be a risk of incurring the cost of multiple initialization loops
    let forTranche = System.Collections.Concurrent.ConcurrentDictionary<int, Lazy<IChangeFeedObserver>>()
    let dispose () = for x in forTranche.Values do x.Value.Dispose()
    let build trancheId = lazy CosmosStoreSource.createTrancheObserver (log, startIngester (log, trancheId), mapContent)
    let forTranche trancheId = forTranche.GetOrAdd(trancheId, build).Value
    let ingest context (checkpoint, docs, ct) =
        let trancheObserver = forTranche context.rangeId
        trancheObserver.Ingest(context, checkpoint, docs, ct)

    interface IChangeFeedObserver with
        member _.Ingest(context, checkpoint, docs, ct) = ingest context (checkpoint, docs, ct)
    interface IDisposable with
        member _.Dispose() = dispose ()

type CosmosStoreSource
    (   log: ILogger,
        monitored: Container, leases: Container, processorName, parseFeedDoc, sink: Propulsion.Sinks.Sink,
        [<O; D null>] ?maxItems,
        [<O; D null>] ?tailSleepInterval,
        [<O; D null>] ?startFromTail,
        [<O; D null>] ?lagReportFreq: TimeSpan,
        [<O; D null>] ?notifyError,
        [<O; D null>] ?customize) =
    let observer = new CosmosStoreObserver<_>(Log.Logger, sink.StartIngester, Seq.collect parseFeedDoc)
    member _.Flush() = (observer: IDisposable).Dispose()
    member _.Start() =
        let stats = Log.Stats(log, monitored.Database.Id, monitored.Id, processorName, ?lagReportFreq = lagReportFreq)
        ChangeFeedProcessor.Start(
            log, monitored, leases, processorName, observer, stats, ?notifyError = notifyError, ?customize = customize,
            ?maxItems = maxItems, ?feedPollDelay = tailSleepInterval,
            startFromTail = defaultArg startFromTail false,
            leaseAcquireInterval = TimeSpan.seconds 5., leaseRenewInterval = TimeSpan.seconds 5., leaseTtl = TimeSpan.seconds 10.)
