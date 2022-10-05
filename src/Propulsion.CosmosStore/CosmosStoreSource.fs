#if COSMOSV2
namespace Propulsion.Cosmos

open Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessing
open Propulsion.Cosmos.Infrastructure // AwaitKeyboardInterrupt
#else
namespace Propulsion.CosmosStore

open Microsoft.Azure.Cosmos
#endif

open Propulsion.Infrastructure // AwaitTaskCorrect
open Propulsion.Internal
open Serilog
open System
open System.Collections.Generic

module Log =

    type ReadMetric =
        {   database : string ; container : string; group : string; rangeId : int
            token : int64; latency : TimeSpan; rc : float; age : TimeSpan; docs : int
            ingestLatency : TimeSpan; ingestQueued : int }
    type LagMetric =
        {   database : string ; container : string; group : string
            rangeLags : (int * int64) [] }
    [<RequireQualifiedAccess; NoEquality; NoComparison>]
    type Metric =
        | Read of ReadMetric
        | Lag of LagMetric

    /// Attach a property to the captured event record to hold the metric information
    // Sidestep Log.ForContext converting to a string; see https://github.com/serilog/serilog/issues/1124
#if COSMOSV2
    let [<Literal>] PropertyTag = "propulsionCosmosEventV2"
#else
    let [<Literal>] PropertyTag = "propulsionCosmosEvent"
#endif
    let internal withMetric (value : Metric) = Log.withScalarProperty PropertyTag value
    let [<return: Struct>] (|MetricEvent|_|) (logEvent : Serilog.Events.LogEvent) : Metric voption =
        let mutable p = Unchecked.defaultof<_>
        logEvent.Properties.TryGetValue(PropertyTag, &p) |> ignore
        match p with Log.ScalarValue (:? Metric as e) -> ValueSome e | _ -> ValueNone

#if COSMOSV2
type CosmosSource =

    static member CreateObserver<'Items>
        (   log : ILogger, context : ChangeFeedObserverContext,
            startIngester : ILogger * int -> Propulsion.Ingestion.Ingester<'Items>,
            mapContent : IReadOnlyList<Microsoft.Azure.Documents.Document> -> 'Items) =
        let mutable rangeIngester = Unchecked.defaultof<_>
        let init rangeLog partitionId = rangeIngester <- startIngester (rangeLog, partitionId)
        let dispose () = rangeIngester.Stop()
        let sw = Stopwatch.start () // we'll end up reporting the warmup/connect time on the first batch, but that's ok
        let ingest (log : ILogger) (ctx : IChangeFeedObserverContext) (docs : IReadOnlyList<Microsoft.Azure.Documents.Document>) = task {
            sw.Stop() // Stop the clock after ChangeFeedProcessor hands off to us
            let epoch, age = ctx.FeedResponse.ResponseContinuation.Trim[|'"'|] |> int64, DateTime.UtcNow - docs[docs.Count-1].Timestamp
            let pt = Stopwatch.start()
            let! struct (cur, max) = rangeIngester.Ingest { epoch = epoch; checkpoint = ctx.Checkpoint(); items = mapContent docs; onCompletion = ignore; isTail = false }
            let readS, postS, rc = sw.ElapsedSeconds, pt.ElapsedSeconds, ctx.FeedResponse.RequestCharge
            let m = Log.Metric.Read {
                database = context.source.database; container = context.source.container; group = context.leasePrefix; rangeId = int ctx.PartitionKeyRangeId
                token = epoch; latency = sw.Elapsed; rc = rc; age = age; docs = docs.Count
                ingestLatency = pt.Elapsed; ingestQueued = cur }
            (log |> Log.withMetric m).Information("Reader {partitionId} {token,9} age {age:dd\.hh\:mm\:ss} {count,4} docs {requestCharge,6:f1}RU {l,5:f1}s Wait {pausedS:f3}s Ahead {cur}/{max}",
                                                  ctx.PartitionKeyRangeId, epoch, age, docs.Count, rc, readS, postS, cur, max)
            sw.Restart() // restart the clock as we handoff back to the ChangeFeedProcessor
        }
        ChangeFeedObserver.Create(log, ingest, init=init, dispose=dispose)

#else
type CosmosStoreSource =

    static member private CreateTrancheObserver<'Items>
        (   log : ILogger,
            trancheIngester : Propulsion.Ingestion.Ingester<'Items>,
            mapContent : IReadOnlyCollection<_> -> 'Items) : IChangeFeedObserver =

        let sw = Stopwatch.start () // we'll end up reporting the warmup/connect time on the first batch, but that's ok
        let ingest (ctx : ChangeFeedObserverContext) checkpoint (docs : IReadOnlyCollection<_>) = task {
            sw.Stop() // Stop the clock after ChangeFeedProcessor hands off to us
            let readElapsed, age = sw.Elapsed, DateTime.UtcNow - ctx.timestamp
            let pt = Stopwatch.start ()
            let! struct (cur, max) = trancheIngester.Ingest { epoch = ctx.epoch; checkpoint = checkpoint; items = mapContent docs; onCompletion = ignore; isTail = false }
            let m = Log.Metric.Read {
                database = ctx.source.Database.Id; container = ctx.source.Id; group = ctx.group; rangeId = int ctx.rangeId
                token = ctx.epoch; latency = readElapsed; rc = ctx.requestCharge; age = age; docs = docs.Count
                ingestLatency = pt.Elapsed; ingestQueued = cur }
            (log |> Log.withMetric m).Information("Reader {partitionId} {token,9} age {age:dd\.hh\:mm\:ss} {count,4} docs {requestCharge,6:f1}RU {l,5:f1}s Wait {pausedS:f3}s Ahead {cur}/{max}",
                                                  ctx.rangeId, ctx.epoch, age, docs.Count, ctx.requestCharge, readElapsed.TotalSeconds, pt.ElapsedSeconds, cur, max)
            sw.Restart() // restart the clock as we handoff back to the ChangeFeedProcessor
        }

        { new IChangeFeedObserver with
#if COSMOSV3
            member _.Ingest(context, checkpoint, docs) = ingest context checkpoint docs |> Async.AwaitTaskCorrect
#else
            member _.Ingest(context, checkpoint, docs) = ingest context checkpoint docs
#endif
          interface IDisposable with
            member _.Dispose() = trancheIngester.Stop() }

    static member CreateObserver<'Items>
        (   log : ILogger,
            startIngester : ILogger * int -> Propulsion.Ingestion.Ingester<'Items>,
            mapContent : IReadOnlyCollection<_> -> 'Items) : IChangeFeedObserver =

        // Its important we don't risk >1 instance https://andrewlock.net/making-getoradd-on-concurrentdictionary-thread-safe-using-lazy/
        // while it would be safe, there would be a risk of incurring the cost of multiple initialization loops
        let forTranche = System.Collections.Concurrent.ConcurrentDictionary<int, Lazy<IChangeFeedObserver>>()
        let dispose () = for x in forTranche.Values do x.Value.Dispose()
        let build trancheId = lazy CosmosStoreSource.CreateTrancheObserver(log, startIngester (log, trancheId), mapContent)
        let forTranche trancheId = forTranche.GetOrAdd(trancheId, build).Value
        let ingest context checkpoint docs =
            let trancheObserver = forTranche context.rangeId
            trancheObserver.Ingest(context, checkpoint, docs)

        { new IChangeFeedObserver with
            member _.Ingest(context, checkpoint, docs) = ingest context checkpoint docs
          interface IDisposable with
            member _.Dispose() = dispose () }
#endif

#if COSMOSV2
    static member Run
        (   log : ILogger,
            client, source, aux, leaseId, startFromTail, createObserver,
            ?maxDocuments, ?lagReportFreq : TimeSpan, ?auxClient) = async {
        let databaseId, containerId, processorName = source.database, source.container, leaseId
#else
    static member Start
        (   log : ILogger,
            monitored : Container, leases : Container, processorName, observer,
            ?maxItems, ?tailSleepInterval, ?startFromTail, ?lagReportFreq : TimeSpan, ?notifyError, ?customize) =
        let databaseId, containerId = monitored.Database.Id, monitored.Id
#endif
        let logLag (interval : TimeSpan) (remainingWork : (int*int64) list) = async {
            let mutable synced, lagged, count, total = ResizeArray(), ResizeArray(), 0, 0L
            for partitionId, gap as partitionAndGap in remainingWork do
                total <- total + gap
                count <- count + 1
                if gap = 0L then synced.Add partitionId else lagged.Add partitionAndGap
            let m = Log.Metric.Lag { database = databaseId; container = containerId; group = processorName; rangeLags = remainingWork |> Array.ofList }
            (log |> Log.withMetric m).Information("ChangeFeed {processorName} Lag Partitions {partitions} Gap {gapDocs:n0} docs {@laggingPartitions} Synced {@syncedPartitions}",
                                                  processorName, count, total, lagged, synced)
            return! Async.Sleep(TimeSpan.toMs interval) }
        let maybeLogLag = lagReportFreq |> Option.map logLag
#if COSMOSV2
        let! _feedEventHost =
            ChangeFeedProcessor.Start
              ( log, client, source, aux, ?auxClient=auxClient, leasePrefix=leaseId, createObserver=createObserver,
                startFromTail=startFromTail, ?reportLagAndAwaitNextEstimation=maybeLogLag, ?maxDocuments=maxDocuments,
                leaseAcquireInterval=TimeSpan.FromSeconds 5., leaseRenewInterval=TimeSpan.FromSeconds 5., leaseTtl=TimeSpan.FromSeconds 10.)
        lagReportFreq |> Option.iter (fun s -> log.Information("ChangeFeed {processorName} Lag stats interval {lagReportIntervalS:n0}s", processorName, s.TotalSeconds))
        do! Async.AwaitKeyboardInterrupt() } // exiting will Cancel the child tasks, i.e. the _feedEventHost
#else
        let source =
            ChangeFeedProcessor.Start
              ( log, monitored, leases, processorName, observer, ?notifyError=notifyError, ?customize=customize,
                ?maxItems = maxItems, ?feedPollDelay = tailSleepInterval, ?reportLagAndAwaitNextEstimation=maybeLogLag,
                startFromTail = defaultArg startFromTail false,
                leaseAcquireInterval=TimeSpan.FromSeconds 5., leaseRenewInterval=TimeSpan.FromSeconds 5., leaseTtl=TimeSpan.FromSeconds 10.)
        lagReportFreq |> Option.iter (fun s -> log.Information("ChangeFeed {processorName} Lag stats interval {lagReportIntervalS:n0}s", processorName, s.TotalSeconds))
        source
#endif
