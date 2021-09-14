#if COSMOSV2
namespace Propulsion.Cosmos

open Microsoft.Azure.Documents
open Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessing
#else
namespace Propulsion.CosmosStore

open Microsoft.Azure.Cosmos
#endif

open Equinox.Core // Stopwatch.Time
open Serilog
open System
open System.Collections.Generic
open System.Diagnostics

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
    let internal metric (value : Metric) (log : ILogger) =
        let enrich (e : Serilog.Events.LogEvent) =
            e.AddPropertyIfAbsent(Serilog.Events.LogEventProperty(PropertyTag, Serilog.Events.ScalarValue(value)))
        log.ForContext({ new Serilog.Core.ILogEventEnricher with member _.Enrich(evt,_) = enrich evt })
    let internal (|SerilogScalar|_|) : Serilog.Events.LogEventPropertyValue -> obj option = function
        | :? Serilog.Events.ScalarValue as x -> Some x.Value
        | _ -> None
    let (|MetricEvent|_|) (logEvent : Serilog.Events.LogEvent) : Metric option =
        match logEvent.Properties.TryGetValue PropertyTag with
        | true, SerilogScalar (:? Metric as e) -> Some e
        | _ -> None

#if COSMOSV2
type CosmosSource =

    static member CreateObserver<'Items,'Batch>
        (   log : ILogger, context : ChangeFeedObserverContext,
            createIngester : ILogger * int -> Propulsion.Ingestion.Ingester<'Items,'Batch>,
            mapContent : IReadOnlyList<Microsoft.Azure.Documents.Document> -> 'Items) =
        let mutable rangeIngester = Unchecked.defaultof<_>
        let init rangeLog partitionId = rangeIngester <- createIngester (rangeLog, partitionId)
        let dispose () = rangeIngester.Stop()
        let sw = Stopwatch.StartNew() // we'll end up reporting the warmup/connect time on the first batch, but that's ok
        let ingest (log : ILogger) (ctx : IChangeFeedObserverContext) (docs : IReadOnlyList<Microsoft.Azure.Documents.Document>) = async {
            sw.Stop() // Stop the clock after ChangeFeedProcessor hands off to us
            let epoch, age = ctx.FeedResponse.ResponseContinuation.Trim[|'"'|] |> int64, DateTime.UtcNow - docs.[docs.Count-1].Timestamp
            let! pt, (cur,max) = rangeIngester.Submit(epoch, ctx.Checkpoint(), mapContent docs) |> Stopwatch.Time
            let readS, postS, rc = float sw.ElapsedMilliseconds / 1000., (let e = pt.Elapsed in e.TotalSeconds), ctx.FeedResponse.RequestCharge
            let m = Log.Metric.Read {
                database = context.source.database; container = context.source.container; group = context.leasePrefix; rangeId = int ctx.PartitionKeyRangeId
                token = epoch; latency = sw.Elapsed; rc = rc; age = age; docs = docs.Count
                ingestLatency = pt.Elapsed; ingestQueued = cur }
            (log |> Log.metric m).Information("Reader {partitionId} {token,9} age {age:dd\.hh\:mm\:ss} {count,4} docs {requestCharge,6:f1}RU {l,5:f1}s Wait {pausedS:f3}s Ahead {cur}/{max}",
                ctx.PartitionKeyRangeId, epoch, age, docs.Count, rc, readS, postS, cur, max)
            sw.Restart() // restart the clock as we handoff back to the ChangeFeedProcessor
        }
        ChangeFeedObserver.Create(log, ingest, init=init, dispose=dispose)

#else
type CosmosStoreSource =

    static member private CreateTrancheObserver<'Items,'Batch>
        (   log : ILogger,
            trancheIngester : Propulsion.Ingestion.Ingester<'Items,'Batch>,
            mapContent : IReadOnlyCollection<Newtonsoft.Json.Linq.JObject> -> 'Items) : IChangeFeedObserver =

        let sw = Stopwatch.StartNew() // we'll end up reporting the warmup/connect time on the first batch, but that's ok
        let ingest (ctx : ChangeFeedObserverContext) checkpoint (docs : IReadOnlyCollection<Newtonsoft.Json.Linq.JObject>) = async {
            sw.Stop() // Stop the clock after ChangeFeedProcessor hands off to us
            let epoch, age = ctx.epoch, DateTime.UtcNow - ctx.timestamp
            let! pt, (cur,max) = trancheIngester.Submit(epoch, checkpoint, mapContent docs) |> Stopwatch.Time
            let readS, postS, rc = float sw.ElapsedMilliseconds / 1000., (let e = pt.Elapsed in e.TotalSeconds), ctx.requestCharge
            let m = Log.Metric.Read {
                database = ctx.source.Database.Id; container = ctx.source.Id; group = ctx.group; rangeId = int ctx.rangeId
                token = epoch; latency = sw.Elapsed; rc = rc; age = age; docs = docs.Count
                ingestLatency = pt.Elapsed; ingestQueued = cur }
            (log |> Log.metric m).Information("Reader {partitionId} {token,9} age {age:dd\.hh\:mm\:ss} {count,4} docs {requestCharge,6:f1}RU {l,5:f1}s Wait {pausedS:f3}s Ahead {cur}/{max}",
                ctx.rangeId, epoch, age, docs.Count, rc, readS, postS, cur, max)
            sw.Restart() // restart the clock as we handoff back to the ChangeFeedProcessor
        }

        { new IChangeFeedObserver with
            member _.Ingest(context, checkpoint, docs) = ingest context checkpoint docs
          interface IDisposable with
            member _.Dispose() = trancheIngester.Stop() }

    static member CreateObserver<'Items,'Batch>
        (   log : ILogger,
            startIngester : ILogger * int -> Propulsion.Ingestion.Ingester<'Items,'Batch>,
            mapContent : IReadOnlyCollection<Newtonsoft.Json.Linq.JObject> -> 'Items) : IChangeFeedObserver =

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
            member _.Dispose() = dispose() }
#endif

    static member Run
        (   log : ILogger,
#if COSMOSV2
            client, source, aux, leaseId, startFromTail, createObserver,
            ?maxDocuments, ?lagReportFreq : TimeSpan, ?auxClient) = async {
        let databaseId, containerId, processorName = source.database, source.container, leaseId
#else
            monitored : Container, leases : Container, processorName, observer,
            startFromTail, ?maxItems, ?lagReportFreq : TimeSpan) = async {
        let databaseId, containerId = monitored.Database.Id, monitored.Id
#endif
        lagReportFreq |> Option.iter (fun s -> log.Information("ChangeFeed Lag stats interval {lagReportIntervalS:n0}s", s.TotalSeconds))
        let logLag (interval : TimeSpan) (remainingWork : (int*int64) list) = async {
            let synced, lagged, count, total = ResizeArray(), ResizeArray(), ref 0, ref 0L
            for partitionId, gap as partitionAndGap in remainingWork do
                total := !total + gap
                incr count
                if gap = 0L then synced.Add partitionId else lagged.Add partitionAndGap
            let m = Log.Metric.Lag { database = databaseId; container = containerId; group = processorName; rangeLags = remainingWork |> Array.ofList }
            (log |> Log.metric m).Information("ChangeFeed Lag Partitions {partitions} Gap {gapDocs:n0} docs {@laggingPartitions} Synced {@syncedPartitions}",
                !count, !total, lagged, synced)
            return! Async.Sleep interval }
        let maybeLogLag = lagReportFreq |> Option.map logLag
        let! _feedEventHost =
            ChangeFeedProcessor.Start
#if COSMOSV2
              ( log, client, source, aux, ?auxClient=auxClient, leasePrefix=leaseId, createObserver=createObserver,
                startFromTail=startFromTail, ?reportLagAndAwaitNextEstimation=maybeLogLag, ?maxDocuments=maxDocuments,
#else
              ( log, monitored, leases, processorName, observer,
                startFromTail=startFromTail, ?reportLagAndAwaitNextEstimation=maybeLogLag, ?maxItems=maxItems,
#endif
                leaseAcquireInterval=TimeSpan.FromSeconds 5., leaseRenewInterval=TimeSpan.FromSeconds 5., leaseTtl=TimeSpan.FromSeconds 10.)
        do! Async.AwaitKeyboardInterrupt() } // exiting will Cancel the child tasks, i.e. the _feedEventHost

