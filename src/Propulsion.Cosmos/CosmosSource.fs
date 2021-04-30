#if COSMOSSTORE
namespace Propulsion.CosmosStore
#else
namespace Propulsion.Cosmos
#endif

open Equinox.Core // Stopwatch.Time
open Microsoft.Azure.Documents
open Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessing
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
    let [<Literal>] PropertyTag = "propulsionCosmosEvent"
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

#if COSMOSSTORE
type CosmosStoreSource =
#else
type CosmosSource =
#endif

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
            (log |> Log.metric m).Information("Read {token,9}/{partitionKeyRangeId} age {age:dd\.hh\:mm\:ss} {count,4} docs {requestCharge,6:f1}RU {l,5:f1}s Ingest {pt:f3}s {cur}/{max}",
                epoch, ctx.PartitionKeyRangeId, age, docs.Count, rc, readS, postS, cur, max)
            sw.Restart() // restart the clock as we handoff back to the ChangeFeedProcessor
        }
        ChangeFeedObserver.Create(log, ingest, init=init, dispose=dispose)

    static member Run
        (   log : ILogger,
            client, source,
            aux, leaseId, startFromTail, createObserver,
            ?maxDocuments, ?lagReportFreq : TimeSpan, ?auxClient) = async {
        let logLag (interval : TimeSpan) (remainingWork : (int*int64) list) = async {
            let synced, lagged, count, total = ResizeArray(), ResizeArray(), ref 0, ref 0L
            for partitionId, lag as value in remainingWork do
                total := !total + lag
                incr count
                if lag = 0L then synced.Add partitionId else lagged.Add value
            let m = Log.Metric.Lag { database = source.database; container = source.container; group = leaseId; rangeLags = remainingWork |> Array.ofList }
            (log |> Log.metric m).Information("ChangeFeed Backlog {backlog:n0} / {count} Lagging {@lagging} Synced {@inSync}",
                !total, !count, lagged, synced)
            return! Async.Sleep interval }
        let maybeLogLag = lagReportFreq |> Option.map logLag
        let! _feedEventHost =
            ChangeFeedProcessor.Start
              ( log, client, source, aux, ?auxClient=auxClient, leasePrefix=leaseId, startFromTail=startFromTail,
                createObserver=createObserver, ?reportLagAndAwaitNextEstimation=maybeLogLag, ?maxDocuments=maxDocuments,
                leaseAcquireInterval=TimeSpan.FromSeconds 5., leaseRenewInterval=TimeSpan.FromSeconds 5., leaseTtl=TimeSpan.FromSeconds 10.)
        do! Async.AwaitKeyboardInterrupt() } // exiting will Cancel the child tasks, i.e. the _feedEventHost
