namespace Propulsion.Cosmos

open Equinox.Core // Stopwatch.Time
open Microsoft.Azure.Documents
open Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessing
open Serilog
open System
open System.Collections.Generic
open System.Diagnostics

type CosmosSource =
    static member CreateObserver<'Items,'Batch>
        (   log : ILogger,
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
            let readS, postS = float sw.ElapsedMilliseconds / 1000., let e = pt.Elapsed in e.TotalSeconds
            log.Information("Read {token,9} age {age:dd\.hh\:mm\:ss} {count,4} docs {requestCharge,6:f1}RU {l,5:f1}s Ingest {pt:f3}s {cur}/{max}",
                epoch, age, docs.Count, ctx.FeedResponse.RequestCharge, readS, postS, cur, max)
            sw.Restart() // restart the clock as we handoff back to the ChangeFeedProcessor
        }
        ChangeFeedObserver.Create(log, ingest, init=init, dispose=dispose)

    static member Run
        (   log : ILogger,
            discovery, connectionPolicy, source,
            aux, leaseId, startFromTail, createObserver,
            ?maxDocuments, ?lagReportFreq : TimeSpan, ?auxDiscovery) = async {
        let logLag (interval : TimeSpan) (remainingWork : (int*int64) list) = async {
            let synced, lagged, count, total = ResizeArray(), ResizeArray(), ref 0, ref 0L
            for partitionId, lag as value in remainingWork do
                total := !total + lag
                incr count
                if lag = 0L then synced.Add partitionId else lagged.Add value
            log.Information("Backlog {backlog:n0} / {count} Lagging {lagging} Synced {in-sync}",
                !total, !count, lagged, synced)
            return! Async.Sleep interval }
        let maybeLogLag = lagReportFreq |> Option.map logLag
        let! _feedEventHost =
            ChangeFeedProcessor.Start
              ( log, discovery, connectionPolicy, source, aux, ?auxDiscovery = auxDiscovery, leasePrefix = leaseId, startFromTail = startFromTail,
                createObserver = createObserver, ?reportLagAndAwaitNextEstimation = maybeLogLag, ?maxDocuments = maxDocuments,
                leaseAcquireInterval = TimeSpan.FromSeconds 5., leaseRenewInterval = TimeSpan.FromSeconds 5., leaseTtl = TimeSpan.FromSeconds 10.)
        do! Async.AwaitKeyboardInterrupt() } // exiting will Cancel the child tasks, i.e. the _feedEventHost