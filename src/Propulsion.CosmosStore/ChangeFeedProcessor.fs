namespace Propulsion.CosmosStore

open Propulsion.Feed.Core
open Propulsion.Internal
open System

type [<AbstractClass; Sealed>] ChangeFeedProcessor private () =
    static member internal Start
        (   monitored: Microsoft.Azure.Cosmos.Container, leases: Microsoft.Azure.Cosmos.Container, processorName: string, leaseOwnerId,
            log, stats: Stats, statsInterval, observers: Observers<_>,
            startFromTail, feedPollInterval, leaseAcquireInterval, leaseRenewInterval, leaseTtl,
            ?maxItems, ?notifyError, ?customize, ?lagEstimationInterval) =
        lagEstimationInterval |> Option.iter stats.ReportEstimationInterval
        observers.LogStart(leaseAcquireInterval, leaseTtl, leaseRenewInterval, feedPollInterval, startFromTail, ?maxItems = maxItems)
        let processorName_ = processorName + ":"
        let (|TokenRangeId|) (leaseToken: string) = leaseToken.Trim[|'"'|] |> int
        let processor =
            let handler (context: Microsoft.Azure.Cosmos.ChangeFeedProcessorContext) (changes: ChangeFeedItems) (checkpointAsync: Func<Task>) ct: Task = task {
                let (TokenRangeId rangeId) =  context.LeaseToken
                let log: exn -> unit = function
                    | :? OperationCanceledException -> () // Shutdown via .Stop triggers this
                    | e -> observers.LogHandlerExn(rangeId, e)
                try let ctx = { group = processorName
                                epoch = context.Headers.ContinuationToken.Trim[|'"'|] |> int64
                                timestamp = changes |> Seq.last |> ChangeFeedItem.timestamp
                                rangeId = rangeId
                                requestCharge = context.Headers.RequestCharge }
                    return! observers.Ingest(ctx, changes, checkpointAsync, ct)
                with Exception.Log log () -> () }
            let notifyError =
                let log = match notifyError with Some f -> f | None -> Action<_, _>(fun i ex -> observers.LogReaderExn(i, ex))
                fun (TokenRangeId rangeId) ex -> log.Invoke(rangeId, ex); Task.CompletedTask
            let logStateChange acquired (TokenRangeId rangeId) = observers.RecordStateChange(rangeId, acquired); Task.CompletedTask
            monitored
                .GetChangeFeedProcessorBuilderWithManualCheckpoint(processorName_, handler)
                .WithLeaseContainer(leases)
                .WithPollInterval(feedPollInterval)
                .WithLeaseConfiguration(acquireInterval = leaseAcquireInterval, expirationInterval = leaseTtl, renewInterval = leaseRenewInterval)
                .WithInstanceName(leaseOwnerId)
                .WithLeaseAcquireNotification(logStateChange true)
                .WithLeaseReleaseNotification(logStateChange false)
                .WithErrorNotification(notifyError)
                |> fun b -> if startFromTail then b else let minTime = DateTime.MinValue in b.WithStartTime(minTime.ToUniversalTime()) // fka StartFromBeginning
                |> fun b -> match maxItems with Some mi -> b.WithMaxItems(mi) | None -> b
                |> fun b -> match customize with Some c -> c b | None -> b
                |> fun b -> b.Build()
        let estimator = monitored.GetChangeFeedEstimator(processorName_, leases)
        let fetchEstimatorStates (map: Microsoft.Azure.Cosmos.ChangeFeedProcessorState -> 'u) ct: Task<'u[]> = task {
            use query = estimator.GetCurrentStateIterator()
            let result = ResizeArray()
            while query.HasMoreResults do
                let! res = query.ReadNextAsync(ct)
                for x in res do result.Add(map x)
            return result.ToArray() }
        let runEstimation ct = task {
            let! leasesStates = fetchEstimatorStates (fun s -> struct ((|TokenRangeId|) s.LeaseToken, s.EstimatedLag)) ct
            Array.sortInPlaceBy ValueTuple.fst leasesStates
            observers.RecordEstimation leasesStates
            stats.ReportEstimation leasesStates }
        let emitStats () =
            let all = (observers : ISourcePositions<_>).Current()
            all |> Array.sortInPlaceBy (fun x -> x.Key)
            for kv in all do
                kv.Value.Dump(log, processorName, int (Propulsion.Feed.TrancheId.toString kv.Key), kv.Value.CurrentCapacity())
        let estimateAndLog ct = task {
            // Dump will cope with absence of update (unless standalone estimation has already updated anyway)
            try do! runEstimation ct with _ -> ()
            emitStats () }
        let startup ct = task {
            match lagEstimationInterval with None -> () | Some interval -> Task.start (fun () -> Task.periodically runEstimation interval ct)
            Task.start (fun () -> Task.periodically estimateAndLog statsInterval ct)
            return! processor.StartAsync() }
        let shutdown () = task {
            try do! processor.StopAsync() with _ -> ()
            (observers : IDisposable).Dispose() // Stop the ingesters
            do! estimateAndLog CancellationToken.None }
        Propulsion.PipelineFactory.PrepareSource2(log, startup, shutdown)
