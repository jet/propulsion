namespace Propulsion.DynamoStore

type DynamoStoreIndexer(log : Serilog.ILogger, context, cache, epochBytesCutoff, ?maxItemsPerEpoch, ?maxVersion, ?storeLog) =
    let maxVersion = defaultArg maxVersion 5_000
    let maxStreams = defaultArg maxItemsPerEpoch 100_000
    do if maxStreams > AppendsEpoch.MaxItemsPerEpoch then invalidArg (nameof maxStreams) "Cannot exceed AppendsEpoch.MaxItemsPerEpoch"
    let storeLog = defaultArg storeLog log
    let log = log.ForContext<DynamoStoreIndexer>()

    let ingester =
        let epochs = AppendsEpoch.Config.create storeLog (epochBytesCutoff, maxVersion, maxStreams) (context, cache)
        let index = AppendsIndex.Config.create storeLog (context, cache)
        let createIngester trancheId =
            let log = log.ForContext("trancheId", trancheId)
            let readIngestionEpoch () = index.ReadIngestionEpochId trancheId
            let markIngestionEpoch epochId = index.MarkIngestionEpochId(trancheId, epochId)
            let ingest (eid, items) = epochs.Ingest(trancheId, eid, items)
            ExactlyOnceIngester.create log (readIngestionEpoch, markIngestionEpoch) (ingest, Array.toSeq)

        // technically this does not have to be ConcurrentDictionary atm
        let ingesterForTranche = System.Collections.Concurrent.ConcurrentDictionary<_, ExactlyOnceIngester.Service<_, _, _, _>>()
        fun trancheId -> ingesterForTranche.GetOrAdd(trancheId, createIngester)

    /// Ingests the spans into the epochs chain for this tranche
    /// NOTE if this is going to be used in an environment where there can be concurrent calls within a single process, an AsyncBatchingGate should be applied
    ///      in this instance, the nature of Lambda is such that this is not the case
    /// NOTE regardless of concurrency within a process, it's critical to avoid having >1 writer hitting the same trancheId as this will result on continual conflicts
    member _.IngestWithoutConcurrency(trancheId, spans) = async {
        let ingester = ingester trancheId
        let! originTranche = ingester.ActiveIngestionEpochId()
        return! ingester.IngestMany(originTranche, spans) |> Async.Ignore }
