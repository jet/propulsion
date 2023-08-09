namespace Propulsion.DynamoStore

type DynamoStoreIndexer(log : Serilog.ILogger, context, cache, epochBytesCutoff, ?maxItemsPerEpoch, ?maxVersion, ?storeLog, ?onlyWarnOnGap) =
    let maxVersion = defaultArg maxVersion 5_000
    let maxStreams = defaultArg maxItemsPerEpoch 100_000
    do if maxStreams > AppendsEpoch.MaxItemsPerEpoch then invalidArg (nameof maxStreams) "Cannot exceed AppendsEpoch.MaxItemsPerEpoch"
    let storeLog = defaultArg storeLog log
    let log = log.ForContext<DynamoStoreIndexer>()
    let onlyWarnOnGap = defaultArg onlyWarnOnGap false

    let ingester =
        let epochs = AppendsEpoch.Factory.create storeLog (epochBytesCutoff, maxVersion, maxStreams, onlyWarnOnGap) (context, cache)
        let index = AppendsIndex.Factory.create storeLog (context, cache)
        let createIngester partitionId =
            let log = log.ForContext("partition", partitionId)
            let readIngestionEpoch () = index.ReadIngestionEpochId partitionId
            let markIngestionEpoch epochId = index.MarkIngestionEpochId(partitionId, epochId)
            let ingest (eid, items) = epochs.Ingest(partitionId, eid, items)
            ExactlyOnceIngester.create log (readIngestionEpoch, markIngestionEpoch) (ingest, Array.toSeq)

        // technically this does not have to be ConcurrentDictionary atm
        let ingesterForPartition = System.Collections.Concurrent.ConcurrentDictionary<_, ExactlyOnceIngester.Service<_, _, _, _>>()
        fun partitionId -> ingesterForPartition.GetOrAdd(partitionId, createIngester)

    /// Ingests the spans into the epochs chain for this partition
    /// NOTE if this is going to be used in an environment where there can be concurrent calls within a single process, an Equinox.Core.Batching.Batcher should be applied
    ///      in this instance, the nature of Lambda is such that this is not the case
    /// NOTE regardless of concurrency within a process, it's critical to avoid having >1 writer hitting the same partition as this will result on continual conflicts
    member _.IngestWithoutConcurrency(partitionId, spans) = async {
        let ingester = ingester partitionId
        let! originEpoch = ingester.ActiveIngestionEpochId()
        return! ingester.IngestMany(originEpoch, spans) |> Async.Ignore }

type DynamoStoreIngester(log, context, ?storeLog, ?onlyWarnOnGap : bool) =

    // Values up to 5 work reasonably, but side effects are:
    // - read usage is more 'lumpy'
    // - readers need more memory to hold the state
    // - Lambda startup time increases
    let epochCutoffMiB = 1
    // Should be large enough to accomodate state of 2 epochs
    // Note the backing memory is not preallocated, so the effects of this being too large will not be immediately apparent
    // (Overusage will hasten the Lambda being killed due to excess memory usage)
    let maxCacheMiB = 5
    let cache = Equinox.Cache(nameof DynamoStoreIngester, sizeMb = maxCacheMiB)
    member val Service = DynamoStoreIndexer(log, context, cache, epochBytesCutoff = epochCutoffMiB * 1024 * 1024, ?storeLog = storeLog, ?onlyWarnOnGap = onlyWarnOnGap)
