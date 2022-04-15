module Propulsion.DynamoStore.DynamoStoreIndexer

type Service internal (epochs : AppendsEpoch.Service, index : AppendsIndex.Service) =

    member _.Ingest(trancheId, allSpans) = async {
        let ingester =
            let log = Serilog.Log.ForContext<Service>().ForContext("trancheId", trancheId)
            let linger = System.TimeSpan.FromMilliseconds 1.
            let readIngestionEpoch () = index.ReadIngestionEpochId trancheId
            let markIngestionEpoch epochId = index.MarkIngestionEpochId(trancheId, epochId)
            let ingest (eid, items) = epochs.Ingest(trancheId, eid, items)
            ExactlyOnceIngester.create log linger (readIngestionEpoch, markIngestionEpoch) (ingest, Array.toSeq)
        let! originTranche = ingester.ActiveIngestionEpochId()
        return! ingester.IngestMany(originTranche, allSpans) |> Async.Ignore }

module Config =

    let create maxItemsPerEpoch store =
        let epochs = AppendsEpoch.Config.create maxItemsPerEpoch store
        let index = AppendsIndex.Config.create store
        Service(epochs, index)
