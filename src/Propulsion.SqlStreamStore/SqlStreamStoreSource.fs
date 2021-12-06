namespace Propulsion.SqlStreamStore

open Propulsion.Feed
open SqlStreamStore
open System

type ReaderSpec =
    {
         consumerGroup: string
         maxBatchSize: int
         tailSleepInterval: TimeSpan
    }
    static member Default(consumerGroup) =
        {
            consumerGroup = consumerGroup
            maxBatchSize = 100
            tailSleepInterval = TimeSpan.FromSeconds(1.)
        }

type SqlStreamStoreSource =

    static member Run
        (   logger: Serilog.ILogger,
            store: IStreamStore,
            checkpointer: IFeedCheckpointStore,
            spec: ReaderSpec,
            sink: Propulsion.ProjectorPipeline<_>,
            statsInterval: TimeSpan) : Async<unit> = async {
        let streamId = "$all"

        let logger =
            let instanceId = Guid.NewGuid()
            logger
                .ForContext("instanceId", string instanceId)
                .ForContext("consumerGroup", spec.consumerGroup)

        let ingester : Propulsion.Ingestion.Ingester<_,_> =
            sink.StartIngester(logger, 0)

        let reader =
            StreamReader(logger,
                         store,
                         checkpointer,
                         ingester.Submit,
                         SourceId.parse streamId,
                         TrancheId.parse spec.consumerGroup,
                         spec.maxBatchSize,
                         spec.tailSleepInterval,
                         statsInterval)

        try let! _freq, position = checkpointer.Start(SourceId.parse streamId, TrancheId.parse spec.consumerGroup, TimeSpan.FromSeconds 5.)
            do! reader.Start(if position = Position.initial then Nullable() else Nullable(Position.toInt64 position))
        with exc ->
            logger.Warning(exc, "Exception encountered while running reader, exiting loop")
            return! Async.Raise exc }
