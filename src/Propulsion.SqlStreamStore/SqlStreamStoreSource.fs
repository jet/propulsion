namespace Propulsion.SqlStreamStore

open System
open SqlStreamStore

type ReaderSpec =
    {
         consumerGroup: string
         maxBatchSize: int
         tailSleepInterval: TimeSpan
    }
    static member Default (consumerGroup) =
        {
            consumerGroup = consumerGroup
            maxBatchSize = 100
            tailSleepInterval = TimeSpan.FromSeconds(1.)
        }

type SqlStreamStoreSource =

    static member Run
            (logger: Serilog.ILogger,
             store: IStreamStore,
             checkpointer: ICheckpointer,
             spec: ReaderSpec,
             sink: Propulsion.ProjectorPipeline<_>,
             statsInterval: TimeSpan) : Async<unit> =

        async {
            let streamId = "$all"

            let logger =
                let instanceId = System.Guid.NewGuid()
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
                             streamId,
                             spec.consumerGroup,
                             spec.maxBatchSize,
                             spec.tailSleepInterval,
                             statsInterval)

            try
                let! position =
                    checkpointer.GetPosition(streamId, spec.consumerGroup)

                do! reader.Start(position)
            with
            | exc ->
                logger.Warning(exc, "Exception encountered while running reader, exiting loop")
                return! Async.Raise exc
        }
