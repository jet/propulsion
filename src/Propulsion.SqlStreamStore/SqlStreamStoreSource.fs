namespace Propulsion.SqlStreamStore

open System
open SqlStreamStore

open StreamReader
open LockedIngester

type SqlStreamStore =

    static member Run
            (logger : Serilog.ILogger,
             store: IStreamStore,
             ledger: ILedger,
             sink : Propulsion.ProjectorPipeline<_>,
             ?consumerGroup: string,
             ?statsInterval: TimeSpan,
             ?readerMaxBatchSize: int,
             ?readerSleepInterval: TimeSpan,
             ?boundedCapacity: int,
             ?onCaughtUp: unit -> unit) : Async<unit> =

        async {
            let consumerGroup = defaultArg consumerGroup "default"
            let streamId = "$all"

            let boundedCapacity = defaultArg boundedCapacity 10

            let logger =
                let instanceId = System.Guid.NewGuid()
                logger
                    .ForContext("instanceId", string instanceId)
                    .ForContext("consumerGroup", consumerGroup)

            let ingester : Propulsion.Ingestion.Ingester<_,_> =
                sink.StartIngester(logger, 0)

            use locked =
                new LockedIngester(logger, ledger, ingester.Submit, consumerGroup, streamId,
                                   boundedCapacity = boundedCapacity,
                                   ?onCaughtUp = onCaughtUp,
                                   ?statsInterval = statsInterval)

            let reader =
                StreamReader(logger, store, locked.SubmitBatch,
                             ?maxBatchSize = readerMaxBatchSize,
                             ?sleepInterval = readerSleepInterval,
                             ?statsInterval = statsInterval)

            let! position =
                ledger.GetPosition { Stream = streamId; ConsumerGroup = consumerGroup }

            let! ingesterComp = Async.StartChild (locked.Start())
            let! readerComp = Async.StartChild (reader.Start(position))

            do! Async.Parallel [ ingesterComp; readerComp ] |> Async.Ignore
        }
