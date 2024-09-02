namespace Propulsion.Feed

open Propulsion.Internal
open System

/// Drives reading from the Source, stopping when the Tail of each of the Tranches has been reached
type SinglePassFeedSource
    (   log: Serilog.ILogger, statsInterval: TimeSpan,
        sourceId,
        crawl: Func<TrancheId, Position, CancellationToken, IAsyncEnumerable<struct (TimeSpan * Batch<_>)>>,
        checkpoints: IFeedCheckpointStore, sink: Propulsion.Sinks.SinkPipeline,
        ?renderPos, ?logReadFailure, ?readFailureSleepInterval, ?logCommitFailure) =
    inherit Propulsion.Feed.Core.TailingFeedSource(
                              log, statsInterval, sourceId, (*tailSleepInterval*)TimeSpan.Zero, checkpoints, (*establishOrigin*)None, sink, defaultArg renderPos string,
                              crawl,
                              ?logReadFailure = logReadFailure, ?readFailureSleepInterval = readFailureSleepInterval, ?logCommitFailure = logCommitFailure,
                              readersStopAtTail = true)

    member x.Start(readTranches) =
        base.Start(fun ct -> x.Pump(readTranches, ct))
