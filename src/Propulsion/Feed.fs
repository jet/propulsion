namespace Propulsion.Feed

open FSharp.UMX
open System
open System.Threading
open System.Threading.Tasks

type SourceId = string<sourceId>
and [<Measure>] sourceId
module SourceId =
    let parse (value: string): SourceId = UMX.tag value
    let toString (value: SourceId): string = UMX.untag value

type TrancheId = string<trancheId>
and [<Measure>] trancheId
module TrancheId =
    let parse (value: string): TrancheId = UMX.tag value
    let toString (value: TrancheId): string = UMX.untag value

/// int64 offset into a sequence of events
type Position = int64<position>
and [<Measure>] position
module Position =
    let initial: Position = %0L
    let parse (value: int64): Position = %value
    let toInt64 (value: Position): int64 = %value
    let toString (value: Position): string = string value

type TranchePositions = (struct (TrancheId * Position)[])

type IFeedCheckpointStore =

    /// Determines the starting position, and checkpointing frequency for a given tranche
    abstract member Start: source: SourceId * tranche: TrancheId * establishOrigin: Func<CancellationToken, Task<Position>> option * ct: CancellationToken -> Task<Position>
    abstract member Commit: source: SourceId * tranche: TrancheId * pos: Position * CancellationToken -> Task
