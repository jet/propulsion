namespace Propulsion.Feed

open FSharp.UMX

type SourceId = string<sourceId>
and [<Measure>] sourceId
module SourceId =
    let parse (value : string) : SourceId = UMX.tag value
    let toString (value : SourceId) : string = UMX.untag value

type TrancheId = string<trancheId>
and [<Measure>] trancheId
module TrancheId =
    let parse (value : string) : TrancheId = UMX.tag value
    let toString (value : TrancheId) : string = UMX.untag value

/// int64 offset into a sequence of events
type Position = int64<position>
and [<Measure>] position
module Position =
    let parse (value : int64) : Position = %value
    let toInt64 (value : Position) : int64 = %value

type IFeedCheckpointStore =
    abstract member ReadPosition: source: SourceId * tranche : TrancheId -> Async<Position option>
    abstract member Commit: source: SourceId * tranche : TrancheId * pos: Position -> Async<unit>
