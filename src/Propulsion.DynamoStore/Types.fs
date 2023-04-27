namespace Propulsion.DynamoStore

open FSharp.UMX

/// Identifies a batch of coalesced deduplicated sets of commits indexed from DynamoDB Streams for a given partition
type internal AppendsEpochId = int<appendsEpochId>
and [<Measure>] appendsEpochId
module internal AppendsEpochId =

    let initial : AppendsEpochId = UMX.tag 0
    let toString : AppendsEpochId -> string = UMX.untag >> string
    let value : AppendsEpochId -> int = UMX.untag
    let next (value : AppendsEpochId) : AppendsEpochId = % (%value + 1)
    let parse : string -> AppendsEpochId = int >> UMX.tag

/// Identifies a chain of epochs within an index that's to be ingested and/or read in sequence
type internal AppendsPartitionId = int<appendsPartitionId>
and [<Measure>] appendsPartitionId
module AppendsPartitionId =

    // Partitioning is not yet implemented
    let wellKnownId : AppendsPartitionId = UMX.tag 0
    let internal toString : AppendsPartitionId -> string = UMX.untag >> string
    let internal toTrancheId : AppendsPartitionId -> Propulsion.Feed.TrancheId = toString >> UMX.tag
    let parse : string -> AppendsPartitionId = int >> UMX.tag
    let internal (|Parse|) : Propulsion.Feed.TrancheId -> AppendsPartitionId = UMX.untag >> int >> UMX.tag

type [<Measure>] checkpoint
type Checkpoint = int64<checkpoint>
module Checkpoint =

    /// The absolute upper limit of number of streams that can be indexed within a single Epoch (defines how Checkpoints are encoded, so cannot be changed)
    let [<Literal>] MaxItemsPerEpoch = 1_000_000
    let private maxItemsPerEpoch = int64 MaxItemsPerEpoch
    let private ofPosition : Propulsion.Feed.Position -> Checkpoint = Propulsion.Feed.Position.toInt64 >> UMX.tag

    let internal positionOfEpochAndOffset (epoch : AppendsEpochId) offset : Propulsion.Feed.Position =
        int64 (AppendsEpochId.value epoch) * maxItemsPerEpoch + int64 offset |> UMX.tag

    let positionOfEpochClosedAndVersion (epoch : AppendsEpochId) isClosed version : Propulsion.Feed.Position =
        let epoch, offset =
            if isClosed then AppendsEpochId.next epoch, 0L
            else epoch, version
        positionOfEpochAndOffset epoch offset

    let private toEpochAndOffset (value : Checkpoint) : struct (AppendsEpochId * int) =
        let d, r = System.Math.DivRem(%value, maxItemsPerEpoch)
        (%int %d : AppendsEpochId), int r

    let internal (|Parse|) : Propulsion.Feed.Position -> struct (AppendsEpochId * int) = ofPosition >> toEpochAndOffset

#if !PROPULSION_DYNAMOSTORE_NOTIFIER
/// Identifies an Index within a given store
type [<Measure>] indexId
type internal IndexId = string<indexId>
module internal IndexId =

    let wellKnownId : IndexId = UMX.tag "0"
    let toString : IndexId -> string = UMX.untag

/// Identifies an Equinox Store Stream; used within an AppendsEpoch
type IndexStreamId = string<indexStreamId>
and [<Measure>] indexStreamId
module IndexStreamId =

    let ofP : string -> IndexStreamId = UMX.tag
    let internal toStreamName : IndexStreamId -> FsCodec.StreamName = UMX.untag >> Propulsion.Streams.StreamName.internalParseSafe

module internal FeedSourceId =

    let wellKnownId : Propulsion.Feed.SourceId = UMX.tag "dynamoStore"

module internal Config =

    open Equinox.DynamoStore

    let private defaultCacheDuration = System.TimeSpan.FromMinutes 20.

    let private create codec initial fold accessStrategy (context, cache) =
        let cs = match cache with None -> CachingStrategy.NoCaching | Some cache -> CachingStrategy.SlidingWindow (cache, defaultCacheDuration)
        DynamoStoreCategory(context, codec, fold, initial, cs, accessStrategy)

    let createSnapshotted codec initial fold (isOrigin, toSnapshot) (context, cache) =
        let accessStrategy = AccessStrategy.Snapshot (isOrigin, toSnapshot)
        create codec initial fold accessStrategy (context, cache)

    let createUnoptimized codec initial fold (context, cache) =
        let accessStrategy = AccessStrategy.Unoptimized
        create codec initial fold accessStrategy (context, cache)

    let createWithOriginIndex codec initial fold context minIndex =
        // TOCONSIDER include way to limit item count being read
        // TOCONSIDER implement a loader hint to pass minIndex to the query as an additional filter
        let isOrigin struct (i, _) = i <= minIndex
        // There _should_ always be an event at minIndex - if there isn't for any reason, the load might go back one event too far
        // Here we trim it for correctness (although Propulsion would technically ignore it)
        let trimPotentialOverstep = Seq.filter (fun struct (i, _e) -> i >= minIndex)
        let accessStrategy = AccessStrategy.MultiSnapshot (isOrigin, fun _ -> failwith "writing not applicable")
        create codec initial (fun s -> trimPotentialOverstep >> fold s) accessStrategy (context, None)

module internal EventCodec =

    let gen<'t when 't :> TypeShape.UnionContract.IUnionContract> =
        FsCodec.SystemTextJson.Codec.Create<'t>() |> FsCodec.Deflate.EncodeTryDeflate
    let private withUpconverter<'c, 'e when 'c :> TypeShape.UnionContract.IUnionContract> up : FsCodec.IEventCodec<'e, _, _> =
        let down (_ : 'e) = failwith "Unexpected"
        FsCodec.SystemTextJson.Codec.Create<'e, 'c, _>(up, down) |> FsCodec.Deflate.EncodeTryDeflate
    let withIndex<'c when 'c :> TypeShape.UnionContract.IUnionContract> : FsCodec.IEventCodec<struct (int64 * 'c), _, _> =
        let up (raw : FsCodec.ITimelineEvent<_>) e = struct (raw.Index, e)
        withUpconverter<'c, struct (int64 * 'c)> up
#endif
