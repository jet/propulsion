namespace Propulsion.DynamoStore

open FSharp.UMX

/// Identifies an Index within a given store
type [<Measure>] indexId
type internal IndexId = string<indexId>
module internal IndexId =

    let wellKnownId : IndexId = UMX.tag "0"
    let toString : IndexId -> string = UMX.untag

/// Identifies a chain of epochs within an index that's to be ingested and/or read in sequence
/// Multiple tranches within an index are analogous to how the Table's data is split into shards
type internal AppendsTrancheId = int<appendsTrancheId>
and [<Measure>] appendsTrancheId
module AppendsTrancheId =

    // Tranches are not yet fully implemented
    let wellKnownId : AppendsTrancheId = UMX.tag 0
    let internal toString : AppendsTrancheId -> string = UMX.untag >> string
    let internal toTrancheId : AppendsTrancheId -> Propulsion.Feed.TrancheId = toString >> UMX.tag
    let internal (|Parse|) : Propulsion.Feed.TrancheId -> AppendsTrancheId = UMX.untag >> int >> UMX.tag

/// Identifies a batch of coalesced deduplicated sets of commits indexed from DynamoDB Streams for a given tranche
type internal AppendsEpochId = int<appendsEpochId>
and [<Measure>] appendsEpochId
module internal AppendsEpochId =

    let initial : AppendsEpochId = UMX.tag 0
    let toString : AppendsEpochId -> string = UMX.untag >> string
    let value : AppendsEpochId -> int = UMX.untag
    let next (value : AppendsEpochId) : AppendsEpochId = % (%value + 1)

/// Identifies an Equinox Store Stream; used within an AppendsEpoch
type IndexStreamId = string<indexStreamId>
and [<Measure>] indexStreamId
module IndexStreamId =

    let ofP : string -> IndexStreamId = UMX.tag
    let internal toStreamName : IndexStreamId -> FsCodec.StreamName = UMX.untag >> Propulsion.Streams.StreamName.internalParseSafe

module FeedSourceId =

    let wellKnownId : Propulsion.Feed.SourceId = UMX.tag "dynamodb"

module internal Config =

    open Equinox.DynamoStore

    let createDecider log stream = Equinox.Decider(log, stream, maxAttempts = 3)

    let private create codec initial fold accessStrategy (context, cache) =
        let cs = match cache with None -> CachingStrategy.NoCaching | Some cache -> CachingStrategy.SlidingWindow (cache, System.TimeSpan.FromMinutes 20.)
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
        let isOrigin (i, _) = i <= minIndex
        // There _should_ always be an event at minIndex - if there isn't for any reason, the load might go back one event too far
        // Here we trim it for correctness (although Propulsion would technically ignore it)
        let trimPotentialOverstep = Seq.filter (fun (i, _e) -> i >= minIndex)
        let accessStrategy = AccessStrategy.MultiSnapshot (isOrigin, fun _ -> failwith "writing not applicable")
        create codec initial (fun s -> trimPotentialOverstep >> fold s) accessStrategy (context, None)

module internal EventCodec =

    open FsCodec.SystemTextJson

    let create<'t when 't :> TypeShape.UnionContract.IUnionContract> () =
        Codec.Create<'t>().ToByteArrayCodec()
    let private withUpconverter<'c, 'e when 'c :> TypeShape.UnionContract.IUnionContract> up : FsCodec.IEventCodec<'e, _, _> =
        let down (_ : 'e) = failwith "Unexpected"
        Codec.Create<'e, 'c, _>(up, down).ToByteArrayCodec()
    let withIndex<'c when 'c :> TypeShape.UnionContract.IUnionContract> : FsCodec.IEventCodec<int64 * 'c, _, _> =
        let up (raw : FsCodec.ITimelineEvent<_>, e) = raw.Index, e
        withUpconverter<'c, int64 * 'c> up

module internal Async =

    let parallelThrottled dop f = Async.Parallel(f, maxDegreeOfParallelism = dop)
