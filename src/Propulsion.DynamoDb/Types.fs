namespace Propulsion.DynamoDb

open FSharp.UMX

/// Identifies an Index within a given store
type [<Measure>] indexId
type IndexId = string<indexId>
module IndexId =

    let wellKnownId : IndexId = UMX.tag "0"
    let toString : IndexId -> string = UMX.untag

/// Identifies a chain of epochs within an index that's to be ingested and/or read in sequence
/// Multiple tranches within an index are analogous to how the Table's data is split into shards
type AppendsTrancheId = int<appendsTrancheId>
and [<Measure>] appendsTrancheId
module AppendsTrancheId =

    let wellKnownId : AppendsTrancheId = UMX.tag 0
    let toString : AppendsTrancheId -> string = UMX.untag >> string

/// Identifies a batch of coalesced deduplicated sets of commits indexed from DynamoDB Streams for a given tranche
type AppendsEpochId = int<appendsEpochId>
and [<Measure>] appendsEpochId
module AppendsEpochId =

    let initial : AppendsEpochId = UMX.tag 0
    let toString : AppendsEpochId -> string = UMX.untag >> string

/// Identifies an Equinox Store Stream; used within an AppendsEpoch
type IndexStreamId = int<indexStreamId>
and [<Measure>] indexStreamId

module internal Config =

    open Equinox.DynamoStore
    let createDecider stream = Equinox.Decider(Serilog.Log.Logger, stream, maxAttempts = 3)

    let private createCached codec initial fold accessStrategy (context, cache) =
        let cacheStrategy = CachingStrategy.SlidingWindow (cache, System.TimeSpan.FromMinutes 20.)
        DynamoStoreCategory(context, codec, fold, initial, cacheStrategy, accessStrategy)

    let createSnapshotted codec initial fold (isOrigin, toSnapshot) (context, cache) =
        let accessStrategy = AccessStrategy.Snapshot (isOrigin, toSnapshot)
        createCached codec initial fold accessStrategy (context, cache)

    let createUnoptimized codec initial fold (context, cache) =
        let accessStrategy = AccessStrategy.Unoptimized
        createCached codec initial fold accessStrategy (context, cache)

    let private createUncached codec initial fold accessStrategy context =
        let cacheStrategy = CachingStrategy.NoCaching
        DynamoStoreCategory(context, codec, fold, initial, cacheStrategy, accessStrategy)

    let createUncachedUnoptimized codec initial fold context =
        let accessStrategy = AccessStrategy.Unoptimized
        createUncached codec initial fold accessStrategy context

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
