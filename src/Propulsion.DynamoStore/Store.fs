module internal Propulsion.DynamoStore.Store

module Dynamo =

    open Equinox.DynamoStore

    let private defaultCacheDuration = System.TimeSpan.FromMinutes 20.
    let private create name codec initial fold accessStrategy (context, cache) =
        let cachingStrategy = match cache with None -> Equinox.CachingStrategy.NoCaching | Some cache -> Equinox.CachingStrategy.SlidingWindow (cache, defaultCacheDuration)
        DynamoStoreCategory(context, name, codec, fold, initial, accessStrategy, cachingStrategy)

    let createSnapshotted name codec initial fold (isOrigin, toSnapshot) (context, cache) =
        let accessStrategy = AccessStrategy.Snapshot (isOrigin, toSnapshot)
        create name codec initial fold accessStrategy (context, cache)

    let createUnoptimized name codec initial fold (context, cache) =
        let accessStrategy = AccessStrategy.Unoptimized
        create name codec initial fold accessStrategy (context, cache)

    let createWithOriginIndex name codec initial fold context minIndex =
        // TOCONSIDER include way to limit item count being read
        // TOCONSIDER implement a loader hint to pass minIndex to the query as an additional filter
        let isOrigin struct (i, _) = i <= minIndex
        // There _should_ always be an event at minIndex - if there isn't for any reason, the load might go back one event too far
        // Here we trim it for correctness (although Propulsion would technically ignore it)
        let trimPotentialOverstep = Array.filter (fun struct (i, _e) -> i >= minIndex)
        let accessStrategy = AccessStrategy.MultiSnapshot (isOrigin, fun _ -> failwith "writing not applicable")
        let fold s = trimPotentialOverstep >> fold s
        create name codec initial fold accessStrategy (context, None)

module internal Codec =

    let gen<'t when 't :> TypeShape.UnionContract.IUnionContract> =
        FsCodec.SystemTextJson.Codec.Create<'t>() |> FsCodec.Compression.EncodeTryCompress
