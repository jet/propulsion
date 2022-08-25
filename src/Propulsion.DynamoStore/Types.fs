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
    let parse : int -> AppendsTrancheId = int >> UMX.tag
    let internal (|Parse|) : Propulsion.Feed.TrancheId -> AppendsTrancheId = UMX.untag >> int >> UMX.tag

/// Identifies a batch of coalesced deduplicated sets of commits indexed from DynamoDB Streams for a given tranche
type internal AppendsEpochId = int<appendsEpochId>
and [<Measure>] appendsEpochId
module internal AppendsEpochId =

    let initial : AppendsEpochId = UMX.tag 0
    let toString : AppendsEpochId -> string = UMX.untag >> string
    let value : AppendsEpochId -> int = UMX.untag
    let next (value : AppendsEpochId) : AppendsEpochId = % (%value + 1)
    let parse : int -> AppendsEpochId = int >> UMX.tag

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
        let isOrigin struct (i, _) = i <= minIndex
        // There _should_ always be an event at minIndex - if there isn't for any reason, the load might go back one event too far
        // Here we trim it for correctness (although Propulsion would technically ignore it)
        let trimPotentialOverstep = Seq.filter (fun struct (i, _e) -> i >= minIndex)
        let accessStrategy = AccessStrategy.MultiSnapshot (isOrigin, fun _ -> failwith "writing not applicable")
        create codec initial (fun s -> trimPotentialOverstep >> fold s) accessStrategy (context, None)

module internal EventCodec =

    let create<'t when 't :> TypeShape.UnionContract.IUnionContract> () =
        FsCodec.SystemTextJson.Codec.Create<'t>() |> FsCodec.Deflate.EncodeTryDeflate
    let private withUpconverter<'c, 'e when 'c :> TypeShape.UnionContract.IUnionContract> up : FsCodec.IEventCodec<'e, _, _> =
        let down (_ : 'e) = failwith "Unexpected"
        FsCodec.SystemTextJson.Codec.Create<'e, 'c, _>(up, down) |> FsCodec.Deflate.EncodeTryDeflate
    let withIndex<'c when 'c :> TypeShape.UnionContract.IUnionContract> : FsCodec.IEventCodec<struct (int64 * 'c), _, _> =
        let up (raw : FsCodec.ITimelineEvent<_>, e) = struct (raw.Index, e)
        withUpconverter<'c, struct (int64 * 'c)> up

module internal Async =

    open Propulsion.Infrastructure // AwaitTaskCorrect

    type Async with
        static member Throttle degreeOfParallelism =
            let s = new System.Threading.SemaphoreSlim(degreeOfParallelism)
            fun computation -> async {
                let! ct = Async.CancellationToken
                do! s.WaitAsync ct |> Async.AwaitTaskCorrect
                try return! computation
                finally s.Release() |> ignore }
    let private parallelThrottledUnsafe dop computations = // https://github.com/dotnet/fsharp/issues/13165
        Async.Parallel(computations, maxDegreeOfParallelism = dop)
    // NOTE as soon as a non-preview Async.Parallel impl in FSharp.Core includes the fix (e.g. 6.0.5 does not, 6.0.5-beta.22329.3 does),
    // we can remove this shimming and replace it with the body of parallelThrottledUnsafe
    let parallelThrottled dop computations = async {
        let throttle = Async.Throttle dop // each batch of 1200 gets the full potential dop - we internally limit what actually gets to run concurrently here
        let! allResults =
            computations
            |> Seq.map throttle
            |> Seq.chunkBySize 1200
            |> Seq.map (parallelThrottledUnsafe dop)
            |> Async.Parallel
        return Array.concat allResults
    }
