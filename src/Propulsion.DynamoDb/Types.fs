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
