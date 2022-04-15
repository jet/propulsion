module Propulsion.DynamoStore.ExactlyOnceIngester

open FSharp.UMX // %

type IngestResult<'req, 'res> = { accepted : 'res array; closed : bool; residual : 'req array }

module Internal =

    let unknown<[<Measure>]'m> = UMX.tag -1
    let next<[<Measure>]'m> (value : int<'m>) = UMX.tag<'m>(UMX.untag value + 1)

type Service<[<Measure>]'id, 'req, 'res, 'outcome> internal
    (   log : Serilog.ILogger,
        readActiveEpoch : unit -> Async<int<'id>>,
        markActiveEpoch : int<'id> -> Async<unit>,
        ingest : int<'id> * 'req array -> Async<IngestResult<'req, 'res>>,
        mapResults : 'res array -> 'outcome seq,
        linger) =

    let uninitializedSentinel : int = %Internal.unknown
    let mutable currentEpochId_ = uninitializedSentinel
    let currentEpochId () = if currentEpochId_ <> uninitializedSentinel then Some %currentEpochId_ else None

    let tryIngest (reqs : (int<'id> * 'req) array array) =
        let rec aux ingestedItems items = async {
            let epochId = items |> Seq.map fst |> Seq.min
            let epochItems, futureEpochItems = items |> Array.partition (fun (e, _ : 'req) -> e = epochId)
            let! res = ingest (epochId, Array.map snd epochItems)
            let ingestedItemIds = Array.append ingestedItems res.accepted
            let logLevel =
                if res.residual.Length <> 0 || futureEpochItems.Length <> 0 || Array.isEmpty res.accepted then Serilog.Events.LogEventLevel.Information
                else Serilog.Events.LogEventLevel.Debug
            log.Write(logLevel, "Added {count}/{total} items to {epochId} Residual {residual} Future {future}",
                      res.accepted.Length, epochItems.Length, epochId, res.residual.Length, futureEpochItems.Length)
            let nextEpochId = Internal.next epochId
            let pushedToNextEpoch = res.residual |> Array.map (fun x -> nextEpochId, x)
            match Array.append pushedToNextEpoch futureEpochItems with
            | [||] ->
                // Any writer noticing we've moved to a new Epoch shares the burden of marking it active in the Series
                let newActiveEpochId = if res.closed then nextEpochId else epochId
                if currentEpochId_ < %newActiveEpochId then
                    log.Information("Marking {epochId} active", newActiveEpochId)
                    do! markActiveEpoch newActiveEpochId
                    System.Threading.Interlocked.CompareExchange(&currentEpochId_, %newActiveEpochId, currentEpochId_) |> ignore
                return ingestedItemIds
            | remaining -> return! aux ingestedItemIds remaining }
        aux [||] (Array.concat reqs)

    /// Concentrates batches of ingestion requests (e.g. from multiple instances of a DynamoDB Streams Lambda) into a single in flight request at a time
    /// In order to avoid the writes either causing concurrency conflicts, or denying each other Write Capacity
    let batchedIngest = Equinox.Core.AsyncBatchingGate(tryIngest, linger)

    /// Run the requests over a chain of epochs.
    /// Returns the subset that actually got handled this time around (exclusive of items that did not trigger events per idempotency rules).
    member _.IngestMany(originEpoch, reqs) : Async<'outcome seq> = async {
        if Array.isEmpty reqs then return Seq.empty else

        let! results = batchedIngest.Execute [| for x in reqs -> originEpoch, x |]
        return results |> mapResults
    }

    /// Exposes the current high water mark epoch - i.e. the tip epoch to which appends are presently being applied.
    /// The fact that any Ingest call for a given item (or set of items) always commences from the same origin is key to exactly once insertion guarantee.
    /// Caller should first store this alongside the item in order to deterministically be able to start from the same origin in idempotent retry cases.
    /// Uses cached values as epoch transitions are rare, and caller needs to deal with the inherent race condition in any case
    member _.ActiveIngestionEpochId() : Async<int<'id>> =
        match currentEpochId () with
        | Some currentEpochId -> async { return currentEpochId }
        | None -> readActiveEpoch()

let create log linger (readIngestionEpoch, markIngestionEpoch) (ingest, mapResult) =
    Service<'id, 'req, 'res, 'outcome>(log, readIngestionEpoch, markIngestionEpoch, ingest, mapResult, linger = linger)
