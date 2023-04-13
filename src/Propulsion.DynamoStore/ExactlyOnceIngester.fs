module Propulsion.DynamoStore.ExactlyOnceIngester

open FSharp.UMX // %

type IngestResult<'req, 'res> = { accepted : 'res[]; closed : bool; residual : 'req[] }

module Internal =

    let unknown<[<Measure>]'m> = UMX.tag -1
    let next<[<Measure>]'m> (value : int<'m>) = UMX.tag<'m>(UMX.untag value + 1)

type Service<[<Measure>]'id, 'req, 'res, 'outcome> internal
    (   log : Serilog.ILogger,
        readActiveEpoch : unit -> Async<int<'id>>,
        markActiveEpoch : int<'id> -> Async<unit>,
        ingest : int<'id> * 'req[] -> Async<IngestResult<'req, 'res>>,
        mapResults : 'res[] -> 'outcome seq) =

    let uninitializedSentinel : int = %Internal.unknown
    let mutable currentEpochId_ = uninitializedSentinel
    let currentEpochId () = if currentEpochId_ <> uninitializedSentinel then Some %currentEpochId_ else None

    let rec walk ingestedItems (items : (int<'id> * 'req)[]) = async {
        let epochId = items |> Seq.map fst |> Seq.min
        let epochItems, futureEpochItems = items |> Array.partition (fun (e, _ : 'req) -> e = epochId)
        let! res = ingest (epochId, Array.map snd epochItems)
        let ingestedItemIds = Array.append ingestedItems res.accepted
        let any x = not (Array.isEmpty x)
        let logLevel = if any res.residual || any futureEpochItems || any res.accepted then Serilog.Events.LogEventLevel.Information
                       else Serilog.Events.LogEventLevel.Debug
        log.Write(logLevel, "Epoch {epochId} Added {count}/{total} items Residual {residual} Future {future}",
                  string epochId, res.accepted.Length, epochItems.Length, res.residual.Length, futureEpochItems.Length)
        let nextEpochId = Internal.next epochId
        let pushedToNextEpoch = res.residual |> Array.map (fun x -> nextEpochId, x)
        match Array.append pushedToNextEpoch futureEpochItems with
        | [||] ->
            // Any writer noticing we've moved to a new Epoch shares the burden of marking it active in the Series
            let newActiveEpochId = if res.closed then nextEpochId else epochId
            if currentEpochId_ < %newActiveEpochId then
                log.Information("Epoch {epochId} activated", string newActiveEpochId)
                do! markActiveEpoch newActiveEpochId
                System.Threading.Interlocked.CompareExchange(&currentEpochId_, %newActiveEpochId, currentEpochId_) |> ignore
            return ingestedItemIds
        | remaining -> return! walk ingestedItemIds remaining }
    let walk = walk [||]

    /// Run the requests over a chain of epochs.
    /// Returns the subset that actually got handled this time around (exclusive of items that did not trigger events per idempotency rules).
    member _.IngestMany(originEpoch, reqs) : Async<'outcome seq> = async {
        if Array.isEmpty reqs then return Seq.empty else

        let! results = walk [| for x in reqs -> originEpoch, x |]
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

let create log (readIngestionEpoch, markIngestionEpoch) (ingest, mapResult) =
    Service<'id, 'req, 'res, 'outcome>(log, readIngestionEpoch, markIngestionEpoch, ingest, mapResult)
