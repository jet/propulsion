module Propulsion.Tests.AppendsEpochTests

open Propulsion.DynamoStore
open Propulsion.DynamoStore.AppendsEpoch
open Serilog
open Swensen.Unquote
open System
open Xunit

let mkSpan sid index cases: Events.StreamSpan = { p = IndexStreamId.ofP sid; i = index; c = cases }
let mkSpanA sid index cases = mkSpan sid index cases |> Array.singleton
let decideIngest' shouldClose spans inputs =
    let ({ accepted = accepted; residual = residual }: ExactlyOnceIngester.IngestResult<_ ,_>, events) =
        Ingest.decide Log.Logger false shouldClose spans inputs
    (accepted, residual), events
let decideIngest = decideIngest' ((<) 10)

[<Fact>]
let ``residual span shouldn't be affected by earlier events in closed spans`` () =
    let spans1 = mkSpanA "Cat-Id" 0L [| "a" |]
    let spans2 = mkSpanA "Cat-Id" 1L [| "b" |]
    let spans3 = mkSpanA "Cat-Id" 2L [| "c" |]

    let (_, events1) = decideIngest spans1 Fold.initial
    let epoch1Closed = (Fold.fold Fold.initial events1).WithClosed()

    let (_, events2) = decideIngest spans2 Fold.initial
    let epoch2Open =  Fold.fold Fold.initial events2

    let (_, residual1), _ = decideIngest spans3 epoch1Closed

    let (accepted2, residual2), _ = decideIngest residual1 epoch2Open

    test <@ residual1 = spans3
            && accepted2 = (spans3 |> Array.map (fun {p = p} -> p))
            && residual2 = [||] @>

[<Fact>]
let ``Already ingested events should be removed by ingestion on closed epoch`` () =
    let spans1 = mkSpanA "Cat-Id" 0L [| "a"; "a" |]
    let spans2 = mkSpanA "Cat-Id" 1L [| "a"; "b" |]

    let (_, events1) = decideIngest spans1 Fold.initial
    let epoch1Closed = (Fold.fold Fold.initial events1).WithClosed()

    let (accepted, residual), _ = decideIngest spans2 epoch1Closed

    test <@ accepted = [||]
            && residual = mkSpanA "Cat-Id" 2L [| "b" |] @>

[<Fact>]
let ``Already ingested events are not ingested on open epoch`` () =
    let streamId = "Cat-Id"
    let spans1 = mkSpanA streamId 0L [| "a"; "a" |]
    let spans2 = mkSpanA streamId 1L [| "a"; "b" |]

    let (_, events1) = decideIngest spans1 Fold.initial
    let epoch1Closed = (Fold.fold Fold.initial events1)

    let (accepted, _), events = decideIngest spans2 epoch1Closed

    test <@ accepted = [| IndexStreamId.ofP streamId |]
            && events = [ Events.Ingested { add = [||]; app = mkSpanA "Cat-Id" 2L [| "b" |] } ] @>

[<Fact>]
let ``Gap within epoch, throw?`` () =
    let streamId = "Cat-Id"
    let spans1 = mkSpanA streamId 0L [| "a" |]
    let spans2 = mkSpanA streamId 2L [| "b" |]

    let (_, events1) = decideIngest spans1 Fold.initial
    let epoch1Closed = (Fold.fold Fold.initial events1)
    let f () = decideIngest spans2 epoch1Closed |> ignore
    raises<InvalidOperationException> <@ f () @>

