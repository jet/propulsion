module Propulsion.Tests.AppendsEpochTests

open Propulsion.DynamoStore
open Propulsion.DynamoStore.AppendsEpoch
open Swensen.Unquote
open Xunit

[<Fact>]
let ``residual span shouldn't be affected by earlier events in closed spans`` () =
    let shouldClose = (<) 10
    let spans1: Events.StreamSpan array = [| { p = IndexStreamId.ofP "Cat-Id"; i = 0L; c = [| "a" |] } |]
    let spans2: Events.StreamSpan array = [| { p = IndexStreamId.ofP "Cat-Id"; i = 1L; c = [| "b" |] } |]
    let spans3: Events.StreamSpan array = [| { p = IndexStreamId.ofP "Cat-Id"; i = 2L; c = [| "3" |] } |]

    let (_, events1) = Ingest.decide shouldClose spans1 Fold.initial
    let epoch1Closed = (Fold.fold Fold.initial events1).WithClosed()

    let (_, events2) = Ingest.decide shouldClose spans2 Fold.initial
    let epoch2Open =  Fold.fold Fold.initial events2

    let ({ residual = residual1 }: ExactlyOnceIngester.IngestResult<_,_>,_) = Ingest.decide shouldClose spans3 epoch1Closed

    let ({ accepted = accepted2; residual = residual2 }: ExactlyOnceIngester.IngestResult<_ ,_>, events) =
        Ingest.decide shouldClose residual1 epoch2Open

    test <@ residual1 = spans3 && accepted2 = (spans3 |> Array.map (fun {p = p} -> p)) && residual2 = [||] @>



