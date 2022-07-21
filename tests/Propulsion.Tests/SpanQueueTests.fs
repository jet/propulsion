module Propulsion.Tests.SpanQueueTests

open FsCheck.Xunit
open Swensen.Unquote
open Xunit

open Propulsion.DynamoStore.DynamoStoreIndexReader

module Span =

    let mks i c = EventsQueue.mk i [| for i in i..i + c - 1 -> string i |]
    let merge = EventsQueue.insert

    let [<Fact>] ``empty Adds Are Invalid`` () =
        raises<System.ArgumentException> <@ merge (mks 0 0) [||] @>

    (* Examples *)

    let [<Fact>] a () = test <@ [| mks 0 1; mks 2 1 |]  = merge (mks 0 1) [| mks 2 1 |] @>
    let [<Fact>] b () = test <@ [| mks 0 2 |]           = merge (mks 0 1) [| mks 1 1 |] @>
    let [<Fact>] c () = test <@ [| mks 0 3 |]           = merge (mks 0 2) [| mks 2 1 |] @>
    let [<Fact>] d () = test <@ [| mks 0 3; mks 4 1 |]  = merge (mks 0 2) [| mks 2 1; mks 4 1 |] @>
    let [<Fact>] e () = test <@ [| mks 0 3; mks 4 1 |]  = merge (mks 0 3) [| mks 1 1; mks 4 1 |] @>
    let [<Fact>] f () = test <@ [| mks 0 3; mks 4 1 |]  = merge (mks 0 3) [| mks 2 1; mks 4 1 |] @>
    let [<Fact>] g () = test <@ [| mks 0 4 |]           = merge (mks 0 3) [| mks 0 2; mks 3 1 |] @>
    let [<Fact>] h () = test <@ [| mks 0 3; mks 4 1 |]  = merge (mks 0 3) [| mks 0 2; mks 4 1 |] @>
    let [<Fact>] i () = test <@ [| mks 0 5 |]           = merge (mks 0 2) [| mks 2 3 |] @>
    let [<Fact>] j () = test <@ [| mks 0 5 |]           = merge (mks 1 1) [| mks 0 1; mks 2 3 |] @>
    let [<Fact>] k () = test <@ [| mks 0 5 |]           = merge (mks 1 4) [| mks 0 1 |] @>
    let [<Fact>] l () = test <@ [| mks 0 1; mks 2 1 |]  = merge (mks 2 1) [| mks 0 1 |] @>
    let [<Fact>] m () = test <@ [| mks 0 1; mks 2 2 |]  = merge (mks 2 1) [| mks 0 1; mks 3 1 |] @>

    (* ...Ones that the Properties found for me which I didnt think of *)

    let [<Fact>] n () = test <@ [| mks 0 3 |]           = merge (mks 1 1) [| mks 0 3 |] @>
    let [<Fact>] o () = test <@ [| mks 0 1 |]           = merge (mks 0 1) [||] @>

    (* Generalized form of the above with some additional stipulations *)

    let [<Property>] properties (FsCheck.NonNegativeInt pos, FsCheck.PositiveInt len, FsCheck.NonNegativeInt retry) (lensAndGaps : _ array) =
        let existing = [|
            let mutable p = 0
            for FsCheck.PositiveInt len, FsCheck.PositiveInt gap in lensAndGaps ->
                let span = mks p len
                p <- p + len + gap
                span |]
        let adding = mks pos len
        let result = merge adding existing

        let reMergeRandomElement (xs : _ array) retryIndex =
            let sel = min retryIndex (xs.Length - 1)
            merge xs[sel] xs

        let includesSpanAdded (x : EventSpan) =
            pos >= x.Index && pos + len <= x.Version
            && adding.c = Array.take adding.c.Length (Array.skip (pos - x.Index) x.c)

        result.Length > 0 // We're always adding at least one item, so there has to be a result
        && result = reMergeRandomElement result retry // re-merging should not yield a different result
        && 1 = (result |> Seq.where includesSpanAdded |> Seq.length)  // What we added should be represented exactly once in the result

module Queue =

    let [<Fact>] ``happy path`` () =
        let state = EventsQueue.State()
        test <@ state.TryAdd("stream", { i = 0; c = [| "0" |] }, false) @>
        test <@ state.TryAdd("stream", { i = 0; c = [| "0"; "1" |] }, false) @>
