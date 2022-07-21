module Propulsion.Tests.SpanQueueTests

open FsCheck.Xunit
open Swensen.Unquote
open Xunit

open Propulsion.DynamoStore.DynamoStoreIndexReader

let mks i c = EventsQueue.mk i [| for i in i..i + c - 1 -> string i |]

module Span =

    let ins = EventsQueue.insert

    let [<Fact>] ``empty Adds Are Invalid`` () =
        raises<System.ArgumentException> <@ ins (mks 0 0) [||] @>

    (* Examples *)

    let [<Fact>] a () = test <@ [| mks 0 1; mks 2 1 |]  = ins (mks 0 1) [| mks 2 1 |] @>
    let [<Fact>] b () = test <@ [| mks 0 2 |]           = ins (mks 0 1) [| mks 1 1 |] @>
    let [<Fact>] c () = test <@ [| mks 0 3 |]           = ins (mks 0 2) [| mks 2 1 |] @>
    let [<Fact>] d () = test <@ [| mks 0 3; mks 4 1 |]  = ins (mks 0 2) [| mks 2 1; mks 4 1 |] @>
    let [<Fact>] e () = test <@ [| mks 0 3; mks 4 1 |]  = ins (mks 0 3) [| mks 1 1; mks 4 1 |] @>
    let [<Fact>] f () = test <@ [| mks 0 3; mks 4 1 |]  = ins (mks 0 3) [| mks 2 1; mks 4 1 |] @>
    let [<Fact>] g () = test <@ [| mks 0 4 |]           = ins (mks 0 3) [| mks 0 2; mks 3 1 |] @>
    let [<Fact>] h () = test <@ [| mks 0 3; mks 4 1 |]  = ins (mks 0 3) [| mks 0 2; mks 4 1 |] @>
    let [<Fact>] i () = test <@ [| mks 0 5 |]           = ins (mks 0 2) [| mks 2 3 |] @>
    let [<Fact>] j () = test <@ [| mks 0 5 |]           = ins (mks 1 1) [| mks 0 1; mks 2 3 |] @>
    let [<Fact>] k () = test <@ [| mks 0 5 |]           = ins (mks 1 4) [| mks 0 1 |] @>
    let [<Fact>] l () = test <@ [| mks 0 1; mks 2 1 |]  = ins (mks 2 1) [| mks 0 1 |] @>
    let [<Fact>] m () = test <@ [| mks 0 1; mks 2 2 |]  = ins (mks 2 1) [| mks 0 1; mks 3 1 |] @>

    (* ...Ones that the Properties found for me which I didnt think of *)

    let [<Fact>] n () = test <@ [| mks 0 3 |]           = ins (mks 1 1) [| mks 0 3 |] @>
    let [<Fact>] o () = test <@ [| mks 0 1 |]           = ins (mks 0 1) [||] @>

    (* Generalized form of the above with some additional stipulations *)

    let [<Property>] properties (FsCheck.NonNegativeInt pos, FsCheck.PositiveInt len, FsCheck.NonNegativeInt retry) (lensAndGaps : _ array) =
        let existing = [|
            let mutable p = 0
            for FsCheck.PositiveInt len, FsCheck.PositiveInt gap in lensAndGaps ->
                let span = mks p len
                p <- p + len + gap
                span |]
        let adding = mks pos len
        let result = ins adding existing

        let reMergeRandomElement (xs : _ array) retryIndex =
            let sel = min retryIndex (xs.Length - 1)
            ins xs[sel] xs

        let includesSpanAdded (x : EventSpan) =
            pos >= x.Index && pos + len <= x.Version
            && adding.c = Array.take adding.c.Length (Array.skip (pos - x.Index) x.c)

        result.Length > 0 // We're always adding at least one item, so there has to be a result
        && result = reMergeRandomElement result retry // re-merging should not yield a different result
        && 1 = (result |> Seq.where includesSpanAdded |> Seq.length)  // What we added should be represented exactly once in the result

module Queue =

    let [<Fact>] ``Indexing happy path`` () =
        let state = EventsQueue.State()
        state.LogIndexed("stream", mks 0 2)
        test <@ Some 2 = state.TryGetWritePos("stream") @>
        state.LogIndexed("stream", mks 2 1)
        test <@ Some 3 = state.TryGetWritePos("stream") @>

    let [<Fact>] ``Indexing overlaps`` () =
        let state = EventsQueue.State()
        state.LogIndexed("stream", mks 0 1)
        state.LogIndexed("stream", mks 0 2)
        test <@ Some 2 = state.TryGetWritePos("stream") @>

    let [<Fact>] ``Handles missing writes due to gaps in index with redundant write`` () =
        let state = EventsQueue.State()
        state.LogIndexed("stream", mks 1 1)
        test <@ Some 0 = state.TryGetWritePos("stream") @>

        let res = trap <@ state.IngestData("stream", mks 0 1).Value @>
        test <@ res.writePos = 0
                && res.spans.Length = 1
                && res.spans.[0].Length = 2 @>

    // TOCONSIDER should it?
    let [<Fact>] ``Tolerates out of order index writes`` () =
        let state = EventsQueue.State()
        state.LogIndexed("stream", mks 1 1)
        state.LogIndexed("stream", mks 0 1)
        test <@ Some 2 = state.TryGetWritePos("stream") @>

    let [<Fact>] ``Drops Ingests that are already indexed`` () =
        let state = EventsQueue.State()
        state.LogIndexed("stream", mks 0 2)
        test <@ None = state.IngestData("stream", mks 0 1) @>

    let [<Fact>] ``Trims Ingests with redundancy`` () =
        let state = EventsQueue.State()
        state.LogIndexed("stream", mks 0 2)
        let res = trap <@ state.IngestData("stream", mks 0 4).Value @>
        test <@ res.writePos = 2
                && res.spans.Length = 1
                && res.spans[0].Length = 2 @>
