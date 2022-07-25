module Propulsion.Tests.SpanQueueTests

open FsCheck.Xunit
open Swensen.Unquote
open Xunit

open Propulsion.DynamoStore.DynamoStoreIndex

let mks i c = EventSpan.Create(i, [| for i in i..i + c - 1 -> string i |])

module Span =

    let ins = StreamQueue.insert

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
        let state = Buffer()
        (true, 2) =! state.LogIndexed("stream", mks 0 2)
        (true, 3) =! state.LogIndexed("stream", mks 2 1)

    let [<Fact>] ``Indexing overlaps`` () =
        let state = Buffer()
        (true, 1) =! state.LogIndexed("stream", mks 0 1)
        (true, 2) =! state.LogIndexed("stream", mks 0 2)

    let [<Fact>] ``Handles missing writes due to gaps in index with redundant invalid write`` () =
        let state = Buffer()
        (false, 0) =! state.LogIndexed("stream", mks 1 1)

        let res = trap <@ state.IngestData("stream", mks 0 1).Value @>
        (0, 1) =! (res.Index, res.Length)
        (true, 2) =! state.LogIndexed("stream", mks 0 2)

    let [<Fact>] ``Handles out of order index writes, ignoring gapped spans`` () =
        let state = Buffer()
        (false, 0) =! state.LogIndexed("stream", mks 1 1)
        (true, 1) =! state.LogIndexed("stream", mks 0 1)
        let res = trap <@ state.IngestData("stream", mks 0 2).Value @>
        (1, 1) =! (res.Index, res.Length)
        (true, 2) =! state.LogIndexed("stream", mks 0 2)

    let [<Fact>] ``Drops Ingests that are already indexed`` () =
        let state = Buffer()
        (true, 2) =! state.LogIndexed("stream", mks 0 2)
        test <@ None = state.IngestData("stream", mks 0 1) @>

    let [<Fact>] ``Drops identical Ingests`` () =
        let state = Buffer()
        (true, 2) =! state.LogIndexed("stream", mks 0 2)
        test <@ None = state.IngestData("stream", mks 0 2) @>

    let [<Fact>] ``Handles overlapping Ingests`` () =
        let state = Buffer()
        (true, 2) =! state.LogIndexed("stream", mks 0 2)
        let res = trap <@ state.IngestData("stream", mks 0 3).Value @>
        (2, 1) =! (res.Index, res.Length)

    let [<Fact>] ``Handles Ingests that prepend but are not ready`` () =
        let state = Buffer()
        (false, 0) =! state.LogIndexed("stream", mks 2 2)
        test <@ None = state.IngestData("stream", mks 1 1) @>
        let res = trap <@ state.IngestData("stream", mks 0 1).Value @>
        (0, 2) =! (res.Index, res.Length)
        (true, 2) =! state.LogIndexed("stream", mks 0 2)

    let [<Fact>] ``Handles Ingests that require trim and become ready`` () =
        let state = Buffer()
        (false, 0) =! state.LogIndexed("stream", mks 1 2)
        let res = trap <@ state.IngestData("stream", mks 0 4).Value @>
        (0, 4) =! (res.Index, res.Length)

    let [<Fact>] ``Handles Ingests that prepend and become ready`` () =
        let state = Buffer()
        (false, 0) =! state.LogIndexed("stream", mks 2 2)
        let res = trap <@ state.IngestData("stream", mks 0 2).Value @>
        (0, 2) =! (res.Index, res.Length)

    let [<Fact>] ``Handles Ingests that fill gaps and become ready`` () =
        let state = Buffer()
        (true, 1) =! state.LogIndexed("stream", mks 0 1)
        (false, 1) =! state.LogIndexed("stream", mks 3 1)
        let res = trap <@  state.IngestData("stream", mks 0 2).Value @>
        (1, 1) =! (res.Index, res.Length)
        let res = trap <@ state.IngestData("stream", mks 2 3).Value @>
        (1,4) =! (res.Index, res.Length)
        (true, 5) =! state.LogIndexed("stream", mks 1 4)

    let [<Fact>] ``Trims Ingests with redundancy`` () =
        let state = Buffer()
        (true, 2) =! state.LogIndexed("stream", mks 0 2)
        let res = trap <@ state.IngestData("stream", mks 0 4).Value @>
        (2, 2) =! (res.Index, res.Length)
        (true, 4) =! state.LogIndexed("stream", mks 2 2)
