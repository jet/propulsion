module Propulsion.Tests.SpanQueueTests

open Swensen.Unquote
open Xunit

[<Struct; NoComparison; NoEquality>]
type StreamState = { syncedPos : int; backlog : EventSpan array }
and EventSpan = { i : int; c : string array }

let mk i xs = { i = i; c = xs }

let merge (y : EventSpan) (xs : EventSpan array) =
    let acc = ResizeArray(xs.Length + 1)
    let mutable y, y1, y2 = y, y.i, y.i + y.c.Length

    let mutable i = 0
    while i < xs.Length do
        let x = xs[i]
        let x1, x2 = x.i, x.i + x.c.Length

        if x2 < y1 then acc.Add x; i <- i + 1 // x goes before, no overlap
        elif y2 < x1 then acc.Add y; acc.AddRange xs[i..]; i <- xs.Length + 1 // y goes before, no overlap -> copy rest as a block
        else // there's an overlap
            y <- if x1 < y1 then mk x1 (Array.append x.c (Array.skip (min y.c.Length (x2-y1)) y.c)) // x goes first
                 else            mk y1 (Array.append y.c (Array.skip (min x.c.Length (y2-x1)) x.c)) // y goes first
            y1 <- y.i
            y2 <- y.i + y.c.Length
            i <- i + 1 // consumed x
    if i = xs.Length then acc.Add y
    acc.ToArray()

let mks i c = mk i [| for i in i..i+c-1 -> string i |]
(*
let [<Fact>] a () = test <@ [| mks 0 1; mks 2 1 |]                  = merge (mks 0 1)        [| mks 2 1 |] @>
let [<Fact>] b () = test <@ [| mks 0 2 |]                           = merge (mks 0 1)        [| mks 1 1 |] @>
let [<Fact>] c () = test <@ [| mks 0 3 |]                           = merge (mks 0 2)        [| mks 2 1 |] @>
let [<Fact>] d () = test <@ [| mks 0 3; mks 4 1 |]                  = merge (mks 0 2)        [| mks 2 1; mks 4 1 |] @>
let [<Fact>] e () = test <@ [| mks 0 3; mks 4 1 |]                  = merge (mks 0 3)        [| mks 1 1; mks 4 1 |] @>
let [<Fact>] f () = test <@ [| mks 0 3; mks 4 1 |]                  = merge (mks 0 3)        [| mks 2 1; mks 4 1 |] @>
let [<Fact>] g () = test <@ [| mks 0 4 |]                           = merge (mks 0 3)        [| mks 0 2; mks 3 1 |] @>
let [<Fact>] g2 () = test <@ [| mks 0 3; mks 4 1 |]                 = merge (mks 0 3)        [| mks 0 2; mks 4 1 |] @>
let [<Fact>] h () = test <@ [| mks 0 5 |]                           = merge (mks 0 2)        [| mks 2 3 |] @>
let [<Fact>] i () = test <@ [| mks 0 5 |]                           = merge (mks 1 1)        [| mks 0 1; mks 2 3 |] @>
let [<Fact>] j () = test <@ [| mks 0 5 |]                           = merge (mks 1 4)        [| mks 0 1 |] @>
let [<Fact>] k () = test <@ [| mks 0 1; mks 2 1 |]                  = merge (mks 2 1)        [| mks 0 1 |] @>
let [<Fact>] l () = test <@ [| mks 0 1; mks 2 2 |]                  = merge (mks 2 1)        [| mks 0 1; mks 3 1 |] @>
let [<Fact>] m () = test <@ [| mks 0 3 |]                           = merge (mks 1 1)        [| mks 0 3 |] @>
*)
(*
let [<FsCheck.Xunit.Property>] properties2 (FsCheck.NonNegativeInt pos, FsCheck.PositiveInt len, FsCheck.NonNegativeInt retry) (lensAndGaps : _ array) =
    let res = merge (mks pos len) [|  let mutable p = 0
                                      for FsCheck.PositiveInt len, FsCheck.PositiveInt gap in lensAndGaps ->
                                         let s = mks p len
                                         p <- p + len + gap
                                         s |]
    let retryRandomElement (xs : _ array) retryIndex =
        let sel = min retryIndex (xs.Length - 1)
        merge xs[sel] xs

    res.Length > 0 // We're always adding at least one item, so there has to be a result
    && res = retryRandomElement res retry // re-merging should not yield a different result
*)
