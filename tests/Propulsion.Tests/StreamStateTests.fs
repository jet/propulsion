module Propulsion.Tests.StreamStateTests

open Propulsion.Streams
open Swensen.Unquote
open Xunit

let canonicalTime = System.DateTimeOffset.UtcNow

let mk p c : StreamSpan<string> =
    [| for x in 0..c-1 -> FsCodec.Core.TimelineEvent.Create(p + int64 x, p + int64 x |> string, null, timestamp = canonicalTime) |]
let merge = StreamSpan.merge
let dropBeforeIndex = StreamSpan.dropBeforeIndex
let is (xs : StreamSpan<string>[]) (res : StreamSpan<string>[]) =
    (xs = null && res = null)
    || (xs, res) ||> Seq.forall2 (fun x y -> (x = null && y = null)
                                             || (x[0].Index = y[0].Index && (x, y) ||> Seq.forall2 (fun x y -> x.EventType = y.EventType)))

let [<Fact>] nothing () =
    let r = merge 0L [| mk 0L 0; mk 0L 0 |]
    test <@ obj.ReferenceEquals(null, r) @>

let [<Fact>] synced () =
    let r = merge 1L [| mk 0L 1; mk 0L 0 |]
    test <@ obj.ReferenceEquals(null, r) @>

let [<Fact>] ``no overlap`` () =
    let r = merge 0L [| mk 0L 1; mk 2L 2 |]
    test <@ r |> is [| mk 0L 1; mk 2L 2 |] @>

let [<Fact>] overlap () =
    let r = merge 0L [| mk 0L 1; mk 0L 2 |]
    test <@ r |> is [| mk 0L 2 |] @>

let [<Fact>] ``remove nulls`` () =
    let r = merge 1L [| mk 0L 1; mk 0L 2 |]
    test <@ r |> is [| mk 1L 1 |] @>

let [<Fact>] adjacent () =
    let r = merge 0L [| mk 0L 1; mk 1L 2 |]
    test <@ r |> is [| mk 0L 3 |] @>

let [<Fact>] ``adjacent to min`` () =
    let r = Array.map (dropBeforeIndex 2L) [| mk 0L 1; mk 1L 2 |]
    test <@ r |> is [| null; mk 2L 1 |] @>

let [<Fact>] ``adjacent to min merge`` () =
    let r = merge 2L [| mk 0L 1; mk 1L 2 |]
    test <@ r |> is [| mk 2L 1 |] @>

let [<Fact>] ``adjacent to min no overlap`` () =
    let r = merge 2L [| mk 0L 1; mk 2L 1 |]
    test <@ r |> is [| mk 2L 1|] @>

let [<Fact>] ``adjacent trim`` () =
    let r = Array.map (dropBeforeIndex 1L) [| mk 0L 2; mk 2L 2 |]
    test <@ r |> is [| mk 1L 1; mk 2L 2 |] @>

let [<Fact>] ``adjacent trim merge`` () =
    let r = merge 1L [| mk 0L 2; mk 2L 2 |]
    test <@ r |> is [| mk 1L 3 |] @>

let [<Fact>] ``adjacent trim append`` () =
    let r = Array.map (dropBeforeIndex 1L) [| mk 0L 2; mk 2L 2; mk 5L 1 |]
    test <@ r |> is [| mk 1L 1; mk 2L 2; mk 5L 1 |] @>

let [<Fact>] ``adjacent trim append merge`` () =
    let r = merge 1L [| mk 0L 2; mk 2L 2; mk 5L 1|]
    test <@ r |> is [| mk 1L 3; mk 5L 1 |] @>

let [<Fact>] ``mixed adjacent trim append`` () =
    let r = Array.map (dropBeforeIndex 1L) [| mk 0L 2; mk 5L 1; mk 2L 2 |]
    test <@ r |> is [| mk 1L 1; mk 5L 1; mk 2L 2 |] @>

let [<Fact>] ``mixed adjacent trim append merge`` () =
    let r = merge 1L [| mk 0L 2; mk 5L 1; mk 2L 2|]
    test <@ r |> is [| mk 1L 3; mk 5L 1 |] @>

let [<Fact>] fail () =
    let r = merge 11614L [| null; mk 11614L 1 |]
    test <@ r |> is [| mk 11614L 1 |] @>

let [<Fact>] ``fail 2`` () =
    let r = merge 11613L [| mk 11614L 1; null |]
    test <@ r |> is [| mk 11614L 1 |] @>

#if MEMORY_USAGE_ANALYSIS
// https://bartoszsypytkowski.com/writing-high-performance-f-code
// https://github.com/SergeyTeplyakov/ObjectLayoutInspector
//<PackageReference Include="ObjectLayoutInspector" Version="0.1.2" />
type Perf(out : Xunit.Abstractions.ITestOutputHelper) =

    let [<Fact>] layout () =
        ObjectLayoutInspector.TypeLayout.GetLayout<StreamState<byte[]>>()
        |> fun s -> s.ToString(true)
        |> out.WriteLine
#endif
