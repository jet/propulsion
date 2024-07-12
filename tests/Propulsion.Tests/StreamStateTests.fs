module Propulsion.Tests.StreamStateTests

open Propulsion.Streams
open Swensen.Unquote
open Xunit

module FsCodecEx =
    open FsCodec
    open System
    /// <summary>An Event or Unfold that's been read from a Store and hence has a defined <c>Index</c> on the Event Timeline.</summary>
    [<NoComparison; NoEquality>]
    type TimelineEvent2<'Format>(index, eventType, data, meta, eventId, correlationId, causationId, timestamp, isUnfold, context, size) =

        static member Create(index, eventType, data, ?meta, ?eventId, ?correlationId, ?causationId, ?timestamp, ?isUnfold, ?context, ?size): ITimelineEvent<'Format> =
            let isUnfold = defaultArg isUnfold false
            let meta =     match meta      with Some x -> x   | None -> Unchecked.defaultof<_>
            let eventId =  match eventId   with Some x -> x   | None -> Guid.Empty
            let ts =       match timestamp with Some ts -> ts | None -> DateTimeOffset.UtcNow
            let size =     defaultArg size 0
            TimelineEvent2(index, eventType, data, meta, eventId, Option.toObj correlationId, Option.toObj causationId, ts, isUnfold, Option.toObj context, size) :> _

        static member Create(index, inner: IEventData<'Format>, ?isUnfold, ?context, ?size): ITimelineEvent<'Format> =
            let isUnfold = defaultArg isUnfold false
            let size =     defaultArg size 0
            TimelineEvent2(index, inner.EventType, inner.Data, inner.Meta, inner.EventId, inner.CorrelationId, inner.CausationId, inner.Timestamp, isUnfold, Option.toObj context, size) :> _

        override _.ToString() =
            let t = if isUnfold then "Unfold" else "Event"
            $"{t} {eventType} @{index}"
        interface ITimelineEvent<'Format> with
            member _.Index = index
            member _.IsUnfold = isUnfold
            member _.Context = context
            member _.Size = size
            member _.EventType = eventType
            member _.Data = data
            member _.Meta = meta
            member _.EventId = eventId
            member _.CorrelationId = correlationId
            member _.CausationId = causationId
            member _.Timestamp = timestamp
open FsCodecEx

let canonicalTime = System.DateTimeOffset.UtcNow

let mk_ p c seg uc: FsCodec.ITimelineEvent<string>[] =
    let mk id et isUnfold = TimelineEvent2.Create(id, et, null, timestamp = canonicalTime, isUnfold = isUnfold, context = seg)
    [| for x in 0..c-1 -> mk (p + int64 x) (p + int64 x |> string) false
       for u in 0..uc-1 -> mk (p + int64 c) $"{p+int64 c}u{u}" true |]
let mk p c = mk_ p c 0 0
let merge = StreamSpan.merge
let dropBeforeIndex = StreamSpan.dropBeforeIndex
let is (xs: FsCodec.ITimelineEvent<string>[][]) (res: FsCodec.ITimelineEvent<string>[][]) =
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

let (===) (xs: 't seq) (ys: 't seq) = (xs, ys) ||> Seq.forall2 (fun x y -> obj.ReferenceEquals(x, y))

let [<FsCheck.Xunit.Property>] ``merges retain freshest unfolds, one per event type`` (counts: _[]) =
    let input = [|
        let mutable pos = 0L
        let mutable seg = 0
        for gapOrOverlap, FsCheck.NonNegativeInt normal, FsCheck.NonNegativeInt unfolds in counts do
            let normal = normal % 10
            let unfolds = unfolds % 120 // |> ignore; 0
            pos <- if gapOrOverlap < 0uy then max 0L (pos+int64 gapOrOverlap) else pos + int64 gapOrOverlap
            yield mk_ pos normal seg unfolds
            pos <- pos + int64 normal
            seg <- seg + 1
    |]
    let spans = merge 0L input
    // Empty spans are dropped
    if spans = null then
        test <@ input |> Array.forall Array.isEmpty @>
    else

    let all = spans |> Array.concat
    let unfolds, events = all |> Array.partition _.IsUnfold
    // Events are always in order
    test <@ (events |> Seq.sortBy _.Index) === events @>
    // Unfolds are always in order
    test <@ unfolds |> Seq.sortBy _.Index === unfolds @>
    // Unfolds are always after events
    test <@ all |> Seq.sortBy _.IsUnfold === all @>
    // One unfold per type
    test <@ unfolds |> Array.groupBy _.EventType |> Array.forall (fun (_et, xs) -> xs.Length = 1) @>
    // Unfolds are always for the same Index (as preceding ones are invalidated by any newer event)
    test <@ unfolds |> Array.forall (fun x -> x.Index = (Array.last all).Index) @>
    // Version that Unfolds pertain to must always be > final event Index
    test <@ match events |> Array.tryLast, unfolds |> Array.tryLast with
            | Some le, Some lu -> lu.Index > le.Index
            | _ -> true @>
    // resulting span sequence must be monotonic, with a gap of at least 1 in the Index ranges per span
    test <@ spans |> Seq.pairwise |> Seq.forall (fun (x, y) -> StreamSpan.ver x < StreamSpan.idx y) @>
    match spans with
    | [||] -> ()
    | xs ->
        let others = Array.take (xs.Length - 1) xs
        // Only the last span can have unfolds
        test <@ others |> Array.forall (Array.forall (fun x -> not x.IsUnfold)) @>

    // TODO verify that slice never orphans unfolds

#if MEMORY_USAGE_ANALYSIS
// https://bartoszsypytkowski.com/writing-high-performance-f-code
// https://github.com/SergeyTeplyakov/ObjectLayoutInspector
//<PackageReference Include="ObjectLayoutInspector" Version="0.1.2" />
type Perf(out: Xunit.Abstractions.ITestOutputHelper) =

    let [<Fact>] layout () =
        ObjectLayoutInspector.TypeLayout.GetLayout<StreamState<byte[]>>()
        |> fun s -> s.ToString(true)
        |> out.WriteLine
#endif
