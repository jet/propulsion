module Propulsion.Tests.StreamStateTests

open Propulsion.Internal
open Propulsion.Streams
open Swensen.Unquote
open Xunit

module FsCodec301 = // Not yet merged, https://github.com/jet/FsCodec/pull/123
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
            $"{t} {eventType} @{index} {context}"
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
open FsCodec301

let canonicalTime = System.DateTimeOffset.UtcNow

let mk_ p c seg uc: FsCodec.ITimelineEvent<string>[] =
    let mk id et isUnfold = TimelineEvent2.Create(id, et, null, timestamp = canonicalTime, isUnfold = isUnfold, context = seg)
    [| for x in 0..c-1 -> mk (p + int64 x) (p + int64 x |> string) false
       for u in 0..uc-1 -> mk (p + int64 c) $"{p+int64 c}u{u}" true |]
let mk p c = mk_ p c 0 0
let mkU p uc = mk_ p 0 0 uc
let merge = StreamSpan.merge
let isSame = LanguagePrimitives.PhysicalEquality
let dropBeforeIndex = StreamSpan.dropBeforeIndex
let is (xs: FsCodec.ITimelineEvent<string>[][]) (res: FsCodec.ITimelineEvent<string>[][]) =
    (xs, res) ||> Seq.forall2 (fun x y -> (Array.isEmpty x && Array.isEmpty y)
                                          || x[0].Index = y[0].Index && (x, y) ||> Seq.forall2 (fun x y -> x.EventType = y.EventType))

let [<Fact>] nothing () =
    let r = merge 0L [| mk 0L 0; mk 0L 0 |]
    test <@ isSame null r @>

let [<Fact>] synced () =
    let r = merge 1L [| mk 0L 1; mk 0L 0 |]
    test <@ isSame null r @>

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
    let r = Array.map (dropBeforeIndex 2L) [| mk 0L 1; mk 1L 2; mkU 1L 1; mkU 2L 2 |]
    test <@ r |> is [| [||]; mk 2L 1; [||]; mkU 2L 2 |] @>

let [<Fact>] ``adjacent to min merge`` () =
    let r = merge 2L [| mk 0L 1; mk 1L 2; mkU 2L 2 |]
    test <@ r |> is [| [| yield! mk 2L 1; yield! mkU 2L 2 |] |] @>

let [<Fact>] ``adjacent to min no overlap`` () =
    let r = merge 2L [| mk_ 0L 2 0 1; mk 2L 1 |]
    test <@ r |> is [| mk 2L 1|] @>

let [<Fact>] ``adjacent trim`` () =
    let r = Array.map (dropBeforeIndex 1L) [| mk 0L 2; mk 2L 2; mkU 2L 2 |]
    test <@ r |> is [| mk 1L 1; mk 2L 2; mkU 2L 2 |] @>

let [<Fact>] ``adjacent trim merge`` () =
    let r = merge 1L [| mk 0L 2; mk 2L 2 |]
    test <@ r |> is [| mk 1L 3 |] @>

let [<Fact>] ``adjacent trim append`` () =
    let r = Array.map (dropBeforeIndex 1L) [| mk 0L 2; mkU 1L 1; mk 2L 2; mk 5L 1 |]
    test <@ r |> is [| mk 1L 1; mkU 1L 1; mk 2L 2; mk 5L 1 |] @>

let [<Fact>] ``adjacent trim append merge`` () =
    let r = merge 1L [| mk 0L 2; mk 2L 2; mk 5L 1|]
    test <@ r |> is [| mk 1L 3; mk 5L 1 |] @>

let [<Fact>] ``mixed adjacent trim append`` () =
    let r = Array.map (dropBeforeIndex 1L) [| mk 0L 2; mk 5L 1; mk 2L 2; mk_ 0L 2 0 2; mk_ 2L 2 0 2 |]
    test <@ r |> is [| mk 1L 1; mk 5L 1; mk 2L 2; mk_ 1L 1 0 2; mk_ 2L 2 0 2 |] @>

let [<Fact>] ``mixed adjacent trim append merge`` () =
    let r = merge 1L [| mk 0L 2; mk 5L 1; mk 2L 2; mkU 4L 2 |]
    test <@ r |> is [| mk 1L 3; mk 5L 1 |] @>

let [<Fact>] fail () =
    let r = merge 11614L [| null; mk 11614L 1 |]
    test <@ r |> is [| mk 11614L 1 |] @>

let [<Fact>] ``fail 2`` () =
    let r = merge 11613L [| mk 11614L 1; null |]
    test <@ r |> is [| mk 11614L 1 |] @>

let (===) (xs: 't seq) (ys: 't seq) = (xs, ys) ||> Seq.forall2 isSame

let [<FsCheck.Xunit.Property(MaxTest = 1000)>] ``merges retain freshest unfolds, one per event type`` counts =
    let input = [|
        let mutable pos = 0L
        let mutable seg = 0
        for gapOrOverlap, FsCheck.NonNegativeInt normal, FsCheck.NonNegativeInt unfolds in (counts : _[]) do
            let events = normal % 10
            let unfolds = unfolds % 10
            pos <- max 0L (pos+int64 gapOrOverlap)
            yield mk_ pos events seg unfolds
            pos <- pos + int64 events
            seg <- seg + 1 |]
    let res = merge 0L input
    // The only way to end up with a null output is by sending either no spans, or all empties
    if res = null then
        test <@ input |> Array.forall Array.isEmpty @>
    else

    // an Empty span sequence is replaced with null
    test <@ res |> Array.any @>
    // A Span sequence does not have any empty spans
    test <@ res |> Array.forall Array.any @>
    let all = res |> Array.concat
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
    // Version that Unfolds pertain to must always be >= final event Index
    test <@ match events |> Array.tryLast, unfolds |> Array.tryLast with
            | Some le, Some lu -> lu.Index >= le.Index
            | _ -> true @>

    // resulting span sequence must be monotonic, with a gap of at least 1 in the Index ranges per span
    test <@ res |> Seq.pairwise |> Seq.forall (fun (x, y) -> StreamSpan.nextIndex x < StreamSpan.index y) @>

    let others = res |> Array.take (res.Length - 1)
    // Only the last span can have unfolds
    test <@ others |> Array.forall (Array.forall (fun x -> not x.IsUnfold)) @>

    match res |> Array.last |> Array.last with
    | u when u.IsUnfold ->
        // If there are unfolds, they can only be the newest ones
        test <@ input |> Array.forall (not << Array.exists (fun x -> x.IsUnfold && x.Index > u.Index)) @>
        // if two sets of unfolds with identical Index values were supplied, the freshest ones must win
        let uc = unbox<int> u.Context
        let newerUnfolds = Seq.concat input |> Seq.filter (fun x -> x.IsUnfold && x.Index = u.Index && unbox<int> x.Context > uc)
        test <@ newerUnfolds === [||] || uc = -1 @>
    | _ -> ()
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
