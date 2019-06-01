module StreamStateTests

open Propulsion.Streams
open Propulsion.Streams.Buffering
open Swensen.Unquote
open System
open Xunit

let canonicalTime = System.DateTimeOffset.UtcNow

/// An Event about to be written, see IEvent for further information
type EventData<'Format> =
    { eventType : string; data : 'Format; meta : 'Format; timestamp: DateTimeOffset }
    interface IEvent<'Format> with
        member __.EventType = __.eventType
        member __.Data = __.data
        member __.Meta = __.meta
        member __.Timestamp = __.timestamp

type EventData =
    static member Create(eventType, data, ?meta, ?timestamp) =
        {   eventType = eventType
            data = data
            meta = defaultArg meta null
            timestamp = match timestamp with Some ts -> ts | None -> DateTimeOffset.UtcNow }

let mk p c : StreamSpan<string> = { index = p; events = [| for x in 0..c-1 -> EventData.Create(p + int64 x |> string, null, timestamp=canonicalTime) |] }
let mergeSpans = Span.merge
let trimSpans = Span.dropBeforeIndex
let is (xs : StreamSpan<string>[]) (res : StreamSpan<string>[]) =
    (xs,res) ||> Seq.forall2 (fun x y -> x.index = y.index && (x.events,y.events) ||> Seq.forall2 (fun x y -> x.EventType = y.EventType))

let [<Fact>] ``nothing`` () =
    let r = mergeSpans 0L [ mk 0L 0; mk 0L 0 ]
    test <@ obj.ReferenceEquals(null,r) @>

let [<Fact>] ``synced`` () =
    let r = mergeSpans 1L [ mk 0L 1; mk 0L 0 ]
    test <@ obj.ReferenceEquals(null,r) @>

let [<Fact>] ``no overlap`` () =
    let r = mergeSpans 0L [ mk 0L 1; mk 2L 2 ]
    test <@ r |> is [| mk 0L 1; mk 2L 2 |] @>

let [<Fact>] ``overlap`` () =
    let r = mergeSpans 0L [ mk 0L 1; mk 0L 2 ]
    test <@ r |> is [| mk 0L 2 |] @>

let [<Fact>] ``remove nulls`` () =
    let r = mergeSpans 1L [ mk 0L 1; mk 0L 2 ]
    test <@ r |> is [| mk 1L 1 |] @>

let [<Fact>] ``adjacent`` () =
    let r = mergeSpans 0L [ mk 0L 1; mk 1L 2 ]
    test <@ r |> is [| mk 0L 3 |] @>

let [<Fact>] ``adjacent to min`` () =
    let r = Array.map (trimSpans 2L) [| mk 0L 1; mk 1L 2 |]
    test <@ r |> is [| mk 2L 0; mk 2L 1 |] @>

let [<Fact>] ``adjacent to min merge`` () =
    let r = mergeSpans 2L [ mk 0L 1; mk 1L 2 ]
    test <@ r |> is [| mk 2L 1 |] @>

let [<Fact>] ``adjacent to min no overlap`` () =
    let r = mergeSpans 2L [ mk 0L 1; mk 2L 1 ]
    test <@ r |> is [| mk 2L 1|] @>

let [<Fact>] ``adjacent trim`` () =
    let r = Array.map (trimSpans 1L) [| mk 0L 2; mk 2L 2 |]
    test <@ r |> is [| mk 1L 1; mk 2L 2 |] @>

let [<Fact>] ``adjacent trim merge`` () =
    let r = mergeSpans 1L [ mk 0L 2; mk 2L 2 ]
    test <@ r |> is [| mk 1L 3 |] @>

let [<Fact>] ``adjacent trim append`` () =
    let r = Array.map (trimSpans 1L) [| mk 0L 2; mk 2L 2; mk 5L 1 |]
    test <@ r |> is [| mk 1L 1; mk 2L 2; mk 5L 1 |] @>

let [<Fact>] ``adjacent trim append merge`` () =
    let r = mergeSpans 1L [ mk 0L 2; mk 2L 2; mk 5L 1]
    test <@ r |> is [| mk 1L 3; mk 5L 1 |] @>

let [<Fact>] ``mixed adjacent trim append`` () =
    let r = Array.map (trimSpans 1L) [| mk 0L 2; mk 5L 1; mk 2L 2 |]
    test <@ r |> is [| mk 1L 1; mk 5L 1; mk 2L 2 |] @>

let [<Fact>] ``mixed adjacent trim append merge`` () =
    let r = mergeSpans 1L [ mk 0L 2; mk 5L 1; mk 2L 2]
    test <@ r |> is [| mk 1L 3; mk 5L 1 |] @>

let [<Fact>] ``fail`` () =
    let r = mergeSpans 11614L [ {index=11614L; events=null}; mk 11614L 1 ]
    test <@ r |> is [| mk 11614L 1 |] @>

let [<Fact>] ``fail 2`` () =
    let r = mergeSpans 11613L [ mk 11614L 1; {index=11614L; events=null} ]
    test <@ r |> is [| mk 11614L 1 |] @>