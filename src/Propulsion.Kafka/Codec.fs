// Defines a canonical format for representations of a span of events when serialized as json
// NB logically, this could become a separated Propulsion.Codec nuget
// (and/or series of nugets, with an implementation per concrete serialization stack)
namespace Propulsion.Codec.NewtonsoftJson

open Newtonsoft.Json
open Newtonsoft.Json.Linq
open Propulsion.Streams

/// Manages injecting prepared json into the data being submitted to DocDb as-is, on the basis we can trust it to be valid json as DocDb will need it to be
// NB this code is cloned from the Equinox repo and should remain in sync with that - there are tests pinning various behaviors to go with it there
type VerbatimUtf8JsonConverter() =
    inherit JsonConverter()

    static let enc = System.Text.Encoding.UTF8

    override __.ReadJson(reader, _, _, _) =
        let token = JToken.Load reader
        if token.Type = JTokenType.Null then null
        else token |> string |> enc.GetBytes |> box

    override __.CanConvert(objectType) =
        typeof<byte[]>.Equals(objectType)

    override __.WriteJson(writer, value, serializer) =
        let array = value :?> byte[]
        if array = null || array.Length = 0 then serializer.Serialize(writer, null)
        else writer.WriteRawValue(enc.GetString(array))

/// Rendition of an event within a RenderedSpan
type [<NoEquality; NoComparison>] RenderedEvent =
    {   /// Event Type associated with event data in `d`
        c: string

        /// Timestamp of original write
        t: System.DateTimeOffset // ISO 8601

        /// Event body, as UTF-8 encoded json ready to be injected directly into the Json being rendered
        [<JsonConverter(typeof<VerbatimUtf8JsonConverter>)>]
        d: byte[] // required

        /// Optional metadata, as UTF-8 encoded json, ready to emit directly (entire field is not written if value is null)
        [<JsonConverter(typeof<VerbatimUtf8JsonConverter>)>]
        [<JsonProperty(Required=Required.Default, NullValueHandling=NullValueHandling.Ignore)>]
        m: byte[] }

    interface Propulsion.Streams.IEvent<byte[]> with
        member __.EventType = __.c
        member __.Data = __.d
        member __.Meta = __.m
        member __.Timestamp = __.t

/// Rendition of a continguous span of events for a given stream
type [<NoEquality; NoComparison>] RenderedSpan =
    {   /// Stream Name
        s: string

        /// base 'i' value for the Events held herein, reflecting the index associated with the first event in the span
        i: int64

        /// The Events comprising this span
        e: RenderedEvent[] }
    /// Parses a contiguous span of Events from a Stream rendered in canonical `RenderedSpan` format
    static member Parse(spanJson : string, ?serializerSettings : Newtonsoft.Json.JsonSerializerSettings) : RenderedSpan =
        match serializerSettings with
        | None -> Newtonsoft.Json.JsonConvert.DeserializeObject<_>(spanJson)
        | Some s -> Newtonsoft.Json.JsonConvert.DeserializeObject<_>(spanJson, s)

/// Helpers for mapping to/from `Propulsion.Streams` canonical event types
module RenderedSpan =

    let ofStreamSpan (stream : string) (span : StreamSpan<_>) : RenderedSpan =
        {   s = stream
            i = span.index
            e = span.events |> Array.map (fun x -> { c = x.EventType; t = x.Timestamp; d = x.Data; m = x.Meta }) }

    let enumStreamEvents (span: RenderedSpan) : StreamEvent<_> seq =
        span.e |> Seq.mapi (fun i e -> { stream = span.s; index = span.i + int64 i; event = e })

    let parseStreamEvents (spanJson: string) : StreamEvent<_> seq =
        spanJson |> RenderedSpan.Parse |> enumStreamEvents

// Rendition of Summary Events representing the agregated state of a Stream at a known point / version
type [<NoEquality; NoComparison>] RenderedSummary =
    {   /// Stream Name
        s: string

        /// Version (the `i` value of the last included event), reflecting the version of the stream's state from which they were produced
        i: int64

        /// The Event-records summarizing the state as at version `i`
        u: RenderedEvent[] }
    /// Parses a contiguous span of Events from a Stream rendered in canonical `RenderedSpan` format
    static member Parse(summaryJson : string, ?serializerSettings : Newtonsoft.Json.JsonSerializerSettings) : RenderedSummary =
        match serializerSettings with
        | None -> Newtonsoft.Json.JsonConvert.DeserializeObject<_>(summaryJson)
        | Some s -> Newtonsoft.Json.JsonConvert.DeserializeObject<_>(summaryJson, s)

/// Helpers for mapping to/from `Propulsion.Streams` canonical event type
module RenderedSummary =

    let ofStreamEvents (stream : string) (index : int64) (events : IEvent<byte[]> seq) : RenderedSummary =
        {   s = stream
            i = index
            u = [| for x in events -> { c = x.EventType; t = x.Timestamp; d = x.Data; m = x.Meta } |] }

    let ofStreamEvent (stream : string) (index : int64) (event : IEvent<byte[]>) : RenderedSummary =
        ofStreamEvents stream index (Seq.singleton event)

    let enumStreamSummaries (span: RenderedSummary) : StreamEvent<_> seq =
        seq { for e in span.u -> { stream = span.s; index = span.i; event = e } }

    let parseStreamSummaries (spanJson: string) : StreamEvent<_> seq =
        spanJson |> RenderedSummary.Parse |> enumStreamSummaries