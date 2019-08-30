// Defines a canonical format for representations of a span of events when serialized as json
// NB logically, this could become a separated Propulsion.Codec nuget
// (and/or series of nugets, with an implementation per concrete serialization stack)
namespace Propulsion.Codec.NewtonsoftJson

open Gardelloyd.NewtonsoftJson
open Newtonsoft.Json
open Propulsion.Streams

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

    interface Gardelloyd.IEvent<byte[]> with
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
        Serdes.Deserialize(spanJson, ?settings = serializerSettings)

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
        Serdes.Deserialize(summaryJson, ?settings = serializerSettings)

/// Helpers for mapping to/from `Propulsion.Streams` canonical event type
module RenderedSummary =

    let ofStreamEvents (stream : string) (index : int64) (events : Gardelloyd.IEvent<byte[]> seq) : RenderedSummary =
        {   s = stream
            i = index
            u = [| for x in events -> { c = x.EventType; t = x.Timestamp; d = x.Data; m = x.Meta } |] }

    let ofStreamEvent (stream : string) (index : int64) (event : Gardelloyd.IEvent<byte[]>) : RenderedSummary =
        ofStreamEvents stream index (Seq.singleton event)

    let enumStreamSummaries (span: RenderedSummary) : StreamEvent<_> seq =
        seq { for e in span.u -> { stream = span.s; index = span.i; event = e } }

    let parseStreamSummaries (spanJson: string) : StreamEvent<_> seq =
        spanJson |> RenderedSummary.Parse |> enumStreamSummaries