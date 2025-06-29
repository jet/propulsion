// Defines a canonical format for representations of a span of events when serialized as json
// NB logically, this could become a separated Propulsion.Codec nuget
// (and/or series of nugets, with an implementation per concrete serialization stack)
namespace Propulsion.Codec.NewtonsoftJson

open FsCodec.NewtonsoftJson
open Newtonsoft.Json
open Propulsion.Sinks

/// Prepackaged serialization helpers with appropriate settings given the types will roundtrip correctly with default Json.net settings
[<AbstractClass; Sealed>]
type Serdes private () =

    static let serdes = FsCodec.NewtonsoftJson.Serdes.Default

    static member Serialize<'T>(value: 'T): string = serdes.Serialize(value)
    static member Deserialize(json: string): 'T = serdes.Deserialize(json)

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

    interface FsCodec.IEventData<byte[]> with
        member x.EventType = x.c
        member x.Data = x.d
        member x.Meta = x.m
        member _.EventId = System.Guid.Empty
        member _.CorrelationId = null
        member _.CausationId = null
        member x.Timestamp = x.t

/// Rendition of a contiguous span of events for a given stream
type [<NoEquality; NoComparison>] RenderedSpan =
    {   /// Stream Name
        s: string

        /// base 'i' value for the Events held herein, reflecting the index associated with the first event in the span
        i: int64

        /// The Events comprising this span
        e: RenderedEvent[] }
    /// Parses a contiguous span of Events from a Stream rendered in canonical `RenderedSpan` format
    static member Parse(spanJson: string): RenderedSpan =
        Serdes.Deserialize(spanJson)

/// Helpers for mapping to/from `Propulsion.Streams` canonical event types
module RenderedSpan =

    let ofStreamSpan streamName (span: Event[]): RenderedSpan =
        let ta (x: EventBody) = FsCodec.Encoding.ToBlob(x).ToArray()
        {   s = FsCodec.StreamName.toString streamName
            i = span[0].Index
            e = span |> Array.map (fun x -> { c = x.EventType; t = x.Timestamp; d = ta x.Data; m = ta x.Meta }) }

    let enum (span: RenderedSpan): StreamEvent seq =
        let streamName = Propulsion.Streams.StreamName.internalParseSafe span.s
        let td (x: byte[]): EventBody = FsCodec.Encoding.OfBlob x
        let inline mkEvent offset (e: RenderedEvent) = FsCodec.Core.TimelineEvent.Create(span.i + int64 offset, e.c, td e.d, td e.m, timestamp = e.t)
        span.e |> Seq.mapi (fun i e -> streamName, mkEvent i e)

    let parse (spanJson: string): StreamEvent seq =
        spanJson |> RenderedSpan.Parse |> enum

// Rendition of Summary Events representing the aggregated state of a Stream at a known point / version
type [<NoEquality; NoComparison>] RenderedSummary =
    {   /// Stream Name
        s: string

        /// Version (the `i` value of the last included event), reflecting the version of the stream's state from which they were produced
        i: int64

        /// The Event-records summarizing the state as at version `i`
        u: RenderedEvent[] }
    /// Parses a contiguous span of Events from a Stream rendered in canonical `RenderedSpan` format
    static member Parse(summaryJson: string): RenderedSummary =
        Serdes.Deserialize(summaryJson)

/// Helpers for mapping to/from `Propulsion.Streams` canonical event contract
module RenderedSummary =

    let ofStreamEvents (streamName: FsCodec.StreamName) (index: int64) (events: FsCodec.IEventData<EventBody> seq): RenderedSummary =
        let ta (x: EventBody): byte[] = FsCodec.Encoding.ToBlob(x).ToArray()
        {   s = FsCodec.StreamName.toString streamName
            i = index
            u = [| for x in events -> { c = x.EventType; t = x.Timestamp; d = ta x.Data; m = ta x.Meta } |] }

    let ofStreamEvent (streamName: FsCodec.StreamName) (index: int64) (event: FsCodec.IEventData<EventBody>): RenderedSummary =
        ofStreamEvents streamName index (Seq.singleton event)

    let enum (span: RenderedSummary): StreamEvent seq =
        let streamName = Propulsion.Streams.StreamName.internalParseSafe span.s
        seq { for e in span.u -> streamName, FsCodec.Core.TimelineEvent.Create(span.i, e.c, FsCodec.Encoding.OfBlob e.d, FsCodec.Encoding.OfBlob e.m, timestamp = e.t, isUnfold = true) }

    let parse (spanJson: string): StreamEvent seq =
        spanJson |> RenderedSummary.Parse |> enum
