// Defines a canonical format for representations of a span of events when serialized as json
// NB logically, this could become a separated Propulsion.Codec nuget
// (and/or series of nugets, with an implementation per concrete serialization stack)
namespace Propulsion.Codec.NewtonsoftJson

open FsCodec
open FsCodec.NewtonsoftJson
open Newtonsoft.Json
open Propulsion.Streams

/// Prepackaged serialization helpers with appropriate settings given the types will roundtrip correctly with default Json.net settings
type Serdes private () =

    static let serdes = lazy NewtonsoftJson.Serdes Settings.Default

    static member Serialize<'T>(value : 'T) : string = serdes.Value.Serialize(value)
    static member Deserialize(json : string) : 'T = serdes.Value.Deserialize(json)

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
        member __.EventType = __.c
        member __.Data = __.d
        member __.Meta = __.m
        member __.EventId = System.Guid.Empty
        member __.CorrelationId = null
        member __.CausationId = null
        member __.Timestamp = __.t

/// Rendition of a contiguous span of events for a given stream
type [<NoEquality; NoComparison>] RenderedSpan =
    {   /// Stream Name
        s: string

        /// base 'i' value for the Events held herein, reflecting the index associated with the first event in the span
        i: int64

        /// The Events comprising this span
        e: RenderedEvent[] }
    /// Parses a contiguous span of Events from a Stream rendered in canonical `RenderedSpan` format
    static member Parse(spanJson : string) : RenderedSpan =
        Serdes.Deserialize(spanJson)

/// Helpers for mapping to/from `Propulsion.Streams` canonical event types
module RenderedSpan =

    let ofStreamSpan (streamName : StreamName) (span : StreamSpan<_>) : RenderedSpan =
        {   s = StreamName.toString streamName
            i = span.index
            e = span.events |> Array.map (fun x -> { c = x.EventType; t = x.Timestamp; d = x.Data; m = x.Meta }) }

    let enum (span: RenderedSpan) : StreamEvent<_> seq =
        let streamName = StreamName.internalParseSafe span.s
        let inline mkEvent offset (e : RenderedEvent) = FsCodec.Core.TimelineEvent.Create(span.i+int64 offset, e.c, e.d, e.m, timestamp=e.t)
        span.e |> Seq.mapi (fun i e -> { stream = streamName; event = mkEvent i e })

    let parse (spanJson: string) : StreamEvent<_> seq =
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
    static member Parse(summaryJson : string) : RenderedSummary =
        Serdes.Deserialize(summaryJson)

/// Helpers for mapping to/from `Propulsion.Streams` canonical event contract
module RenderedSummary =

    let ofStreamEvents (streamName : StreamName) (index : int64) (events : FsCodec.IEventData<byte[]> seq) : RenderedSummary =
        {   s = StreamName.toString streamName
            i = index
            u = [| for x in events -> { c = x.EventType; t = x.Timestamp; d = x.Data; m = x.Meta } |] }

    let ofStreamEvent (streamName : StreamName) (index : int64) (event : FsCodec.IEventData<byte[]>) : RenderedSummary =
        ofStreamEvents streamName index (Seq.singleton event)

    let enum (span: RenderedSummary) : StreamEvent<_> seq =
        let streamName = StreamName.internalParseSafe span.s
        seq { for e in span.u -> { stream = streamName; event = FsCodec.Core.TimelineEvent.Create(span.i, e.c, e.d, e.m, timestamp=e.t, isUnfold=true) } }

    let parse (spanJson: string) : StreamEvent<_> seq =
        spanJson |> RenderedSummary.Parse |> enum
