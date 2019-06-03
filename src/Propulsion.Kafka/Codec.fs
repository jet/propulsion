namespace Propulsion.Kafka.Codec

open Newtonsoft.Json
open Newtonsoft.Json.Linq

/// Manages injecting prepared json into the data being submitted to DocDb as-is, on the basis we can trust it to be valid json as DocDb will need it to be
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

/// Helpers for mapping to/from `Equinox.Codec` types
module RenderedSpan =
    let ofStreamSpan (stream : string) (span : Propulsion.Streams.StreamSpan<_>) : RenderedSpan =
        {   s = stream
            i = span.index
            e = [| for x in span.events -> { c = x.EventType; t = x.Timestamp; d = x.Data; m = x.Meta } |] }
    let enumEvents (span : RenderedSpan) : seq<Propulsion.Streams.IEvent<byte[]>> = Seq.cast span.e
    let enumStreamEvents (span: RenderedSpan) : seq<Propulsion.Streams.StreamEvent<_>> = 
        enumEvents span |> Seq.mapi (fun i e -> { stream = span.s; index = span.i + int64 i; event = e })