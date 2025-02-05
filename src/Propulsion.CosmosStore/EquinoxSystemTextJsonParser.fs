namespace Propulsion.CosmosStore

open Equinox.CosmosStore.Core
open Propulsion.Internal
open Propulsion.Sinks

/// <summary>Maps fields in an Event within an Equinox.Cosmos V1+ Event (in a Batch or Tip) to the interface defined by Propulsion.Streams.</summary>
/// <remarks>NOTE No attempt is made to filter out Tip (`id=-1`) batches from the ChangeFeed, as in Equinox versions >= 3, Tip batches can bear events.</remarks>
[<RequireQualifiedAccess>]
#if !COSMOSV3
module EquinoxSystemTextJsonParser =

    type System.Text.Json.JsonElement with member x.Cast<'T>() = System.Text.Json.JsonSerializer.Deserialize<'T>(x)
    type System.Text.Json.JsonDocument with member x.Cast<'T>() = x.RootElement.Cast<'T>()
    let timestamp (doc: System.Text.Json.JsonDocument) =
        let unixEpoch = System.DateTime.UnixEpoch
        let ts = let r = doc.RootElement in r.GetProperty("_ts")
        unixEpoch.AddSeconds(ts.GetDouble())

    /// Parses an Equinox.Cosmos Batch from a CosmosDB Item
    /// returns ValueNone if it does not bear required elements of a `Equinox.Cosmos` >= 1.0 Batch, or the streamFilter predicate rejects it
    let tryParseEquinoxBatchOrTip streamFilter (d: System.Text.Json.JsonDocument) =
        let r = d.RootElement
        let tryProp (id: string): ValueOption<System.Text.Json.JsonElement> =
            let mutable p = Unchecked.defaultof<_>
            if r.TryGetProperty(id, &p) then ValueSome p else ValueNone
        let hasProp = tryProp >> ValueOption.isSome

        match tryProp "p" with
        | ValueSome je when je.ValueKind = System.Text.Json.JsonValueKind.String && hasProp "i" && hasProp "n" && hasProp "e" ->
            let sn = je.GetString() |> FsCodec.StreamName.parse // we expect all Equinox data to adhere to "{category}-{streamId}" form (or we'll throw)
            if streamFilter sn then ValueSome (struct (sn, d.Cast<Batch>(), tryProp "u")) else ValueNone
        | _ -> ValueNone

    /// Enumerates the Events and/or Unfolds represented within an Equinox.CosmosStore Batch or Tip Item
    let enumEquinoxCosmosBatchOrTip (u: System.Text.Json.JsonElement voption) (batch: Batch): Event seq =
        let inline gen isUnfold i (x: Equinox.CosmosStore.Core.Event) =
            let d = EncodedBody.ofUnfoldBody (x.D, x.d) |> FsCodec.SystemTextJson.Encoding.ToEncodedUtf8
            let m = EncodedBody.ofUnfoldBody (x.M, x.m) |> FsCodec.SystemTextJson.Encoding.ToEncodedUtf8
            let inline len s = if isNull s then 0 else String.length s
            let size = x.c.Length + FsCodec.Encoding.ByteCount d + FsCodec.Encoding.ByteCount m
                       + len x.correlationId + len x.causationId + 80
            FsCodec.Core.TimelineEvent.Create(i, x.c, d, m, timestamp = x.t,
                                              size = size, correlationId = x.correlationId, causationId = x.causationId, isUnfold = isUnfold)
        let events = batch.e |> Seq.mapi (fun offset -> gen false (batch.i + int64 offset))
        // an Unfold won't have a corr/cause id, but that's OK - can't use Tip type as don't want to expand compressed form etc
        match u |> ValueOption.map (fun x -> x.Cast<Equinox.CosmosStore.Core.Event[]>()) with
        | ValueNone | ValueSome null | ValueSome [||] -> events
        | ValueSome unfolds -> seq {
            yield! events
            for x in unfolds do
                gen true batch.n x }
    let inline tryEnumStreamEvents_ withUnfolds streamFilter jsonDocument: seq<StreamEvent> voption =
        tryParseEquinoxBatchOrTip streamFilter jsonDocument
        |> ValueOption.map (fun struct (s, xs, u) -> enumEquinoxCosmosBatchOrTip (if withUnfolds then u else ValueNone) xs |> Seq.map (fun x -> s, x))

    /// Attempts to parse the Events from an Equinox.CosmosStore Batch or Tip Item represented as a JsonDocument
    /// returns ValueNone if it does not bear the hallmarks of a valid Batch, or the streamFilter predicate rejects
    let tryEnumStreamEvents streamFilter jsonDocument: seq<StreamEvent> voption =
        tryEnumStreamEvents_ false streamFilter jsonDocument

    /// Extracts all events that pass the streamFilter from a Feed item
    let whereStream streamFilter jsonDocument: StreamEvent seq =
        tryEnumStreamEvents streamFilter jsonDocument |> ValueOption.defaultValue Seq.empty

    /// Extracts all events passing the supplied categoryFilter from a Feed Item
    let whereCategory categoryFilter jsonDocument: StreamEvent seq =
        whereStream (FsCodec.StreamName.Category.ofStreamName >> categoryFilter) jsonDocument

    /// Extracts all events from the specified category list from a Feed Item
    let ofCategories (categories: string[]) jsonDocument: StreamEvent seq =
        whereCategory (fun c -> Array.contains c categories) jsonDocument

    /// Attempts to parse the Events and/or Unfolds from an Equinox.CosmosStore Batch or Tip Item represented as a JsonDocument
    /// returns ValueNone if it does not bear the hallmarks of a valid Batch, or the streamFilter predicate rejects
    let tryEnumStreamEventsAndUnfolds streamFilter jsonDocument: seq<StreamEvent> voption =
        tryEnumStreamEvents_ true streamFilter jsonDocument

    /// Extracts Events and Unfolds that pass the streamFilter from a Feed item
    let eventsAndUnfoldsWhereStream streamFilter jsonDocument: StreamEvent seq =
        tryEnumStreamEventsAndUnfolds streamFilter jsonDocument |> ValueOption.defaultValue Seq.empty
#else
module EquinoxNewtonsoftParser =

    type Newtonsoft.Json.Linq.JObject with
        member document.Cast<'T>() =
            document.ToObject<'T>()

    let timestamp (doc: Newtonsoft.Json.Linq.JObject) =
        let unixEpoch = System.DateTime.UnixEpoch
        unixEpoch.AddSeconds(doc.Value<double>("_ts"))

    /// Sanity check to determine whether the Document represents an `Equinox.Cosmos` >= 1.0 based batch
    let isEquinoxBatch (d: Newtonsoft.Json.Linq.JObject) =
        d.ContainsKey "p" && d.ContainsKey "i" && d.ContainsKey "n" && d.ContainsKey "e"

    /// Enumerates the events represented within a batch
    let enumEquinoxCosmosEvents (batch: Batch): StreamEvent seq =
        let streamName = FsCodec.StreamName.parse batch.p // we expect all Equinox data to adhere to "{category}-{streamId}" form (or we'll throw)
        batch.e |> Seq.mapi (fun offset x -> streamName, FsCodec.Core.TimelineEvent.Create(batch.i + int64 offset, x.c, FsCodec.Encoding.OfBlob x.d, FsCodec.Encoding.OfBlob x.m, timestamp = x.t))

    /// Collects all events with a Document [typically obtained via the CosmosDb ChangeFeed] that potentially represents an Equinox.Cosmos event-batch
    let enumStreamEvents d: StreamEvent seq =
        if isEquinoxBatch d then d.Cast<Batch>() |> enumEquinoxCosmosEvents
        else Seq.empty
#endif
