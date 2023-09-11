namespace Propulsion.CosmosStore

open Equinox.CosmosStore.Core

open Propulsion.Sinks

/// Maps fields in an Event within an Equinox.Cosmos V1+ Event (in a Batch or Tip) to the interface defined by Propulsion.Streams
/// <remarks>NOTE No attempt is made to filter out Tip (`id=-1`) batches from the ChangeFeed; Equinox versions >= 3, Tip batches can bear events.</remarks>
[<RequireQualifiedAccess>]
#if !COSMOSV3
module EquinoxSystemTextJsonParser =

    type System.Text.Json.JsonDocument with
        member document.Cast<'T>() =
            System.Text.Json.JsonSerializer.Deserialize<'T>(document.RootElement)
    type Batch with
        member _.MapData x =
            System.Text.Json.JsonSerializer.SerializeToUtf8Bytes x
    let timestamp (doc: System.Text.Json.JsonDocument) =
        let unixEpoch = System.DateTime.UnixEpoch
        let ts = let r = doc.RootElement in r.GetProperty("_ts")
        unixEpoch.AddSeconds(ts.GetDouble())

    /// Parses an Equinox.Cosmos Batch from a CosmosDB Item
    /// returns ValueNone if it does not bear required elements of a `Equinox.Cosmos` >= 1.0 Batch, or the streamFilter predicate rejects it
    let tryParseEquinoxBatch streamFilter (d: System.Text.Json.JsonDocument) =
        let r = d.RootElement
        let tryProp (id: string): ValueOption<System.Text.Json.JsonElement> =
            let mutable p = Unchecked.defaultof<_>
            if r.TryGetProperty(id, &p) then ValueSome p else ValueNone
        let hasProp = tryProp >> ValueOption.isSome

        match tryProp "p" with
        | ValueSome je when je.ValueKind = System.Text.Json.JsonValueKind.String && hasProp "i" && hasProp "n" && hasProp "e" ->
            let sn = je.GetString() |> FsCodec.StreamName.parse // we expect all Equinox data to adhere to "{category}-{streamId}" form (or we'll throw)
            if streamFilter sn then ValueSome (struct (sn, d.Cast<Batch>())) else ValueNone
        | _ -> ValueNone

    /// Enumerates the events represented within a batch
    let enumEquinoxCosmosEvents (batch: Batch): Event seq =
        batch.e |> Seq.mapi (fun offset x -> FsCodec.Core.TimelineEvent.Create(batch.i + int64 offset, x.c, batch.MapData x.d, batch.MapData x.m, timestamp = x.t))

    /// Attempts to parse a Document/Item from the Store
    /// returns ValueNone if it does not bear the hallmarks of a valid Batch, or the streamFilter predicate rejects
    let tryEnumStreamEvents streamFilter d: seq<StreamEvent> voption =
        tryParseEquinoxBatch streamFilter d
        |> ValueOption.map (fun struct (s, xs) -> enumEquinoxCosmosEvents xs |> Seq.map (fun x -> s, x))

    /// Collects all events that pass the streamFilter from a Document [typically obtained via the CosmosDb ChangeFeed] that potentially represents an Equinox.Cosmos event-batch
    let whereStream streamFilter d: StreamEvent seq =
        tryEnumStreamEvents streamFilter d |> ValueOption.defaultValue Seq.empty

    /// Collects all events passing the supplied categoryFilter from a Document [typically obtained via the CosmosDb ChangeFeed] that potentially represents an Equinox.Cosmos event-batch
    let whereCategory categoryFilter d: StreamEvent seq =
        whereStream (FsCodec.StreamName.Category.ofStreamName >> categoryFilter) d

    /// Collects all events from the specified category list from a Document [typically obtained via the CosmosDb ChangeFeed] that potentially represents an Equinox.Cosmos event-batch
    let ofCategories categories d: StreamEvent seq =
        whereCategory (fun c -> Array.contains c categories) d
#else
module EquinoxNewtonsoftParser =

    type Newtonsoft.Json.Linq.JObject with
        member document.Cast<'T>() =
            document.ToObject<'T>()
    type Batch with
        member _.MapData x = x

    let timestamp (doc : Newtonsoft.Json.Linq.JObject) =
        let unixEpoch = System.DateTime.UnixEpoch
        unixEpoch.AddSeconds(doc.Value<double>("_ts"))

    /// Sanity check to determine whether the Document represents an `Equinox.Cosmos` >= 1.0 based batch
    let isEquinoxBatch (d : Newtonsoft.Json.Linq.JObject) =
        d.ContainsKey "p" && d.ContainsKey "i" && d.ContainsKey "n" && d.ContainsKey "e"

    /// Enumerates the events represented within a batch
    let enumEquinoxCosmosEvents (batch : Batch) : StreamEvent seq =
        let streamName = FsCodec.StreamName.parse batch.p // we expect all Equinox data to adhere to "{category}-{streamId}" form (or we'll throw)
        batch.e |> Seq.mapi (fun offset x -> streamName, FsCodec.Core.TimelineEvent.Create(batch.i + int64 offset, x.c, batch.MapData x.d, batch.MapData x.m, timestamp=x.t))

    /// Collects all events with a Document [typically obtained via the CosmosDb ChangeFeed] that potentially represents an Equinox.Cosmos event-batch
    let enumStreamEvents d : StreamEvent seq =
        if isEquinoxBatch d then d.Cast<Batch>() |> enumEquinoxCosmosEvents
        else Seq.empty
#endif
