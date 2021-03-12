#if COSMOSSTORE
namespace Propulsion.CosmosStore

open Equinox.CosmosStore.Core
#else
namespace Propulsion.Cosmos

open Equinox.Cosmos.Store
#endif

open Microsoft.Azure.Documents
open Propulsion.Streams

/// Maps fields in an Event within an Equinox.Cosmos V1+ Event (in a Batch or Tip) to the interface defined by Propulsion.Streams
/// <remarks>NOTE No attempt is made to filter out Tip (`id=-1`) batches from the ChangeFeed; Equinox versions >= 3, Tip batches can bear events.</remarks>
[<RequireQualifiedAccess>]
module EquinoxCosmosParser =
    type Document with
        member document.Cast<'T>() =
            let tmp = Document()
            tmp.SetPropertyValue("content", document)
            tmp.GetPropertyValue<'T>("content")

    /// Sanity check to determine whether the Document represents an `Equinox.Cosmos` >= 1.0 based batch
    let isEquinoxBatch (d : Document) =
        d.GetPropertyValue "p" <> null && d.GetPropertyValue "i" <> null
        && d.GetPropertyValue "n" <> null && d.GetPropertyValue "e" <> null

    /// Enumerates the events represented within a batch
    let enumEquinoxCosmosEvents (batch : Batch) : StreamEvent<byte[]> seq =
        let streamName = FsCodec.StreamName.parse batch.p // we expect all Equinox data to adhere to "{category}-{aggregateId}" form (or we'll throw)
        batch.e |> Seq.mapi (fun offset x -> { stream = streamName; event = FsCodec.Core.TimelineEvent.Create(batch.i+int64 offset, x.c, x.d, x.m, timestamp=x.t) })

    /// Collects all events with a Document [typically obtained via the CosmosDb ChangeFeed] that potentially represents an Equinox.Cosmos event-batch
    let enumStreamEvents (d : Document) : StreamEvent<byte[]> seq =
        if isEquinoxBatch d then d.Cast<Batch>() |> enumEquinoxCosmosEvents
        else Seq.empty
