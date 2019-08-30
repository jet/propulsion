namespace Propulsion.Cosmos

open Microsoft.Azure.Documents
open Propulsion.Streams

/// Maps fields in an Event within an Equinox.Cosmos V1+ Event (in a Batch or Tip) to the interface defined by Propulsion.Streams
/// <remarks>
/// NOTE until `tip-isa-batch` gets merged, this causes a null-traversal of `-1`-index pages that presently do not contain data.
/// This is intentional in the name of forward compatibility for projectors - enabling us to upgrade the data format without necessitating
///   updates of all projectors (even if there can potentially be significant at-least-once-ness to the delivery).</remarks>
[<RequireQualifiedAccess>]
module EquinoxCosmosParser =
    type Document with
        member document.Cast<'T>() =
            let tmp = new Document()
            tmp.SetPropertyValue("content", document)
            tmp.GetPropertyValue<'T>("content")

    // TODO remove when using Equinox rc5, where Event isa IEvent
    let (|PropulsionEvent|) (x: Equinox.Cosmos.Store.Event) =
        { new Gardelloyd.IEvent<_> with
            member __.EventType = x.c
            member __.Data = x.d
            member __.Meta = x.m
            member __.Timestamp = x.t }

    /// Sanity check to determine whether the Document represents an `Equinox.Cosmos` >= 1.0 based batch
    let isEquinoxBatch (d : Document) = 
        d.GetPropertyValue "p" <> null && d.GetPropertyValue "i" <> null
        && d.GetPropertyValue "n" <> null && d.GetPropertyValue "e" <> null

    /// Enumerates the events represented within a batch
    let enumEquinoxCosmosEvents (batch : Equinox.Cosmos.Store.Batch) : StreamEvent<byte[]> seq =
        batch.e |> Seq.mapi (fun offset (PropulsionEvent x) -> { stream = batch.p; index = batch.i + int64 offset; event = x })

    /// Collects all events with a Document [typically obtained via the CosmosDb ChangeFeed] that potentially represents an Equinox.Cosmos event-batch
    let enumStreamEvents (d : Document) : StreamEvent<byte[]> seq =
        if isEquinoxBatch d then d.Cast<Equinox.Cosmos.Store.Batch>() |> enumEquinoxCosmosEvents
        else Seq.empty