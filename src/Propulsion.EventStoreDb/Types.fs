namespace Propulsion.EventStoreDb

open FSharp.UMX

module FeedSourceId =

    let wellKnownId : Propulsion.Feed.SourceId = UMX.tag "eventStoreDb"
