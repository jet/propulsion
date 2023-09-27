namespace Propulsion.EventStoreDb

open FSharp.UMX

module internal FeedSourceId =

    let wellKnownId: Propulsion.Feed.SourceId = UMX.tag "eventStoreDb"
