namespace Propulsion.SqlStreamStore

open FSharp.UMX

module internal FeedSourceId =

    let wellKnownId: Propulsion.Feed.SourceId = UMX.tag "sqlStreamStore"

module internal NotNull =

    let inline map f = function null -> null | x -> f x
