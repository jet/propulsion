namespace Propulsion.MessageDb

open FSharp.UMX
open Npgsql

module internal FeedSourceId =
    let wellKnownId : Propulsion.Feed.SourceId = UMX.tag "messageDb"

module internal Npgsql =
    let connect connectionString ct = task {
        let conn = new NpgsqlConnection(connectionString)
        do! conn.OpenAsync(ct)
        return conn }
