module Tests

open System
open System.Collections.Generic
open System.Threading.Tasks
open FSharp.Control
open Npgsql
open NpgsqlTypes
open Propulsion.Feed
open Propulsion.Streams
open TypeShape.UnionContract
open Xunit
open Propulsion.MessageDb
open Swensen.Unquote
open Propulsion.Infrastructure
open Propulsion.Internal

module Simple =
    type Hello = {name: string}
    type Event =
        | Hello of Hello
        interface IUnionContract
    let codec = FsCodec.SystemTextJson.Codec.Create<Event>()

let writeMessagesToCategory category = task {
    use conn = new NpgsqlConnection("Host=localhost; Port=5433; Username=message_store; Password=;")
    do! conn.OpenAsync()
    let batch = conn.CreateBatch()
    for _ in 1..50 do
        let streamName = $"{category}-{Guid.NewGuid():N}"
        for _ in 1..20 do
            let cmd = NpgsqlBatchCommand()
            cmd.CommandText <- "select 1 from write_message(@Id::text, @StreamName, @EventType, @Data, 'null', null)"
            cmd.Parameters.AddWithValue("Id", NpgsqlDbType.Uuid, Guid.NewGuid()) |> ignore
            cmd.Parameters.AddWithValue("StreamName", NpgsqlDbType.Text, streamName) |> ignore
            cmd.Parameters.AddWithValue("EventType", NpgsqlDbType.Text, "Hello") |> ignore
            cmd.Parameters.AddWithValue("Data", NpgsqlDbType.Jsonb, """{"name": "world"}""") |> ignore

            batch.BatchCommands.Add(cmd)
    do! batch.ExecuteNonQueryAsync() :> Task }

[<Fact>]
let ``It processes events for a category`` () = async {
    let log = Serilog.Log.Logger
    let consumerGroup = $"{Guid.NewGuid():N}"
    let category1 = $"{Guid.NewGuid():N}"
    let category2 = $"{Guid.NewGuid():N}"
    do! writeMessagesToCategory category1 |> Async.AwaitTaskCorrect
    do! writeMessagesToCategory category2 |> Async.AwaitTaskCorrect
    let reader = MessageDbCategoryClient("Host=localhost; Database=message_store; Port=5433; Username=message_store; Password=;")
    let checkpoints = ReaderCheckpoint.CheckpointStore("Host=localhost; Database=message_store; Port=5433; Username=postgres; Password=postgres", "public", $"TestGroup{consumerGroup}", TimeSpan.FromSeconds 10)
    do! checkpoints.CreateSchemaIfNotExists()
    let stats = { new Propulsion.Streams.Stats<_>(log, TimeSpan.FromMinutes 1, TimeSpan.FromMinutes 1)
                      with member _.HandleExn(log, x) = ()
                           member _.HandleOk x = () }
    let stop = ref (fun () -> ())
    let handled = HashSet<_>()
    let handle struct(stream, evts: StreamSpan<_>) = async {
        lock handled (fun _ -> for evt in evts do handled.Add((stream, evt.Index)) |> ignore)
        test <@ Array.chooseV Simple.codec.TryDecode evts |> Array.forall ((=) (Simple.Hello {name = "world"})) @>
        if handled.Count >= 2000 then
            stop.contents()
        return struct (Propulsion.Streams.SpanResult.AllProcessed, ()) }
    use sink = Propulsion.Streams.Default.Config.Start(log, 2, 2, handle, stats, TimeSpan.FromMinutes 1)
    let source = MessageDbSource(
        log, TimeSpan.FromMinutes 1,
        reader, 1000, TimeSpan.FromMilliseconds 100,
        checkpoints, sink, [| category1; category2 |])
    use src = source.Start()
    // who says you can't do backwards referencing in F#
    stop.contents <- src.Stop

    Task.Delay(TimeSpan.FromSeconds 30).ContinueWith(fun _ -> src.Stop()) |> ignore

    do! src.AwaitShutdown()
    // 2000 total events
    test <@ handled.Count = 2000 @>
    // 20 in each stream
    test <@ handled |> Array.ofSeq |> Array.groupBy fst |> Array.map (snd >> Array.length) |> Array.forall ((=) 20) @>
    // they were handled in order within streams
    let ordering = handled |> Array.ofSeq |> Array.groupBy fst |> Array.map (snd >> Array.map snd)
    test <@ ordering |> Array.forall ((=) [| 0L..19L |]) @>
}
