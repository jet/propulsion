module Propulsion.MessageDb.Integration.Tests

open Npgsql
open NpgsqlTypes
open Propulsion.Internal
open Propulsion.MessageDb
open Swensen.Unquote
open System
open System.Collections.Generic
open System.Diagnostics
open System.Threading.Tasks
open Xunit

module Simple =
    type Hello = { name : string}
    type Event =
        | Hello of Hello
        interface TypeShape.UnionContract.IUnionContract
    let codec = FsCodec.SystemTextJson.Codec.Create<Event>()

let createStreamMessage streamName =
    let cmd = NpgsqlBatchCommand()
    cmd.CommandText <- "select 1 from write_message(@Id::text, @StreamName, @EventType, @Data, null, null)"
    cmd.Parameters.AddWithValue("Id", NpgsqlDbType.Uuid, Guid.NewGuid()) |> ignore
    cmd.Parameters.AddWithValue("StreamName", NpgsqlDbType.Text, streamName) |> ignore
    cmd.Parameters.AddWithValue("EventType", NpgsqlDbType.Text, "Hello") |> ignore
    cmd.Parameters.AddWithValue("Data", NpgsqlDbType.Jsonb, """{"name": "world"}""") |> ignore
    cmd

[<Literal>]
let ConnectionString = "Host=localhost; Port=5432; Username=message_store; Password=;"

let connect () = task {
    let conn = new NpgsqlConnection(ConnectionString)
    do! conn.OpenAsync()
    return conn
}

let writeMessagesToStream (conn: NpgsqlConnection) streamName = task {
    let batch = conn.CreateBatch()
    for _ in 1..20 do
        let cmd = createStreamMessage streamName
        batch.BatchCommands.Add(cmd)
    do! batch.ExecuteNonQueryAsync() :> Task }

let writeMessagesToCategory conn category = task {
    for _ in 1..50 do
        let streamName = $"{category}-{Guid.NewGuid():N}"
        do! writeMessagesToStream conn streamName
}

let stats log = { new Propulsion.Streams.Stats<_>(log, TimeSpan.FromMinutes 1, TimeSpan.FromMinutes 1)
                  with member _.HandleOk x = ()
                       member _.HandleExn(log, x) = () }

let makeCheckpoints consumerGroup = task {
    let checkpoints = ReaderCheckpoint.CheckpointStore("Host=localhost; Database=message_store; Port=5432; Username=postgres; Password=postgres", "public", $"TestGroup{consumerGroup}", TimeSpan.FromSeconds 10)
    do! checkpoints.CreateSchemaIfNotExists()
    return checkpoints }

[<Fact>]
let ``It processes events for a category`` () = task {
    use! conn = connect ()
    let log = Serilog.Log.Logger
    let consumerGroup = $"{Guid.NewGuid():N}"
    let category1 = $"{Guid.NewGuid():N}"
    let category2 = $"{Guid.NewGuid():N}"
    do! writeMessagesToCategory conn category1
    do! writeMessagesToCategory conn category2
    let! checkpoints = makeCheckpoints consumerGroup
    let stats = stats log
    let mutable stop = ignore
    let handled = HashSet<_>()
    let handle stream (events: Propulsion.Sinks.Event[]) _ct = task {
        lock handled (fun _ ->
           for evt in events do
               handled.Add((stream, evt.Index)) |> ignore)
        test <@ Array.chooseV Simple.codec.TryDecode events |> Array.forall ((=) (Simple.Hello { name = "world" })) @>
        if handled.Count >= 2000 then
            stop ()
        return struct (Propulsion.Sinks.StreamResult.AllProcessed, ()) }
    use sink = Propulsion.Sinks.Factory.StartConcurrentAsync(log, 2, 2, handle, stats)
    let source = MessageDbSource(
        log, TimeSpan.FromMinutes 1,
        ConnectionString, 1000, TimeSpan.FromMilliseconds 100,
        checkpoints, sink, [| category1; category2 |])
    use src = source.Start()
    stop <- src.Stop

    Task.Delay(TimeSpan.FromSeconds 30).ContinueWith(fun _ -> src.Stop()) |> ignore

    do! src.Await()

    // 2000 total events
    test <@ handled.Count = 2000 @>
    // 20 in each stream
    test <@ handled |> Array.ofSeq |> Array.groupBy fst |> Array.map (snd >> Array.length) |> Array.forall ((=) 20) @>
    // they were handled in order within streams
    let ordering = handled |> Seq.groupBy fst |> Seq.map (snd >> Seq.map snd >> Seq.toArray) |> Seq.toArray
    test <@ ordering |> Array.forall ((=) [| 0L..19L |]) @> }

type ActivityCapture() =
    let operations = ResizeArray()
    let listener =
         let l = new ActivityListener()
         l.Sample <- fun _ -> ActivitySamplingResult.AllDataAndRecorded
         l.ShouldListenTo <- fun s -> s.Name = "Npgsql"
         l.ActivityStopped <- fun act -> operations.Add(act)

         ActivitySource.AddActivityListener(l)
         l

    member _.Operations = operations
    interface IDisposable with
        member _.Dispose() = listener.Dispose()

[<Fact>]
let ``It doesn't read the tail event again`` () = task {
    let log = Serilog.LoggerConfiguration().CreateLogger()
    let consumerGroup = $"{Guid.NewGuid():N}"
    let category = $"{Guid.NewGuid():N}"
    use! conn = connect ()
    do! writeMessagesToStream conn $"{category}-1"
    let! checkpoints = makeCheckpoints consumerGroup

    let stats = stats log

    let handle _ _ _ = task {
        return struct (Propulsion.Sinks.StreamResult.AllProcessed, ()) }
    use sink = Propulsion.Sinks.Factory.StartConcurrentAsync(log, 1, 1, handle, stats)
    let batchSize = 10
    let source = MessageDbSource(
        log, TimeSpan.FromMilliseconds 1000,
        ConnectionString, batchSize, TimeSpan.FromMilliseconds 1000,
        checkpoints, sink, [| category |])

    use capture = new ActivityCapture()

    do! source.RunUntilCaughtUp(TimeSpan.FromSeconds(10), stats.StatsInterval) :> Task

    // 3 batches fetched, 1 checkpoint read, and 1 checkpoint write
    test <@ capture.Operations.Count = 5 @> }
