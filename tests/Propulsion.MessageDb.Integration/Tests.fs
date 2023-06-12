module Propulsion.MessageDb.Integration

open Propulsion.Internal
open Propulsion.MessageDb
open Swensen.Unquote
open System
open System.Collections.Generic
open System.Diagnostics
open System.Threading.Tasks
open Xunit
open OpenTelemetry
open OpenTelemetry.Trace
open OpenTelemetry.Resources

let source = new ActivitySource("Propulsion.MessageDb.Integration")

module Simple =
    type Hello = { name : string}
    type Event =
        | Hello of Hello
        interface TypeShape.UnionContract.IUnionContract
    let codec = FsCodec.SystemTextJson.Codec.Create<Event>()
    type State = unit
    let initial = ()
    let fold state events = state

let ConnectionString =
    match Environment.GetEnvironmentVariable "MSG_DB_CONNECTION_STRING" with
    | null -> "Host=localhost; Database=message_store; Port=5432; Username=message_store"
    | s -> s
let CheckpointConnectionString =
    match Environment.GetEnvironmentVariable "CHECKPOINT_CONNECTION_STRING" with
    | null -> "Host=localhost; Database=message_store; Port=5432; Username=postgres; Password=postgres"
    | s -> s

let decider categoryName id =
    let client = Equinox.MessageDb.MessageDbClient(ConnectionString)
    let ctx = Equinox.MessageDb.MessageDbContext(client)
    let category = Equinox.MessageDb.MessageDbCategory(ctx, Simple.codec, Simple.fold, Simple.initial)
    Equinox.Decider.resolve Serilog.Log.Logger category categoryName (Equinox.StreamId.gen string id)

let writeMessagesToCategory category = task {
    for _ in 1..50 do
        let streamId = Guid.NewGuid().ToString("N")
        let decider = decider category streamId
        let decide _ = List.replicate 20 (Simple.Event.Hello { name = "world" })
        do! decider.Transact(decide, load = Equinox.LoadOption.AssumeEmpty) }

let stats log = { new Propulsion.Streams.Stats<_>(log, TimeSpan.FromMinutes 1, TimeSpan.FromMinutes 1)
                  with member _.HandleOk x = ()
                       member _.HandleExn(log, x) = () }

let makeCheckpoints consumerGroup = task {
    let checkpoints = ReaderCheckpoint.CheckpointStore(CheckpointConnectionString, "public", $"TestGroup{consumerGroup}", TimeSpan.FromSeconds 10)
    do! checkpoints.CreateSchemaIfNotExists()
    return checkpoints }

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


type Tests() =
    let sdk =
        Sdk.CreateTracerProviderBuilder()
           .SetResourceBuilder(ResourceBuilder.CreateDefault().AddService(serviceName = "Tests"))
           .AddSource("Equinox")
           .AddSource("Equinox.MessageDb")
           .AddSource("Propulsion")
           .AddSource("Propulsion.MessageDb.Integration")
           .AddSource("Npgsql")
           .AddOtlpExporter(fun opts -> opts.Endpoint <- Uri("http://localhost:4317"))
           .AddConsoleExporter()
           .Build()

    [<Fact>]
    let ``It processes events for a category`` () = task {
        use _ = source.StartActivity("It processes events for a category", ActivityKind.Server)
        let log = Serilog.Log.Logger
        let consumerGroup = $"{Guid.NewGuid():N}"
        let category1 = $"{Guid.NewGuid():N}"
        let category2 = $"{Guid.NewGuid():N}"
        do! writeMessagesToCategory category1
        do! writeMessagesToCategory category2
        let! checkpoints = makeCheckpoints consumerGroup
        let stats = stats log
        let mutable stop = ignore
        let handled = HashSet<_>()
        let handle stream (events: Propulsion.Sinks.Event[]) _ct = task {
            lock handled (fun _ -> for evt in events do handled.Add((stream, evt.Index)) |> ignore)
            test <@ Array.chooseV Simple.codec.TryDecode events |> Array.forall ((=) (Simple.Hello { name = "world" })) @>
            if handled.Count >= 2000 then stop ()
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

    [<Fact>]
    let ``It doesn't read the tail event again`` () = task {
        use _ = source.StartActivity("It doesn't read the tail event again", ActivityKind.Server)
        let log = Serilog.LoggerConfiguration().CreateLogger()
        let consumerGroup = $"{Guid.NewGuid():N}"
        let category = $"{Guid.NewGuid():N}"
        let decider = decider category "1"
        do! decider.Transact((fun _ -> List.replicate 20 (Simple.Hello { name = "world" })), load = Equinox.LoadOption.AssumeEmpty)
        let! checkpoints = makeCheckpoints consumerGroup

        let stats = stats log

        let handle _ _ _ = task {
            return struct (Propulsion.Sinks.StreamResult.AllProcessed, ()) }
        use sink = Propulsion.Sinks.Factory.StartConcurrentAsync(log, 1, 1, handle, stats)
        let batchSize = 10
        let source = MessageDbSource(
            log, TimeSpan.FromMilliseconds 1000,
            ConnectionString, batchSize, TimeSpan.FromMilliseconds 10000,
            checkpoints, sink, [| category |])

        use capture = new ActivityCapture()

        do! source.RunUntilCaughtUp(TimeSpan.FromSeconds(10), stats.StatsInterval) :> Task

        // 3 batches fetched, 1 checkpoint read, and 1 checkpoint write
        test <@ capture.Operations.Count = 5 @> }

    interface IDisposable with
      member _.Dispose() = sdk.Shutdown() |> ignore
