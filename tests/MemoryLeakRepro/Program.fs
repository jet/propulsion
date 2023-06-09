open System
open System.Threading
open System.Threading.Tasks
open FSharp.Control
open Propulsion.Feed
open Propulsion.Internal
open Propulsion.MessageDb
open Propulsion.MessageDb.Internal
open Propulsion.Sinks
open Propulsion.Streams
open Serilog

Log.Logger <- LoggerConfiguration().WriteTo.Console().CreateLogger()

let memoryCheckpoints =
    { new IFeedCheckpointStore with
        member _.Start(source, tranche, origin, ct) =
            Task.FromResult((TimeSpan.FromSeconds 1000, Position.parse 0L))

        member _.Commit(source, tranche, pos, ct) = Task.CompletedTask }

// Running this results in a memory leak
let rec propulsion () =

    let sinkStats =
        { new Propulsion.Streams.Stats<_>(Log.Logger, TimeSpan.FromMinutes 1, TimeSpan.FromMinutes 1) with
            override _.HandleOk(_) = ()
            override _.HandleExn(logger, x) = () }

    let handle _ _ _ =
        task { return struct (StreamResult.AllProcessed, ()) }

    let sink = Factory.StartConcurrentAsync(Log.Logger, 2, 2, handle, sinkStats)

    let source =
        MessageDbSource(
            Log.Logger,
            TimeSpan.FromSeconds 15,
            // Maximum Pool Size is 1 to make sure that the leak is in propulsion and not npgsql
            "Host=localhost; Username=message_store; Password=; Maximum Pool Size=1",
            500,
            TimeSpan.FromMilliseconds 0,
            memoryCheckpoints,
            sink,
            [| "MyCategory" |]
        )

    let src = source.Start()

    src.AwaitWithStopOnCancellation() |> Async.RunSynchronously

// Running this does not result in a memory leak
let manual () =
    task {
        let client =
            MessageDbCategoryClient("Host=localhost; Username=message_store; Password=; Maximum Pool Size=1")

        while true do
            let! _ = client.ReadCategoryMessages(TrancheId.parse "MyCategory", 0L, 500, CancellationToken.None)
            ()
    }

// This does not result in a leak
let taskSeqTest () =
    taskSeq {
        let client =
            MessageDbCategoryClient("Host=localhost; Username=message_store; Password=; Maximum Pool Size=1")

        while true do
            let! _ = client.ReadCategoryMessages(TrancheId.parse "MyCategory", 0L, 500, CancellationToken.None)
            yield ()
    }


// No memory leak here
// manual().Wait()

// No memory leak here
// (TaskSeq.iter ignore (taskSeqTest ())).Wait()

// Giant freaking memory leak in your face here
propulsion ()
