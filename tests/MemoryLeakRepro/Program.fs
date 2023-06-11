open System
open System.Threading
open System.Threading.Tasks
open FSharp.Control
open FSharp.UMX
open Propulsion.Feed
open Propulsion.Internal
open Propulsion.Sinks
open Propulsion.Streams
open Serilog

Log.Logger <- LoggerConfiguration().WriteTo.Console().CreateLogger()


let memoryCheckpoints =
    { new IFeedCheckpointStore with
        member _.Start(source, tranche, origin, ct) =
            Task.FromResult((TimeSpan.FromSeconds 1000, Position.parse 0L))
        member _.Commit(source, tranche, pos, ct) = Task.CompletedTask }

let sinkStats =
        { new Propulsion.Streams.Stats<_>(Log.Logger, TimeSpan.FromMinutes 1, TimeSpan.FromMinutes 1) with
            override _.HandleOk(_) = ()
            override _.HandleExn(logger, x) = () }

type NoopSource internal
    (   log : ILogger, statsInterval,
        tailSleepInterval,
        checkpoints : IFeedCheckpointStore, sink : Sink, tranches) =
    inherit Propulsion.Feed.Core.TailingFeedSource
        (   log, statsInterval, UMX.tag "mySource", tailSleepInterval, checkpoints,
            None, sink, string,
            Propulsion.Feed.Core.TailingFeedSource.readOne (fun _ -> task { return { items = [||]; checkpoint = Position.initial; isTail = true } }))

    abstract member ListTranches : ct : CancellationToken -> Task<Propulsion.Feed.TrancheId[]>
    default _.ListTranches(_ct) = task { return tranches }

    abstract member Pump : CancellationToken -> Task<unit>
    default x.Pump(ct) = base.Pump(x.ListTranches, ct)

    abstract member Start : unit -> Propulsion.SourcePipeline<Propulsion.Feed.Core.FeedMonitor>
    default x.Start() = base.Start(x.Pump)

let rec propulsion () =
    let handle _ _ _ =
        task { return struct (StreamResult.AllProcessed, ()) }

    let sink = Factory.StartConcurrentAsync(Log.Logger, 2, 2, handle, sinkStats)

    let source =
        NoopSource(
            Log.Logger,
            TimeSpan.FromSeconds 15,
            TimeSpan.FromMilliseconds 10,
            memoryCheckpoints,
            sink,
            [| TrancheId.parse "MyCategory" |]
        )

    let src = source.Start()

    src.AwaitWithStopOnCancellation() |> Async.RunSynchronously


// Giant freaking memory leak in your face here
propulsion ()
