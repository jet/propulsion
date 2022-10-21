module Propulsion.Tests.SourceTests

open FSharp.Control
open Propulsion.Feed
open Propulsion.Internal
open Serilog
open Swensen.Unquote
open System
open System.Threading.Tasks
open Xunit

type Scenario(testOutput) =

    let log = TestOutputLogger.forTestOutput testOutput

    let store = Equinox.MemoryStore.VolatileStore()
    let checkpoints = ReaderCheckpoint.MemoryStore.create log ("consumerGroup", TimeSpan.FromMinutes 1) store
    let stats = { new Propulsion.Streams.Stats<_>(log, TimeSpan.FromMinutes 1, TimeSpan.FromMinutes 1)
                  with member _.HandleExn(log, x) = ()
                       member _.HandleOk x = () }
    let handle _ = async { return struct (Propulsion.Streams.SpanResult.AllProcessed, ()) }
    let sink = Propulsion.Streams.Default.Config.Start(log, 2, 2, handle, stats, TimeSpan.FromMinutes 1)
    let dispose () =
        sink.Stop()
        sink.AwaitShutdown() |> Async.RunSynchronously

    [<Fact>]
    let ``TailingFeedSource Stop / AwaitCompletion semantics`` () = async {
        let crawl _  = AsyncSeq.singleton <| struct (TimeSpan.FromSeconds 0.1, ({ items = Array.empty; isTail = false; checkpoint = Unchecked.defaultof<_> } : Core.Batch<_>))
        let source = Propulsion.Feed.Core.TailingFeedSource(log, TimeSpan.FromMinutes 1, SourceId.parse "sid", TimeSpan.FromMinutes 1,
                                                            checkpoints, (*establishOrigin*)None, sink, crawl, string)
        use src = source.Start(source.Pump(fun _ -> async { return [| TrancheId.parse "tid" |] }))
        Task.Delay(TimeSpan.FromSeconds 0.5).ContinueWith(fun _ -> src.Stop()) |> ignore
        // Yields sink exception, if any
        do! src.Monitor.AwaitCompletion(propagationDelay = TimeSpan.FromSeconds 0.5, awaitFullyCaughtUp = true)
        // Yields source exception, if any
        do! src.AwaitShutdown()
        test <@ src.RanToCompletion @>
    }

    [<Fact>]
    let SinglePassSource () = async {
        let crawl _ : AsyncSeq<struct (TimeSpan * Core.Batch<_>)> =
            AsyncSeq.singleton (TimeSpan.FromSeconds 0.1, { items = Array.empty; isTail = true; checkpoint = Unchecked.defaultof<_> })
        let source = Propulsion.Feed.Core.SinglePassFeedSource(log, TimeSpan.FromMinutes 1, SourceId.parse "sid", crawl, checkpoints, sink, string)
        use src = source.Start(fun () -> async { return [| TrancheId.parse "tid" |] })
        // Yields sink exception, if any
        do! src.Monitor.AwaitCompletion(propagationDelay = TimeSpan.FromSeconds 1, awaitFullyCaughtUp = true)
        // Yields source exception, if any
        do! src.AwaitShutdown()
        test <@ src.RanToCompletion @>
    }

    interface IDisposable with member _.Dispose() = dispose ()
