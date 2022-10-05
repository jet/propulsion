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

    let log = TestOutputLogger.forTestOutput(testOutput)

    [<Fact>]
    let ``source AwaitCompletion semantics`` () = async {
        let store = Equinox.MemoryStore.VolatileStore()
        let checkpoints = ReaderCheckpoint.MemoryStore.create log ("consumerGroup", TimeSpan.FromMinutes 1) store
        let crawl _  = AsyncSeq.singleton <| struct (TimeSpan.FromSeconds 0.1, ({ items = Array.empty; isTail = false; checkpoint = Unchecked.defaultof<_> } : Core.Batch<_>))
        let stats = { new Propulsion.Streams.Stats<_>(log, TimeSpan.FromMinutes 1, TimeSpan.FromMinutes 1)
                      with member _.HandleExn(log, x) = ()
                           member _.HandleOk x = () }
        let handle _ = async { return struct (Propulsion.Streams.SpanResult.AllProcessed, ()) }
        let sink = Propulsion.Streams.Default.Config.Start(log, 2, 2, handle, stats, TimeSpan.FromMinutes 1)
        let source = Propulsion.Feed.Core.TailingFeedSource(log, TimeSpan.FromMinutes 1, SourceId.parse "sid", TimeSpan.FromMinutes 1,
                                                            checkpoints, (*establishOrigin*)None, sink, crawl, string)
        let src = source.Start(source.Pump(fun _ -> async { return [| TrancheId.parse "tid" |] }))
        Task.Delay(TimeSpan.FromSeconds 0.5).ContinueWith(fun _ -> src.Stop()) |> ignore
        do! src.Monitor.AwaitCompletion(propagationDelay = TimeSpan.FromSeconds 0.5, awaitFullyCaughtUp = true)
        do! src.AwaitShutdown()
        test <@ src.RanToCompletion @>
    }
