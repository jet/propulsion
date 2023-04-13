module Propulsion.Tests.SourceTests

open FSharp.Control
open Propulsion.Feed
open Propulsion.Internal
open Serilog
open Swensen.Unquote
open System
open Xunit

type Scenario(testOutput) =

    let log = TestOutputLogger.forTestOutput testOutput

    let store = Equinox.MemoryStore.VolatileStore()
    let checkpoints = ReaderCheckpoint.MemoryStore.create log ("consumerGroup", TimeSpan.FromMinutes 1) store
    let stats = { new Propulsion.Streams.Stats<_>(log, TimeSpan.FromMinutes 1, TimeSpan.FromMinutes 1)
                  with member _.HandleOk x = ()
                       member _.HandleExn(log, x) = () }
    let handle _ _ _ = task { return struct (Propulsion.Streams.SpanResult.AllProcessed, ()) }
    let sink = Propulsion.Streams.Default.Config.Start(log, 2, 2, handle, stats, TimeSpan.FromMinutes 1)
    let dispose () =
        sink.Stop()
        sink.Await() |> Async.RunSynchronously

    [<Fact>]
    let ``TailingFeedSource Stop / AwaitCompletion semantics`` () = task {
        let crawl _ _ _ = TaskSeq.singleton <| struct (TimeSpan.FromSeconds 0.1, ({ items = Array.empty; isTail = true; checkpoint = Unchecked.defaultof<_> } : Core.Batch<_>))
        let source = Propulsion.Feed.Core.TailingFeedSource(log, TimeSpan.FromMinutes 1, SourceId.parse "sid", TimeSpan.FromMinutes 1,
                                                            checkpoints, (*establishOrigin*)None, sink, crawl, string)
        use src = source.Start(fun ct -> source.Pump((fun _ -> task { return [| TrancheId.parse "tid" |] }), ct))
        // Yields sink exception, if any
        do! src.Monitor.AwaitCompletion(propagationDelay = TimeSpan.FromSeconds 1, awaitFullyCaughtUp = true)
        // source runs until someone explicitly stops it, or it throws
        src.Stop()
        // TailingFeedSource does not implicitly wait for completion or flush
        do! source.Checkpoint() |> Task.ignore
        // Yields source exception, if any
        do! src.Await()
        test <@ src.RanToCompletion @> }

    [<Theory; InlineData true; InlineData false>]
    let SinglePassFeedSource withWait = async {
        let crawl _ _ _ = TaskSeq.singleton <| struct (TimeSpan.FromSeconds 0.1, ({ items = Array.empty; isTail = true; checkpoint = Unchecked.defaultof<_> } : Core.Batch<_>))
        let source = Propulsion.Feed.Core.SinglePassFeedSource(log, TimeSpan.FromMinutes 1, SourceId.parse "sid", crawl, checkpoints, sink, string)
        use src = source.Start(fun _ct -> task { return [| TrancheId.parse "tid" |] })
        // SinglePassFeedSource completion includes Waiting for Completion of all Batches on all Tranches and Flushing of Checkpoints
        // Hence waiting with the Monitor is not actually necessary (though it provides progress logging which otherwise would be less thorough)
        if withWait then
            // Yields sink exception, if any
            do! src.Monitor.AwaitCompletion(propagationDelay = TimeSpan.FromSeconds 1, awaitFullyCaughtUp = true) |> Async.AwaitTask
        // Yields source exception, if any
        do! src.Await()
        test <@ src.RanToCompletion @> }

    interface IDisposable with member _.Dispose() = dispose ()
