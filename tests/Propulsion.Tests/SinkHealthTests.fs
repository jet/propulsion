module Propulsion.Tests.SinkHealthTests

open FSharp.Control
open Propulsion.Feed
open Swensen.Unquote
open System
open Xunit

type Scenario(testOutput) =

    let log = TestOutputLogger.forTestOutputEx true testOutput

    let store = Equinox.MemoryStore.VolatileStore()
    let checkpoints = ReaderCheckpoint.MemoryStore.create log ("consumerGroup", TimeSpan.FromMinutes 1) store
    let stats = { new Propulsion.Streams.Stats<_>(log, TimeSpan.FromSeconds 5, TimeSpan.FromSeconds 10)
                  with member _.HandleOk x = ()
                       member _.HandleExn(log, x) = ()
                       member _.Classify e =
                           match e with
                           | e when e.Message = "transient" -> Propulsion.Streams.OutcomeKind.Tagged "transient"
                           | x -> base.Classify x }
    let handle _ _ _ = task { return failwith "transient" }
    let sink = Propulsion.Sinks.Factory.StartConcurrentAsync(log, 2, 2, handle, stats)
    let dispose () =
        sink.Stop()
        sink.Await() |> Async.RunSynchronously
    let sid = FsCodec.StreamName.Internal.trust "a-1"
    let mk p c: FsCodec.ITimelineEvent<_> = FsCodec.Core.TimelineEvent.Create(p, c, Propulsion.Sinks.EventBody.Empty)
    let items: Propulsion.Sinks.StreamEvent[] = [|
        sid, mk 0 "EventType"
    |]
    let crawl _ _ _ = TaskSeq.singleton <| struct (TimeSpan.FromSeconds 0.1, ({ items = items; isTail = true; checkpoint = Unchecked.defaultof<_> }: Core.Batch<_>))

    [<Fact>]
    let TailingFeedSource () = task {
        let source = Propulsion.Feed.Core.TailingFeedSource(log, TimeSpan.FromMinutes 1, SourceId.parse "sid", TimeSpan.FromMinutes 1,
                                                            checkpoints, (*establishOrigin*)None, sink, string, crawl)
        use src = source.Start(fun ct -> source.Pump((fun _ -> task { return [| TrancheId.parse "tid" |] }), ct))
        // Yields sink exception, if any
        do! src.Monitor.AwaitCompletion(propagationDelay = TimeSpan.FromSeconds 1, awaitFullyCaughtUp = true)
        // source runs until someone explicitly stops it, or it throws
        src.Stop()
        // Ensure checkpoints are written
        do! src.FlushAsync() |> Task.ignore<TranchePositions>
        // Yields source exception, if any
        do! src.Await()
        test <@ src.RanToCompletion @> }

    [<Fact>]
    let SinglePassFeedSource () = task {
        let source = Propulsion.Feed.Core.SinglePassFeedSource(log, TimeSpan.FromSeconds 5, SourceId.parse "sid", crawl, checkpoints, sink, string)
        use src = source.Start(fun _ct -> task { return [| TrancheId.parse "tid" |] })
        // Await completion of processing; Yields source exception, if any
        do! src.Await()
        test <@ src.RanToCompletion @> }

    interface IDisposable with member _.Dispose() = dispose ()
