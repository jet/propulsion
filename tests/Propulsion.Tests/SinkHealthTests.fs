module Propulsion.Tests.SinkHealthTests

open FSharp.Control
open Propulsion.Internal
open Propulsion.Feed
open Swensen.Unquote
open System
open Xunit

type Scenario(testOutput) =

    let log = TestOutputLogger.forTestOutputEx true testOutput

    let checkpoints = ReaderCheckpoint.MemoryStore.createNull ()
    let abendThreshold = TimeSpan.FromSeconds 3.
    let stats = { new Propulsion.Streams.Stats<_>(log, TimeSpan.FromSeconds 2, TimeSpan.FromSeconds 10, abendThreshold = abendThreshold)
                  with member _.HandleOk x = ()
                       member _.HandleExn(log, x) = ()
                       member _.Classify e =
                           match e with
                           | e when e.Message = "transient" -> Propulsion.Streams.OutcomeKind.Tagged "transient"
                           | x -> base.Classify x }
    let sid n = FsCodec.StreamName.Internal.trust n
    let stuckSid = sid "a-stuck"
    let failingSid = sid "a-bad"
    let handle sn _ = async {
        if sn = stuckSid then
            do! Async.Sleep (TimeSpan.FromMilliseconds 50)
            return (Propulsion.Sinks.StreamResult.NoneProcessed, ())
        elif sn = failingSid then
            return failwith "transient"
        else
            do! Async.Sleep (TimeSpan.FromSeconds 1)
            return Propulsion.Sinks.StreamResult.AllProcessed, () }
    let sink = Propulsion.Sinks.Factory.StartConcurrent(log, 2, 2, handle, stats)
    let dispose () =
        sink.Stop()
        sink.Await() |> Async.Catch |> Async.RunSynchronously |> ignore
    let mk p c: FsCodec.ITimelineEvent<_> = FsCodec.Core.TimelineEvent.Create(p, c, Propulsion.Sinks.EventBody.Empty)
    let items: Propulsion.Sinks.StreamEvent[] =
        [|  sid "a-ok", mk 0 "EventType"
            failingSid, mk 0 "EventType"
            stuckSid, mk 0 "EventType"  |]
    let crawl _ _ _ = TaskSeq.singleton <| struct (TimeSpan.FromSeconds 0.1, ({ items = items; isTail = true; checkpoint = Unchecked.defaultof<_> }: Core.Batch<_>))

    let extractHealthCheckExn (ex: Choice<_, exn>) =
        trap <@ match ex with
                | Choice2Of2 (:? Propulsion.Streams.HealthCheckException as e) -> e
                | x -> failwith $"unexpected $A{x}" @>

    [<Fact>]
    let run () = async {
        let source = Propulsion.Feed.Core.SinglePassFeedSource(log, TimeSpan.FromSeconds 5, SourceId.parse "sid", crawl, checkpoints, sink, string)
        let src = source.Start(fun _ct -> task { return [| TrancheId.parse "tid" |] })
        let! monEx = src.Monitor.AwaitCompletion(propagationDelay = TimeSpan.FromSeconds 1, awaitFullyCaughtUp = true) |> Propulsion.Internal.Async.ofTask |> Async.Catch
        let me = extractHealthCheckExn monEx

        // Await completion of processing; sink should yield exception, and Parallel should emit that
        let! e = [|  src.AwaitWithStopOnCancellation()
                     sink.AwaitWithStopOnCancellation() |]
                 |> Async.Parallel
                 |> Async.ignore<unit[]>
                 |> Async.Catch
        let pe = extractHealthCheckExn e
        // Apps will do this, so it should render reasonably
        log.Information(pe, "Stuck {stuck} Failing {failing}", pe.StuckStreams, pe.FailingStreams)
        test <@ let all = Seq.append pe.StuckStreams pe.FailingStreams
                pe.StuckStreams.Length = 1
                && pe.FailingStreams.Length = 1
                && all |> Seq.exists (fun struct (_s, age, _c) -> age > abendThreshold) @>
        test <@ Obj.isSame me pe @>
        test <@ Obj.isSame me (sink.Await() |> Async.Catch |> Async.RunSynchronously |> extractHealthCheckExn) @> }

    interface IDisposable with member _.Dispose() = dispose ()
