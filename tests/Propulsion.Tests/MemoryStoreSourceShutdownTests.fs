module Propulsion.Tests.MemoryStoreSourceShutdownTests

open Propulsion.Internal
open System
open System.Threading
open Serilog
open Swensen.Unquote
open Xunit

type Scenario(testOutput) =

    // state interval = 1s (also used as ingesterStateInterval), stats interval = 0.5s
    let stateInterval = TimeSpan.seconds 1000
    let statsInterval = TimeSpan.ms 500
    let abendThreshold = TimeSpan.ms 500

    let mutable trackingPhase = 0
    let mutable ingesterStoppedSeen = false
    let mutable logsAfterGrace = 0

    // Intercepts all logs after shutdown starts.
    // Phase 1 = grace window; Phase 2 = post-grace failsafe window where we calidate no futher inout
    let countingSink =
        { new Core.ILogEventSink with
            member _.Emit e =
                match Volatile.Read &trackingPhase with
                | 1 -> if e.MessageTemplate.Text.Contains "ingester stopped" then
                            Volatile.Write(&ingesterStoppedSeen, true)
                | 2 -> Interlocked.Increment &logsAfterGrace |> ignore
                | 0 | _ -> () }
    let log = LoggerConfiguration().WriteTo.Sink(TestOutputLogger testOutput).WriteTo.Sink(countingSink).CreateLogger()

    let stats =
        { new Propulsion.Streams.Stats<_>(log, statsInterval, stateInterval, abendThreshold = abendThreshold)
          with member _.HandleOk _ = ()
               member _.HandleExn(_, _) = () }

    let handle _sn _events = async { return failwith "forced failure" }
    // Pass ingesterStateInterval explicitly so the Ingester logs every stateInterval (1s)
    let sink = Propulsion.Sinks.Factory.StartConcurrent(log, 2, 2, handle, stats, ingesterStateInterval = stateInterval)

    [<Fact>]
    let ``MemoryStoreSource stops all components on pipeline shutdown`` () = async {
        let store = Equinox.MemoryStore.VolatileStore<FsCodec.Encoded>()
        let src = Propulsion.MemoryStore.MemoryStoreSource(log, store, [| "Cat" |], sink).Start()

        // Submit an event to trigger the failing handler
        let event = FsCodec.Core.TimelineEvent.Create(0L, "EventType", FsCodec.Encoding.OfBlob ReadOnlyMemory.Empty)
        store.TrySync("Cat-stream1", "Cat", "stream1", 0, [| event |]) |> ignore

        // Wait for sink to fail (HealthCheckException triggered when handler keeps failing beyond abendThreshold)
        let! caught = sink.AwaitWithStopOnCancellation() |> Async.Catch
        test <@ match caught with Choice2Of2 (:? Propulsion.Streams.HealthCheckException) -> true | _ -> false @>

        // Start tracking before shutdown so we can observe the ingester stop message.
        Volatile.Write(&trackingPhase, 1)

        // Stop and await shutdown of the source
        src.Stop()
        do! src.Await()

        // Allow 500ms for the Ingester's shutdown stats dump to complete, and require the explicit stop log.
        do! Async.Sleep(TimeSpan.ms 500)
        test <@ ingesterStoppedSeen @>

        // After the grace period, any log output is a regression.
        Volatile.Write(&trackingPhase, 2)

        // Wait > 2 stateIntervals: a still-running Ingester would log ~2 more times.
        do! Async.Sleep(TimeSpan.ms 2500)
        test <@ logsAfterGrace = 0 @>
    }
