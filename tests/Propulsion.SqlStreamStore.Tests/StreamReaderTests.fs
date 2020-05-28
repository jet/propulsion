module Propulsion.Sql.Tests.StreamReaderTests

open System
open Xunit

open FsCheck
open Propulsion.SqlStreamStore.StreamReader
open System.Threading.Tasks

open Propulsion.Sql.Tests.Generators

let config =
    { Config.QuickThrowOnFailure with MaxTest = 10 }

[<Fact>]
let ``StreamReader processes the stream fully - single batch`` () =
    let logger = Serilog.LoggerConfiguration().MinimumLevel.Debug().CreateLogger()

    let prop =
        Prop.forAll (Arb.fromGen streams) <| fun data ->
            async {
                let! store = TestStore.InMemory.create data

                let outputs = ResizeArray()

                let tcs = TaskCompletionSource()

                let submit =
                    fun batch ->
                        async {
                            do outputs.Add(batch)
                            tcs.SetResult()
                        }

                let reader =
                    StreamReader(logger, store.Store, submit, data.Length, TimeSpan.FromMilliseconds(10.))

                reader.Start(Nullable())
                |> Async.Start

                if data.Length > 0 then
                    do Task.waitOrFail tcs

                    let batch = outputs.[0]

                    Xunit.Assert.Equal(0, int batch.firstPosition)
                    Xunit.Assert.Equal(data.Length - 1, int batch.lastPosition)
                    Xunit.Assert.True(batch.isEnd)
                    Xunit.Assert.Equal(data.Length, batch.Length)
            }
            |> Async.RunSynchronously

    Check.One(config, prop)

[<Fact>]
let ``StreamReader processes the stream fully - multiple batches and non-zero starting position`` () =
    let logger = Serilog.LoggerConfiguration().MinimumLevel.Debug().CreateLogger()

    let generator =
        gen {
            let! data = streams

            // generate a batch size between 1 and 3/4 of the input stream length
            let! batchSize =
                Gen.choose (1, max 1 (int (0.75 * float data.Length)))

            let! initialPos =
                Gen.choose (-1, int (0.25 * float data.Length))

            return data, batchSize, initialPos
        }

    let prop =
        Prop.forAll (Arb.fromGen generator) <| fun (data, batchSize, initialPos) ->
            async {
                let! store = TestStore.InMemory.create data

                let outputs = ResizeArray()

                let tcs = TaskCompletionSource()

                let submit =
                    fun batch ->
                        async {
                            do outputs.Add(batch)
                            if batch.isEnd then
                                tcs.SetResult()
                        }

                let reader =
                    StreamReader(logger, store.Store, submit, batchSize, TimeSpan.FromMilliseconds(10.))

                reader.Start(if initialPos = -1 then Nullable() else Nullable(int64 initialPos))
                |> Async.Start

                let remaining = data.Length - (initialPos + 1)

                if remaining > 0 then
                    do Task.waitOrFail tcs

                    Xunit.Assert.Equal(initialPos + 1, int outputs.[0].firstPosition)
                    Xunit.Assert.Equal(data.Length - 1, int outputs.[outputs.Count - 1].lastPosition)

                    Xunit.Assert.Equal(remaining, outputs |> Seq.sumBy (fun x -> x.Length))

            }
            |> Async.RunSynchronously

    Check.One(config, prop)

[<Fact>]
let ``StreamReader processes the stream fully - multiple batches over time`` () =
    let logger = Serilog.LoggerConfiguration().MinimumLevel.Debug().CreateLogger()

    let generator =
        gen {
            let! numberOfBatches = Gen.choose (2, 10)

            let! data =
                Gen.listOfLength numberOfBatches streams

            return data
        }

    let prop =
        Prop.forAll (Arb.fromGen generator) <| fun data ->
            async {
                let store = TestStore.InMemory.createEmpty ()

                let outputs = ResizeArray()

                let tcs = TaskCompletionSource()

                let totalLength = data |> Seq.sumBy List.length

                let mutable processed = 0

                let submit =
                    fun (batch: InternalBatch) ->
                        async {
                            do outputs.Add(batch)
                            processed <- processed + batch.Length

                            if processed = totalLength then
                                tcs.SetResult()
                        }

                let reader =
                    StreamReader(logger, store.Store, submit, sleepInterval = TimeSpan.FromMilliseconds(10.))

                reader.Start(Nullable())
                |> Async.Start

                async {
                    let arr = Array.ofList data

                    for i in 0 .. arr.Length - 1 do
                        do! TestStore.populate arr.[i] store |> Async.Ignore
                        do! Async.Sleep(10)
                }
                |> Async.Start

                if data.Length > 0 then
                    do Task.waitOrFail tcs

                    Xunit.Assert.Equal(0, int outputs.[0].firstPosition)
                    Xunit.Assert.Equal(totalLength - 1, int outputs.[outputs.Count - 1].lastPosition)

                    Xunit.Assert.Equal(totalLength, outputs |> Seq.sumBy (fun x -> x.Length))

            }
            |> Async.RunSynchronously

    Check.One(config, prop)
