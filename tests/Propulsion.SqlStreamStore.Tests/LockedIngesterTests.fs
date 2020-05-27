module Propulsion.Sql.Tests.LockedIngesterTests

open System
open Xunit

open FsCheck
open Propulsion.Streams
open Propulsion.SqlStreamStore.LockedIngester

open System.Threading.Tasks

open Generators

let config =
    { Config.QuickThrowOnFailure with MaxTest = 10 }

[<Fact>]
let ``LockedIngester processes incoming batches in order`` () =
    let logger = Serilog.LoggerConfiguration().MinimumLevel.Debug().CreateLogger()

    let prop =
        Prop.forAll (Arb.fromGen batches) <| fun data ->
            async {
                let store = TestStore.InMemory.createEmpty()

                let outputs = ResizeArray()

                let tcs = TaskCompletionSource()

                let mutable remaining = data.Length

                let submit =
                    fun (pos, commit, items) ->
                        async {
                            try
                                outputs.Add(pos, items)
                                do! commit
                                return 0, 0 // write result is ignored
                            finally
                                remaining <- remaining - 1
                                if remaining = 0 then
                                    tcs.SetResult()
                        }

                let ingester = new LockedIngester(logger, store.Ledger, submit, "test", "$all")

                ingester.Start()
                |> Async.Start

                for batch in data do
                    do! ingester.SubmitBatch(batch)

                if data.Length > 0 then
                    do Task.waitOrFail tcs

                    // checks specifically that batches are submitted in order.
                    let zipped =
                        Seq.zip data outputs
                        |> Array.ofSeq

                    for (input, (outputPos, outputItems)) in zipped do
                        Xunit.Assert.Equal(input.firstPosition, outputPos)

                        Xunit.Assert.Equal<StreamEvent<_>>(input.messages, outputItems)

                    // checks a more general property that epoch numbers are monotonically growing.
                    Xunit.Assert.True(Seq.map fst outputs |> Seq.pairwise |> Seq.forall (fun (pred, succ) -> pred < succ))
            }
            |> Async.RunSynchronously

    Check.One(config, prop)

[<Fact>]
let ``LockedIngester doesn't process overlapping work`` () =
    let logger = Serilog.LoggerConfiguration().MinimumLevel.Debug().CreateLogger()

    let generator =
        gen {
            let! batches = batches
            let! commitedPosition =
                Gen.choose(0, batches |> Seq.sumBy (fun b -> b.Length))

            return batches, commitedPosition
        }

    let prop =
        Prop.forAll (Arb.fromGen generator) <| fun (data, pos) ->
            async {
                let store = TestStore.InMemory.createEmpty()

                store.Ledger.CommitPosition({ Stream = "$all"; ConsumerGroup = "test"; Position = Nullable(int64 pos) })
                |> Async.RunSynchronously

                let outputs = ResizeArray()

                let tcs = TaskCompletionSource()

                let mutable remaining =
                    data
                    |> Seq.filter (fun batch -> batch.lastPosition > int64 pos)
                    |> Seq.length

                let submit =
                    fun (pos, commit, items) ->
                        async {
                            try
                                outputs.Add(items)
                                do! commit
                                return 0, 0 // write result is ignored
                            finally
                                remaining <- remaining - 1
                                if remaining = 0 then
                                    tcs.SetResult()
                        }

                let ingester = new LockedIngester(logger, store.Ledger, submit, "test", "$all")

                ingester.Start()
                |> Async.Start

                for batch in data do
                    do! ingester.SubmitBatch(batch)

                let lastPossiblePosition =
                    let length = data |> Seq.sumBy (fun b -> b.Length)
                    length - 1

                if data.Length > 0 && pos < lastPossiblePosition then
                    do Task.waitIndefinitely tcs

                    let processedItems =
                        [|
                            for batch in outputs do
                                for event in batch do
                                    yield event.event.EventId
                        |]
                        |> Set.ofArray

                    let expected =
                        [|
                            for batch in data do
                                for msg in batch.messages do
                                    if msg.event.Index > int64 pos then
                                        yield msg.event.EventId
                        |]
                        |> Set.ofSeq

                    Xunit.Assert.Equal<Guid>(expected, processedItems)
            }
            |> Async.RunSynchronously

    Check.One(config, prop)
