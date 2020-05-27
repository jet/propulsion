namespace Propulsion.Sql.Tests

open System
open System.Threading.Tasks

open SqlStreamStore
open SqlStreamStore.Streams
open Propulsion.SqlStreamStore

type TestStore(store: IStreamStore, ledger: ILedger) =

    member this.Store = store
    member this.Ledger = ledger

    interface IDisposable with
        member this.Dispose() =
            store.Dispose()

module TestStore =

    let populate (data: seq<StreamId * NewStreamMessage>) (store: TestStore) =
        async {
            for streamId, msg in data do
                do! store.Store.AppendToStream(streamId.Value, ExpectedVersion.Any, msg)
                    |> Async.AwaitTask
                    |> Async.Ignore
            return store
        }

    [<RequireQualifiedAccess>]
    module InMemory =

        let createEmpty() =
            let store = new InMemoryStreamStore()
            let ledger = InMemoryLedger()
            new TestStore(store, ledger)


        let create (data: seq<StreamId * NewStreamMessage>) =
            async {
                let store = createEmpty()
                return! populate data store
            }

module Task =

    let waitIndefinitely (tcs: TaskCompletionSource<unit>) =
        tcs.Task.Result

    let waitOrFailMs (timeoutMs: int) (tcs: TaskCompletionSource<unit>) =
        let res =
            Task.WaitAny(
                [|
                    tcs.Task :> Task
                |], TimeSpan.FromMilliseconds(float timeoutMs))

        if res = -1 then
            failwith "Timeout expired while waiting for TCS to signal task completed"

    let waitOrFail tcs = waitOrFailMs 60000 tcs
