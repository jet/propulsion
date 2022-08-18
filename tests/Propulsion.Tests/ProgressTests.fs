module Propulsion.Tests.ProgressTests

open FsCodec
open Propulsion.Streams
open Swensen.Unquote
open System.Collections.Generic
open Xunit

let sn x = StreamName.create x x
let mkDictionary xs = Dictionary<StreamName,int64>(dict xs)

let [<Fact>] ``Empty has zero streams pending or progress to write`` () =
    let sut = Progress.ProgressState<_>()
    let queue = sut.InScheduledOrder(None)
    test <@ Seq.isEmpty queue @>

let [<Fact>] ``Can add multiple batches with overlapping streams`` () =
    let sut = Progress.ProgressState<_>()
    let noBatchesComplete () = failwith "No bathes should complete"
    sut.AppendBatch(noBatchesComplete, mkDictionary [sn "a",1L; sn "b",2L])
    sut.AppendBatch(noBatchesComplete, mkDictionary [sn "b",2L; sn "c",3L])

let [<Fact>] ``Marking Progress removes batches and triggers the callbacks`` () =
    let sut = Progress.ProgressState<_>()
    let mutable callbacks = 0
    let complete () = callbacks <- callbacks + 1
    sut.AppendBatch(complete, mkDictionary [sn "a",1L; sn "b",2L])
    sut.MarkStreamProgress(sn "a",1L) |> ignore
    sut.MarkStreamProgress(sn "b",3L) |> ignore
    1 =! callbacks

let [<Fact>] ``Empty batches get removed immediately`` () =
    let sut = Progress.ProgressState<_>()
    let mutable callbacks = 0
    let complete () = callbacks <- callbacks + 1
    sut.AppendBatch(complete, mkDictionary [||])
    sut.AppendBatch(complete, mkDictionary [||])
    2 =! callbacks

let [<Fact>] ``Marking progress is not persistent`` () =
    let sut = Progress.ProgressState<_>()
    let mutable callbacks = 0
    let complete () = callbacks <- callbacks + 1
    sut.AppendBatch(complete, mkDictionary [sn "a",1L])
    sut.MarkStreamProgress(sn "a",2L) |> ignore
    sut.AppendBatch(complete, mkDictionary [sn "a",1L; sn "b",2L])
    1 =! callbacks

// TODO: lots more coverage of newer functionality - the above were written very early into the exercise
