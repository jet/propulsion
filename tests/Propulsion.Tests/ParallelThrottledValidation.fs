module Propulsion.Tests.ParallelThrottledValidation

open Swensen.Unquote
open Xunit

// Fixed in 6.0.5-beta.22329.3 and 6.0.6 (we explicitly target 6.0.7 in this test suite)
// 6.0.5 is built from a 6.0.4 tag so does not include the fix (but it would win over the beta version in semver)
let [<Fact>] ``Async.Parallel blows stack when cancelling many <= in FSharp.Core 6.0.5`` () =
    let gen (i: int) = async {
        if i = 0 then
            do! Async.Sleep 1000
            return failwith (string i)
        else do! Async.Sleep (i+3000) }
    let count = 3000
    let comps = Seq.init count gen
    let result = Async.Parallel(comps, 16) |> Async.Catch |> Async.RunSynchronously
    test <@ match result with
            | Choice2Of2 e -> int e.Message < count
            | x -> failwithf "unexpected %A" x @>
