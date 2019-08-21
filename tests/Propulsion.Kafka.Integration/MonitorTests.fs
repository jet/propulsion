module Propulsion.Kafka.Integration.MonitorTests

open Jet.ConfluentKafka.FSharp
open Propulsion.Kafka
open Propulsion.Kafka.MonitorImpl
open Swensen.Unquote
open System.Diagnostics
open Xunit

let partitionInfo partition consumerOffset lag : PartitionInfo =
    {   partition = partition
        consumerOffset = Valid consumerOffset
        earliestOffset = Valid 0L
        highWatermarkOffset = Valid 0L
        lag = lag }

let consumerInfo consumerOffset lag =
    [|  partitionInfo 0 consumerOffset lag // custom partition
        partitionInfo 1 consumerOffset 0L // non-lagging partition
    |] |> Window

// Tests based on https://github.com/linkedin/Burrow/wiki/Consumer-Lag-Evaluation-Rules#examples
// In all cases, we include a partition which has no lag, to ensure that the absence of lag on one
// partition does not cause the monitor to ignore lag on another partition.

let checkRules = Array.ofSeq << Rules.checkRulesForAllPartitions

[<Fact>]
let ``No errors because rule 1 is met`` () =
    let consumerInfos = [|
        consumerInfo 10L 0L
        consumerInfo 20L 0L
        consumerInfo 30L 0L
        consumerInfo 40L 0L
        consumerInfo 50L 0L
        consumerInfo 60L 0L
        consumerInfo 70L 1L
        consumerInfo 80L 3L
        consumerInfo 90L 5L
        consumerInfo 100L 5L
    |]

    let result = checkRules consumerInfos

    test
        <@ result =
                [|  0, OkReachedZero
                    1, OkReachedZero |] @>

[<Fact>]
let ``Error because rule 2 is violated`` () =
    let consumerInfos = [|
        consumerInfo 10L 1L
        consumerInfo 10L 1L
        consumerInfo 10L 1L
        consumerInfo 10L 1L
        consumerInfo 10L 1L
        consumerInfo 10L 2L
        consumerInfo 10L 2L
        consumerInfo 10L 2L
        consumerInfo 10L 3L
        consumerInfo 10L 3L
    |]

    let result = checkRules consumerInfos

    test <@ result =
                [|  0, ErrorPartitionStalled 3L
                    1, OkReachedZero |] @>

[<Fact>]
let ``Error because rule 3 is violated`` () =
    let consumerInfos = [|
        consumerInfo 10L 1L
        consumerInfo 20L 1L
        consumerInfo 30L 1L
        consumerInfo 40L 1L
        consumerInfo 50L 1L
        consumerInfo 60L 2L
        consumerInfo 70L 2L
        consumerInfo 80L 2L
        consumerInfo 90L 3L
        consumerInfo 100L 3L
    |]

    let result = checkRules consumerInfos

    test <@ result =
                [|  0, WarningLagIncreasing
                    1, OkReachedZero |] @>

[<Fact>]
let ``No error because rule 3 is not violated`` () =
    let consumerInfos = [|
        consumerInfo 10L 5L
        consumerInfo 20L 3L
        consumerInfo 30L 5L
        consumerInfo 40L 2L
        consumerInfo 50L 1L
        consumerInfo 60L 1L
        consumerInfo 70L 2L
        consumerInfo 80L 1L
        consumerInfo 90L 4L
        consumerInfo 100L 6L
    |]

    let result = checkRules consumerInfos

    test <@ result =
                [|  0, Healthy
                    1, OkReachedZero |] @>
