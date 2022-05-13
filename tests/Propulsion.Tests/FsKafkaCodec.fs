module Propulsion.Tests.FsKafkaCodec

open Swensen.Unquote
open Xunit

let [<Fact(*Explicit = true*)>] ``Characterization test to validate that underlying FsCodec lib has correct SourceLink config`` () =
    let res = Propulsion.Codec.NewtonsoftJson.Serdes.Serialize {| test = "value"|}
    res =! """{"test":"value"}"""
