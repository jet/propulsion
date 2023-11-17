module Propulsion.Tests.AppendsIndexTests

open FSharp.UMX
open Propulsion.DynamoStore
open Propulsion.DynamoStore.AppendsIndex
open Swensen.Unquote
open Xunit

let u8 = System.Text.Encoding.UTF8

[<Fact>]
let ``serializes, writing as expected`` () =
    let enc = Events.codec.Encode((), Events.Started { partition = AppendsPartitionId.wellKnownId; epoch = % 2 })
    let struct (_, body) = enc.Data
    let body = u8.GetString(body.Span)
    test <@ body = """{"partition":0,"tranche":null,"epoch":2}""" @>

[<Fact>]
let ``deserializes, with upconversion`` () =
    let data = struct (0, System.ReadOnlyMemory(u8.GetBytes("""{"tranche":3,"epoch":2}""")))
    let e = FsCodec.Core.TimelineEvent.Create(0, "Started", data)
    let dec = Events.codec.Decode e
    let expected = Events.Started { partition = % 3 ; epoch = % 2 }
    test <@ ValueSome expected = dec @>

[<FsCheck.Xunit.Property>]
let roundtrips value =
    let e = Events.codec.Encode((), value)
    let t = FsCodec.Core.TimelineEvent.Create(-1L, e.EventType, e.Data)
    let decoded = Events.codec.Decode t
    test <@ ValueSome value = decoded @>
