module Propulsion.Tests.DynamoImportTests

open Swensen.Unquote
open System.Diagnostics
open System.IO
open System.Text.Json
open Xunit

type Line = { Item : Item }
 and Item = { p : StringVal; i : NumVal; c : ListVal<StringVal> }
 and StringVal = { S : string }
 and NumVal = { N : string }
 and ListVal<'t> = { L : 't[] }

let [<Fact(*Explicit = true*)>] ``Can import DynamoDB JSON from S3 export`` () =
    use r = new StreamReader "/Users/rbartelink/Downloads/yksfpbkvjm2y5jt62n2z6ro3bu.json" // "/Users/rbartelink/Downloads/pjaelfxv5uzzdd6zbj6vyxqlxm.json"
    let mutable more, c = true, 0
    while more do
        let l = r.ReadLine()
        let i = JsonSerializer.Deserialize<Line>(l).Item
        let cs = if obj.ReferenceEquals(null, i.c) then Array.empty else [| for s in i.c.L -> s.S |]
        Trace.WriteLine($"p={i.p.S} i={int i.i.N} c={System.String.Join(',', cs )}")
        c <- c + cs.Length
        more <- not r.EndOfStream
    test <@ c = 0 @>

(*
let [<Fact(*Explicit = true*)>] ``full import`` () =
    let path = "/Users/rbartelink/Downloads/yksfpbkvjm2y5jt62n2z6ro3bu.json" // "/Users/rbartelink/Downloads/pjaelfxv5uzzdd6zbj6vyxqlxm.json"
    let (AppendsTrancheId.Parse tid) = TrancheId.parse "2"
    let indexer = ()
    DynamoExportIngester.run tid path indexer
*)
