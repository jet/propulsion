namespace Propulsion

open System.Runtime.InteropServices
open Propulsion.Internal

type StreamFilter([<Optional>] allowCats, [<Optional>] denyCats, [<Optional>] allowSns, [<Optional>] denySns,
                  [<Optional>] allowEts, [<Optional>] denyEts,
                  [<Optional; DefaultParameterValue(false)>] ?incIndexes,
                  [<Optional; DefaultParameterValue(null)>] ?log) =
    let log = lazy defaultArg log Serilog.Log.Logger
    let defA x = match x with null -> Array.empty | xs -> Seq.toArray xs

    let allowCats, denyCats, incIndexes = defA allowCats, defA denyCats, defaultArg incIndexes false
    let allowSns, denySns = defA allowSns, defA denySns
    let allowEts, denyEts = defA allowEts, defA denyEts
    let isPlain = Seq.forall (fun x -> System.Char.IsLetterOrDigit x || x = '_')
    let asRe = Seq.map (fun x -> if isPlain x then $"^{x}$" else x)
    let (|Filter|) exprs =
        let values, pats = Seq.partition isPlain exprs
        let valuesContains = let set = System.Collections.Generic.HashSet(values) in set.Contains
        let aPatternMatches (x: string) = pats |> Seq.exists (fun p -> System.Text.RegularExpressions.Regex.IsMatch(x, p))
        fun cat -> valuesContains cat || aPatternMatches cat
    let filter map (allow, deny) =
        match allow, deny with
        | [||], [||] -> fun _ -> true
        | Filter includes, Filter excludes -> fun x -> let x = map x in (Array.isEmpty allow || includes x) && not (excludes x)
    let validStream = filter FsCodec.StreamName.toString (allowSns, denySns)
    let isTransactionalStream (sn: FsCodec.StreamName) = let sn = FsCodec.StreamName.toString sn in not (sn.StartsWith('$'))

    member _.CreateStreamFilter([<Optional>] maybeCategories) =
        let handlerCats = defA maybeCategories
        let allowCats = Array.append handlerCats allowCats
        let validCat = filter FsCodec.StreamName.Category.ofStreamName (allowCats, denyCats)
        let allowCats = match allowCats with [||] -> [| ".*" |] | xs -> xs
        let denyCats = if incIndexes then denyCats else Array.append denyCats [| "^\$" |]
        let allowSns, denySns = match allowSns, denySns with [||], [||] -> [|".*"|], [||] | x -> x
        let allowEts, denyEts = match allowEts, denyEts with [||], [||] -> [|".*"|], [||] | x -> x
        log.Value.Information("Categories ☑️ {@allowCats} 🚫{@denyCats} Streams ☑️ {@allowStreams} 🚫{denyStreams} Events ☑️ {allowEts} 🚫{@denyEts}",
                              asRe allowCats, asRe denyCats, asRe allowSns, asRe denySns, asRe allowEts, asRe denyEts)
        fun sn ->
            validCat sn
            && validStream sn
            && (incIndexes || isTransactionalStream sn)

    member val EventFilter = filter (fun (x: Propulsion.Sinks.Event) -> x.EventType) (allowEts, denyEts)
