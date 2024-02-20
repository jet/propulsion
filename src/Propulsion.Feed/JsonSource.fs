namespace Propulsion.Feed

open FSharp.Control

/// <summary>Parses CR separated file with items dumped from a Cosmos Container containing Equinox Items<br/>
/// Such items can be extracted via Equinox.Tool via <c>eqx query -o JSONFILE cosmos</c>.</summary>
/// <remarks>Any alternate way way that yields the full JSON will also work ///  e.g. the cosmic tool at https://github.com/creyke/Cosmic <br/>
///   dotnet tool install -g cosmic <br/>
///   # then connect/select db per https://github.com/creyke/Cosmic#basic-usage <br/>
///   cosmic query 'select * from c order by c._ts' > file.out <br/>
/// </remarks>
type [<Sealed; AbstractClass>] JsonSource private () =

    static member Start(log, statsInterval, filePath, skip, parseFeedDoc, checkpoints, sink, ?truncateTo) =
        let isNonCommentLine (line: string) = System.Text.RegularExpressions.Regex.IsMatch(line, "^\s*#") |> not
        let truncate = match truncateTo with Some count -> Seq.truncate count | None -> id
        let lines = Seq.append (System.IO.File.ReadLines filePath |> truncate) (Seq.singleton null) // Add a trailing EOF sentinel so checkpoint positions can be line numbers even when finished reading
        let crawl _ _ _ = taskSeq {
            let mutable i = 0
            for line in lines do
                i <- i + 1
                let isEof = line = null
                if isEof || (i >= skip && isNonCommentLine line) then
                    let lineNo = int64 i + 1L
                    try let items = if isEof then Array.empty
                                    else System.Text.Json.JsonDocument.Parse line |> parseFeedDoc |> Seq.toArray
                        struct (System.TimeSpan.Zero, ({ items = items; isTail = isEof; checkpoint = Position.parse lineNo }: Core.Batch<_>))
                    with e -> raise <| exn($"File Parse error on L{lineNo}: '{line.Substring(0, 200)}'", e) }
        let source = Propulsion.Feed.Core.SinglePassFeedSource(log, statsInterval, SourceId.parse filePath, crawl, checkpoints, sink, string)
        source.Start(fun _ct -> task { return [| TrancheId.parse "0" |] })
