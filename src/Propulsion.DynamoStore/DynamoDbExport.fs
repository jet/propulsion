module Propulsion.DynamoStore.DynamoDbExport

open FSharp.Control
open System.IO
open System.Text.Json

type StreamSpan = AppendsEpoch.Events.StreamSpan

type [<Struct>] Line =  { Item : Item }
 and [<Struct>] Item = { p : StringVal; n : NumVal; c : ListVal<StringVal> }
 and [<Struct>] StringVal = { S : string }
 and [<Struct>] NumVal = { N : string }
 and ListVal<'t> = { L : 't[] }

let private read maxEventsCutoff (path : string) : AsyncSeq<StreamSpan array> = asyncSeq {
    use r = new StreamReader(path)
    let mutable more = true
    let mutable buffer, c = ResizeArray(), 0
    while more do
        let l = r.ReadLine()
        let i = JsonSerializer.Deserialize<Line>(l).Item
        let cs = if obj.ReferenceEquals(null, i.c) then Array.empty else [| for s in i.c.L -> s.S |]
        let index = int i.n.N - cs.Length
        let span : StreamSpan = { p = IndexStreamId.ofP i.p.S; i = index; c = cs }
        c <- c + span.c.Length
        if c > maxEventsCutoff && buffer.Count > 0 then
            yield buffer.ToArray()
            buffer.Clear()
            c <- 0
        buffer.Add(span)
        more <- not r.EndOfStream
    if buffer.Count > 0 then yield buffer.ToArray() }

type Importer(log, storeLog, context) =

    // Values up to 5 work reasonably, but side effects are:
    // - read usage is more 'lumpy'
    // - readers need more memory to hold the state
    // - Lambda startup time increases
    let epochCutoffMiB = 1
    // Should be large enough to accomodate state of 2 epochs
    // Note the backing memory is not preallocated, so the effects of this being too large will not be immediately apparent
    // (Overusage will hasten the Lambda being killed due to excess memory usage)
    let maxCacheMiB = 5
    let cache = Equinox.Cache("indexer", sizeMb = maxCacheMiB)
    let service = DynamoStoreIndexer(log, context, cache, epochBytesCutoff = epochCutoffMiB * 1024 * 1024)

    member _.VerifyAndOrImportDynamoDbJsonFile(trancheId, maxEventsCutoff, gapsLimit, path) = async {
        let! state, (totalB, totalSpans) = DynamoStoreIndex.Reader.loadIndex (log, storeLog, context) trancheId
        state.Dump(log, totalB, totalSpans, gapsLimit)
        match path with
        | None -> ()
        | Some path ->
            let ingest spans = service.IngestWithoutConcurrency(trancheId, spans)
            return!
                read maxEventsCutoff path
                |> AsyncSeq.iterAsync ingest
    }
