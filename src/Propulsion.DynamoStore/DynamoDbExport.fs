module Propulsion.DynamoStore.DynamoDbExport

open System.Collections.Generic
open FSharp.Control
open System.IO
open System.Text.Json

module DynamoDbJsonParser =

    type [<Struct>] Line =  { Item : Item }
     and [<Struct>] Item = { p : StringVal; n : NumVal; c : ListVal<StringVal> }
     and [<Struct>] StringVal = { S : string }
     and [<Struct>] NumVal = { N : string }
     and ListVal<'t> = { L : 't[] }

    let read (path : string) : seq<string * DynamoStoreIndex.EventSpan> = seq {
        use r = new StreamReader(path)
        let mutable more = true
        while more do
            let line = r.ReadLine()
            let item = JsonSerializer.Deserialize<Line>(line).Item
            let eventTypes = if obj.ReferenceEquals(null, item.c) then Array.empty
                             else [| for s in item.c.L -> s.S |]
            let index = int item.n.N - eventTypes.Length
            yield item.p.S, { i = index; c = eventTypes }
            more <- not r.EndOfStream }

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
        let! buffer, (totalB, totalSpans) = DynamoStoreIndex.Reader.loadIndex (log, storeLog, context) trancheId
        buffer.Dump(log, Some totalB, totalSpans, gapsLimit)

        match path with
        | None -> () // no further work; exit
        | Some path ->

        let pending = Dictionary<string, DynamoStoreIndex.EventSpan>()
        let mutable emittedSpans = 0L
        let emit limit = async {
            let batch =
                let gen : seq<AppendsEpoch.Events.StreamSpan> = seq {
                    for KeyValue (stream, span) in pending ->
                        { p = IndexStreamId.ofP stream; i = span.Index; c = span.c }  }
                let mutable t = 0
                let fits (x : AppendsEpoch.Events.StreamSpan) =
                    t <- t + x.c.Length
                    t < limit
                gen |> Seq.takeWhile fits |> Seq.toArray
            do! service.IngestWithoutConcurrency(trancheId, batch)
            for ss in batch do
                buffer.LogIndexed(string ss.p, { i = int ss.i; c = ss.c })
                pending.Remove(string ss.p) |> ignore
            emittedSpans <- emittedSpans + batch.LongLength
            return pending.Values |> Seq.sumBy (fun x -> x.c.Length) }

        let mutable readyEvents = 0
        for stream, eventSpan in DynamoDbJsonParser.read path do
            match buffer.IngestData(stream, eventSpan) with
            | None -> ()
            | Some readySpan ->
                match pending.TryGetValue stream with
                | false, _ ->
                    pending.Add(stream, readySpan)
                | true, existing ->
                    readyEvents <- readyEvents - existing.Length
                    pending[stream] <- readySpan
                readyEvents <- readyEvents + readySpan.Length

                if readyEvents > maxEventsCutoff then
                    let! bufferedEvents = emit maxEventsCutoff
                    readyEvents <- bufferedEvents

        // Force emission of remainder, even if it's over the cutoff
        let! _ = emit System.Int32.MaxValue
        buffer.Dump(log, None, totalSpans + emittedSpans, gapsLimit) }
