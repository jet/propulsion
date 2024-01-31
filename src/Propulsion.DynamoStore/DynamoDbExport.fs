module Propulsion.DynamoStore.DynamoDbExport

open System.Collections.Generic
open System.IO
open System.Text.Json

module DynamoDbJsonParser =

    type [<Struct>] Line = { Item: Item }
     and [<Struct>] Item = { p: StringVal; n: NumVal; c: ListVal<StringVal> }
     and [<Struct>] StringVal = { S: string }
     and [<Struct>] NumVal = { N: string }
     and ListVal<'t> = { L: 't[] }

    let read (path: string): seq<struct (string * DynamoStoreIndex.EventSpan)> = seq {
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

/// Manages import of DynamoDB JSON files (extracted from data/*.json.gz in a DynamoDB S3 export)
type Importer(buffer: DynamoStoreIndex.Buffer, emit, dump) =

    let pending = Dictionary<string, DynamoStoreIndex.EventSpan>()

    let mutable totalIngestedSpans = 0L
    let dump () = dump totalIngestedSpans

    let flush eventsToWriteLimit = async {
        let batch =
            let gen: seq<AppendsEpoch.Events.StreamSpan> = seq {
                for KeyValue (stream, span) in pending ->
                    { p = IndexStreamId.ofP stream; i = span.Index; c = span.c }  }
            let mutable t = 0
            let fits (x: AppendsEpoch.Events.StreamSpan) =
                t <- t + x.c.Length
                t <= eventsToWriteLimit
            gen |> Seq.takeWhile fits |> Seq.toArray
        do! emit batch
        for streamSpan in batch do
            let _ok, _ = buffer.LogIndexed(string streamSpan.p, { i = int streamSpan.i; c = streamSpan.c })
            pending.Remove(string streamSpan.p) |> ignore
        totalIngestedSpans <- totalIngestedSpans + batch.LongLength
        return pending.Values |> Seq.sumBy _.c.Length }

    /// Ingest a file worth of data, flushing whenever we've accumulated enough pending data to be written
    member _.IngestDynamoDbJsonFile(file, bufferedEventsFlushThreshold) = async {
        let mutable readyEvents = 0
        let mutable items, events = 0L, 0L
        for stream, eventSpan in DynamoDbJsonParser.read file do
            items <- items + 1L; events <- events + int64 eventSpan.Length
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

                if readyEvents > bufferedEventsFlushThreshold then
                    let! bufferedEvents = flush bufferedEventsFlushThreshold
                    readyEvents <- bufferedEvents
        dump ()
        return {| items = items; events = events |} }

    /// Attempt emission of remaining buffered items
    member _.Flush() = async {
        let! _ = flush System.Int32.MaxValue
        dump () }
