module Propulsion.DynamoStore.DynamoStoreIndex

[<Struct>]
type EventSpan =
    { i : int; c : string array }
    static member Create(index, eventTypes) = { i = index; c = eventTypes }
    member x.Index = x.i
    member x.Length = x.c.Length
    member x.Version = x.Index + x.Length

module EventSpan =

    let choose writePos (x : EventSpan) =
        if writePos <= x.Index then Some x
        elif writePos > x.Version then None
        else // Split required
            Some { i = writePos; c = Array.skip (x.Index - writePos) x.c }

module EventsQueue =

    open System.Collections.Generic

    /// Responsible for coalescing overlapping and/or adjacent spans
    /// Assumes, and upholds guarantee that input queue is ordered correctly
    let insert (y : EventSpan) (xs : EventSpan array) =
        if y.Length = 0 then invalidArg "y" "Can't be zero length"
        let acc = ResizeArray(xs.Length + 1)
        let mutable y, i = y, 0
        while i < xs.Length do
            let x = xs[i]
            if x.Version < y.Index then // x goes before, no overlap (bias to keep existing)
                acc.Add x
                i <- i + 1
            elif y.Version < x.Index then // y has new info => goes before, no overlap -> copy rest as a block
                acc.Add y
                acc.AddRange xs[i..] // trust the rest to already be minimal and not require coalescing
                i <- xs.Length + 1 // trigger exit without y being added twice
            else // there's an overlap - merge the existing span with the incoming one. Then wait for any successors that might also coalesce
                y <- if x.Index < y.Index then
                         EventSpan.Create(x.Index, Array.append x.c (Array.skip (min y.Length (x.Version - y.Index)) y.c)) // x goes first
                     else
                         EventSpan.Create(y.Index, Array.append y.c (Array.skip (min x.Length (y.Version - x.Index)) x.c)) // y goes first
                i <- i + 1 // mark x as consumed; shift to next
        if i = xs.Length then acc.Add y
        acc.ToArray()

    type StreamState = { writePos : int; spans : EventSpan array }

    type State() =

        let streams = Dictionary<string, StreamState>()
        let tryGet stream = match streams.TryGetValue stream with true, x -> ValueSome x | false, _ -> ValueNone

        let add removeReady stream span =
            match tryGet stream with
            | ValueNone ->
                let updated = if span.i = 0 && removeReady then { writePos = span.c.Length; spans = Array.empty }
                              else { writePos = 0; spans = Array.singleton span }
                streams.Add(stream, updated)
                Some updated
            | ValueSome v ->
                match EventSpan.choose v.writePos span with
                | None -> None // we've already written beyond the position of this span so nothing new to write
                | Some trimmed ->
                    let pass1 = { v with spans = insert trimmed v.spans }
                    let updated =
                        let head = Array.head pass1.spans
                        if removeReady && v.writePos = head.Index then { writePos = head.Version; spans = Array.tail pass1.spans }
                        else pass1
                    streams[stream] <- updated
                    if updated.spans.Length > 0 && updated.writePos = updated.spans[0].Index then Some updated
                    else None

        member _.LogIndexed(stream, span) =
            add true stream span |> ignore

        // Returns Span due to be written (if applicable)
        member _.IngestData(stream, span) =
            add false stream span

        member _.TryGetWritePos(stream) =
            tryGet stream |> ValueOption.map (fun x -> x.writePos)

        member _.Dump(log : Serilog.ILogger, gapsLimit) =
            let mutable totalS, totalE, incomplete, lim = 0, 0L, 0, gapsLimit
            for KeyValue (stream, v) in streams do
                totalS <- totalS + 1
                totalE <- totalE + int64 v.writePos
                if v.spans.Length > 0 && lim >= 0 then
                    if lim = 0 then
                        log.Error("Gapped Streams Dump limit ({gapsLimit}) reached; use commandline flag to show more", gapsLimit)
                    else
                        log.Warning("{stream} @{wp} Gap {g} Events {qi}", stream, v.writePos, v.spans[0].Index - v.writePos, v.spans[0].c)
                    lim <- lim - 1
                if v.spans.Length > 0 then incomplete <- incomplete + 1
            log.Information("Index State {streams:n0}s {events:n0}e Gapped Streams {incomplete:n0}", totalS, totalE, incomplete)

module Reader =

    let walk (log : Serilog.ILogger, storeLog, context) trancheId = async {
        let state = EventsQueue.State()
        let mutable more, epochId = true, AppendsEpochId.initial
        while more do
            let! closed, spans = Impl.loadIndexEpochSpans (log, storeLog) context trancheId epochId
            for x in spans do
                let stream = x.p |> IndexStreamId.toStreamName |> FsCodec.StreamName.toString
                if x.c.Length = 0 then log.Information("Stream {stream} contains zero length span", stream)
                else state.LogIndexed(stream, EventSpan.Create(int x.i, x.c))
            more <- closed
            epochId <- AppendsEpochId.next epochId
        return state }
