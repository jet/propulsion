module Propulsion.DynamoStore.DynamoStoreIndex

/// Represents a (potentially coalesced) span of events as loaded from either the Index or a DynamoDB Export
[<Struct>]
type EventSpan =
    { i : int; c : string array }
    static member Create(index, eventTypes) = { i = index; c = eventTypes }
    member x.Index = x.i
    member x.Length = x.c.Length
    member x.Version = x.Index + x.Length

module Buffer =

    /// Given a span of events, select portion that's not already ingested (i.e. falls beyond our write Position)
    let inline private chooseUnwritten writePos (x : EventSpan) =
        if writePos <= x.Index then Some x
        elif writePos > x.Version then None
        else Some { i = writePos; c = Array.skip (x.Index - writePos) x.c }

    /// Responsible for coalescing overlapping and/or adjacent spans
    /// Requires, and ensures, that queue is ordered correctly before and afterwards
    let insert (y : EventSpan) (xs : EventSpan array) =
        if y.Length = 0 then invalidArg "y" "Can't be zero length"
        let acc = ResizeArray(xs.Length + 1)
        let mutable y, i = y, 0
        while i < xs.Length do
            let x = xs[i]
            if x.Version < y.Index then // x goes before, no overlap
                acc.Add x
                i <- i + 1
            elif y.Version >= x.Index then // there's an overlap - merge existing with incoming, await successors that might also coalesce
                y <- if x.Index < y.Index then // (bias to keep existing)
                         EventSpan.Create(x.Index, Array.append x.c (Array.skip (min y.Length (x.Version - y.Index)) y.c)) // x goes first
                     else
                         EventSpan.Create(y.Index, Array.append y.c (Array.skip (min x.Length (y.Version - x.Index)) x.c)) // y goes first
                i <- i + 1 // mark x as consumed; shift to next
            else // y has new info => goes before, no overlap -> copy rest as a block
                acc.Add y
                acc.AddRange(Seq.skip i xs) // trust the rest to already be minimal and not require coalescing
                i <- xs.Length + 1 // trigger exit without y being added twice
        if i = xs.Length then acc.Add y // Add residual (iff we didn't already do so within the loop)
        acc.ToArray()

    type StreamState = { writePos : int; spans : EventSpan array }

    type State() =

        let streams = System.Collections.Generic.Dictionary<string, StreamState>()
        let tryGet stream = match streams.TryGetValue stream with true, x -> Some x | false, _ -> None

        let add removeReady stream span =
            match tryGet stream with
            | None ->
                let updated = if removeReady && span.i = 0 then { writePos = span.Version; spans = Array.empty }
                              else { writePos = 0; spans = Array.singleton span }
                streams.Add(stream, updated)
                Some updated
            | Some v ->
                match chooseUnwritten v.writePos span with
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

        // Returns Span ready to be written (if applicable)
        member _.IngestData(stream, span) =
            add false stream span

        member _.TryGetWritePos(stream) =
            tryGet stream |> Option.map (fun x -> x.writePos)

        member _.Dump(log : Serilog.ILogger, totalSizeB : int64, totalSpans, gapsLimit) =
            let mutable totalS, totalE, incomplete, lim = 0, 0L, 0, gapsLimit
            for KeyValue (stream, v) in streams do
                totalS <- totalS + 1
                totalE <- totalE + int64 v.writePos
                if v.spans.Length > 0 && lim >= 0 then
                    if lim = 0 then log.Error("Gapped Streams Dump limit ({gapsLimit}) reached; use commandline flag to show more", gapsLimit)
                    else log.Warning("Stream {stream}@{wp}: Missing {gap} events before {successorEventTypes}",
                                     stream, v.writePos, v.spans[0].Index - v.writePos, v.spans[0].c)
                    lim <- lim - 1
                if v.spans.Length > 0 then incomplete <- incomplete + 1
            let level = if incomplete > 0 then Serilog.Events.LogEventLevel.Warning else Serilog.Events.LogEventLevel.Information
            log.Write(level, "TOTAL {events:n0} events {streams:n0} streams ({spans:n0} spans, {mib:n0} MiB) Gapped Streams {incomplete:n0}",
                      totalE, totalS, totalSizeB, totalSpans, float totalSizeB / 1024. / 1024., incomplete)

module Reader =

    // Returns flattened list of all spans, and flag indicating whether tail reached
    let private loadIndexEpoch (log : Serilog.ILogger) (epochs : AppendsEpoch.Reader.Service) trancheId epochId
        : Async<AppendsEpoch.Events.StreamSpan array * bool * int64> = async {
        let sw = System.Diagnostics.Stopwatch.StartNew()
        let! maybeStreamBytes, _version, state = epochs.Read(trancheId, epochId, 0)
        let sizeB, t = defaultArg maybeStreamBytes 0L, sw.Elapsed
        let spans = state.changes |> Array.collect (fun struct (_i, spans) -> spans)
        let totalEvents = spans |> Array.sumBy (fun x -> x.c.Length)
        let totalStreams = spans |> AppendsEpoch.flatten |> Seq.length
        log.Information("Tranche {trancheId} Epoch {epochId} {totalE} events {totalS} streams ({spans} spans, {batches} batches, {k} KiB) {loadS:n1}s",
                        string trancheId, string epochId, totalEvents, totalStreams, spans.Length, state.changes.Length, sizeB / 1024L, t.TotalSeconds)
        return spans, state.closed, sizeB }

    let loadIndex (log, storeLog, context) trancheId : Async<Buffer.State * (int64 * int64)> = async {
        let indexEpochs = AppendsEpoch.Reader.Config.create storeLog context
        let mutable epochId, more, totalB, totalSpans = AppendsEpochId.initial, true, 0L, 0L
        let state = Buffer.State()
        while more do
            let! spans, closed, streamBytes = loadIndexEpoch log indexEpochs trancheId epochId
            totalB <- totalB + streamBytes; totalSpans <- totalSpans + spans.LongLength
            for x in spans do
                let stream = x.p |> IndexStreamId.toStreamName |> FsCodec.StreamName.toString
                if x.c.Length = 0 then log.Warning("Stream {stream} contains zero length span", stream)
                else state.LogIndexed(stream, EventSpan.Create(int x.i, x.c))
            more <- closed
            epochId <- AppendsEpochId.next epochId
        return state, (totalB, totalSpans) }
