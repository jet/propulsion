module Propulsion.DynamoStore.DynamoStoreIndex

/// Represents a (potentially coalesced) span of events as loaded from either the Index or a DynamoDB Export
[<Struct>]
type EventSpan =
    { i : int; c : string array }
    static member Create(index, eventTypes) = { i = index; c = eventTypes }
    member x.Index = x.i
    member x.Length = x.c.Length
    member x.Version = x.Index + x.Length

module StreamQueue =

    /// Given a span of events, select portion that's not already ingested (i.e. falls beyond our write Position)
    let inline internal chooseUnwritten writePos (x : EventSpan) =
        if writePos <= x.Index then Some x
        elif writePos >= x.Version then None
        else Some { i = writePos; c = Array.skip (writePos - x.Index) x.c }

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

type BufferStreamState = { writePos : int; spans : EventSpan array }

type Buffer() =

    let streams = System.Collections.Generic.Dictionary<string, BufferStreamState>()

    let add isIndex stream span =
        match streams.TryGetValue stream with
        | false, _ ->
            if isIndex && span.i <> 0 then
                false, { writePos = 0; spans = Array.empty }  // Flag notification missing predecessor events
            else
                let wp, spans = if span.i = 0 && isIndex then span.Version,    Array.empty
                                                         else 0,               Array.singleton span
                let updated = { writePos = wp; spans = spans }
                streams.Add(stream, updated)
                true, updated
        | true, v ->
            match StreamQueue.chooseUnwritten v.writePos span with
            | None -> true, v // we've already written beyond the position of this span so nothing new to write
            | Some trimmed ->
                if isIndex && trimmed.i <> v.writePos then false, v
                else
                    let pass1 = { v with spans = StreamQueue.insert trimmed v.spans }
                    let updated =
                        if not isIndex || v.writePos <> pass1.spans[0].i then pass1
                        else { writePos = pass1.spans[0].Version; spans = Array.tail pass1.spans }
                    streams[stream] <- updated
                    true, updated

    member _.LogIndexed(stream, span) =
        let ok, v = add true stream span
        ok, v.writePos

    // Returns Span ready to be written (if applicable)
    member _.IngestData(stream, span) =
        let _ok, updated = add false stream span
        if updated.spans.Length > 0 && updated.writePos = updated.spans[0].Index then Some updated.spans[0]
        else None

    member val Streams : System.Collections.Generic.IReadOnlyDictionary<_, _> = streams

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
        log.Information("Epoch {epochId} {totalE} events {totalS} streams ({spans} spans, {batches} batches, {k:n3} MiB) {loadS:n1}s",
                        string epochId, totalEvents, totalStreams, spans.Length, state.changes.Length, float sizeB / 1024. / 1024., t.TotalSeconds)
        return spans, state.closed, sizeB }

    let loadIndex (log, storeLog, context) trancheId gapsLimit: Async<Buffer * int64> = async {
        let indexEpochs = AppendsEpoch.Reader.Config.create storeLog context
        let mutable epochId, more, totalB, totalSpans = AppendsEpochId.initial, true, 0L, 0L
        let state = Buffer()
        let mutable invalidSpans = 0
        while more do
            let! spans, closed, streamBytes = loadIndexEpoch log indexEpochs trancheId epochId
            totalB <- totalB + streamBytes
            for x in spans do
                let stream = x.p |> IndexStreamId.toStreamName |> FsCodec.StreamName.toString
                if x.c.Length = 0 then log.Warning("Stream {stream} contains zero length span", stream) else
                let ok, writePos = state.LogIndexed(stream, EventSpan.Create(int x.i, x.c))
                if not ok then
                    invalidSpans <- invalidSpans + 1
                    if invalidSpans = gapsLimit then log.Error("Gapped Streams Dump limit ({gapsLimit}) reached; use commandline flag to show more", gapsLimit)
                    elif invalidSpans < gapsLimit then log.Warning("Gapped Span in {stream}@{wp}: Missing {gap} events before {successorEventTypes}",
                                                                   stream, writePos, x.i - int64 writePos, x.c)
                 else totalSpans <- totalSpans + 1L
            more <- closed
            epochId <- AppendsEpochId.next epochId
        let totalMib = float totalB / 1024. / 1024.
        log.Information("Tranche {tranche} Current Index size {mib:n1} MiB; {gapped} Invalid spans", string trancheId, totalMib, invalidSpans)
        return state, totalSpans }
