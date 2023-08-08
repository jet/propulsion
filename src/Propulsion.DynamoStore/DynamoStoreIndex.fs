module Propulsion.DynamoStore.DynamoStoreIndex

open Propulsion.Internal

/// Represents a (potentially coalesced) span of events as loaded from either the Index or a DynamoDB Export
type [<Struct>] EventSpan =
    { i : int; c : string[] }
    static member Create(index, eventTypes) = { i = index; c = eventTypes }
    member x.Index = x.i
    member x.Length = x.c.Length
    member x.Version = x.Index + x.Length

module StreamQueue =

    /// Given a span of events, select portion that's not already ingested (i.e. falls beyond our write Position)
    let inline internal chooseUnwritten writePos (x : EventSpan) =
        if writePos <= x.Index then ValueSome x
        elif writePos >= x.Version then ValueNone
        else ValueSome { i = writePos; c = Array.skip (writePos - x.Index) x.c }

    /// Responsible for coalescing overlapping and/or adjacent spans
    /// Requires, and ensures, that queue is ordered correctly before and afterwards
    let insert (y : EventSpan) (xs : EventSpan[]) =
        if y.Length = 0 then invalidArg (nameof y) "Can't be zero length"
        if xs = null then nullArg (nameof xs)
        if Array.isEmpty xs then Array.singleton y else

        let mutable acc = null
        let mutable y, i = y, 0
        while i < xs.Length do
            let x = xs[i]
            if x.Version < y.Index then // x goes before, no overlap
                if acc = null then acc <- ResizeArray(xs.Length + 1) // assume we'll be making it longer
                acc.Add x
                i <- i + 1
            elif y.Version >= x.Index then // there's an overlap - merge existing with incoming, await successors that might also coalesce
                y <- if x.Index < y.Index then // (bias to keep existing)
                         EventSpan.Create(x.Index, Array.append x.c (Array.skip (min y.Length (x.Version - y.Index)) y.c)) // x goes first
                     else
                         EventSpan.Create(y.Index, Array.append y.c (Array.skip (min x.Length (y.Version - x.Index)) x.c)) // y goes first
                i <- i + 1 // mark x as consumed; shift to next
            else // y has new info => goes before, no overlap -> copy rest as a block
                if acc = null then acc <- ResizeArray(xs.Length - i + 1)
                acc.Add y
                acc.AddRange(Seq.skip i xs) // trust the rest to already be minimal and not require coalescing
                i <- xs.Length + 1 // trigger exit without y being added twice
        if i <> xs.Length then acc.ToArray()
        // Add residual (iff we didn't already do so within the loop)
        elif acc = null then Array.singleton y
        else acc.Add y; acc.ToArray()

type [<Struct>] BufferStreamState = { writePos : int; spans : EventSpan[] }

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
            | ValueNone -> true, v // we've already written beyond the position of this span so nothing new to write
            | ValueSome trimmed ->
                if isIndex && trimmed.i <> v.writePos then false, v
                else
                    let pass1 = { v with spans = StreamQueue.insert trimmed v.spans }
                    let updated =
                        let s = pass1.spans
                        if not isIndex || v.writePos <> s[0].i then pass1
                        else { writePos = s[0].Version; spans = if s.Length = 1 then Array.empty else Array.tail s }
                    streams[stream] <- updated
                    true, updated

    member _.LogIndexed(stream, span) =
        let ok, v = add true stream span
        ok, v.writePos

    // Returns Span ready to be written (if applicable)
    member _.IngestData(stream, span) =
        let _ok, u = add false stream span
        u.spans |> Array.tryHead |> Option.filter (fun h -> h.Index = u.writePos)

    member val Items : System.Collections.Generic.IReadOnlyDictionary<_, _> = streams

module Reader =

    // Returns flattened list of all spans, and flag indicating whether tail reached
    let private loadIndexEpoch (log : Serilog.ILogger) (epochs : AppendsEpoch.Reader.Service) partitionId epochId
        : Async<AppendsEpoch.Events.StreamSpan[] * bool * int64> = async {
        let ts = Stopwatch.timestamp ()
        let! maybeStreamBytes, _version, state = epochs.Read(partitionId, epochId, 0)
        let sizeB, loadS = defaultValueArg maybeStreamBytes 0L, Stopwatch.elapsedSeconds ts
        let spans = state.changes |> Array.collect (fun struct (_i, spans) -> spans)
        let totalEvents = spans |> Array.sumBy (fun x -> x.c.Length)
        let totalStreams = spans |> AppendsEpoch.flatten |> Seq.length
        log.Information("Epoch {epochId} {totalE} events {totalS} streams ({spans} spans, {batches} batches, {k:n3} MiB) {loadS:n1}s",
                        string epochId, totalEvents, totalStreams, spans.Length, state.changes.Length, Log.miB sizeB, loadS)
        return spans, state.closed, sizeB }

    let loadIndex (log, storeLog, context) partitionId gapsLimit: Async<struct (Buffer * int64)> = async {
        let indexEpochs = AppendsEpoch.Reader.Factory.create storeLog context
        let mutable epochId, more, totalB, totalSpans = AppendsEpochId.initial, true, 0L, 0L
        let state = Buffer()
        let mutable invalidSpans = 0
        while more do
            let! spans, closed, streamBytes = loadIndexEpoch log indexEpochs partitionId epochId
            totalB <- totalB + streamBytes
            for { p = IndexStreamId.StreamName sn } as x in spans do
                let stream = FsCodec.StreamName.toString sn
                if x.c.Length = 0 then log.Warning("Stream {stream} contains zero length span", stream) else
                let ok, writePos = state.LogIndexed(stream, EventSpan.Create(int x.i, x.c))
                if not ok then
                    invalidSpans <- invalidSpans + 1
                    if invalidSpans = gapsLimit then
                        log.Error("Gapped Streams Dump limit ({gapsLimit}) reached; use commandline flag to show more", gapsLimit)
                    elif invalidSpans < gapsLimit then
                        log.Warning("Gapped Span in {stream}@{wp}: Missing {gap} events before {successorEventTypes}",
                                    stream, writePos, x.i - int64 writePos, x.c)
                 else totalSpans <- totalSpans + 1L
            more <- closed
            epochId <- AppendsEpochId.next epochId
        log.Information("Partition {partition} Current Index size {mib:n1} MiB; {gapped} Invalid spans",
                        string partitionId, Log.miB totalB, invalidSpans)
        return state, totalSpans }
