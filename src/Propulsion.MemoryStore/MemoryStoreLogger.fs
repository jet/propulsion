module Propulsion.MemoryStore.MemoryStoreLogger

open System
open System.Threading

let private propEvents name (xs : System.Collections.Generic.KeyValuePair<string,string> seq) (log : Serilog.ILogger) =
    let items = seq { for kv in xs do yield sprintf "{\"%s\": %s}" kv.Key kv.Value }
    log.ForContext(name, sprintf "[%s]" (String.concat ",\n\r" items))

let private propEventJsonUtf8 name (events : FsCodec.ITimelineEvent<ReadOnlyMemory<byte>>[]) (log : Serilog.ILogger) =
    log |> propEvents name (seq {
        for e in events do
            let d = e.Data
            if not d.IsEmpty then System.Collections.Generic.KeyValuePair<_,_>(e.EventType, System.Text.Encoding.UTF8.GetString d.Span) })

let renderSubmit (log : Serilog.ILogger) struct (epoch, categoryName, streamId, events: FsCodec.ITimelineEvent<'F>[]) =
    if log.IsEnabled Serilog.Events.LogEventLevel.Verbose then
        let log =
            if (not << log.IsEnabled) Serilog.Events.LogEventLevel.Debug then log
            elif typedefof<'F> <> typeof<ReadOnlyMemory<byte>> then log
            else log |> propEventJsonUtf8 "Json" (unbox events)
        let types = events |> Seq.map (fun e -> e.EventType)
        log.ForContext("types", types).Debug("Submit #{epoch} {categoryName}-{streamId}x{count}", epoch, categoryName, streamId, events.Length)
    elif log.IsEnabled Serilog.Events.LogEventLevel.Debug then
        let types = seq { for e in events -> e.EventType } |> Seq.truncate 5
        log.Debug("Submit #{epoch} {categoryName}-{streamId}x{count} {types}", epoch, categoryName, streamId, events.Length, types)
let renderCompleted (log : Serilog.ILogger) struct (epoch, categoryName, streamId) =
    log.Verbose("Done!  #{epoch} {categoryName}-{streamId}", epoch, categoryName, streamId)

/// Wires specified <c>Observable</c> source (e.g. <c>VolatileStore.Committed</c>) to the Logger
let subscribe log source =
    let mutable epoch = -1L
    let aux struct (sn, events) =
        let struct (categoryName, streamId) = FsCodec.StreamName.split sn
        let epoch = Interlocked.Increment &epoch
        renderSubmit log (epoch, categoryName, streamId, events)
    if log.IsEnabled Serilog.Events.LogEventLevel.Debug then Observable.subscribe aux source
    else { new IDisposable with member _.Dispose() = () }
