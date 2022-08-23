[<AutoOpen>]
module Propulsion.Tool.Infrastructure

open Serilog

module Log =

    let forMetrics = Log.ForContext("isMetric", true)

    let isStoreMetrics x = Serilog.Filters.Matching.WithProperty("isMetric").Invoke x

module Sinks =

    let equinoxMetricsOnly (l : LoggerConfiguration) =
        l.WriteTo.Sink(Equinox.CosmosStore.Core.Log.InternalMetrics.Stats.LogSink())
         .WriteTo.Sink(Equinox.DynamoStore.Core.Log.InternalMetrics.Stats.LogSink())
    let console verbose (configuration : LoggerConfiguration) =
        let outputTemplate =
            let t = "{Timestamp:HH:mm:ss} {Level:u1} {Message:lj} {SourceContext} {Properties}{NewLine}{Exception}"
            if verbose then t else t.Replace("{Properties}", "")
        configuration.WriteTo.Console(theme = Sinks.SystemConsole.Themes.AnsiConsoleTheme.Code, outputTemplate = outputTemplate)

[<System.Runtime.CompilerServices.Extension>]
type Logging() =

    [<System.Runtime.CompilerServices.Extension>]
    static member Configure(configuration : LoggerConfiguration, ?verbose) =
        configuration
            .Enrich.FromLogContext()
        |> fun c -> if verbose = Some true then c.MinimumLevel.Debug() else c

    [<System.Runtime.CompilerServices.Extension>]
    static member private Sinks(configuration : LoggerConfiguration, configureMetricsSinks, configureConsoleSink, ?isMetric) =
        let removeMetricsProps (c : LoggerConfiguration) : LoggerConfiguration =
            let trim (e : Serilog.Events.LogEvent) =
                e.RemovePropertyIfPresent(Propulsion.Streams.Log.PropertyTag)
                e.RemovePropertyIfPresent(Propulsion.Feed.Core.Log.PropertyTag)
                e.RemovePropertyIfPresent(Propulsion.CosmosStore.Log.PropertyTag)
                e.RemovePropertyIfPresent(Equinox.CosmosStore.Core.Log.PropertyTag)
                e.RemovePropertyIfPresent(Equinox.DynamoStore.Core.Log.PropertyTag)
            c.Enrich.With({ new Serilog.Core.ILogEventEnricher with member _.Enrich(evt,_) = trim evt })
        let configure (a : Configuration.LoggerSinkConfiguration) : unit =
            a.Logger(configureMetricsSinks >> ignore) |> ignore // unconditionally feed all log events to the metrics sinks
            a.Logger(fun l -> // but filter what gets emitted to the console sink
                let l = match isMetric with None -> l | Some predicate -> l.Filter.ByExcluding(System.Func<Serilog.Events.LogEvent, bool> predicate)
                l |> removeMetricsProps |> configureConsoleSink |> ignore)
            |> ignore
        configuration.WriteTo.Async(bufferSize = 65536, blockWhenFull = true, configure = System.Action<_> configure)

    [<System.Runtime.CompilerServices.Extension>]
    static member Sinks(configuration : LoggerConfiguration, configureMetricsSinks, verboseStore, verboseConsole) =
        configuration.Sinks(configureMetricsSinks, Sinks.console verboseConsole, ?isMetric = if verboseStore then None else Some Log.isStoreMetrics)
