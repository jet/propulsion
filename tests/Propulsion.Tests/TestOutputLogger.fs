namespace Propulsion.Tests

open Serilog

// Derived from https://github.com/damianh/CapturingLogOutputWithXunit2AndParallelTests
// NB VS does not surface these atm, but other test runners / test reports do
type TestOutputLogger(testOutput: Xunit.Abstractions.ITestOutputHelper) =
    let t = "[{Timestamp:HH:mm:ss} {Level:u1}] {Message:lj} {Properties:j}{NewLine}{Exception}"
    let formatter = Serilog.Formatting.Display.MessageTemplateTextFormatter(t, null);
    let writeSerilogEvent logEvent =
        use writer = new System.IO.StringWriter()
        formatter.Format(logEvent, writer);
        let message = writer.ToString().TrimEnd('\n')
        testOutput.WriteLine message
        System.Diagnostics.Debug.WriteLine message
    interface Serilog.Core.ILogEventSink with member x.Emit logEvent = writeSerilogEvent logEvent

module TestOutputLogger =

    let removeMetrics (e: Serilog.Events.LogEvent) =
        e.RemovePropertyIfPresent(Propulsion.Streams.Log.PropertyTag)
        e.RemovePropertyIfPresent(Propulsion.Feed.Core.Log.PropertyTag)
    let trim (c: LoggerConfiguration) =
        c.Filter.ByExcluding(Serilog.Filters.Matching.WithProperty("isMetric"))
         .Enrich.With({ new Serilog.Core.ILogEventEnricher with member _.Enrich(evt, _) = removeMetrics evt })
    let createLoggerEx trimmed sink =
        (LoggerConfiguration() |> if trimmed then trim else id)
            .WriteTo.Sink(sink)
            .CreateLogger()
    let forTestOutputEx trimmed testOutput = TestOutputLogger testOutput |> createLoggerEx trimmed
    let forTestOutput testOutput = forTestOutputEx false testOutput
