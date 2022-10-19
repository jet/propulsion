namespace Propulsion.Tests

open Serilog

// Derived from https://github.com/damianh/CapturingLogOutputWithXunit2AndParallelTests
// NB VS does not surface these atm, but other test runners / test reports do
type TestOutputLogger(testOutput : Xunit.Abstractions.ITestOutputHelper) =
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

    let createLogger sink =
        LoggerConfiguration()
            .WriteTo.Sink(sink)
            .CreateLogger()

    let forTestOutput testOutput = TestOutputLogger testOutput |> createLogger
