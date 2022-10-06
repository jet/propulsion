namespace Propulsion.Tests

open Serilog

// Derived from https://github.com/damianh/CapturingLogOutputWithXunit2AndParallelTests
// NB VS does not surface these atm, but other test runners / test reports do
type TestOutputLogger(testOutput : Xunit.Abstractions.ITestOutputHelper) =
    let formatter = Serilog.Formatting.Display.MessageTemplateTextFormatter("{Timestamp:yyyy-MM-dd HH:mm:ss.fff zzz} [{Level}] {Message}{NewLine}{Exception}", null);
    let writeSerilogEvent logEvent =
        use writer = new System.IO.StringWriter()
        formatter.Format(logEvent, writer);
        writer |> string |> testOutput.WriteLine
        writer |> string |> System.Diagnostics.Debug.Write
    interface Serilog.Core.ILogEventSink with member x.Emit logEvent = writeSerilogEvent logEvent

module TestOutputLogger =

    let createLogger sink =
        LoggerConfiguration()
            .WriteTo.Sink(sink)
            .CreateLogger()

    let forTestOutput testOutput = TestOutputLogger testOutput |> createLogger
