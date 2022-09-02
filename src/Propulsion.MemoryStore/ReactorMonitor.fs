module Propulsion.Reactor.Monitor

open Propulsion.Internal
open Propulsion.Reactor.Internal // Retry

/// Run a phase of processing, repeating to
/// a) verify correct idempotent handling
/// b) handle cases where the observed effect is not observable immediately, but is eventually upon retrying
let check wait backoff timeout warnThreshold label f = async {
    let waits = Stopwatch.start ()
    let wait waitArg = async {
        waits.Start()
        do! wait waitArg
        waits.Stop()
    }
    let sw = Stopwatch.start ()
    let! retried = Retry.withBackoffAndTimeout backoff timeout (f wait)
    // In general, in the MemoryStore case, we should never have retries; for other stores, it's obviously entirely possible
    if sw.Elapsed > warnThreshold || retried.Length > 0 then
        Serilog.Log.Information("Check {label} {tot:n3}s waits {wt:n3}s retries {c} {retried}",
                                label, sw.ElapsedSeconds, waits.ElapsedSeconds, retried.Length,
                                seq { for e in retried -> e.GetType().Name }) }
