module Propulsion.Reactor.Internal

open Propulsion.Internal
open System

module Async =

    /// Wraps a computation, cancelling (and triggering a timeout exception) if it doesn't complete within the specified timeout
    let timeoutAfter (timeout : TimeSpan) (c : Async<'a>) = async {
        let! r = Async.StartChild(c, int timeout.TotalMilliseconds)
        return! r }

module Retry =

    /// Wraps a computation such that:
    /// - Until the timeout, exceptions trigger backing off for 10 ms and retrying
    /// - After the timeout, any exception triggered by the computation will propagate to the caller
    /// NOTE does not guarantee completion within the timeout, nor does it trigger cancellation (see timeoutAfter)
    let private keepTrying (backoffMs : int) timeout computation = async {
        let cutoff = IntervalTimer timeout
        let mutable exceptions, finished = ResizeArray(), false
        while not finished do
            try do! computation
                finished <- true
            with e when not (cutoff.IfDueRestart()) ->
                exceptions.Add e
                do! Async.Sleep backoffMs
        return exceptions.ToArray() }

    /// Continually retries a computation within a period, with a specified backoff in the case of failure
    /// If it has not succeeded within that time, we allow one more period before triggering a timeout
    /// This is to give us a reasonable chance of seeing the underlying failure rather than a timeout exception
    let withBackoffAndTimeout (backoff : TimeSpan) period computation =
        keepTrying (int backoff.TotalMilliseconds) period computation
        |> Async.timeoutAfter (period+period)
