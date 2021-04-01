namespace Propulsion.Feed

[<AutoOpen>]
module internal AsyncHelpers =

    open System

    type Async with
        static member Sleep(t : TimeSpan) : Async<unit> = Async.Sleep(int t.TotalMilliseconds)
        /// Re-raise an exception so that the current stacktrace is preserved
        static member Raise(e : #exn) : Async<'T> = Async.FromContinuations (fun (_,ec,_) -> ec e)
