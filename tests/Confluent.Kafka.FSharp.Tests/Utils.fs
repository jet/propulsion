[<AutoOpen>]
module Jet.ConfluentKafka.Tests.Utils

open System.Threading
open System.Threading.Tasks
open NUnit.Framework

let newId() = System.Guid.NewGuid().ToString("N")

type Assert with
    static member ThrowsAsync<'ExpectedExn, 'T when 'ExpectedExn :> exn> (workflow : Async<'T>) : unit =
        let testDelegate = AsyncTestDelegate(fun () -> Async.StartAsTask workflow :> Task)
        Assert.ThrowsAsync<'ExpectedExn>(testDelegate) |> ignore

[<AbstractClass>]
type AsyncBuilderAbstract() =
    member __.Zero() = async.Zero()
    member __.Return t = async.Return t
    member __.ReturnFrom t = async.ReturnFrom t
    member __.Bind(f,g) = async.Bind(f,g)
    member __.Combine(f,g) = async.Combine(f,g)
    member __.Delay f = async.Delay f
    member __.While(c,b) = async.While(c,b)
    member __.For(xs,b) = async.For(xs,b)
    member __.Using(d,b) = async.Using(d,b)
    member __.TryWith(b,e) = async.TryWith(b,e)
    member __.TryFinally(b,f) = async.TryFinally(b,f)

type TaskBuilder(?ct : CancellationToken) =
    inherit AsyncBuilderAbstract()
    member __.Run f : Task<'T> = Async.StartAsTask(f, ?cancellationToken = ct)

type UnitTaskBuilder() =
    inherit AsyncBuilderAbstract()
    member __.Run f : Task = Async.StartAsTask f :> _

/// Async builder variation that automatically runs top-level expression as task
let task = new TaskBuilder()

/// Async builder variation that automatically runs top-level expression as untyped task
let utask = new UnitTaskBuilder()