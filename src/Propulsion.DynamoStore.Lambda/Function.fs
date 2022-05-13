namespace Propulsion.DynamoStore.Lambda

open Amazon.Lambda.Core
open Amazon.Lambda.DynamoDBEvents
open Propulsion.DynamoStore
open Serilog

[<assembly: LambdaSerializer(typeof<Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer>)>]
()

type Function() =

    let cfg = Configuration()
    let conn = Connector(cfg)
    let cache = Equinox.Cache("indexer", sizeMb = 10)
    // TOCONSIDER surface metrics from write activities to prometheus by wiring up Metrics Sink (for now we log them instead)
    let removeMetrics (e : Serilog.Events.LogEvent) = e.RemovePropertyIfPresent(Equinox.DynamoStore.Core.Log.PropertyTag)
    let template = "{Level:u1} {Message} {Properties}{NewLine}{Exception}"
    let log =
        LoggerConfiguration()
            .Enrich.With({ new Serilog.Core.ILogEventEnricher with member _.Enrich(evt,_) = removeMetrics evt })
            .WriteTo.Console(outputTemplate = template)
            .CreateLogger()
    let service = DynamoStoreIndexer(log, conn.Context, cache)

    member _.FunctionHandler(dynamoEvent : DynamoDBEvent, _context : ILambdaContext) =
        DynamoStreamsLambda.ingest log service dynamoEvent |> Async.StartImmediateAsTask
