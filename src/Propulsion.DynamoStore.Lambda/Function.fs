namespace Propulsion.DynamoStore.Lambda

open Amazon.Lambda.Core
open Amazon.Lambda.DynamoDBEvents
open Propulsion.DynamoStore

[<assembly: LambdaSerializer(typeof<Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer>)>]
()

type Function() =

    let cfg = Configuration()
    let conn = Connector(cfg)
    let cache = Equinox.Cache("indexer", sizeMb = 10)
    let service = DynamoStoreIndexer.Config.create 100_000 (conn.Context, cache)

    member _.FunctionHandler(dynamoEvent : DynamoDBEvent, context : ILambdaContext) =
        DynamoStreamsLambda.ingest service dynamoEvent context |> Async.StartImmediateAsTask
