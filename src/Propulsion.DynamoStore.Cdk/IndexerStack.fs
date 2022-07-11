namespace Propulsion.DynamoStore.Cdk

open Propulsion.DynamoStore.Constructs
open Amazon.CDK
open System

type IndexerStackProps
    (   /// DynamoDB Streams Source ARN
        streamArn : string,

        /// DynamoDB Index Table Name
        tableName : string,

        /// Lambda memory allocation - default 128 MB
        ?memorySize : int,
        /// Lambda max batch size - default 1000
        ?batchSize : int,
        /// Lambda max batch size - default 180s
        ?timeout : TimeSpan) =
    inherit StackProps()
    member val StreamArn = streamArn
    member val TableName = tableName
    member val MemorySize = defaultArg memorySize 128
    member val BatchSize = defaultArg batchSize 1000
    member val Timeout = defaultArg timeout (TimeSpan.FromSeconds 180)

type IndexerStack(scope, id, props : IndexerStackProps) as stack =
    inherit Stack(scope, id, props)

    let props : DynamoStoreIndexerLambdaProps =
        {   streamArn = props.StreamArn
            regionName = stack.Region; tableName = props.TableName
            memorySize = props.MemorySize; batchSize = props.BatchSize; timeout = props.Timeout }
    let _ = DynamoStoreIndexerLambda(stack, "Indexer", props = props)
