namespace Propulsion.DynamoStore.Constructs

open Amazon.CDK.AWS.IAM
open Amazon.CDK.AWS.Lambda
open System

type DynamoStoreIndexerLambdaProps =
    {   /// DynamoDB Streams Source ARN
        streamArn : string

        /// DynamoDB Region for Index Table
        regionName : string
        /// DynamoDB Index Table Name
        tableName : string

        /// Lambda memory allocation
        memorySize : int
        /// Lambda max batch size
        batchSize : int
        /// Lambda max batch size
        timeout : TimeSpan

        /// Folder path for linux-arm64 publish output (can also be a link to a .zip file)
        codePath : string }

type DynamoStoreIndexerLambda(scope, id, props : DynamoStoreIndexerLambdaProps) as stack =
    inherit Constructs.Construct(scope, id)

    let role =
        let role = Role(stack, "IndexerLambdaRole", RoleProps(
            AssumedBy = ServicePrincipal "lambda.amazonaws.com" ,
            // Basic required permissions, chiefly CloudWatch access
            ManagedPolicies = [| ManagedPolicy.FromAwsManagedPolicyName "service-role/AWSLambdaBasicExecutionRole" |]))
        // For the Store Table being indexed, enable access to walk the DDB Streams Data
        do  let streamsPolicy = PolicyStatement()
            streamsPolicy.AddActions("dynamodb:DescribeStream", "dynamodb:GetShardIterator", "dynamodb:GetRecords")
            streamsPolicy.AddResources props.streamArn
            role.AddToPolicy streamsPolicy |> ignore
        // For the Index Table, we'll be doing Equinox Transact operations, i.e. both Get/Query + Put/Update
        let indexTable = Amazon.CDK.AWS.DynamoDB.Table.FromTableName(stack, "indexTable", props.tableName)
        do  let indexTablePolicy = PolicyStatement()
            indexTablePolicy.AddActions("dynamodb:GetItem", "dynamodb:Query", "dynamodb:UpdateItem", "dynamodb:PutItem")
            indexTablePolicy.AddResources indexTable.TableArn
            role.AddToPolicy indexTablePolicy |> ignore
        role

    // See dotnet-templates/propulsion-indexer-cdk project file for logic to extract content from tools section of .Lambda nupkg file
    let code = Code.FromAsset(props.codePath)
    let fn : Function = Function(stack, "Indexer", FunctionProps(
        Role = role, Description="Propulsion DynamoStore Indexer",
        Code = code, Architecture = Architecture.ARM_64, Runtime = Runtime.DOTNET_6,
        Handler = "Propulsion.DynamoStore.Indexer::Propulsion.DynamoStore.Indexer.Function::Handle",
        MemorySize = float props.memorySize, Timeout = Amazon.CDK.Duration.Seconds props.timeout.TotalSeconds,
        Environment = dict [
            "EQUINOX_DYNAMO_SYSTEM_NAME", props.regionName
            "EQUINOX_DYNAMO_TABLE_INDEX", props.tableName ]))
    do fn.AddEventSourceMapping("EquinoxSource", EventSourceMappingOptions(
        EventSourceArn = props.streamArn,
        StartingPosition = StartingPosition.TRIM_HORIZON,
        // >1000 has proven not to work well in 128 MB memory
        // Anything up to 9999 is viable and tends to provide the best packing density and throughput under load. The key cons are:
        // - granularity of checkpointing is increased (so killing a projector may result in more re-processing than it would with a lower batch size)
        // - while aggregate write (and read) costs will go down, individual requests will be larger which may affect latency and/or required RC provisioning
        BatchSize = float props.batchSize)) |> ignore
