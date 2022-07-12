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
        let role = Role(stack, "LambdaRole", RoleProps(
            AssumedBy = ServicePrincipal "lambda.amazonaws.com" ,
            // Basic required permissions, chiefly CloudWatch access
            ManagedPolicies = [| ManagedPolicy.FromAwsManagedPolicyName "service-role/AWSLambdaBasicExecutionRole" |]))
//        // Enable access to DDB Streams Top Level Streams List
//        do  let topLevel = PolicyStatement()
//            topLevel.AddActions("dynamodb:ListStreams")
//            topLevel.AddAllResources()
//            role.AddToPolicy(topLevel) |> ignore
        // For the specific stream being indexed, enable access to walk the DDB Streams Data
        do  let streamLevel = PolicyStatement()
            streamLevel.AddActions("dynamodb:DescribeStream", "dynamodb:GetShardIterator", "dynamodb:GetRecords")
            streamLevel.AddResources(props.streamArn)
            role.AddToPolicy(streamLevel) |> ignore
        let indexTable = Amazon.CDK.AWS.DynamoDB.Table.FromTableName(stack, "indexTable", props.tableName)
        do  let indexTableLevel = PolicyStatement()
            indexTableLevel.AddActions("dynamodb:GetItem", "dynamodb:Query", "dynamodb:UpdateItem", "dynamodb:PutItem")
            indexTableLevel.AddResources([| indexTable.TableArn |])
            role.AddToPolicy(indexTableLevel) |> ignore
        role

    // See .Cdk project file for logic to extract content from tools section of .Lambda nupgk file
    let code = Code.FromAsset(props.codePath)
    let fn : Function = Function(stack, "Indexer", FunctionProps(
        Role = role,
        Code = code, Architecture = Architecture.ARM_64, Runtime = Runtime.DOTNET_6,
        Handler = "Propulsion.DynamoStore.Lambda::Propulsion.DynamoStore.Lambda.Function::FunctionHandler",
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
