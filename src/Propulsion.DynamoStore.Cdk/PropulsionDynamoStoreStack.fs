namespace PropulsionDynamoStoreCdk

open Amazon.CDK
open Amazon.CDK.AWS.IAM
open Amazon.CDK.AWS.Lambda

type PropulsionDynamoStoreStack(scope, id, props, ?fromTail) as stack =
    inherit Stack(scope, id, props)

    let streamArn = CfnParameter(stack, "streamsArn", CfnParameterProps(
        Type = "String",
        Description = "DynamoDB Streams ARN for source"))

    let role =
        let role = Role(stack, "DynamoStoreIndexerLambda", RoleProps(
            AssumedBy = ServicePrincipal "lambda.amazonaws.com" ,
            // Basic required permissions, chiefly CloudWatch access
            ManagedPolicies = [| ManagedPolicy.FromAwsManagedPolicyName "service-role/AWSLambdaBasicExecutionRole" |]))
        // Enable access to DDB Streams Top Level Streams List
        // TODO figure out of this is even necessary ?
        do  let topLevel = PolicyStatement()
            topLevel.AddActions("dynamodb:ListStreams")
            topLevel.AddAllResources()
            role.AddToPolicy(topLevel) |> ignore
        // For the specific stream being indexed, enable access to walk the DDB Streams Data
        do  let streamLevel = PolicyStatement()
            streamLevel.AddActions("dynamodb:DescribeStream", "dynamodb:GetShardIterator", "dynamodb:GetRecords")
            streamLevel.AddResources([| streamArn.ValueAsString |])
            role.AddToPolicy(streamLevel) |> ignore
        role

    // NOTE: see README.md; must be built before using this via
    //       dotnet publish ../Propulsion.DynamoStore.Lambda -c Release -r linux-arm64 --self-contained true
    let code = Code.FromAsset("../Propulsion.DynamoStore.Lambda/bin/Release/net6.0/linux-arm64/publish/")
    let fn : Function = Function(stack, "PropulsionDynamoStoreLambda", FunctionProps(
        Role = role,
        Code = code, Architecture = Architecture.ARM_64, Runtime = Runtime.DOTNET_6,
        Handler = "Propulsion.DynamoStore.Lambda::Propulsion.DynamoStore.Lambda.Function::FunctionHandler",
        MemorySize = 128., Timeout = Duration.Minutes 3.))
    do fn.AddEventSourceMapping("EquinoxSource", EventSourceMappingOptions(
        EventSourceArn = streamArn.ValueAsString,
        StartingPosition = (if fromTail = Some true then StartingPosition.LATEST else StartingPosition.TRIM_HORIZON),
        // >1000 has proven not to work well in 128 MB memory
        // Anything up to 9999 is viable and tends to provide the best packing density and throughput under load. The key cons are:
        // - granularity of checkpointing is increased (so killing a projector may result in more re-processing than it would with a lower batch size)
        // - while aggregate write (and read) costs will go down, individual requests will be larger which may affect latency and/or required RC provisioning
        BatchSize = 1000.)) |> ignore
