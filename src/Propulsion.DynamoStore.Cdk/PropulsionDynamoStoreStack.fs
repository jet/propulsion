namespace PropulsionDynamoStoreCdk

open Amazon.CDK
open Amazon.CDK.AWS.DynamoDB
open Amazon.CDK.AWS.IAM
open Amazon.CDK.AWS.Lambda

type PropulsionDynamoStoreStack(streamArn, scope, id, props, ?fromTail) as stack =
    inherit Stack(scope, id, props)

    // let table = Amazon.CDK.AWS.DynamoDB.Table.FromTableAttributes(stack, "et", TableAttributes(TableArn = tableArn))

    let role =
        let role = Role(stack, "PropulsionDynamoStoreIndexerLambda", RoleProps(
            AssumedBy = ServicePrincipal "lambda.amazonaws.com" ,
            // Basic required permissions, chiefly CloudWatch access
            ManagedPolicies = [| ManagedPolicy.FromAwsManagedPolicyName "service-role/AWSLambdaBasicExecutionRole" |]))
        // Enable access to DDB Streams Top Level Streams List
        do  let topLevel = PolicyStatement()
            topLevel.AddActions("dynamodb:ListStreams")
            topLevel.AddAllResources()
            role.AddToPolicy(topLevel) |> ignore
        // For the specific stream being indexed, enable access to walk the DDB Streams Data
        do  let streamLevel = PolicyStatement()
            streamLevel.AddActions("dynamodb:DescribeStream", "dynamodb:GetShardIterator", "dynamodb:GetRecords")
            streamLevel.AddResources([|streamArn|])
            role.AddToPolicy(streamLevel) |> ignore
        role

    // let code =
    //     let bucket, key = unbox ()
    //     Code.FromBucket(bucket, key)
    let code = Unchecked.defaultof<_>
    let fn : Function = Function(stack, "PropulsionDynamoStore", FunctionProps(
        Role = role,
        Code = code, Architecture = Architecture.ARM_64,
        Timeout = Duration.Minutes 3.))
    do fn.AddEventSourceMapping("esm", EventSourceMappingOptions(
        StartingPosition = (if fromTail = Some true then StartingPosition.LATEST else StartingPosition.TRIM_HORIZON),
        // >1000 has proven not to work well in 128 MB memory
        BatchSize = 1000)) |> ignore
