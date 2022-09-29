namespace Propulsion.DynamoStore.Constructs

open Amazon.CDK.AWS.IAM
open Amazon.CDK.AWS.Lambda
open Amazon.CDK.AWS.SNS
open System

type DynamoStoreNotifierLambdaProps =
    {   /// DynamoDB Streams Source ARN
        streamArn : string

        /// SNS Topic Arn to publish to (Default: Assign a fresh one)
        topicArn : string option

        /// Lambda memory allocation
        memorySize : int
        /// Lambda max batch size
        batchSize : int
        /// Lambda max batch size
        timeout : TimeSpan

        /// Folder path for linux-arm64 publish output (can also be a link to a .zip file)
        codePath : string }

type DynamoStoreNotifierLambda(scope, id, props : DynamoStoreNotifierLambdaProps) as stack =
    inherit Constructs.Construct(scope, id)

    let topic =
        match props.topicArn with
        | Some ta -> Topic.FromTopicArn(stack, "Output", ta)
        | None -> Topic(stack, "Updates", TopicProps(DisplayName = "Tranche Position updates topic"))
    let role =
        let role = Role(stack, "LambdaRole", RoleProps(
            AssumedBy = ServicePrincipal "lambda.amazonaws.com" ,
            // Basic required permissions, chiefly CloudWatch access
            ManagedPolicies = [| ManagedPolicy.FromAwsManagedPolicyName "service-role/AWSLambdaBasicExecutionRole" |]))
        // For the Index Table we're supplying notifications for, enable access to walk the DDB Streams Data
        do  let streamsPolicy = PolicyStatement()
            streamsPolicy.AddActions("dynamodb:DescribeStream", "dynamodb:GetShardIterator", "dynamodb:GetRecords")
            streamsPolicy.AddResources props.streamArn
            role.AddToPolicy(streamsPolicy) |> ignore
        // Configure publish access on the output SNS topic
        do  let snsPolicy = PolicyStatement()
            snsPolicy.AddActions "SNS:Publish"
            snsPolicy.AddResources topic.TopicArn
            role.AddToPolicy snsPolicy |> ignore
        role

    // See dotnet-templates/propulsion-indexer-cdk project file for logic to extract content from tools section of .Lambda nupkg file
    let code = Code.FromAsset(props.codePath)
    let fn : Function = Function(stack, "Notifier", FunctionProps(
        Role = role, Description="Propulsion DynamoStore Notifier",
        Code = code, Architecture = Architecture.ARM_64, Runtime = Runtime.DOTNET_6,
        Handler = "Propulsion.DynamoStore.Notifier::Propulsion.DynamoStore.Notifier.Function::Handle",
        MemorySize = float props.memorySize, Timeout = Amazon.CDK.Duration.Seconds props.timeout.TotalSeconds,
        Environment = dict [ "SNS_TOPIC_ARN", topic.TopicArn ]))
    do fn.AddEventSourceMapping("IndexSource", EventSourceMappingOptions(
        EventSourceArn = props.streamArn,
        StartingPosition = StartingPosition.TRIM_HORIZON,
        BatchSize = float props.batchSize)) |> ignore
