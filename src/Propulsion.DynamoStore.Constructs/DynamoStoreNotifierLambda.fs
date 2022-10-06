namespace Propulsion.DynamoStore.Constructs

open Amazon.CDK.AWS.IAM
open Amazon.CDK.AWS.Lambda
open Amazon.CDK.AWS.SNS
open System

[<NoComparison; NoEquality>]
type DynamoStoreNotifierLambdaProps =
    {   /// DynamoDB Streams Source ARN
        indexStreamArn : string

        /// SNS FIFO Topic Arn to publish to (Default: Assign a fresh one)
        updatesTarget : UpdatesTarget

        /// Lambda memory allocation
        memorySize : int
        /// Lambda max batch size
        batchSize : int
        /// Lambda execution timeout
        timeout : TimeSpan

        /// Folder path for linux-arm64 publish output (can also be a link to a .zip file)
        codePath : string }
and UpdatesTarget =
    /// Establish a FIFO topic as part of the Stack
    | Default
    /// Target an existing topic - doesn't matter if it's FIFO
    | ExistingTopic of topicArn : string
    /// Establish a non-FIFO Topic (NOTE you'll want to constrain Reactor Lambdas to max one instance by assigning it Reserved capacity)
    | NonFifoTopic

type DynamoStoreNotifierLambda(scope, id, props : DynamoStoreNotifierLambdaProps) as stack =
    inherit Constructs.Construct(scope, id)

    let topic =
        let createTopic (fifo : bool) = Topic(stack, "Updates", TopicProps(
            DisplayName = "Tranche Position updates topic",
            TopicName = "Updates",
            Fifo = fifo))
        match props.updatesTarget with
        | ExistingTopic fifoTopicArn -> Topic.FromTopicArn(stack, "Output", fifoTopicArn)
        | Default -> createTopic true
        | NonFifoTopic -> createTopic false
    let role =
        let role = Role(stack, "LambdaRole", RoleProps(
            AssumedBy = ServicePrincipal "lambda.amazonaws.com" ,
            // Basic required permissions, chiefly CloudWatch access
            ManagedPolicies = [| ManagedPolicy.FromAwsManagedPolicyName "service-role/AWSLambdaBasicExecutionRole" |]))
        // For the Index Table we're supplying notifications for, enable access to walk the DDB Streams Data
        do  let streamsPolicy = PolicyStatement()
            streamsPolicy.AddActions("dynamodb:DescribeStream", "dynamodb:GetShardIterator", "dynamodb:GetRecords")
            streamsPolicy.AddResources props.indexStreamArn
            role.AddToPolicy(streamsPolicy) |> ignore
        // Configure publish access on the output SNS topic
        do  let snsPolicy = PolicyStatement()
            snsPolicy.AddActions "SNS:Publish"
            snsPolicy.AddResources topic.TopicArn
            role.AddToPolicy snsPolicy |> ignore
        role

    // See dotnet-templates/propulsion-dynamostore-cdk project file for MSBuild logic extracting content from tools folder of the nupkg file
    let code = Code.FromAsset(props.codePath)
    let fn : Function = Function(stack, "Notifier", FunctionProps(
        Role = role, Description = "Propulsion DynamoStore Notifier",
        Code = code, Architecture = Architecture.ARM_64, Runtime = Runtime.DOTNET_6,
        Handler = "Propulsion.DynamoStore.Notifier::Propulsion.DynamoStore.Notifier.Function::Handle",
        MemorySize = float props.memorySize, Timeout = Amazon.CDK.Duration.Seconds props.timeout.TotalSeconds,
        Environment = dict [ "SNS_TOPIC_ARN", topic.TopicArn ]))
    do fn.AddEventSourceMapping("IndexSource", EventSourceMappingOptions(
        EventSourceArn = props.indexStreamArn,
        StartingPosition = StartingPosition.TRIM_HORIZON,
        BatchSize = float props.batchSize)) |> ignore
