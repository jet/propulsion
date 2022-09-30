namespace Propulsion.DynamoStore.Constructs

open Amazon.CDK.AWS.IAM
open Amazon.CDK.AWS.Lambda
open Amazon.CDK.AWS.Lambda.EventSources
open Amazon.CDK.AWS.SNS
open Amazon.CDK.AWS.SQS
open System

[<NoComparison; NoEquality>]
type DynamoStoreReactorLambdaProps =
    {   /// SNS Topic Arn to trigger execution from
        updatesSource : UpdatesSource

        /// DynamoDB Region
        regionName : string
        /// DynamoDB Store Table Name
        storeTableName : string
        /// DynamoDB Index Table Name
        indexTableName : string

        /// Lambda memory allocation
        memorySize : int
        /// Lambda max batch size
        batchSize : int
        /// Lambda execution timeout
        timeout : TimeSpan

        lambdaDescription : string

        /// Folder path for code output (can also be a link to a .zip file)
        codePath : string
        lambdaArchitecture : Architecture
        lambdaRuntime : Runtime
        lambdaHandler : string }
and UpdatesSource =
    | UpdatesTopic of topicArn : string
    | UpdatesQueue of queueArn : string

type DynamoStoreReactorLambda(scope, id, props : DynamoStoreReactorLambdaProps) as stack =
    inherit Constructs.Construct(scope, id)

    let lambdaTimeout, queueVisibilityTimeout =
        let raw = props.timeout.TotalSeconds
        Amazon.CDK.Duration.Seconds raw, Amazon.CDK.Duration.Seconds (raw + 3.)
    let queue =
        match props.updatesSource with
        | UpdatesQueue queueArn -> Queue.FromQueueArn(stack, "Input", queueArn)
        | UpdatesTopic topicArn ->
            let topic = Topic.FromTopicArn(stack, "updates", topicArn)
            let queue = Queue(stack, "notifications", QueueProps(VisibilityTimeout = queueVisibilityTimeout))
            topic.AddSubscription(Subscriptions.SqsSubscription(queue, Subscriptions.SqsSubscriptionProps(
                RawMessageDelivery = true))) // need MessageAttributes to transfer
            queue
    let role =
        let role = Role(stack, "LambdaRole", RoleProps(
            AssumedBy = ServicePrincipal "lambda.amazonaws.com" ,
            // Basic required permissions, chiefly CloudWatch access
            ManagedPolicies = [| ManagedPolicy.FromAwsManagedPolicyName "service-role/AWSLambdaBasicExecutionRole" |]))
        // // Configure publish access on the output SNS topic
        // do  let snsPolicy = PolicyStatement()
        //     snsPolicy.AddActions "SNS:Publish"
        //     snsPolicy.AddResources topic.TopicArn
        //     role.AddToPolicy snsPolicy |> ignore
        role
    let code = Code.FromAsset(props.codePath)
    let fn : Function = Function(stack, "Reactor", FunctionProps(
        Role = role, Description = props.lambdaDescription,
        Code = code, Architecture = props.lambdaArchitecture, Runtime = props.lambdaRuntime, Handler = props.lambdaHandler,
        MemorySize = float props.memorySize, Timeout = lambdaTimeout,
        Environment = dict [
            "EQUINOX_DYNAMO_SYSTEM_NAME", props.regionName
            "EQUINOX_DYNAMO_TABLE",       props.storeTableName
            "EQUINOX_DYNAMO_TABLE_INDEX", props.indexTableName ]))
    do fn.AddEventSource(SqsEventSource(queue, SqsEventSourceProps(
        ReportBatchItemFailures = true,
        BatchSize = float props.batchSize)))
