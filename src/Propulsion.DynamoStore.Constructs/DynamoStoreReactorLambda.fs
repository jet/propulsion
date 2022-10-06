namespace Propulsion.DynamoStore.Constructs

open Amazon.CDK.AWS.IAM
open Amazon.CDK.AWS.Lambda
open Amazon.CDK.AWS.Lambda.EventSources
open Amazon.CDK.AWS.SNS
open Amazon.CDK.AWS.SQS
open System

[<NoComparison; NoEquality>]
type DynamoStoreReactorLambdaProps =
    {   /// SNS/SQS FIFO Topic Arn to trigger execution from
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
    /// Attach an SQS FIFO SNS Queue to a nominated FIFO topic
    /// NOTE: this naturally constrains the parallelism of the Lambda to 1 per Tranche and is the recommended configuration
    | UpdatesTopic of fifoTopicArn : string
    /// Attach a non-FIFO SNS Queue to the nominated (non-FIFO) topic,
    /// constraining parallel execution by using ReservedConcurrency for the Labda
    | UpdatesNonFifoTopic of topicArn : string * reservedConcurrency : int
    /// Use an existing queue without applying a ReservedConcurrency configuration for the Lambda
    | UpdatesQueue of queueArn : string

type DynamoStoreReactorLambda(scope, id, props : DynamoStoreReactorLambdaProps) as stack =
    inherit Constructs.Construct(scope, id)

    let lambdaTimeout, queueVisibilityTimeout =
        let raw = props.timeout.TotalSeconds
        Amazon.CDK.Duration.Seconds raw, Amazon.CDK.Duration.Seconds (raw + 3.)
    let queue, (reservedConcurrency : Nullable<float>) =
        let attachQueueToTopic (fifo : bool) (topic : ITopic) =
            let queue = Queue(stack, "notifications", QueueProps(VisibilityTimeout = queueVisibilityTimeout, Fifo = fifo))
            topic.AddSubscription(Subscriptions.SqsSubscription(queue, Subscriptions.SqsSubscriptionProps(
                RawMessageDelivery = true))) // need MessageAttributes to be included in the delivered message
            queue
        match props.updatesSource with
        | UpdatesQueue queueArn -> Queue.FromQueueArn(stack, "Input", queueArn), Nullable()
        | UpdatesTopic topicArn -> Topic.FromTopicArn(stack, "updates", topicArn) |> attachQueueToTopic true :> _, Nullable()
        | UpdatesNonFifoTopic (topicArn, dop) -> Topic.FromTopicArn(stack, "updates", topicArn) |> attachQueueToTopic false :> _, Nullable dop
    let role =
        let role = Role(stack, "LambdaRole", RoleProps(
            AssumedBy = ServicePrincipal "lambda.amazonaws.com" ,
            // Basic required permissions, chiefly CloudWatch access
            ManagedPolicies = [| ManagedPolicy.FromAwsManagedPolicyName "service-role/AWSLambdaBasicExecutionRole" |]))
        let grantReadWriteOnTable constructName tableName =
            let table = Amazon.CDK.AWS.DynamoDB.Table.FromTableName(stack, constructName, tableName)
            let tablePolicy = PolicyStatement()
            tablePolicy.AddActions("dynamodb:GetItem", "dynamodb:Query", "dynamodb:UpdateItem", "dynamodb:PutItem")
            tablePolicy.AddResources table.TableArn
            role.AddToPolicy tablePolicy |> ignore
        // For the Index Table, we:
        // 1) Read/Query the index content
        // 2) we'll be doing Equinox Transact operations for the Checkpoints, i.e. both Get/Query + Put/Update
        grantReadWriteOnTable "indexTable" props.indexTableName
        // For the Store Table, we'll be doing Transact operations, i.e. both Get/Query + Put/Update
        grantReadWriteOnTable "storeTable" props.storeTableName
        role
    let code = Code.FromAsset(props.codePath)
    let fn : Function = Function(stack, "Reactor", FunctionProps(
        Role = role, Description = props.lambdaDescription,
        Code = code, Architecture = props.lambdaArchitecture, Runtime = props.lambdaRuntime, Handler = props.lambdaHandler,
        MemorySize = float props.memorySize, Timeout = lambdaTimeout, ReservedConcurrentExecutions = reservedConcurrency,
        Environment = dict [
            "EQUINOX_DYNAMO_SYSTEM_NAME", props.regionName
            "EQUINOX_DYNAMO_TABLE",       props.storeTableName
            "EQUINOX_DYNAMO_TABLE_INDEX", props.indexTableName ]))
    do fn.AddEventSource(SqsEventSource(queue, SqsEventSourceProps(
        ReportBatchItemFailures = true, // Required so Lambda can requeue individual unhandled notification at message level
        BatchSize = float props.batchSize)))
