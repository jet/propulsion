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
            let queue = Queue(stack, "notifications", QueueProps(VisibilityTimeout = queueVisibilityTimeout, Fifo = true))
            topic.AddSubscription(Subscriptions.SqsSubscription(queue, Subscriptions.SqsSubscriptionProps(RawMessageDelivery = true))) // need MessageAttributes to transfer
            queue
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
        MemorySize = float props.memorySize, Timeout = lambdaTimeout,
        Environment = dict [
            "EQUINOX_DYNAMO_SYSTEM_NAME", props.regionName
            "EQUINOX_DYNAMO_TABLE",       props.storeTableName
            "EQUINOX_DYNAMO_TABLE_INDEX", props.indexTableName ]))
    do fn.AddEventSource(SqsEventSource(queue, SqsEventSourceProps(
        ReportBatchItemFailures = true,
        BatchSize = float props.batchSize)))
