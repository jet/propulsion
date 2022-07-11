## Propulsion.DynamoStore.Cdk

Given a pair of DynamoDB Tables (provisioned using the `eqx` tool; see below)
- Store table: holds application events
  - read/written via `Equinox.DynamoStore`
  - appends are streamed via DynamoDB Streams with a 24h retention period
- Index table: holds an index for the table
  - written by `Propulsion.DynamoStore.Lambda`
  - read by `Propulsion.DynamoStore.DynamoStoreSource` in Reactor/Projector applications
  
  (internally the index is simply an `Equinox.DynamoStore`)

This project Uses the [AWS Cloud Development Kit (CDK)](https://docs.aws.amazon.com/cdk/v2/guide/home.html) to reliably manage configuration/deployment of:

1. The indexing logic in `Propulsion.DynamoStore.Lambda` (which gets `dotnet publish`ed into an AWS Lambda package, which the Stack references)
2. Associated role and triggers that route from the DynamoDB Stream to the Lambda

## Prerequisites

1. A source Table, with DDB Streams configured

       eqx initaws -rru 10 -wru 10 dynamo -t equinox-test

   TODO: `eqx initaws` should emit the streams ARN (for use in the deploy step below)

2. An index table

       eqx initaws -rru 5 -wru 5 -S off dynamo -t equinox-test-index

3. CDK Toolkit installed

       npm install -g aws-cdk

   See https://docs.aws.amazon.com/cdk/v2/guide/getting_started.html#getting_started_install for more details/context

4. (re) Build/publish the Lambda package (TODO to be automated)

       dotnet publish ../Propulsion.DynamoStore.Lambda -c Release -r linux-arm64 --self-contained true

## To deploy

    cdk deploy -c streamArn=arn:aws:dynamodb:us-east-1:111111111111:table/equinox-test/stream/2022-07-05T11:49:13.013 -c indexTableName=equinox-test-index

## Useful commands

* `dotnet build`     check the CDK logic builds (no particular need to do this as `synth`/`deploy` triggers this implicitly)
* `cdk ls`           list all stacks in the app
* `cdk synth`        emits the synthesized CloudFormation template
* `cdk deploy`       deploy this stack to your default AWS account/region
* `cdk diff`         compare deployed stack with current state
* `cdk docs`         open CDK documentation
