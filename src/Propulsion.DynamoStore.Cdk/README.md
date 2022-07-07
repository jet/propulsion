## Propulsion.DynamoStore.Cdk

Uses the CDK Toolkit to manage configuration/deployment of:
1. The indexing logic in `Propulsion.DynamoStore.Lambda` (which gets compiled into a Lambda package)
2. Associated role and triggers that route from DynamoDB Streams onward (it's assumed you'll use `eqx )

## Prerequisites

0. CDK Toolkit installed

       npm install -g aws-cdk
   
   See https://docs.aws.amazon.com/cdk/v2/guide/getting_started.html#getting_started_install for more details/context

2. A source Table, with DDB Streams configured

       eqx initaws -rru 10 -wru 10 dynamo -t equinox-test

   TODO: `eqx initaws` should emit the streams ARN (for use in the deploy step below)

3. An index table

       eqx initaws -rru 5 -wru 5 -S off dynamo -t equinox-test-index

4. Build/publish the Lambda package (TODO to be automated)

       dotnet publish ../Propulsion.DynamoStore.Lambda -c Release -r linux-arm64 --self-contained true

## To deploy

    cdk deploy --parameters streamsArn=arn:aws:dynamodb:us-east-1:111111111111:table/equinox-test/stream/2022-07-05T11:49:13.013

## To redeploy

Subsequent deploys may omit the `--parameters`, as they'll be retained. i.e.

    dotnet publish ../Propulsion.DynamoStore.Lambda -c Release -r linux-arm64 --self-contained true && cdk deploy

## Useful commands

* `dotnet build`     check the CDK logic builds (no particular need to do this as `synth`/`deploy` triggers this implicitly)
* `cdk ls`           list all stacks in the app
* `cdk synth`        emits the synthesized CloudFormation template
* `cdk deploy`       deploy this stack to your default AWS account/region
* `cdk diff`         compare deployed stack with current state
* `cdk docs`         open CDK documentation
