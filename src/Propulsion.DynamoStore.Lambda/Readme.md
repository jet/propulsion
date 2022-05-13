# AWS Lambda DynamoDB Indexer

After deploying, you must configure an Amazon DynamoDB stream as an event source to trigger your Lambda function.

## Deployment

Once you have edited your template and code you can deploy your application using the [Amazon.Lambda.Tools Global Tool](https://github.com/aws/aws-extensions-for-dotnet-cli#aws-lambda-amazonlambdatools) from the command line.

Install Amazon.Lambda.Tools Global Tools if not already installed.
```
    dotnet tool install -g Amazon.Lambda.Tools
```

If already installed check if new version is available.
```
    dotnet tool update -g Amazon.Lambda.Tools
```

Deploy function to AWS Lambda

* aws-lambda-tools-defaults.json - default argument settings for use with Visual Studio and command line deployment tools for AWS

```
    cd "Propulsion.DynamoStore.Lambda/src/Propulsion.DynamoStore.Lambda"
    dotnet lambda deploy-function
```
