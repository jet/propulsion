module Propulsion.DynamoStore.Cdk.Program

open Amazon.CDK

let streamNameArg = "dev/streamArn"
let indexTableNameArg = "dev/indexTableName"

[<EntryPoint>]
let main _ =
    let app = App(null)

    // TODO probably replace with Logic to look it up in SSM Parameter Store
    let storeStreamArn, indexTableName =
        match app.Node.TryGetContext streamNameArg, app.Node.TryGetContext indexTableNameArg with
        | :? string as sa, (:? string as tn) when sa <> null && tn <> null -> sa, tn
        | _ -> failwith $"Please supply DynamoDB Streams ARN and DynamoDB Index Table Name via -c {streamNameArg} and -c {indexTableNameArg} respectively"
    let _mainIndexer = IndexerStack(app, "MainIndexer", IndexerStackProps(storeStreamArn, indexTableName)) in ()

    app.Synth() |> ignore
    0
