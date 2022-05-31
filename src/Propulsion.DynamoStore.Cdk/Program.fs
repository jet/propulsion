module PropulsionDynamoStoreCdk.Program

open Amazon.CDK

[<EntryPoint>]
let main _ =
    let app = App(null)
    let streamArn = ""
    PropulsionDynamoStoreStack(streamArn, app, "PropulsionDynamoStoreCdkStack", StackProps()) |> ignore

    app.Synth() |> ignore
    0
