module PropulsionDynamoStoreCdk.Program

open Amazon.CDK

[<EntryPoint>]
let main _ =
    let app = App(null)
    PropulsionDynamoStoreStack(app, "PropulsionDynamoStoreStack", StackProps()) |> ignore

    app.Synth() |> ignore
    0
