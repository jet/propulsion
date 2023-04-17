# Propulsion.MessageDb

This project houses a Propulsion source for [MessageDb](http://docs.eventide-project.org/user-guide/message-db/).

## Quickstart

The smallest possible sample looks like this, it is intended to give an overview of how the different pieces relate. 
For a more production ready example to take a look at [jets' templates](https://github.com/jet/dotnet-templates)

```fsharp
let quickStart log stats categories handle = async {
    // The group is used as a key to store and retrieve checkpoints 
    let groupName = "MyGroup"
    // The checkpoint store will receive the highest version
    // that has been handled and flushes it to the
    // table on an interval
    let checkpoints = ReaderCheckpoint.CheckpointStore("Host=localhost; Port=5433; Username=postgres; Password=postgres",
                                                       "public", groupName, TimeSpan.FromSeconds 10)
    // Creates the checkpoint table in the schema
    // You can also create this manually
    do! checkpoints.CreateSchemaIfNotExists()
    
    let connStr = "Host=localhost; Database=message_store; Port=5433; Username=message_store; Password=;"
    let maxReadAhead = 100
    let maxConcurrentStreams = 2
    use sink = Propulsion.Sinks.Factory.StartConcurrent(log, maxReadAhead, maxConcurrentStreams, handle, stats)
        
    use src = 
        MessageDbSource(
            log, statsInterval = TimeSpan.FromMinutes 1,
            connStr, batchSize = 1000, 
            // Controls the time to wait once fully caught up
            // before requesting a new batch of events
            tailSleepInterval = TimeSpan.FromMilliseconds 100,
            checkpoints, sink,
            // An array of message-db categories to subscribe to 
            // Propulsion guarantees that events within streams are
            // handled in order, it makes no guarantees across streams (Even within categories)
            categories
        ).Start()
          
    do! src.Await() }
    
let handle stream (events: Propulsion.Sinks.Event[]) = async {
    // ... process the events
    return Propulsion.Sinks.StreamResult.AllProcessed, () }
    
quickStart Log.Logger (createStats ()) [| category |] handle
```
