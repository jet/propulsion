# Propulsion [![Build Status](https://dev.azure.com/jet-opensource/opensource/_apis/build/status/jet.Propulsion)](https://dev.azure.com/jet-opensource/opensource/_build/latest?definitionId=16) [![release](https://img.shields.io/github/release/jet/propulsion.svg)](https://github.com/jet/propulsion/releases)  [![NuGet](https://img.shields.io/nuget/vpre/propulsion)](https://www.nuget.org/packages/Propulsion/) [![license](https://img.shields.io/github/license/jet/propulsion.svg)](LICENSE) ![code size](https://img.shields.io/github/languages/code-size/jet/propulsion.svg)[![Discord](https://img.shields.io/discord/514783899440775168?color=blue&label=Chat%20in%20%23equinox%20on%20DDD-CQRS-ES%20Discord)](https://discord.gg/sEZGSHNNbH) [![docs status](https://img.shields.io/badge/DOCUMENTATION-WIP-important.svg?style=popout)](DOCUMENTATION.md)

Propulsion provides a granular suite of .NET NuGet packages for building Reactive event processing pipelines. It caters for:

- **Event Sourcing Reactions**: Handling projections and reactions based on event feeds from stores such as EventStoreDB and MessageDB, and the Equinox Stores (DynamoStore, CosmosStore, MemoryStore).
- **Unit and Integration testing support**: The `AwaitCompletion` mechanisms in `MemoryStore` and `FeedSource` provide a clean way to structure test suites in a manner that achieves high test coverage without flaky tests or slow tests.
- **Generic Ingestion and Publishing pipelines**: The same abstractions can also be used for consuming and/or publishing to any target.
- **Serverless event pipelines**: The core components do not assume a long-lived process.
  - The `DynamoStore`-related components implement support for running an end-to-end event sourced system using only Amazon DynamoDB and Lambda without requiring a long-lived host process.
- **Strong metrics support**: Feed Sources and Projectors provide comprehensive logging and metrics. (At present, the primary integration is with Prometheus, but the mechanism is exposed in a pluggable manner).

If you're looking for a good discussion forum on these kinds of topics, look no further than the [DDD-CQRS-ES Discord](https://github.com/ddd-cqrs-es/community)'s [#equinox channel](https://discord.com/channels/514783899440775168/1002635005429825657) ([invite link](https://discord.gg/sEZGSHNNbH)).

## Core Components

- `Propulsion` [![NuGet](https://img.shields.io/nuget/v/Propulsion.svg)](https://www.nuget.org/packages/Propulsion/) Implements core functionality in a channel-independent fashion. [Depends](https://www.fuget.org/packages/Propulsion) on `FSharp.Control.TaskSeq`, `MathNet.Numerics`, `Serilog`: 

    1. `StreamsSink`: High performance pipeline that handles parallelized event processing. Ingestion of events, and checkpointing of progress are handled asynchronously. Each aspect of the pipeline is decoupled such that it can be customized as desired. 
    2. `Streams.Prometheus`: Helper that exposes per-scheduler metrics for Prometheus scraping.
    3. `ParallelProjector`: Scaled down variant of `StreamsSink` that does not preserve stream level ordering semantics
    4. `FeedSource`: Handles continual reading and checkpointing of events from a set of feeds ('tranches') of a 'source' that collectively represent a change data capture source for a given system (roughly analogous to how a CosmosDB Container presents a changefeed). A `readTranches` function is used to identify the Tranches (sub-feeds) on startup. The Feed Source then operates a logical reader thread per Tranche. Tranches represent content as an incrementally retrievable change feed consisting of batches of `FsCodec.ITimelineEvent` records. Each batch has an optional associated checkpointing callback that's triggered only when the Sink has handled all events within it.
    5. `Monitor.AwaitCompletion`: Enables efficient waiting for completion of reaction processing within an integration test.
    6. `PeriodicSource`: Handles regular crawling of an external datasource (such as a SQL database) where there is no way to save progress and then resume from that saved token (based on either the intrinsic properties of the data, or of the store itself). The source is expected to present its content as an `IAsyncEnumerable` of `FsCodec.StreamName * FsCodec.IEventData * context`. Checkpointing occurs only when all events have been deemed handled by the Sink.
    7. `SinglePassFeedSource`: Handles single pass loading of large datasets (such as a SQL database), completing when the full data has been ingested.
    8. `JsonSource`: Simple source that feeds items from a File containing JSON (such a file can be generated via `eqx query -o JSONFILE from cosmos` etc)

  NOTE `Propulsion.Feed` is a namespace within the main `Propulsion` package that provides helpers for checkpointed consumption of a feed of stream-based inputs.
  - Supported inputs include custom bindings (e.g. a third-party Feed API) or various other input configurations (e.g. periodically correlating with inputs from a non-streamed source such as a SQL Database).
  - Provides a generic API for checkpoint storage, with diverse implementations hosted in the sibling packages associated with each concrete store (supported stores include DynamoStore, CosmosStore, SQL Server, Postgres).
  - Using a feed normally requires a checkpoint store that inmplements `IFeedCheckpointStore` from e.g., `Propulsion.CosmosStore|DynamoStore|MessageDb|SqlStreamStore`

- `Propulsion.Prometheus` [![NuGet](https://img.shields.io/nuget/v/Propulsion.Prometheus.svg)](https://www.nuget.org/packages/Propulsion.Prometheus/) Provides helpers for checkpointed consumption of a feed of stream-based inputs. Provides for custom bindings (e.g. a third-party Feed API) or various other input configurations (e.g. periodically correlating with inputs from a non-streamed source such as a SQL Database). Provides a generic API for checkpoint storage, with diverse implementations hosted in the sibling packages associated with each concrete store (supported stores include DynamoStore, CosmosStore, SQL Server, Postgres). [Depends](https://www.fuget.org/packages/Propulsion.Prometheus) on `Propulsion`, a `IFeedCheckpointStore` implementation (from e.g., `Propulsion.CosmosStore|DynamoStore|MessageDb|SqlStreamStore`)

   1. `Propulsion.Prometheus`: Exposes processing throughput statistics to Prometheus.
   2. `Propulsion.Feed.Prometheus`: Exposes reading statistics to Prometheus (including metrics from `DynamoStore.DynamoStoreSource`, `EventStoreDb.EventStoreSource`, `MessageDb.MessageDbSource` and `SqlStreamStore.SqlStreamStoreSource`). 

- `Propulsion.MemoryStore` [![NuGet](https://img.shields.io/nuget/v/Propulsion.MemoryStore.svg)](https://www.nuget.org/packages/Propulsion.MemoryStore/). Provides bindings to `Equinox.MemoryStore`. [Depends](https://www.fuget.org/packages/Propulsion.MemoryStore) on `Equinox.MemoryStore` v `4.0.0`, `FsCodec.Box`, `Propulsion`

    1. `MemoryStoreSource`: Presents a Source that adapts an `Equinox.MemoryStore` to feed into a `Propulsion.Sink`. Typically used as part of an overall test suite to enable efficient and deterministic testing where reactions are relevant to a given scenario.
    2. `Monitor.AwaitCompletion`: Enables efficient **deterministic** waits for Reaction processing within integration or unit tests.
    3. `ReaderCheckpoint`: ephemeral checkpoint storage for `Propulsion.DynamoStore`/`EventStoreDb`/`Feed`/`MessageDb`/`SqlStreamSteamStore` in test contexts.

## Store-specific Components

- `Propulsion.CosmosStore` [![NuGet](https://img.shields.io/nuget/v/Propulsion.CosmosStore.svg)](https://www.nuget.org/packages/Propulsion.CosmosStore/) Provides bindings to Azure CosmosDB. [Depends](https://www.fuget.org/packages/Propulsion.CosmosStore) on `Equinox.CosmosStore` v `4.0.0`

    1. `CosmosStoreSource`: reading from CosmosDb's ChangeFeed  using `Microsoft.Azure.Cosmos`
    2. `CosmosStoreSink`: writing to `Equinox.CosmosStore` v `4.0.0`.
    3. `CosmosStorePruner`: pruning from `Equinox.CosmosStore` v `4.0.0`.
    4. `ReaderCheckpoint`: checkpoint storage for `Propulsion.DynamoStore`/`EventStoreDb`/`Feed`/`MessageDb`/`SqlStreamSteamStore` using `Equinox.CosmosStore` v `4.0.0`.

  (Reading and position metrics are exposed via `Propulsion.CosmosStore.Prometheus`)

- `Propulsion.DynamoStore` [![NuGet](https://img.shields.io/nuget/v/Propulsion.DynamoStore.svg)](https://www.nuget.org/packages/Propulsion.DynamoStore/) Provides bindings to `Equinox.DynamoStore`. [Depends](https://www.fuget.org/packages/Propulsion.DynamoStore) on `Equinox.DynamoStore` v `4.0.0`

    1. `AppendsIndex`/`AppendsEpoch`: `Equinox.DynamoStore` aggregates that together form the DynamoStore Index
    2. `DynamoStoreIndexer`: writes to `AppendsIndex`/`AppendsEpoch` (used by `Propulsion.DynamoStore.Indexer`, `Propulsion.Tool`)
    3. `DynamoStoreSource`: reads from `AppendsIndex`/`AppendsEpoch` (see `DynamoStoreIndexer`)
    4. `ReaderCheckpoint`: checkpoint storage for `Propulsion.DynamoStore`/`EventStoreDb`/`Feed`/`MessageDb`/`SqlStreamSteamStore` using `Equinox.DynamoStore` v `4.0.0`.

  (Reading and position metrics are exposed via `Propulsion.Prometheus`)

- `Propulsion.DynamoStore.Indexer` [![NuGet](https://img.shields.io/nuget/v/Propulsion.DynamoStore.Indexer.svg)](https://www.nuget.org/packages/Propulsion.DynamoStore.Indexer/) AWS Lambda to index appends into an Index Table. [Depends](https://www.fuget.org/packages/Propulsion.DynamoStore.Indexer) on `Propulsion.DynamoStore`, `Amazon.Lambda.Core`, `Amazon.Lambda.DynamoDBEvents`, `Amazon.Lambda.Serialization.SystemTextJson`

    1. `Handler`: parses Dynamo DB Streams Source Mapping input, feeds into `Propulsion.DynamoStore.DynamoStoreIndexer`
    2. `Connector`: Store / environment variables wiring to connect `DynamoStoreIndexer` to the `Equinox.DynamoStore` Index Event Store
    3. `Function`: AWS Lambda Function that can be fed via a DynamoDB Streams Event Source Mapping; passes to `Handler`

  (Diagnostics are exposed via Console to CloudWatch)

- `Propulsion.DynamoStore.Notifier` [![NuGet](https://img.shields.io/nuget/v/Propulsion.DynamoStore.Notifier.svg)](https://www.nuget.org/packages/Propulsion.DynamoStore.Notifier/) AWS Lambda to report new events indexed by the Indexer to an SNS Topic, in order to enable triggering AWS Lambdas to service Reactions without requiring a long-lived host application. [Depends](https://www.fuget.org/packages/Propulsion.DynamoStore.Notifier) on `Amazon.Lambda.Core`, `Amazon.Lambda.DynamoDBEvents`, `Amazon.Lambda.Serialization.SystemTextJson`, `AWSSDK.SimpleNotificationService`

    1. `Handler`: parses Dynamo DB Streams Source Mapping input, generates a message per updated Partition in the batch
    2. `Function`: AWS Lambda Function that can be fed via a DynamoDB Streams Event Source Mapping; passes to `Handler`

  (Diagnostics are exposed via Console to CloudWatch)

- `Propulsion.DynamoStore.Constructs` [![NuGet](https://img.shields.io/nuget/v/Propulsion.DynamoStore.Constructs.svg)](https://www.nuget.org/packages/Propulsion.DynamoStore.Constructs/) AWS Lambda CDK deploy logic. [Depends](https://www.fuget.org/packages/Propulsion.DynamoStore.Constructs) on `Amazon.CDK.Lib` (and, indirectly, on the binary assets included as content in the `Propulsion.DynamoStore.Indexer`/`Propulsion.DynamoStore.Notifier` NuGet packages)

    1. `DynamoStoreIndexerLambda`: CDK wiring for `Propulsion.DynamoStore.Indexer`
    2. `DynamoStoreNotifierLambda`: CDK wiring for `Propulsion.DynamoStore.Notifier`
    3. `DynamoStoreReactorLambda`: CDK wiring for a Reactor that's triggered based on messages supplied by `Propulsion.DynamoStore.Notifier` 

- `Propulsion.DynamoStore.Lambda` [![NuGet](https://img.shields.io/nuget/v/Propulsion.DynamoStore.Lambda.svg)](https://www.nuget.org/packages/Propulsion.DynamoStore.Lambda/) Helpers for implementing Lambda Reactors. [Depends](https://www.fuget.org/packages/Propulsion.DynamoStore.Lambda) on `Amazon.Lambda.SQSEvents`

    1. `SqsNotificationBatch.parse`: parses a batch of notification events (queued by a `Notifier`) in a `Amazon.Lambda.SQSEvents.SQSEvent`
    2. `SqsNotificationBatch.batchResponseWithFailuresForPositionsNotReached`: Correlates the updated checkpoints with the input `SQSEvent`, generating a `SQSBatchResponse` that will requeue any notifications that have not yet been serviced.

  (Used by [`eqxShipping` template](https://github.com/jet/dotnet-templates/tree/master/equinox-shipping))

- `Propulsion.EventStoreDb` [![NuGet](https://img.shields.io/nuget/v/Propulsion.EventStoreDb.svg)](https://www.nuget.org/packages/Propulsion.EventStoreDb/). Provides bindings to [EventStoreDB](https://www.eventstore.org), writing via `Propulsion.EventStore.EventStoreSink`. [Depends](https://www.fuget.org/packages/Propulsion.EventStoreDb) on `Equinox.EventStoreDb` v `4.0.0`
    1. `EventStoreSource`: reading from an EventStoreDB >= `20.10` `$all` stream using the gRPC interface into a `Propulsion.Sink`.
    2. `EventStoreSink`: writing to `Equinox.EventStoreDb` v `4.0.0`

- `Propulsion.Kafka` [![NuGet](https://img.shields.io/nuget/v/Propulsion.Kafka.svg)](https://www.nuget.org/packages/Propulsion.Kafka/) Provides bindings for producing and consuming both streamwise and in parallel. Includes a standard codec for use with streamwise projection and consumption, `Propulsion.Kafka.Codec.NewtonsoftJson.RenderedSpan`. [Depends](https://www.fuget.org/packages/Propulsion.Kafka) on `FsKafka` v `1.7.0`-`1.9.99`

- `Propulsion.MessageDb` [![NuGet](https://img.shields.io/nuget/v/Propulsion.MessageDb.svg)](https://www.nuget.org/packages/Propulsion.MessageDb/). Provides bindings to [MessageDb](http://docs.eventide-project.org/user-guide/message-db/), maintaining checkpoints in a postgres table [Depends](https://www.fuget.org/packages/Propulsion.MessageDb) on `Propulsion`, `Npgsql` >= `7.0.7` [#181](https://github.com/jet/propulsion/pull/181) :pray: [@nordfjord](https://github.com/nordfjord)
  1. `MessageDbSource`: reading from one or more MessageDB categories into a `Propulsion.Sink`
  2. `CheckpointStore`: checkpoint storage for `Propulsion.Feed` using `Npgsql` (can be initialized via `propulsion initpg -c connstr -s schema`)

- `Propulsion.SqlStreamStore` [![NuGet](https://img.shields.io/nuget/v/Propulsion.SqlStreamStore.svg)](https://www.nuget.org/packages/Propulsion.SqlStreamStore/). Provides bindings to [SqlStreamStore](https://github.com/SQLStreamStore/SQLStreamStore), maintaining checkpoints in a SQL Server table.  [Depends](https://www.fuget.org/packages/Propulsion.SqlStreamStore) on `Propulsion`, `SqlStreamStore`, `Dapper` v `2.0`, `Microsoft.Data.SqlClient` v `1.1.3`

  1. `SqlStreamStoreSource`: reading from a SqlStreamStore `$all` stream into a `Propulsion.Sink`
  2. `ReaderCheckpoint`: checkpoint storage for `Propulsion.EventStoreDb`/`Feed`/`SqlStreamSteamStore` using `Dapper`, `Microsoft.Data.SqlClient`

The ubiquitous `Serilog` dependency is solely on the core module, not any sinks.

### `dotnet tool` provisioning / projections test tool

- `Propulsion.Tool` [![Tool NuGet](https://img.shields.io/nuget/v/Propulsion.Tool.svg)](https://www.nuget.org/packages/Propulsion.Tool/): Tool used to initialize a Change Feed Processor `aux` container for `Propulsion.CosmosStore` and demonstrate basic projection, including to Kafka. See [quickstart](#quickstart).

    - `init`: CosmosDB: Initialize an `-aux` Container for use by the CosmosDb client library ChangeFeedProcessor
    - `initpg`: MessageDb: Initialize a checkpoints table in a Postgres Database
    - `index`: DynamoStore: validate and/or reindex DynamoStore Index
    - `checkpoint`: CosmosStore/DynamoStore/EventStoreDb/Feed/MessageDb/SqlStreamStore: adjust checkpoints in DynamoStore/CosmosStore/SQL Server/Postgres
    - `project`: CosmosDB/DynamoStore/EventStoreDb/MessageDb: walk change feeds/indexes and/or project to Kafka

## Deprecated components

Propulsion supports recent versions of Equinox and other Store Clients within reason - these components are
intended for use on a short term basis as a way to manage phased updates from older clients to current ones by
adjusting package references while retaining source compatibility to the maximum degree possible.

- `Propulsion.CosmosStore3` [![NuGet](https://img.shields.io/nuget/v/Propulsion.CosmosStore3.svg)](https://www.nuget.org/packages/Propulsion.CosmosStore3/) Provides bindings to Azure CosmosDB. [Depends](https://www.fuget.org/packages/Propulsion.CosmosStore3) on `Equinox.CosmosStore` v `3.0.7`, `Microsoft.Azure.Cosmos` v `3.27.0`

    - **Deprecated; only intended for use in migration from Propulsion.Cosmos and/or Equinox.Cosmos**
    1. `CosmosStoreSource`: reading from CosmosDb's ChangeFeed  using `Microsoft.Azure.Cosmos` (relies on explicit checkpointing that entered GA in `3.21.0`)
    2. `CosmosStoreSink`: writing to `Equinox.CosmosStore` v `3.0.7`.
    3. `CosmosStorePruner`: pruning from `Equinox.CosmosStore` v `3.0.7`.
    4. `ReaderCheckpoint`: checkpoint storage for `Propulsion.EventStoreDb`/`DynamoStore`/'Feed'/`SqlStreamSteamStore` using `Equinox.CosmosStore` v `3.0.7`.

  (Reading and position metrics are exposed via `Propulsion.CosmosStore.Prometheus`)

- `Propulsion.EventStore` [![NuGet](https://img.shields.io/nuget/v/Propulsion.EventStore.svg)](https://www.nuget.org/packages/Propulsion.EventStore/). Provides bindings to [EventStore](https://www.eventstore.org), writing via `Propulsion.EventStore.EventStoreSink` [Depends](https://www.fuget.org/packages/Propulsion.EventStore) on `Equinox.EventStore` v `4.0.0`

    - **Deprecated as reading (and writing) relies on the legacy EventStoreDB TCP interface**
    - Contains ultra-high throughput striped reader implementation
    - Presently Used by [`proSync` template](https://github.com/jet/dotnet-templates/tree/master/propulsion-sync)

  (Reading and position metrics are emitted to Console / Serilog; no Prometheus support)

## Related repos

- See [the Equinox QuickStart](https://github.com/jet/equinox#quickstart) for examples of using this library to project to Kafka from `Equinox.CosmosStore`, `Equinox.DynamoStore` and/or `Equinox.EventStoreDb`.

- See [the `dotnet new` templates repo](https://github.com/jet/dotnet-templates) for examples using the packages herein:

    - [Propulsion-specific templates](https://github.com/jet/dotnet-templates#propulsion-related):
      - `proProjector` template for `CosmosStoreSource`+`StreamsSink` logic consuming from a CosmosDb `ChangeFeedProcessor`.
      - `proProjector` template (in `--kafka` mode) for producer logic using `StreamsProducerSink` or `ParallelProducerSink`.
      - `proConsumer` template for example consumer logic using `ParallelConsumer` and `StreamsConsumer` etc.
      
    - [Propulsion+Equinox templates](https://github.com/jet/dotnet-templates#producerreactor-templates-combining-usage-of-equinox-and-propulsion):
      - `eqxShipping`: Event-sourced example with a Process Manager. Includes a `Watchdog` component that uses a `StreamsSink`, with example wiring for `CosmosStore`, `DynamoStore` and `EventStoreDb`  
      - `proIndexer`. single-source `StreamsSink` based Reactor. More legible version of `proReactor` template, currently only supports `Propulsion.CosmosStore`, and provides some specific extensions such as updating snapshots.
      - `proReactor` generic template, supporting multiple sources and multiple processing modes
      - `summaryConsumer` consumes from the output of a `proReactor --kafka`, saving them in an `Equinox.CosmosStore` store
      - `trackingConsumer` consumes from Kafka, feeding into example Ingester logic in an `Equinox.CosmosStore` store 
      - `proSync` is a fully fledged store <-> store synchronization tool syncing from a `CosmosStoreSource` or `EventStoreSource` to a `CosmosStoreSink` or `EventStoreSink`
      - `feedConsumer`,`feedSource`: illustrating usage of `Propulsion.Feed.FeedSource`
      - `periodicIngester`: illustrating usage of `Propulsion.Feed.PeriodicSource`
      - `proArchiver`, `proPruner`: illustrating usage of [hot/cold](https://github.com/jet/equinox/blob/master/DOCUMENTATION.md#hot-cold) support and support for secondary fallback in `Equinox.CosmosStore`

- See [the `FsKafka` repo](https://github.com/jet/FsKafka) for `BatchedProducer` and `BatchedConsumer` implementations (together with the `KafkaConsumerConfig` and `KafkaProducerConfig` used in the Parallel and Streams wrappers in `Propulsion.Kafka`)

# Overview

## The Equinox Perspective

Propulsion and Equinox have a [Yin and yang](https://en.wikipedia.org/wiki/Yin_and_yang) relationship; their use cases naturally interlock and overlap.

See [the Equinox Documentation's Overview Diagrams](https://github.com/jet/equinox/blob/master/DOCUMENTATION.md#overview) for the perspective from the other side (TL;DR the same topology, with elements that are de-emphasized here central over there, and vice versa)

## [C4](https://c4model.com) Context diagram

Equinox focuses on the **Consistent Processing** element of building an event-sourced decision processing system, offering relevant components that interact with a specific **Consistent Event Store**. Propulsion elements support the building of complementary facilities as part of an overall Application. Conceptually one can group such processing based on high level roles such as:

- **Ingesters**: gather/consume data/events from outside the Bounded Context of the System. This role covers aspects such as feeding reference data into **Read Models**, ingesting changes into a consistent model via **Consistent Processing**. _These services are not acting in reaction to events emanating from the system's **Consistent Event Store**, as opposed to..._
- **Publishers**: react to events as they are fed from the **Consistent Event Store** by filtering, rendering and emitting to feeds for downstream systems. _While these services may in some cases rely on synchronous queries via **Consistent Processing**, they are not themselves transacting or driving follow-on work; which brings us to..._
- **Reactors**: drive reactive actions triggered based on upstream feeds, or events observed in the **Consistent Event Store**. _These services handle anything beyond the duties of **Ingesters** or **Publishers**, and will often drive follow-on processing via Process Managers and/or transacting via **Consistent Processing**. In some cases, a reactor app's function may be to progressively compose a notification for a **Publisher** to eventually publish._

The overall territory is laid out here in this [C4](https://c4model.com) System Context Diagram:

![Propulsion c4model.com Context Diagram](http://www.plantuml.com/plantuml/proxy?cache=no&src=https://raw.github.com/jet/propulsion/master/diagrams/context.puml&fmt=svg)

<!-- ## [C4](https://c4model.com) Container diagram

The relevant pieces of the above break down as follows, when we emphasize the [Containers](https://c4model.com) aspects relevant to Propulsion:

![Propulsion c4model.com Container Diagram](http://www.plantuml.com/plantuml/proxy?cache=no&src=https://raw.github.com/jet/propulsion/master/diagrams/container.puml&fmt=svg)

 -->

**[See Overview section in `DOCUMENTATION`.md for further drill down](https://github.com/jet/propulsion/blob/master/DOCUMENTATION.md#overview)**

## QuickStart

### 1. Use `propulsion` tool to run a CosmosDb ChangeFeedProcessor or DynamoStoreSource projector

```powershell
dotnet tool uninstall Propulsion.Tool -g
dotnet tool install Propulsion.Tool -g --prerelease

propulsion init -ru 400 cosmos # generates a -aux container for the ChangeFeedProcessor to maintain consumer group progress within
# -V for verbose ChangeFeedProcessor logging
# `-g projector1` represents the consumer group - >=1 are allowed, allowing multiple independent projections to run concurrently
# stats specifies one only wants stats regarding items (other options include `kafka` to project to Kafka)
# cosmos specifies source overrides (using defaults in step 1 in this instance)
propulsion -V sync -g projector1 stats from cosmos

# load events with 2 parallel readers, detailed store logging and a read timeout of 20s
propulsion -VS sync -g projector1 stats from dynamo -rt 20 -d 2
```

### 2. Use `propulsion` tool to Run a CosmosDb ChangeFeedProcessor or DynamoStoreSource projector, emitting to a Kafka topic 

```powershell
$env:PROPULSION_KAFKA_BROKER="instance.kafka.mysite.com:9092" # or use -b

# `-V` for verbose logging
# `-g projector3` represents the consumer group; >=1 are allowed, allowing multiple independent projections to run concurrently
# `-l 5` to report ChangeFeed lags every 5 minutes
# `kafka` specifies one wants to emit to Kafka
# `temp-topic` is the topic to emit to
# `cosmos` specifies source overrides (using defaults in step 1 in this instance)
propulsion -V sync -g projector3 -l 5 kafka temp-topic from cosmos
```

### 3. Use `propulsion` tool to inspect DynamoStore Index

Summarize current state of the index being prepared by `Propulsion.DynamoStore.Indexer`

    propulsion index dynamo -t equinox-test

Example output:

    19:15:50 I Current Partitions / Active Epochs [[0, 354], [2, 15], [3, 13], [4, 13], [5, 13], [6, 64], [7, 53], [8, 53], [9, 60]]  
    19:15:50 I Inspect Index Partitions list events 👉 eqx -C dump '$AppendsIndex-0' dynamo -t equinox-test-index  
    19:15:50 I Inspect Batches in Epoch 2 of Index Partition 0 👉 eqx -C dump '$AppendsEpoch-0_2' -B dynamo -t equinox-test-index

### 4. Use `propulsion` tool to validate DynamoStoreSource Index  

Validate `Propulsion.DynamoStore.Indexer` has not missed any events (normally you guarantee this by having alerting on Lambda failures) 
 
    propulsion index -p 0 dynamo -t equinox-test

### 5. Use `propulsion` tool to reindex and/or add missing notifications

In addition to being able to validate the index (see preceding step), the tool facilitates ingestion of missing events from a complete [DynamoDB JSON Export](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DataExport.html). Steps are as follows:

0. Enable Point in Time Restores in DynamoDB
1. Export data to S3, download and extract JSON from `.json.gz` files
2. Run ingestion job    

       propulsion index -t 0 $HOME/Downloads/DynamoDbS3Export/*.json dynamo -t equinox-test

## CONTRIBUTING

See [CONTRIBUTING.md](CONTRIBUTING.md)

## TEMPLATES

The best place to start, sample-wise is with the the [Equinox QuickStart](https://github.com/jet/equinox#quickstart), which walks you through sample code, tuned for approachability, from `dotnet new` templates stored [in a dedicated repo](https://github.com/jet/dotnet-templates).

## BUILDING

Please note the [QuickStart](#quickstart) is probably the best way to gain an overview, and the templates are the best way to see how to consume it; these instructions are intended mainly for people looking to make changes. 

NB The `Propulsion.Kafka.Integration` tests are reliant on a `TEST_KAFKA_BROKER` environment variable pointing to a Broker that has been configured to auto-create ephemeral Kafka Topics as required by the tests (each test run blindly writes to a guid-named topic and trusts the broker will accept the write without any initialization step)

### build, including tests

```powershell
dotnet build build.proj -v n
```

## FAQ

<a name="why-not-cfp-all-the-things"></a>
### Why do you employ Kafka as an additional layer, when downstream processes could simply subscribe directly and individually to the relevant CosmosDB change feed(s)? Is it to accommodate other messages besides those emitted from events and snapshot updates? :pray: [@Roland Andrag](https://github.com/randrag)

Well, Kafka is definitely not a critical component or a panacea.

You're correct that the bulk of things that can be achieved using Kafka can be accomplished via usage of the ChangeFeed. One thing to point out is that in the context of enterprise systems, having a well maintained Kafka cluster does have less incremental (or total) cost than it might do if you're building a smaller system from nothing.

Some negatives of consuming from the ChangeFeed directly:

- each CFP reader induces RU consumption (its a set of continuous queries against each and every physical partition of which the Cosmos Container is composed)
- you can't apply a server-side filter, so you pay to see the full content of any document that's touched
- there's an elevated risk of implementation shortcuts that couple the reaction logic to low level specifics of the store or the data structures 
- (as you alluded to), if there's some logic or work involved in the production of events you'd emit to Kafka, each consumer would need to duplicate that

Many of these concerns can be alleviated to varying degrees by splitting the storage up into multiple Containers (potentially using database level RU allocations) such that each consumer will intrinsically be interested in a large proportion of the data it will observe, the write amplification effects of having multiple consumers will always be more significant when reading directly than when having a single reader emit to Kafka. The design of Kafka is specifically geared to running lots of concurrent readers.

However, splitting event categories into Containers solely to optimize these effects can also make the management of the transactional workload more complex; the ideal for any given Container is thus to balance the concerns of:

- ensuring that datasets for which you want to ringfence availability / RU allocations don't share with containers/databases for which running hot (potentially significant levels of rate limiting but overall high throughput in aggregate as a result of maximizing the percentage of the allocated capacity that's being used over time)
- avoiding prematurely splitting data prior to it being required by the constraints of CosmosDB (i.e. you want to let splitting primarily be driven by reaching the [10GB] physical partition size limit)
- not having logical partition hotspots that lead to a small subset of physical partitions having significantly above average RU consumption
- having relatively consistent document sizes
- economies of scale - if each container (or database if you provision at that level) needs to be individually managed (with a degree of headroom to ensure availability for load spikes etc), you'll tend to require higher aggregate RU assignment for a given overall workload based on a topology that has more containers

### Any tips for testing Propulsion (projection) in an integration/end-to-end fashion? :pray: [@James Booth](https://github.com/absolutejam)

> I know for unit testing, I can just test the obvious parts. Or if end to end testing is even required

Depends what you want to achieve. One important technique for doing end-to-end scenarios, especially where some reaction is supposed to feed back into Equinox is to use `Equinox.MemoryStore` as the store, and then wire the Propulsion Sink (that will be fed from your real store when deployed in a production scenario) consume from that using `Propulsion.MemoryStore.MemoryStoreProjector`. 

Other techniques I've seen/heard are:
- rig things to use ephemeral ESDB or Cosmos databases (the CosmosDB emulator works but has restrictions; perhaps you can use serverless or database level RU allocated DBs in a shared environment) to run your system with an isolated throwaway storage with better performance, stability and/or cost properties for test purposes.
- Once you have a store, the next question is how to validate your projector (Publisher / Reactor) apps. In some cases, people opt to spin up a large subset of the production system (maybe in docker-compose etc) and then check for externally visible effects in tests.
- While it's important to do end-to-end tests with as much of the whole system as possible, that does tend to make for a messy test suite that quickly becomes unmaintainable. In general, the solution is to do smaller test scenarios that achieve that same goal by triangulating on subsets of the overall reactions as smaller scenarios. See the `Shipping.Watchdog.Integration` test suite in the `equinox-shipping` template for an example.

In general I'd be looking to use `MemoryStoreProjector` as a default technique, as it provides:
- the best performance by far (all synchronous and in-memory, without any simulators)
- a deterministic wait mechanism; after arranging a particular system state, you can pause until a reaction has been processed by using the projector's `AwaitCompletion` facility to efficiently wait for the exact moment at which the event has been handled by the reactor component without padded Sleep sequences (or, worse: retry loops).   

To answer more completely, I'd say given a scenario involving Propulsion and Equinox, you'll typically have the following ingredients:

1. writing to the store - you can either assume that's well-tested infra or take the view that you need to validate that you wired it up properly
2. serialization/deserialization - you can either have unit tests and/or property tests to validate round-tripping as an orthogonal concern, or you can take the view that it's critical to know it really works with real data
3. reading from the store's change feed and propagating to handler - that's harder to config and has the biggest variability in a test scenario so either:

   - you want to take it out of the equation
   - OR you want to know its wired properly
  
4. validating that triggered reactions are handled and complete cleanly - yes you can and should unit test that, but maybe you want to know it works end-to-end with a much larger proportion of the overall system in play

5. does it trigger follow-on work, i.e. a cascade of reactions. You can either do triangulation and say its proven if I observe the trigger for the next bit, or you may want to prove that end to end

6. does the entire system as a whole really work - sometimes you want to be able to validate workflows rather than having to pay the complexity tax of going in the front door for every aspect (though you'll typically want to have a meaningful set of smoke tests that validate basic system integrity without requiring manual testing or back-door interfaces) 

### Any reason you didn’t use one of the different subscription models available in ESDB? :pray: [@James Booth](https://github.com/absolutejam)

#### TL;DR Differing goals

While the implementation and patterns in Propulsion happen to overlap to a degree with the use cases of the ESDB's subscription mechanisms, the main reason they are not used directly stems from the needs and constraints that Propulsion was evolved to cover. 

One thing that should be clear is that Propulsion is definitely *not* attempting to be the simplest conceivable projection library with a low concept count that's easy to get started with. If you were looking to build such a library, you'll likely give yourself some important guiding non-goals to enable that, e.g., if you had to add 3 concepts to get a 50% improvement in throughput, whether or not that's worth it depends on the context - if you're trying to have a low concept count, you might be prepared to leave some performance on the table to enable that.

For Propulsion, almost literally, job one was to be able to shift 1TB of ordered events in streams to/from ESDB/Cosmos/Kafka in well under 24h - a naive implementation reading and writing in small batches takes more like 24d to do the same thing. A key secondary goal was to be able to keep them in sync continually after that point (it's definitely more than a one time bulk ingestion system).

While Propulsion scales down to running simple subscriptions, its got quite a few additional concepts compared to using something built literally for that exact job; the general case of arbitrary projections was almost literally an afterthought.

That's not to say that Propulsion's concepts make for a more complex system when all is said and done; there are lots of scenarios where you avoid having to do concurrent/async tricks one might otherwise do more explicitly in a more simplistic subscription system.

When looking at the vast majority of typical projections/reactions/denormalizers one runs in an event-sourced system it should come as no surprise that EventStoreDB's subscription features offer plenty ways of achieving those common goals with a good balance of:
- time to implement
- ease of operation
- good enough performance

_That's literally the company's goal: enabling rapidly building systems to solve business problems, without overfitting to any specific industry or application's needs_. 

The potential upsides that Propulsion can offer when used as a projection system can definitely be valuable _when actually needed_, but on average, they can equally be overkill for a given specific requirement.

With that context set, here are some notable aspects of using Propulsion for Projectors rather than building bespoke wiring on a case by case basis:
- similar APIs designs and concepts regardless of whether events arrive via CosmosDB, DynamoDB, EventStoreDB, MessageDB, SqlStreamStore, Kafka (or custom sources via `Propulsion.Feed`)
- consistent dashboards across all those sources
- generally excellent performance for high throughput scenarios (it was built for that)
- good handling for processing of workloads that don't have uniform (and low) cost per handler invocation, i.e., rate-limited writes of events to `Equinox.CosmosStore` or `Equinox.DynamoStore` (compared to e.g. using a store such as Redis)
- orthogonality to Equinox features while still offering a degree of commonality of concepts and terminology
- provide a degree of isolation from the low level drivers, e.g.
  - moving from the deprecated Cosmos CFP V2 to any future `Azure.Cosmos` V4 SDK will be a matter of changing package references and fixing some minimal compilation errors, as opposed to learning a whole new API set
  - moving from EventStore's TCP API / `EventStore.ClientAPI` to the gRPC based >= v20 clients is a simple package switch
  - migrating a workload from EventStoreDB to or from CosmosDB or DynamoDN can be accomplished more cleanly if you're only changing the wiring of your projector host while making no changes to the handler implementations or the bulk of the reactor applicatino 
  - SqlStreamStore and MessageDB also fit into the overall picture; using Propulsion can gives a cleaner mix and match / onramp to/from ESDB (Note however that migrating SSS <-> ESDB is a relatively trivial operation vs migrating from raw EventStore usage to `Equinox.CosmosStore` or `Equinox.DynamoStore`, i.e. "we're using Propulsion to isolate us from deciding between SSS or ESDB" may not be a good enough reason on its own)
- Specifically when consuming from CosmosDB, being able to do that over a longer wire by feeding to Kafka to limit RU consumption from projections is a relatively minor change.

#### A Brief History of Propulsion's feature set

The order in which the need for various components arose (as a side effect of building out [Equinox](https://github.com/jet/equinox); solving specific needs in terms of feeding events into and out of EventStoreDB, CosmosDB and Kafka) was also an influence on the abstractions within and general facilities of Propulsion.

  - `Propulsion.Cosmos`'s `Source` was the first bit done; it's a light wrapper over the [CFP V2 client](https://github.com/Azure/azure-documentdb-changefeedprocessor-dotnet). Key implications from that are:
    - order of events in a logical partition can and should be maintained
    - global ordering of events across all logical streams is not achievable due to how CosmosDB works (the only ordering guarantees are at logical partition level, the data can physically split at any time as data grows)
  - `Propulsion.Kafka`'s `Sink` was next; the central goal here is to be able to replicate events being read from CosmosDB onto a Kafka Topic _maintaining the ordering guarantees_.
   There are two high level ways of achieving ordering guarantees in Kafka:
    1. only ever have a single event in flight; only when you've got the ack for a write do you send the next one. *However, literally doing that compromises throughput massively*.
    2. use Kafka's transaction facilities (not implemented in `Confluent.Kafka` at the time)
 
    => The approach used is to continuously emit messages concurrently in order to maintain throughput, but guarantee to never emit messages for the same _key_ at the same time.
  - `Propulsion.Cosmos`'s `Sink` was next up. It writes to CosmosDB using `Equinox.Cosmos`. Key implications:  
    - because rate-limiting is at the physical partition level, it's crucial for throughput that you keep other partitions busy while wait/retry loops are triggered by hotspots (and you absolutely don't want to exacerbate this effect by competing with yourself)
    - you want to batch the writing of multiple events/documents to minimize round-trips (write RU costs are effectively _O(log N)_ despite high level guidance characterizing it as _O(N)_))
    - you can only touch one logical partition for any given write (CosmosDB does not expose transactions across logical partitions)
    - when you hit a hotspot and need to retry, ideally you'd pack events queued as the last attempt was being executed into the retry attempt
    - there is no one-size fits all batch size ([yet](https://github.com/Azure/azure-cosmos-dotnet-v3/issues/1763)) that balances

      1. not overloading the source and
      2. maintaining throughput
       
      You'll often need a small batch size, which implies larger per-event checkpointing overhead unless you make the checkpointing asynchronous

      => The implementation thus:
        - manages reading asynchronously from the writing in order to maintain throughput (you define a batch size _and_ a number of batches to read ahead)
        - schedules write attempts at stream level (the reader concurrently ingests successor events, making all buffered events available when retrying)
        - writes checkpoints asynchronously as and when all the items involved complete within the (stream-level) processing
  - At the point where `Propulsion.EventStore`'s `Source` and `Sink` were being implemented (within weeks of the `CosmosStore` equivalents; largely overlapping), the implications from realizing goals of providing good throughput while avoiding adding new concepts if that can be avoided are:
    - The cheapest (lowest impact in terms of triggering scattered reads across disks on an ESDB server, with associated latency implications) and most general API set for reading events is to read the `$all` stream
    - Maintaining checkpoints in an EventStoreDB that you're also monitoring is prone to feedback effects (so using the Async checkpointing strategy used for `.CosmosStore` but saving them in an external store such as an `Equinox.CosmosStore` makes sense)
    - If handlers and/or sinks don't have uniform processing time per message and/or are subject to rate limiting, most of the constraints of the `CosmosSStoreink` apply too; you don't want to sit around retrying the last request out of a batch of 100 while tens of thousands of provisioned RUs are sitting idle in Cosmos with throughput sitting close to zero

#### Conclusion/comparison checklist

The things Propulsion in general accomplishes in the projections space:
- Uniform dashboards for throughput, successes vs failures, and latency distributions over CosmosDB, DynamoDB, EventStoreDB, MessageDb, SqlStreamStore, Kafka (and custom application-specific Feeds via `Propulsion.Feed`)
- Source-neutral metrics to support trustworthy alerting and detailed analysis of busy, failing and stuck projections
- reading, checkpointing, parsing and running handlers are all independent asynchronous activities
- enables handlers to handle backlog of accumulated items for a stream as a batch if desired
- maximize concurrency across streams
- strong intrinsic support for handling idempotent processing in the face of at least once delivery and/or catchup scenarios
- `CosmosStoreSource` provides for automatic load balancing over multiple instances of a Reactor application akin to how Kafka Clients manage that (as a light wrapper over the Cosmos SDK's ChangeFeedProcessor lease management system without any custom semantics beyond the proven scheme the SDK implements)
- provide good instrumentation as to latency, errors, throughput in a pluggable way akin to how Equinox does stuff (e.g. it has built-in Prometheus support)
- handlers/reactors/projections can be ported from `Propulsion.Cosmos` to `Propulsion.CosmosStore3` and `Propulsion.CosmosStore` by swapping driver modules; similar to how `Equinox.Cosmos` vs `Equinox.EventStore` provides a common programming model despite the underpinnings being fundamentally quite different in nature
- good stories for isolating from specific drivers - i.e., there's a `Propulsion.CosmosStore` (for the V3 SDK) with close-to-identical interfaces (Similarly there's a `Propulsion.EventStoreDb` using the gRPC-based SDKs, replacing the deprecated `Propulsion.EventStore`)
- Kafka reading and writing generally fits within the same patterns - i.e. if you want to push CosmosDb CFP output to Kafka and consume over that as a 'longer wire' without placing extra load on the source if you have 50 consumers, you can stand up a ~250 line `dotnet new proProjector` app, and tweak the ~30 lines of consumer app wireup to connect to Kafka instead of CosmosDB

Things EventStoreDB's subscriptions can do that are not presently covered in Propulsion:
- `$et-`, `$ec-` streams
- honoring the full `$all` order
- and more; EventStoreDB is a well designed purpose-built solution catering for diverse system sizes and industries

### What's the deal with the early history of this repo?

This repo is derived from [`FsKafka`](https://github.com/jet/FsKafka); the history has been edited to focus only on edits to the `Propulsion` libraries.

### Your question here

- Please feel free to log question-issues; they'll get answered here

# FURTHER READING

See [`DOCUMENTATION.md`](DOCUMENTATION.md) and [Equinox's `DOCUMENTATION.md`](https://github.com/jet/equinox/blob/master/DOCUMENTATION.md)
