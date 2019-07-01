# Propulsion [![Build Status](https://dev.azure.com/jet-opensource/opensource/_apis/build/status/jet.Propulsion)](https://dev.azure.com/jet-opensource/opensource/_build/latest?definitionId=16) [![release](https://img.shields.io/github/release-pre/jet/propulsion.svg)](https://github.com/jet/propulsion/releases) [![NuGet](https://img.shields.io/nuget/vpre/Propulsion.svg?logo=nuget)](https://www.nuget.org/packages/Propulsion/) [![license](https://img.shields.io/github/license/jet/propulsion.svg)](LICENSE) ![code size](https://img.shields.io/github/languages/code-size/jet/propulsion.svg) [<img src="https://img.shields.io/badge/slack-DDD--CQRS--ES%20%23equinox-yellow.svg?logo=slack">](https://t.co/MRxpx0rLH2)

This is pre-production code; unfortunately it's also pre-documentation atm (there will be eventually, but it's competing with other issues for time...).

If you're looking for a good discussion forum on these kinds of topics, look no further than the [DDD-CQRS-ES Slack](https://github.com/ddd-cqrs-es/slack-community)'s [#equinox channel](https://ddd-cqrs-es.slack.com/messages/CF5J67H6Z) ([invite link](https://t.co/MRxpx0rLH2)).

## Components

The components within this repository are delivered as a multi-targeted Nuget package targeting `net461` (F# 3.1+) and `netstandard2.0` (F# 4.5+) profiles

- `Propulsion` [![NuGet](https://img.shields.io/nuget/vpre/Propulsion.svg)](https://www.nuget.org/packages/Propulsion/) Implements core functionality in a channel-independent fashion including `ParallelProjector`, `StreamsProjector`. [Depends](https://www.fuget.org/packages/Propulsion) on `MathNet.Numerics`, `Serilog`
- `Propulsion.Cosmos` [![NuGet](https://img.shields.io/nuget/vpre/Propulsion.Cosmos.svg)](https://www.nuget.org/packages/Propulsion.Cosmos/) Provides bindings to Azure CosmosDb a) writing to `Equinox.Cosmos` :- `CosmosSink` b) reading from CosmosDb's changefeed by wrapping the [`dotnet-changefeedprocessor` library](https://github.com/Azure/azure-documentdb-changefeedprocessor-dotnet) :- `CosmosSource`. [Depends](https://www.fuget.org/packages/Propulsion.Cosmos) on `Equinox.Cosmos`, `Microsoft.Azure.DocumentDB.ChangeFeedProcessor`, `Serilog`
- `Propulsion.EventStore` [![NuGet](https://img.shields.io/nuget/vpre/Propulsion.EventStore.svg)](https://www.nuget.org/packages/Propulsion.EventStore/). Provides bindings to [EventStore](eventstore.org), writing via `Propulsion.EventStore.EventStoreSink` [Depends](https://www.fuget.org/packages/Propulsion.EventStore) on `Equinox.EventStore`, `Serilog`
- `Propulsion.Kafka` [![NuGet](https://img.shields.io/nuget/vpre/Propulsion.Kafka.svg)](https://www.nuget.org/packages/Propulsion.Kafka/) Provides bindings for producing and consuming both streamwise and in parallel. Includes a standard codec for use with streamwise projection and consumption, `Propulsion.Kafka.Codec.NewtonsoftJson.RenderedSpan`. Implements a `KafkaMonitor` that can log status information based on [Burrow](https://github.com/linkedin/Burrow). [Depends](https://www.fuget.org/packages/Propulsion.Kafka) on `Jet.ConfluentKafka.FSharp` v ` >= 1.1.0`, `Serilog`
- `Propulsion.Kafka0` [![NuGet](https://img.shields.io/nuget/vpre/Propulsion.Kafka0.svg)](https://www.nuget.org/packages/Propulsion.Kafka0/). Same functionality/purpose as `Propulsion.Kafka` but targets older `Confluent.Kafka`/`librdkafka` version for for interoperability with systems that have a hard dependency on that. [Depends](https://www.fuget.org/packages/Propulsion.Kafka0) on `Confluent.Kafka [0.11.3]`, `librdkafka.redist [0.11.4]`, `Serilog`

The ubiquitous `Serilog` dependency is solely on the core module, not any sinks, i.e. you configure to emit to `NLog` etc.

### `dotnet tool` provisioning / projections test tool

- `Propulsion.Tool` [![Tool NuGet](https://img.shields.io/nuget/v/Propulsion.Tool.svg)](https://www.nuget.org/packages/Propulsion.Tool/): Tool used to initialize a Change Feed Processor `aux` container for `Propulsion.Cosmos` and demonstrate basic projection, including to Kafka. (Install via: `dotnet tool install Propulsion.Tool -g`)

## Related repos

- See [the Jet `dotnet new` templates repo](https://github.com/jet/dotnet-templates) for examples using the packages herein:

    - `proProjector` template for example `CosmosSource` logic consuming from a CosmosDb `ChangeFeedProcessor`.
    - `proProjector` template (in `-k` mode) for example producer logic using `StreamsProducer`, `StreamsProjector` and `ParallelProducer`.
    - `proConsumer` template for example consumer logic using `ParallelConsumer` and `StreamsConsumer`.
    - `proSync` template for examples of binding a `CosmosSource` or `EventStoreSource` to a `CosmosSink` or `EventStoreSink`.

- See [the `Jet.ConfluentKafka.FSharp` repo](https://github.com/jet/Jet.ConfluentKafka.FSharp) for `BatchedProducer` and `BatchedConsumer` implementations (together with the `KafkaConsumerConfig` and `KafkaProducerConfig` used in the Parallel and Streams wrappers in `Propulsion.Kafka`)

- See [the Equinox QuickStart](https://github.com/jet/equinox#quickstart) for examples of using this library to project to Kafka from `Equinox.Cosmos` and/or `Equinox.EventStore`.

## QuickStart

### 1. Use `propulsion` tool to run a CosmosDb ChangeFeedProcessor

```powershell
# TEMP: need to uninstall and use --version flag while this is in RC
dotnet tool uninstall Propulsion.Tool -g
dotnet tool install Propulsion.Tool -g --version 1.0.1-rc*

propulsion init -ru 400 cosmos # generates a -aux container for the ChangeFeedProcessor to maintain consumer group progress within
# -v for verbose ChangeFeedProcessor logging
# `projector1` represents the consumer group - >=1 are allowed, allowing multiple independent projections to run concurrently
# stats specifies one only wants stats regarding items (other options include `kafka` to project to Kafka)
# cosmos specifies source overrides (using defaults in step 1 in this instance)
propulsion -v project projector1 stats cosmos
```

### 2. Use `propulsion` tool to Run a CosmosDb ChangeFeedProcessor, emitting to a Kafka topic

```powershell
$env:PROPULSION_KAFKA_BROKER="instance.kafka.mysite.com:9092" # or use -b

# `-v` for verbose logging
# `projector3` represents the consumer group; >=1 are allowed, allowing multiple independent projections to run concurrently
# `-l 5` to report ChangeFeed lags every 5 minutes
# `kafka` specifies one wants to emit to Kafka
# `temp-topic` is the topic to emit to
# `cosmos` specifies source overrides (using defaults in step 1 in this instance)
propulsion -v project projector3 -l 5 kafka temp-topic cosmos
```

# Projectors

See [this medium post regarding some patterns used at Jet in this space](https://medium.com/@eulerfx/scaling-event-sourcing-at-jet-9c873cac33b8) for a broad overview of the reasons one might consider employing a projection system.

# `Propulsion.Cosmos` Projection facilites

 An integral part of the `Equinox.Cosmos` featureset is the ability to project events based on the [Azure DocumentDb ChangeFeed mechanism](https://docs.microsoft.com/en-us/azure/cosmos-db/change-feed). Key elements involved in realizing this are:
- the [storage model needs to be designed in such a way that the aforementioned processor can do its job efficiently](https://github.com/jet/equinox/blob/master/DOCUMENTATION.md#cosmos-storage-model)
- there needs to be an active ChangeFeed Processor per container that monitors events being written, tracking the position of the most recently propagated events

In CosmosDb, every document lives within a [logical partition, which is then hosted by a variable number of processor instances entitled _physical partitions_](https://docs.microsoft.com/en-gb/azure/cosmos-db/partition-data) (`Equinox.Cosmos` documents pertaining to an individual stream bear the same partition key in order to ensure correct ordering guarantees for the purposes of projection). Each front end processor has responsibility for a particular subset range of the partition key space.

The ChangeFeed�s real world manifestation is as a long running Processor per frontend processor that repeatedly tails a query across the set of documents being managed by a given partition host (subject to topology changes - new processors can come and go, with the assigned ranges shuffling to balance the load per processor). e.g. if you allocate 30K RU/s to a container and/or store >20GB of data, it will have at least 3 processors, each handling 1/3 of the partition key space, and running a change feed from that is a matter of maintaining 3 continuous queries, with a continuation token each being held/leased/controlled by a given Change Feed Processor.

## Effect of ChangeFeed on Request Charges

It should be noted that the ChangeFeed is not special-cased by CosmosDb itself in any meaningful way - something somewhere is going to be calling a DocumentDb API queries, paying Request Charges for the privilege (even a tail request based on a continuation token yielding zero documents incurs a charge). Its important to consider that every event written by `Equinox.Cosmos` into the CosmosDb container will induce an approximately equivalent cost due to the fact that a freshly inserted document will be included in the next batch propagated by the Processor (each update of a document also �moves� that document from it�s present position in the change order past the the notional tail of the ChangeFeed). Thus each insert/update also induces an (unavoidable) request charge based on the fact that the document will be included aggregate set of touched documents being surfaced per batch transferred from the ChangeFeed (charging is per KiB or part thereof). _The effect of this cost is multipled by the number of ChangeFeedProcessor instances one is running._

## Change Feed Processors

The CosmosDb ChangeFeed�s real world manifestation is a continuous query per DocumentDb Physical Partition node processor.

For .NET, this is wrapped in a set of APIs presented within the standard `Microsoft.Azure.DocumentDb[.Core]` APIset (for example, the [`Sagan` library](https://github.com/jet/sagan) is built based on this, _but there be dragons; implementing a correct one you can trust, with tests, reliability and good performance is no trivial undertaking_).

A ChangeFeed _Processor_ consists of (per CosmosDb processor/range)
- a host process running somewhere that will run the query and then do something with the results before marking off progress
- a continuous query across the set of documents that fall within the partition key range hosted by a given physical partition host

The implementation in this repo uses [Microsoft�s .NET `ChangeFeedProcessor` implementation](https://github.com/Azure/azure-documentdb-changefeedprocessor-dotnet), which is a proven component used for diverse purposes including as the underlying substrate for various Azure Functions wiring (_though NOT bug free at the present time_).

See the [PR that added the initial support for CosmosDb Projections](https://github.com/jet/equinox/pull/87) and [the QuickStart](https://github.com/jet/equinox/blob/master/README.md#quickstart) for instructions.

# Feeding to Kafka

While [Kafka is not for Event Sourcing](https://medium.com/serialized-io/apache-kafka-is-not-for-event-sourcing-81735c3cf5c), if you have the scale to run automate the care and feeding of Kafka infrastructure, it can a great toof for the job of Replicating events and/or Rich Events in a scalable manner.

The [Apache Kafka intro docs](https://kafka.apache.org/intro) provide a clear terse overview of the design and attendant benefits this brings to bear.

As noted in the [Effect of ChangeFeed on Request Charges](https://github.com/jet/equinox/blob/master/DOCUMENTATION.md#effect-of-changefeed-on-request-charges) section, it can make sense to replicate a subset of the ChangeFeed to a Kafka topic (both for projections being consumed within a Bounded Context and for cases where you are generating a Pubished Notification Event) purely from the point of view of optimising request charges (and not needing to consider projections when considering how to scale up provisioning for load). Other benefits are mechanical sympathy based - Kafka can be the right tool for the job in scaling out traversal of events for a variety of use cases given one has sufficient traffic to warrant the complexity.

See the [PR that added the initial support for CosmosDb Projections](https://github.com/jet/equinox/pull/87) and [the QuickStart](README.md#quickstart) for instructions.

- https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
- https://www.confluent.io/wp-content/uploads/confluent-kafka-definitive-guide-complete.pdf

## CONTRIBUTING

Please raise GitHub issues for any questions so others can benefit from the discussion.

This is an Open Source project for many reasons, with some central goals:

- quality reference code (the code should be clean and easy to read; where it makes sense, it should remain possible to clone it locally and use it in tweaked form)
- optimal resilience and performance (getting performance right can add huge value for some systems, i.e., making it prettier but disrupting the performance would be bad)
- this code underpins non-trivial production systems (having good tests is not optional for reasons far deeper than having impressive coverage stats)

We'll do our best to be accomodating to PRs and issues, but please appreciate that [we emphasize decisiveness for the greater good of the project and its users](https://www.hanselman.com/blog/AlwaysBeClosingPullRequests.aspx); _new features [start with -100 points](https://blogs.msdn.microsoft.com/oldnewthing/20090928-00/?p=16573)_.

Within those constraints, contributions of all kinds are welcome:

- raising [Issues](https://github.com/jet/propulsion/issues) is always welcome (but we'll aim to be decisive in the interests of keeping the list navigable).
- bugfixes with good test coverage are always welcome; in general we'll seek to move them to NuGet prerelease and then NuGet release packages with relatively short timelines (there's unfortunately not presently a MyGet feed for CI packages rigged).
- improvements / tweaks, _subject to filing a GitHub issue outlining the work first to see if it fits a general enough need to warrant adding code to the implementation and to make sure work is not wasted or duplicated_

## TEMPLATES

The best place to start, sample-wise is with the [QuickStart](#quickstart), which walks you through sample code, tuned for approachability, from `dotnet new` templates stored [in a dedicated repo](https://github.com/jet/dotnet-templates).

## BUILDING

Please note the [QuickStart](#quickstart) is probably the best way to gain an overview, and the templates are the best way to see how to consume it; these instructions are intended mainly for people looking to make changes. 

NB The `Propulsion.Kafka.Integration` tests are reliant on a `TEST_KAFKA_BROKER` environment variable pointing to a Broker that has been configured to auto-create ephemeral Kafka Topics as required by the tests (each test run blindly writes to a guid-named topic and trusts the broker will accept the write without any initialization step)

### build, including tests on net461 and netcoreapp2.1

```powershell
dotnet build build.proj -v n
```

## FAQ

### What's the deal with the history of this repo?

This repo is derived from [`Jet.ConfluentKafka.FSharp`](https://github.com/jet/Jet.ConfluentKafka.FSharp); the history has been edited to focus only on edits to the `Propulsion` libraries.

### Your question here

- Please feel free to log question-issues; they'll get answered here