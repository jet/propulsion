# Propulsion [![Build Status](https://dev.azure.com/jet-opensource/opensource/_apis/build/status/jet.Propulsion)](https://dev.azure.com/jet-opensource/opensource/_build/latest?definitionId=16) [![release](https://img.shields.io/github/release-pre/jet/propulsion.svg)](https://github.com/jet/propulsion/releases) [![NuGet](https://img.shields.io/nuget/vpre/Propulsion.svg?logo=nuget)](https://www.nuget.org/packages/Propulsion/) [![license](https://img.shields.io/github/license/jet/propulsion.svg)](LICENSE) ![code size](https://img.shields.io/github/languages/code-size/jet/propulsion.svg) [<img src="https://img.shields.io/badge/slack-DDD--CQRS--ES%20%23equinox-yellow.svg?logo=slack">](https://t.co/MRxpx0rLH2)

This is pre-production code; unfortunately it's also pre-documentation atm (will be adding more over the coming weeks).

If you're looking for a good discussion forum on these kinds of topics, look no further than the [DDD-CQRS-ES Slack](https://github.com/ddd-cqrs-es/slack-community)'s [#equinox channel](https://ddd-cqrs-es.slack.com/messages/CF5J67H6Z) ([invite link](https://t.co/MRxpx0rLH2)).

## Components

The components within this repository are delivered as a multi-targeted Nuget package targeting `net461` (F# 3.1+) and `netstandard2.0` (F# 4.5+) profiles

- [![NuGet](https://img.shields.io/nuget/vpre/Propulsion.svg)](https://www.nuget.org/packages/Propulsion/) `Propulsion`. Implements core functionality in a channel-independent fashion including `ParallelProjector`, `StreamsProjector`. [Depends](https://www.fuget.org/packages/Propulsion) on `MathNet.Numerics`, `Serilog`
- [![NuGet](https://img.shields.io/nuget/vpre/Propulsion.Cosmos.svg)](https://www.nuget.org/packages/Propulsion.Cosmos/) `Propulsion.Cosmos`. Provides bindings to Azure CosmosDb a) writing to `Equinox.Cosmos` :- `CosmosSink` b) reading from CosmosDb's changefeed by wrapping the [`dotnet-changefeedprocessor` library](https://github.com/Azure/azure-documentdb-changefeedprocessor-dotnet) :- `CosmosSource`. [Depends](https://www.fuget.org/packages/Propulsion.Cosmos) on `Equinox.Cosmos`, `Microsoft.Azure.DocumentDB.ChangeFeedProcessor`, `Serilog`
- [![NuGet](https://img.shields.io/nuget/vpre/Propulsion.EventStore.svg)](https://www.nuget.org/packages/Propulsion.EventStore/) `Propulsion.EventStore`. Provides bindings to [EventStore](eventstore.org), writing via `Propulsion.EventStore.EventStoreSink` [Depends](https://www.fuget.org/packages/Propulsion.EventStore) on `Equinox.EventStore`, `Serilog`
- [![NuGet](https://img.shields.io/nuget/vpre/Propulsion.Kafka.svg)](https://www.nuget.org/packages/Propulsion.Kafka/) `Propulsion.Kafka`. Provides bindings for producing and consuming both streamwise and in parallel. Includes a standard codec for use with streamwise projection and consumption, `Propulsion.Kafka.Codec.RenderedSpan`. [Depends](https://www.fuget.org/packages/Propulsion.Kafka) on `Jet.ConfluentKafka.FSharp` v ` >= 1.0.1`, `Serilog`

The ubiquitous Serilog dependency is solely on the core module, not any sinks, i.e. you configure to emit to `NLog` etc.

## Related repos

- See [the Jet `dotnet new` templates repo](https://github.com/jet/dotnet-templates) for examples using the packages herein:

    - `eqxprojector` template for example `CosmosSource` logic consuming from a CosmosDb `ChangeFeedProcessor`.
    - `eqxprojector` template (in `-k` mode) for example producer logic using `StreamsProducer`, `StreamsProjector` and `ParallelProducer`.
    - `eqxprojector` template (in `-k` mode) for example consumer logic using `ParallelConsumer` and `StreamsConsumer`.
    - `eqxsync` template for examples of binding a `CosmosSource` or `EventStoreSource` to a `CosmosSink` or `EventStoreSink`.

- See [the `Jet.ConfluentKafka.FSharp` repo](https://github.com/jet/Jet.ConfluentKafka.FSharp) for `BatchedProducer` and `BatchedConsumer` implementations (together with the `KafkaConsumerConfig` and `KafkaProducerConfig` used in the Parallel and Streams wrappers in `Propulsion.Kafka`)

- See [the Equinox QuickStart](https://github.com/jet/equinox#quickstart) for examples of using this library to project to Kafka from `Equinox.Cosmos` and/or `Equinox.EventStore`.

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

NB The `Propulsion.Kafka.Integration` tests are reliant on a `TEST_KAFKA_BROKER` environment variable pointing to a Broker that has been configured to auto-create ephemeral Kafka Topics as required by the tests (each test run writes to a guid-named topic)

### build, including tests on net461 and netcoreapp2.1

```powershell
dotnet build build.proj -v n
```

## FAQ

### What's the deal with the history of this repo?

This repo is derived from [`Jet.ConfluentKafka.FSharp`](https://github.com/jet/Jet.ConfluentKafka.FSharp); the history has been edited to focus only on edits to the `Propulsion` libraries.

### Your question here

- Please feel free to log question-issues; they'll get answered here