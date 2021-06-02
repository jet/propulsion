# Propulsion [![Build Status](https://dev.azure.com/jet-opensource/opensource/_apis/build/status/jet.Propulsion)](https://dev.azure.com/jet-opensource/opensource/_build/latest?definitionId=16) [![release](https://img.shields.io/github/release/jet/propulsion.svg)](https://github.com/jet/propulsion/releases) [![NuGet](https://img.shields.io/nuget/vpre/Propulsion.svg?logo=nuget)](https://www.nuget.org/packages/Propulsion/) [![license](https://img.shields.io/github/license/jet/propulsion.svg)](LICENSE) ![code size](https://img.shields.io/github/languages/code-size/jet/propulsion.svg) [<img src="https://img.shields.io/badge/slack-DDD--CQRS--ES%20%23equinox-yellow.svg?logo=slack">](https://j.mp/ddd-es-cqrs) [![docs status](https://img.shields.io/badge/DOCUMENTATION-WIP-important.svg?style=popout)](DOCUMENTATION.md) 

While the bulk of this code is in production across various Walmart systems, the [documentation](DOCUMENTATION.md) is very much a work in progress (ideally there'd be a nice [summary of various projection patterns](https://github.com/jet/propulsion/issues/21), but also much [broader information discussing the tradeoffs implied in an event-centric system as a whole](https://github.com/ylorph/The-Inevitable-Event-Centric-Book/issues)

If you're looking for a good discussion forum on these kinds of topics, look no further than the [DDD-CQRS-ES Slack](https://github.com/ddd-cqrs-es/slack-community)'s [#equinox channel](https://ddd-cqrs-es.slack.com/messages/CF5J67H6Z) ([invite link](https://j.mp/ddd-es-cqrs)).

## Components

The components within this repository are delivered as a multi-targeted Nuget package targeting `net461` (F# 3.1+) and `netstandard2.0` (F# 4.5+) profiles

- `Propulsion` [![NuGet](https://img.shields.io/nuget/v/Propulsion.svg)](https://www.nuget.org/packages/Propulsion/) Implements core functionality in a channel-independent fashion including `ParallelProjector`, `StreamsProjector`. [Depends](https://www.fuget.org/packages/Propulsion) on `MathNet.Numerics`, `Serilog`
- `Propulsion.Cosmos` [![NuGet](https://img.shields.io/nuget/v/Propulsion.Cosmos.svg)](https://www.nuget.org/packages/Propulsion.Cosmos/) Provides bindings to Azure CosmosDB. [Depends](https://www.fuget.org/packages/Propulsion.Cosmos) on `Equinox.Cosmos`, `Microsoft.Azure.DocumentDB.ChangeFeedProcessor`, `Serilog`
  
  1. `CosmosSource`: reading from CosmosDb's ChangeFeed by wrapping the [`dotnet-changefeedprocessor` library](https://github.com/Azure/azure-documentdb-changefeedprocessor-dotnet).
  2. `CosmosSink`: writing to `Equinox.Cosmos`.
  3. `CosmosPruner`: pruning `Equinox.Cosmos`.
  4. `ReaderCheckpoint`: checkpoint storage for `Propulsion.Feed`
  
- `Propulsion.CosmosStore` [![NuGet](https://img.shields.io/nuget/v/Propulsion.CosmosStore.svg)](https://www.nuget.org/packages/Propulsion.CosmosStore/) Provides bindings to Azure CosmosDB. [Depends](https://www.fuget.org/packages/Propulsion.CosmosStore) on `Equinox.CosmosStore`, `Microsoft.Azure.DocumentDB.ChangeFeedProcessor`, `Microsoft.Azure.DocumentDB.Core`
  
  1. `CosmosStoreSource`: reading from CosmosDb's ChangeFeed by wrapping the [`dotnet-changefeedprocessor` library](https://github.com/Azure/azure-documentdb-changefeedprocessor-dotnet). **NOTE not yet implemented using the V3 SDK as yet as [the Azure Cosmos SDK Team have yet to re-expose CheckpointAsync and other such required APIs](https://github.com/jet/propulsion/issues/15)**
  2. `CosmosStoreSink`: writing to `Equinox.CosmosStore`.
  3. `CosmosStorePruner`: pruning from `Equinox.CosmosStore`.
  4. `ReaderCheckpoint`: checkpoint storage for `Propulsion.Feed`
  
- `Propulsion.EventStore` [![NuGet](https://img.shields.io/nuget/v/Propulsion.EventStore.svg)](https://www.nuget.org/packages/Propulsion.EventStore/). Provides bindings to [EventStore](https://www.eventstore.org), writing via `Propulsion.EventStore.EventStoreSink` [Depends](https://www.fuget.org/packages/Propulsion.EventStore) on `Equinox.EventStore`, `Serilog`
- `Propulsion.Feed` [![NuGet](https://img.shields.io/nuget/v/Propulsion.Feed.svg)](https://www.nuget.org/packages/Propulsion.Feed/) Provides helpers for streamwise consumption of a feed of information with an arbitrary interface (e.g. a third-party Feed API), including the maintenance of checkpoints within such a feed. [Depends](https://www.fuget.org/packages/Propulsion.Feed) on `Propulsion`, a `IFeedCheckpointStore` implementation (from e.g., `Propulsion.Cosmos` or `Propulsion.CosmosStore`)
- `Propulsion.Kafka` [![NuGet](https://img.shields.io/nuget/v/Propulsion.Kafka.svg)](https://www.nuget.org/packages/Propulsion.Kafka/) Provides bindings for producing and consuming both streamwise and in parallel. Includes a standard codec for use with streamwise projection and consumption, `Propulsion.Kafka.Codec.NewtonsoftJson.RenderedSpan`. [Depends](https://www.fuget.org/packages/Propulsion.Kafka) on `FsKafka` v `1.5.0`, `Serilog`
- `Propulsion.Kafka0` [![NuGet](https://img.shields.io/nuget/v/Propulsion.Kafka0.svg)](https://www.nuget.org/packages/Propulsion.Kafka0/). Same functionality/purpose as `Propulsion.Kafka` but uses `FsKafka0` instead of `FsKafka` in order to target an older `Confluent.Kafka`/`librdkafka` version pairing for interoperability with systems that have a hard dependency on that. [Depends](https://www.fuget.org/packages/Propulsion.Kafka0) on `FsKafka0` v `1.5.0` (which depends on `Confluent.Kafka [0.11.3]`, `librdkafka.redist [0.11.4]`), `Serilog`
- `Propulsion.SqlStreamStore` [![NuGet](https://img.shields.io/nuget/v/Propulsion.SqlStreamStore.svg)](https://www.nuget.org/packages/Propulsion.SqlStreamStore/). Provides bindings to [SqlStreamStore](https://github.com/SQLStreamStore/SQLStreamStore), maintaining checkpoints in a SQL table using Dapper [Depends](https://www.fuget.org/packages/Propulsion.SqlStreamStore) on `SqlStreamStore`, `Dapper` v `2.0`, `Microsoft.Data.SqlClient` v `1.1.3`, `Serilog`

The ubiquitous `Serilog` dependency is solely on the core module, not any sinks, i.e. you configure to emit to `NLog` etc.

### `dotnet tool` provisioning / projections test tool

- `Propulsion.Tool` [![Tool NuGet](https://img.shields.io/nuget/v/Propulsion.Tool.svg)](https://www.nuget.org/packages/Propulsion.Tool/): Tool used to initialize a Change Feed Processor `aux` container for `Propulsion.Cosmos` and demonstrate basic projection, including to Kafka. (Install via: `dotnet tool install Propulsion.Tool -g`)

## Related repos

- See [the Equinox QuickStart](https://github.com/jet/equinox#quickstart) for examples of using this library to project to Kafka from `Equinox.Cosmos` and/or `Equinox.EventStore`.

- See [the `dotnet new` templates repo](https://github.com/jet/dotnet-templates) for examples using the packages herein:

    - [Propulsion-specific templates](https://github.com/jet/dotnet-templates#propulsion-related):
      - `proProjector` template for `CosmosSource`+`StreamsProjector` logic consuming from a CosmosDb `ChangeFeedProcessor`.
      - `proProjector` template (in `--kafka` mode) for producer logic using `StreamsProducerSink` or `ParallelProducerSink`.
      - `proConsumer` template for example consumer logic using `ParallelConsumer` and `StreamsConsumer` etc.

    - [Propulsion+Equinox templates](https://github.com/jet/dotnet-templates#producerreactor-templates-combining-usage-of-equinox-and-propulsion):
      - `proReactor` template, which includes multiple sources and multiple processing modes
      - `summaryConsumer` template, consumes from the output of a `proReactor --kafka`, saving them in an `Equinox.Cosmos` store
      - `trackingConsumer`template, which consumes from Kafka, feeding into example Ingester logic
      - `proSync` template is a fully fledged store <-> store synchronization tool syncing from a `CosmosSource` or `EventStoreSource` to a `CosmosSink` or `EventStoreSink`

- See [the `FsKafka` repo](https://github.com/jet/FsKafka) for `BatchedProducer` and `BatchedConsumer` implementations (together with the `KafkaConsumerConfig` and `KafkaProducerConfig` used in the Parallel and Streams wrappers in `Propulsion.Kafka`)

# Overview

## The Equinox Perspective

Propulsion and Equinox have a [Yin and yang](https://en.wikipedia.org/wiki/Yin_and_yang) relationship; the use cases for both naturally interlock and overlap.

It can be relevant to peruse [the Equinox Documentation's Overview Diagrams](https://github.com/jet/equinox/blob/master/DOCUMENTATION.md#overview) for the perspective from the other side (TL;DR its largely the same topology, with elements that are de-emphasized here central over there, and vice versa)

## [C4](https://c4model.com) Context diagram

While Equinox focuses on the **Consistent Processing** element of building an event-sourced decision processing system, offering tailored components that interact with a specific **Consistent Event Store**, Propulsion elements support the building of complementary facilities as part of an overall Application:

- **Ingesters**: read stuff from outside the Bounded Context of the System. This kind of service covers aspects such as feeding reference data into **Read Models**, ingesting changes into a consistent model via **Consistent Processing**. _These services are not acting in reaction to events emanating from the **Consistent Event Store**, as opposed to..._
- **Publishers**: react to events as they are arrive from the **Consistent Event Store** by filtering, rendering and producing to feeds for downstreams. _While these services may in some cases rely on synchronous queries via **Consistent Processing**, it's never transacting or driving follow-on work; which brings us to..._
- **Reactors**: drive reactive actions triggered by either upstream feeds, or events observed in the **Consistent Event Store**. _These services handle anything beyond the duties of **Ingesters** or **Publishers**, and will often drive follow-on processing via Process Managers and/or transacting via **Consistent Processing**. In some cases, a reactor app's function may be to progressively compose a notification for a **Publisher** to eventually publish._

The overall territory is laid out here in this [C4](https://c4model.com) System Context Diagram:

![Propulsion c4model.com Context Diagram](http://www.plantuml.com/plantuml/proxy?cache=no&src=https://raw.github.com/jet/propulsion/master/diagrams/context.puml&fmt=svg)

<!-- ## [C4](https://c4model.com) Container diagram

The relevant pieces of the above break down as follows, when we emphasize the [Containers](https://c4model.com) aspects relevant to Propulsion:

![Propulsion c4model.com Container Diagram](http://www.plantuml.com/plantuml/proxy?cache=no&src=https://raw.github.com/jet/propulsion/master/diagrams/container.puml&fmt=svg)

 -->

**[See Overview section in `DOCUMENTATION`.md for further drill down](https://github.com/jet/propulsion/blob/master/DOCUMENTATION.md#overview)**

## QuickStart

### 1. Use `propulsion` tool to run a CosmosDb ChangeFeedProcessor

```powershell
dotnet tool uninstall Propulsion.Tool -g
dotnet tool install Propulsion.Tool -g

propulsion init -ru 400 cosmos # generates a -aux container for the ChangeFeedProcessor to maintain consumer group progress within
# -V for verbose ChangeFeedProcessor logging
# `-g projector1` represents the consumer group - >=1 are allowed, allowing multiple independent projections to run concurrently
# stats specifies one only wants stats regarding items (other options include `kafka` to project to Kafka)
# cosmos specifies source overrides (using defaults in step 1 in this instance)
propulsion -V project -g projector1 stats cosmos
```

### 2. Use `propulsion` tool to Run a CosmosDb ChangeFeedProcessor, emitting to a Kafka topic

```powershell
$env:PROPULSION_KAFKA_BROKER="instance.kafka.mysite.com:9092" # or use -b

# `-V` for verbose logging
# `-g projector3` represents the consumer group; >=1 are allowed, allowing multiple independent projections to run concurrently
# `-l 5` to report ChangeFeed lags every 5 minutes
# `kafka` specifies one wants to emit to Kafka
# `temp-topic` is the topic to emit to
# `cosmos` specifies source overrides (using defaults in step 1 in this instance)
propulsion -V project -g projector3 -l 5 kafka temp-topic cosmos
```

## CONTRIBUTING

See [CONTRIBUTING.md](CONTRIBUTING.md)

## TEMPLATES

The best place to start, sample-wise is with the [QuickStart](#quickstart), which walks you through sample code, tuned for approachability, from `dotnet new` templates stored [in a dedicated repo](https://github.com/jet/dotnet-templates).

## BUILDING

Please note the [QuickStart](#quickstart) is probably the best way to gain an overview, and the templates are the best way to see how to consume it; these instructions are intended mainly for people looking to make changes. 

NB The `Propulsion.Kafka.Integration` tests are reliant on a `TEST_KAFKA_BROKER` environment variable pointing to a Broker that has been configured to auto-create ephemeral Kafka Topics as required by the tests (each test run blindly writes to a guid-named topic and trusts the broker will accept the write without any initialization step)

### build, including tests on net461 and netcoreapp3.1

```powershell
dotnet build build.proj -v n
```

## FAQ

<a name="why-not-cfp-all-the-things"></a>
### why do you employ Kafka as an additional layer, when downstream processes could simply subscribe directly and individually to the relevant Cosmos db change feed(s)? Is it to accommodate other messages besides those emitted from events and snapshot updates? :pray: [@Roland Andrag](https://github.com/randrag)

Well, Kafka is definitely not a critical component or a panacea.

You're correct that the bulk of things that can be achieved using Kafka can be accomplished via usage of the ChangeFeed. One thing to point out is that in the context of enterprise systems, having a well maintained Kafka cluster does have less incremental cost that it might do if you're building a smaller system from nothing.

Some of the negatives of consuming from the CF direct:

- each CFP reader imposes RU charges (its a set of continuous queries against each and every physical range of which the cosmos store is composed)
- you can't apply a server-side filter, so you pay for everything you see
- you're very prone to falling into coupling issues
- (as you alluded to), if there's some logic or work involved in the production of events you'd emit to Kafka, each consumer would need to duplicate that

While many of these concerns can be alleviated to varying degrees by splitting the storage up into multiple containers in order that each consumer will intrinsically be interested in a large proportion of the data it will observe (potentially using database level RU allocations), the write amplification effects of having multiple consumers will always be more significant when reading directly than when using Kafka,  the design of which is well suited to running lots of concurrent readers.

Splitting event categories into containers solely to optimize these effects can also make the management of the transactional workload more complex; the ideal for any given container is to balance the concerns of:

- ensuring that datasets for which you want to ringfence availability / RU allocations don't share with containers/databases for which running hot (potentially significant levels of rate limiting but overall high throughput in aggregate as a result of using a high percentage of the allocated capacity)
- avoiding prematurely splitting data prior to it being required by the constraints of CosmosDB (i.e. you want to let splitting primarily be driven by reaching the [10GB] physical partition range)
- not having logical partition hotspots that lead to a small number of physical partitions having significantly above average RU consumption
- having relatively consistent document sizes
- economies of scale - if each container (or database if you provision at that level) needs to individually managed (with a degree of headroom to ensure availability for load spikes etc), you'll tend to require higher aggregate RU assignment for a given overall workload based on a topology that has more containers

### any tips for testing Propulsion (projection) in an integration/end-to-end fashion? :pray: [@James Booth](https://github.com/absolutejam)

> I know for unit testing, I can just test the obvious parts. Or if end to end testing is even required

Depends what you want to achieve. One important technique for doing end-to-end scenarios, especially where some reaction is supposed to feed back into Equinox is to use `Equinox.MemoryStore` as the store, and then wire Propulsion to consume from that using [`MemoryStoreProjector`](https://github.com/jet/dotnet-templates/blob/237e1586902b691b8e79f6c14c73faf2b12d89cb/equinox-shipping/Watchdog.Integration/PropulsionInfrastructure.fs#L16). 
That technique has been internally validated and that code from dotnet-templates is on the road to becoming `Propulsion.MemoryStore` a la https://github.com/jet/propulsion/issues/64

Other techniques I've seen/heard are:
- rig things to use ephemeral ESDB or cosmos databases (the emulator is only windows but works, you can use serverless or database level RU allocated DBs) to run your system against an ephemeral store. For such cases, you would tend to spin up all your projector apps (maybe in docker-compose etc) and then check for externally visible effects.

In general I'd be looking to use `MemoryStoreProjector` as a default technique, but there are no particularly deep examples to work off beyond the one adjacent to the impl (which is not a typical straightforward projection scenario)

To answer more completely, I'd say given a scenario involving Propulsion and Equinox, you'll typically have the following ingredients:

1. writing to the store - you can either assume that's well-tested infra or say you want to know you wired it up properly
2. serialization/deserialization - you can either have unit tests and/or property tests to validate roundtripping, or say its critical to know it really works with real data
3. reading from feed, propagating to handler - that's harder to config and has the biggest variablity in a test scenario so either
  
  a) you want to take it out of the equation
  b) you want to know its wired properly
  
4. does handler work complete - yes you and should can unit test that, but maybe you want to know it really does work in a broader context with more real stuff

5. does it trigger follow-on work, i.e. a cascade of reactions. you can either do triangulation and say its proven if I observe the trigger for the next bit, or you can want to prove it end to end

6. do overall thing really work - sometimes you want to be able to validate workflows rather than having to pay the tax of going in the front door for all the things

### Any reason you didn’t use one of the different subscription models available in ESDB? :pray: [@James Booth](https://github.com/absolutejam)

#### TL;DR Differing goals

While the implementation and patterns in Propulsion happen to overlap to a degree with the use cases of the ESDB's subscription mechanisms, the primary reason they are not used directly stem from the needs and constraints that Propulsion was evolved to cover. 

One thing that should be clear is that Propulsion is definitely *not* solving for the need of being the simplest conceivable projection library with a low concept count that's easy to get started with. If you're looking to build such a library, you'll likely give yourself some important guiding non-goals, e.g., if we need to add 3 more concepts to get a 20% improvement in throughput, then we'd prefer to retain the simplicity.

For Propulsion, almost literally, job one was to be able to shift 1TB of ordered events in streams to/from ESDB/Cosmos/Kafka in well under 24h - a general naive thing reading and writing in small batches takes more like 24d to do the same thing. A secondary goal is to keep them in sync continually after that point (it's definitely more than a one time bulk ingestion system).

While Propulsion scales down to running simple subscriptions (and I've built systems using it for exactly that), its got quite a few additional concepts compared to using something built literally for that job because that use case was almost literally an afterthought.

That's not to say that all those concepts overall make for a more complex system when all is said and done; there are lots of scenarios where you avoid having to do concurrent/async tricks one might otherwise do more explicitly in a more basic subscription system.

_When looking at the vast majority of typical projections/reactions/denormalizers one runs in an event-sourced system it should come as no surprise that EventStoreDB's subscription features offer lots of excellent ways of achieving those common goals with a good balance of:
- time to implement
- ease of operation
- good enough performance_
That's literally the company's goal: enabling rapidly building systems to solve business problems. 

The potential upsides that Propulsion can offer when used as a Projection system can definitely be valuable _when actually needed_, but on average, they'll frequently simply be massive overkill.

OK, with that context set, some key things that are arguably upsides of using Propulsion for Projectors rather than building a minimal thing without it:
- similar APIs regardless of whether events arrive via CosmosDB, EventStoreDB or Kafka
- consistent dashboards across all those sources
- generally excellent performance for high throughput scenarios (it was built for that)
- good handling for processing of workloads that don't have uniform (and low) cost per handler invocation, i.e., rate-limited writes of events to `Equinox.Cosmos` versus feeding stuff to Redis
- orthogonality to Equinox features but still offering a degree of commonality of concepts and terminology
- provide a degree of isolation from the low level drivers, e.g.:
  - moving from Cosmos CFP V2 to the Azure.Cosmos V4 SDK will be a matter of changing package references and fixing some minimal compilation errors, as opposed to learning a whole new API set
  - moving from EventStore's TCP API / EventStore.Client as per V5 to the gRPC based >= v20 clients also becomes a package switch (massive TODO though: actually port it!)
  - migrating a workload from EventStoreDB to CosmosDB or vice versa can be accomplished more cleanly if you're only changing the wiring of your projector host while making no changes to the handler implementations 
  - SqlStreamStore fits logically in as well; using it gives a cleaner mix and match / onramp to/from ESDB (Note however that migrating SSS <-> ESDB is a relatively trivial operation vs migrating from raw EventStore usage to Equinox.Cosmos, i.e. "we're using Propulsion to isolate us from deciding between SSS or ESDB" is not a good enough reason on its own)
- Specifically when consuming from Cosmos, being able to do that over a longer wire by feeding to Kafka to limit RU consumption from projections is a minor change. (Having to do reorganize like that for performance reasons is much more rarely a concern for EventStoreDB)

#### A Brief History of Propulsion's feature set

The order in which the need for various components arose (as a side effect of building out [Equinox](https://github.com/jet/equinox); solving specific needs in terms of feeding events into and out of EventStoreDB, CosmosDB and Kafka) was also an influence on the abstractions within and general facilities of Propulsion.

  - `Propulsion.Cosmos`'s `Source` was the first bit done; it's a light wrapper over the [CFP V2 client](https://github.com/Azure/azure-documentdb-changefeedprocessor-dotnet). Key implications from that are:
    - order of events in a logical partition can be maintained
    - global ordering of events across all logical streams is not achievable due to how CosmosDB works (only ordering guarantees are only logical partition level, the data is sharded into notes which can split as data grows)
  - `Propulsion.Kafka`'s `Sink` was next; the central goal here is be able to replicate events being read from CosmosDB onto a Kafka Topic _maintaining the ordering guarantees_. Implications:
    - There are two high level ways of achieving ordering guarantees in Kafka:
       1. only ever have a single event in flight; only when you've got the ack for a write do you send the next one. *However, literally doing that compromises throughput massively*.
       2. use Kafka's transaction facilities (not implemented in `Confluent.Kafka` at the time)
    => The approach used is to continuously emit messages concurrently in order to maintain throughput, but guarantee to never emit messages for the same _key_ at the same time.
  - `Propulsion.Cosmos`'s `Sink` was next up. It writes to CosmosDB using `Equinox.Cosmos`. Key implications:  
    - because rate-limiting is at the physical partition level, it's crucial for throughput that you keep other partitions busy while wait/retry loops are triggered on hotspots (and you absolutely don't want to exacerbate this effect by competing with yourself)
    - you want to ideally batch the writing of multiple events/documents to minimize round-trips (and write RU costs are effectively _O(log N)_ despite high level guidance characterizing it as _O(N)_))
    - you can only touch one logical partition for any given write
    - when you hit a hotspot and need to retry, ideally you'd pack events queued up behind you into the retry too
    - there is no one-size fits all batch size ([yet](https://github.com/Azure/azure-cosmos-dotnet-v3/issues/1763)) that balances
      a) not overloading the source
      b) maintaining throughput
      => You'll often need a small batch size, which implies larger per-event checkpointing overhead unless you make the checkpointing asynchronous
    => The implementation thus:
      - manages reading async from writing in order to maintain throughput (you define a batch size _and_ a number of batches to read ahead)
      - schedules write attempts at stream level (the reader concurrently ingests successor events, making all buffered events available when retrying)
      - writes checkpoints asynchronously when all the items involved complete within the (stream-level) processing
  - At the point where `Propulsion.EventStore`'s `Source` and `Sink` were being implemented (within weeks of the `Cosmos` equivalents; largely overlapping), the implications from realizing goals of providing good throughput while avoiding adding new concepts if it can be avoided are:
    - The cheapest (lowest impact in terms of triggering scattered reads across disks on an ESDB server, with associated latency implications) and most general API set for reading events is to read the `$all` stream
    - Maintaining checkpoints in an EventStoreDB that you're also monitoring is prone to feedback events (so using the Async checkpointing strategy used for `.Cosmos` but saving them in an external store such as an `Equinox.Cosmos` one makes sense)
    - If handlers and/or sinks don't have uniform processing time per message and/or are subject to rate limiting, most of the constraints of the `CosmosSink` apply too; you don't want to sit around retrying the last request out of a batch of 100 while tens of thousands of provisioned RUs are sitting idle in Cosmos and throughput is stalled

#### Conclusion/comparison checklist

The things Propulsion in general accomplishes in the projections space:
- make reading, checkpointing, parsing and running independent async things (all big perf boosts with Cosmos, less relevant for ESDB)
- allow handlers to handle all accumulated items for a stream as a batch if desired
- concurrency across streams
- (for Cosmos, but could be achieved for ESDB), provide for running multiple instances of consumers leasing physical partitions roughly how Kafka does it (aka the ChangeFeedProcessor lease management - Propulsion just wraps that and does not seek to impose any significant semantics on top of that)
- provide good instrumentation as to latency, errors, throughput in a pluggable way [akin to how Equinox does stuff (e.g. it has a Prometheus plugin)]
- good stories for isolating from specific drivers - i.e., there will be a Propulsion.CosmosDb which allows you to run identical consumer code using the V3 SDK instead of the V2 one (akin to how it will at some point provide a .EventStoreDb using gRPC to go with the V5 SDK based .EventStore :wink:)
- handlers/reactors/the projections can be ported to .Cosmos by swapping driver modules; similar to how Equinox.Cosmos vs Equinox.EventStore provides a common programming model despite the underpinnings being fundamentally quite different in nature
- Kafka reading and writing generally fits within the same patterns - i.e. if you want to push CosmosDb CFP output to Kafka and consume over that as a 'longer wire' thing without placing extra load on the source if you e.g. have 50 consumers, that's just an extra 250 line dotnet new proProjector app, and a tweak to ~30 lines of consumer app wireup to connect to Kafka instead of Cosmos
- Uniform dashboards for throughput and alerting for stuck projectors over Cosmos, EventStore, Kafka

Things ESDB's# subscriptions can do that are not covered in Propulsion (highlights, by no means a complete list):
- `$et-`, `$ec-` streams
- honoring the global `$all` order
- stacks more things - ESDB is a well designed purpose based solution used by thousands of systems with a massive mix of throughput and complexity constraints

### What's the deal with the history of this repo?

This repo is derived from [`FsKafka`](https://github.com/jet/FsKafka); the history has been edited to focus only on edits to the `Propulsion` libraries.

### Your question here

- Please feel free to log question-issues; they'll get answered here

# FURTHER READING

See [`DOCUMENTATION.md`](DOCUMENTATION.md) and [Equinox's `DOCUMENTATION.md`](https://github.com/jet/equinox/blob/master/DOCUMENTATION.md)