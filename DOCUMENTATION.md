# Documentation

Please refer to the [FAQ](README.md#FAQ), [README.md](README.md) and the [Issues](https://github.com/jet/propulsion/issues) for background info on what's outstanding (aside from there being lots of room for more and better docs).

# Background reading

In general, the primary background information is covered in the [Background Reading section of Equinox's Documentation](https://github.com/jet/equinox/blob/master/DOCUMENTATION.md#background-reading).

_Enterprise Integration Patterns, Hohpe/Woolf, 2004_: Timeless book on messaging, routing integration and more.

- **Your link here** - Please add materials that helped you on your journey so far here via PRs!

# Glossary

There's a [glossary of terms in the Equinox Documentation](https://github.com/jet/equinox/blob/master/DOCUMENTATION.md#glossary).

# Overview

The following diagrams are based on the style defined in [@simonbrowndotje](https://github.com/simonbrowndotje)'s [C4 model](https://c4model.com/), rendered using [@skleanthous](https://github.com/skleanthous)'s [PlantUmlSkin](https://github.com/skleanthous/C4-PlantumlSkin/blob/master/README.md). It's highly recommended to view [the talk linked from `c4model.com`](https://www.youtube.com/watch?v=x2-rSnhpw0g&feature=emb_logo). See [README.md acknowledgments section](https://github.com/jet/equinox#acknowledgements)

# WIP: THE DIAGRAMS!

# Projectors

See [this medium post regarding some patterns used at Jet in this space](https://medium.com/@eulerfx/scaling-event-sourcing-at-jet-9c873cac33b8) for a broad overview of ways to structure large scale projection systems.

# `Propulsion.Cosmos` Projection facilities

 An integral part of the `Equinox.Cosmos` value proposition is the ability to project events based on the [Azure CosmosDB ChangeFeed mechanism](https://docs.microsoft.com/en-us/azure/cosmos-db/change-feed). Key elements involved in realizing this are:
- the [storage model needs to be designed in such a way that the aforementioned processor can do its job efficiently](https://github.com/jet/equinox/blob/master/DOCUMENTATION.md#cosmos-storage-model)
- there needs to be an active ChangeFeed Processor per Container that monitors events being written while tracking the position of the most recently propagated events

In CosmosDB, every document lives within a [logical partition, which is then hosted by a variable number of processor instances entitled _physical partitions_](https://docs.microsoft.com/en-gb/azure/cosmos-db/partition-data) (`Equinox.Cosmos` documents pertaining to an individual stream bear the same partition key in order to ensure correct ordering guarantees for the purposes of projection). Each front end node of which a CosmosDB Container is comprised has responsibility for a particular subset range of the partition key space.

In concrete terms, the ChangeFeed's is as a long running Processor per frontend node that repeatedly tails (think of it as a `SELECT * FROM <all docs for the node> WHERE lastUpdated > <checkpoint>`) across the set of documents being managed by a given partition host (subject to topology changes; processor instances can spin up and down, with the assigned ranges shuffling to balance the load per processor). e.g. if you allocate 30K RU/s to a container and/or store >20GB of data, it will have at least 3 processors, each handling 1/3 of the partition key space, and running a change feed from that is a matter of maintaining 3 continuous queries, with a continuation token each being held/leased/controlled by a given Change Feed Processor.

## Effect of ChangeFeed on Request Charges

It should be noted that the ChangeFeed is not special-cased by CosmosDB itself in any meaningful way; something somewhere is going to be making CosmosDB API queries, paying Request Charges for the privilege (even a tail request based on a continuation token yielding zero documents incurs a charge). It's thus important to consider that every Event written by `Equinox.Cosmos` into the CosmosDB Container will induce an approximately equivalent cost due to the fact that the freshly inserted document will be included in the next batch propagated by the Processor (each update of a document also 'moves' that document from it's present position in the change order past the the notional tail of the ChangeFeed). Thus each insert/update also induces an (unavoidable) request charge based on the fact that the document will be included aggregate set of touched documents being surfaced per batch transferred from the ChangeFeed (charging is per KiB or part thereof). **_The effect of this cost is multiplied by the number of ChangeFeedProcessors (consumer groups) one is running._**

## Change Feed Processors

As outlined above, the CosmosDB ChangeFeed's real world manifestation is as a continuous query per CosmosDB Container ("physical partition") _node_.

For .NET, this is wrapped in a set of APIs presented within the `Microsoft.Azure.DocumentDB[.Core]` packages.

A ChangeFeed _Processor_ consists of (per CosmosDB processor/range) the following elements:
- a _host_ process running somewhere that will run the query and then do something with the results before marking off progress
- a continuous query across the set of documents that fall within the partition key range hosted by a given physical partition host
- that progress then needs to be maintained durably in some form of checkpoint stores

The implementation in this repo uses [Microsoft's .NET `ChangeFeedProcessor` implementation: `Microsoft.Azure.DocumentDB.ChangeFeedProcessor`](https://github.com/Azure/azure-documentdb-changefeedprocessor-dotnet), which is a proven component used for diverse purposes including as the underlying substrate for various Azure Functions wiring.

(It should be noted that the `Microsost.Azure.Cosmos` packages (aka the V3 SDK) combine both the Change Feed querying and the ChangeFeedProcessor logic into a single package - both Equinox and Propulsion will ultimately move to use the V4 SDKs, but right now neither the V3 nor V4 are ready -- there are feature gaps (e.g. CheckpointAsync and diagnostics) and bugs (more RU consumption without a benefit) which are currently being addressed on the Microsoft side)

See the [PR that added the initial support for CosmosDb Projections](https://github.com/jet/equinox/pull/87) and [the QuickStart](https://github.com/jet/equinox/blob/master/README.md#quickstart) for instructions.

# `Propulsion.Kafka`

## Feeding to Kafka

While [Kafka is not for Event Sourcing](https://medium.com/serialized-io/apache-kafka-is-not-for-event-sourcing-81735c3cf5c), if you have the scale to run automate the care and feeding of Kafka infrastructure, it can a great tool for the job of Replicating events and/or Rich Events in a scalable manner.

The [Apache Kafka intro docs](https://kafka.apache.org/intro) provide a clear terse overview of the design and attendant benefits this brings to bear; it's strongly recommended to get any background info from that source.

As noted in the [Effect of ChangeFeed on Request Charges](https://github.com/jet/equinox/blob/master/DOCUMENTATION.md#effect-of-changefeed-on-request-charges) section, it can make sense to replicate a subset of the ChangeFeed to a Kafka topic (both for projections being consumed within a Bounded Context and for cases where you are generating a Pubished Notification Event) purely from the point of view of optimizing request charges (and not needing to consider projections when considering how to scale up provisioning for load). Other benefits are mechanical sympathy based - Kafka can be the right tool for the job in scaling out traversal of events for a variety of use cases given one has sufficient traffic to warrant the complexity.

See the [PR that added the initial support for CosmosDb Projections](https://github.com/jet/equinox/pull/87) and [the QuickStart](README.md#quickstart) for instructions.

- https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
- https://www.confluent.io/wp-content/uploads/confluent-kafka-definitive-guide-complete.pdf