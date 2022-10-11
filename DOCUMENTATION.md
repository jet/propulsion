# Documentation

Please refer to the [FAQ](README.md#FAQ), [README.md](README.md) and the [Issues](https://github.com/jet/propulsion/issues) for background info on what's outstanding (aside from there being lots of room for more and better docs).

# Background reading

In general, the primary background information is covered in the [Background Reading section of Equinox's Documentation](https://github.com/jet/equinox/blob/master/DOCUMENTATION.md#background-reading).

- _Enterprise Integration Patterns, Hohpe/Woolf, 2004_: Timeless book on messaging, routing integration and more.
- _Designing Data Intensive Applications, Kleppmann, 2015_: Timeless book on infrastructure and asynchrony in distributed systems, and much more.

- **Your link here** - Please add materials that helped you on your journey so far here via PRs!

# Glossary

There's a [glossary of terms in the Equinox Documentation](https://github.com/jet/equinox/blob/master/DOCUMENTATION.md#glossary).

# Overview

The following diagrams are based on the style defined in [@simonbrowndotje](https://github.com/simonbrowndotje)'s [C4 model](https://c4model.com/), rendered using [@skleanthous](https://github.com/skleanthous)'s [PlantUmlSkin](https://github.com/skleanthous/C4-PlantumlSkin/blob/master/README.md). It's highly recommended to view [the talk linked from `c4model.com`](https://www.youtube.com/watch?v=x2-rSnhpw0g&feature=emb_logo). See [README.md acknowledgments section](https://github.com/jet/equinox#acknowledgements)

## Context diagram

While Equinox focuses on the **Consistent Processing** element of building an event-sourced decision processing system, offering tailored components that interact with a specific **Consistent Event Store**, Propulsion elements support the building of complementary facilities as part of an overall Application:

- **Ingesters**: read stuff from outside the Bounded Context of the System. This kind of service covers aspects such as feeding reference data into **Read Models**, ingesting changes into a consistent model via **Consistent Processing**. _These services are not acting in reaction to events emanating from the **Consistent Event Store**, as opposed to..._
- **Publishers**: react to events as they are arrive from the **Consistent Event Store** by filtering, rendering and producing to feeds for downstreams. _While these services may in some cases rely on synchronous queries via **Consistent Processing**, it's never transacting or driving follow-on work; which brings us to..._
- **Reactors**: drive reactive actions triggered by either upstream feeds, or events observed in the **Consistent Event Store**. _These services handle anything beyond the duties of **Ingesters** or **Publishers**, and will often drive follow-on processing via Process Managers and/or transacting via **Consistent Processing**. In some cases, a reactor app's function may be to progressively compose a notification for a **Publisher** to eventually publish._

The overall territory is laid out here in this [C4](https://c4model.com) System Context Diagram:

![Propulsion c4model.com Context Diagram](http://www.plantuml.com/plantuml/proxy?cache=no&src=https://raw.github.com/jet/propulsion/docs/diagrams/context.puml&fmt=svg)

## Container diagram: Ingesters

![Propulsion c4model.com Container Diagram: Ingesters](http://www.plantuml.com/plantuml/proxy?cache=no&src=https://raw.github.com/jet/propulsion/docs/diagrams/IngestersContainer.puml&fmt=svg)

## Container diagram: Publishers

![Propulsion c4model.com Container Diagram: Publishers](http://www.plantuml.com/plantuml/proxy?cache=no&src=https://raw.github.com/jet/propulsion/docs/diagrams/PublishersContainer.puml&fmt=svg)

## Container diagram: Reactors

![Propulsion c4model.com Container Diagram: Reactors](http://www.plantuml.com/plantuml/proxy?cache=no&src=https://raw.github.com/jet/propulsion/docs/diagrams/ReactorsContainer.puml&fmt=svg)

# `Propulsion.CosmosStore` facilities

 An integral part of the `Equinox.CosmosStore` value proposition is the intrinsic ability that Azure CosmosDB affords to project changes via the [ChangeFeed mechanism](https://docs.microsoft.com/en-us/azure/cosmos-db/change-feed). Key elements involved in realizing this are:
- the [storage model needs to be designed in such a way that the aforementioned processor can do its job efficiently](https://github.com/jet/equinox/blob/master/DOCUMENTATION.md#cosmos-storage-model)
- there needs to be one or more active ChangeFeed Processor Host Applications per Container that monitor events being written (while tracking the position of the most recently propagated events). (This could also be an Azure Function driven by a Change Feed Trigger, but the full wiring for that has not been implemented to date.)

In CosmosDB, every Item (document) belongs to a logical partition, defined by the Item's partition key (similar to the concept of a stream in EventStoreDB). `Equinox.CosmosStore`'s schema uses the stream name as the partition key, and never updates anything except the Tip document, in order to guarantee that the Change Feed will always deliver items from any given stream in `Index` order. A Container is split into [physical partitions hosting a variable number of these logical partitions](https://docs.microsoft.com/en-gb/azure/cosmos-db/partition-data), with an individually accessible endpoint node per physical partition (the multiple nodes/connections are managed internally within the `Microsoft.Azure.Cosmos` SDK).

In concrete terms, the ChangeFeed's consists of a long running Processor per physical partition node that repeatedly tails (think of it as a `SELECT * FROM <all documents/items for the node> WHERE lastUpdated > <checkpoint>`) across the set of documents being managed by a given partition host. The topology is subject to continual change (the SDK internally tracks the topology metadata to become aware of new nodes); Processor instances can spin up and down, with the assigned ranges shuffling to balance the load per Processor. e.g. if you allocate 30K RU/s to a container and/or store >20GB of data, it will have at least 3 processors, each handling 1/3 of the partition key space, and running a Change Feed against that involves maintaining 3 continuous queries, with a continuation token per physical being held/leased/controlled by a given Change Feed Processor. The Change Feed implementation within the SDK stores the continuation token and lease information per physical partition within a nominated Leases Container.

## Effect of ChangeFeed on Request Charges

It should be noted that the ChangeFeed is not special-cased by CosmosDB itself in any meaningful way; an active loop somewhere is continually making CosmosDB API queries, paying Request Charges for the privilege (even a tail request based on a continuation token yielding zero documents incurs a charge). It's thus important to consider that every Event written by `Equinox.CosmosStore` into the CosmosDB Container will induce a read cost due to the fact that the freshly inserted document will be included in the next batch propagated by the Processor (each update of a document also 'moves' that document from it's present position in the change order past the the notional tail of the ChangeFeed). Thus each insert/update also induces an (unavoidable) read request charge based on the fact that the document will be included in the aggregate set of touched documents being surfaced by the ChangeFeed batch callback (charging is proportional to the size of the affected item per KiB or part thereof; reads are cheaper than writes). **_The effect of this cost is reads-triggered-by-writes is multiplied by the number of Processors (consumer groups) one is running._**

## Change Feed Processors

As outlined above, the CosmosDB ChangeFeed's real world manifestation is as a continuous query per CosmosDB Container _node_ ("physical partition").

For .NET, this is wrapped in a set of APIs within the `Microsoft.Azure.Cosmos` package.

A ChangeFeed _Processor_ consists of (per CosmosDB processor/range) the following elements:
- a _host_ process running somewhere that will run the query and then do something with the results before marking off progress (the instances need to coordinate to distribute the consumption processing load fairly, typically via a Leases Container)
- a continuous query across the set of items/documents that fall within the partition key range hosted by a given physical partition host
- that progress then needs to be maintained durably in some form of checkpoint store (aka a 'Leases Container'), which by convention are maintained in an ancillary container alongside the source one, e.g. a given `<monitoredContainer>` will typically have a `<monitoredContainer>-aux` Leases Container sitting alongside it.

The `Propulsion.CosmosStore` in this repo uses the evolution of the [original `ChangeFeedProcessor` implementation: `Microsoft.Azure.DocumentDB.ChangeFeedProcessor`](https://github.com/Azure/azure-documentdb-changefeedprocessor-dotnet), that is integrated into the [OSS `Microsoft.Azure.Cosmos` impl](https://github.com/Azure/azure-cosmos-dotnet-v3). NOTE the necessary explicit checkpointing support was not exposed in the `Microsoft.Azure.Cosmos` package until version `3.21.0`.

See the [PR that added the initial support for CosmosDb Projections](https://github.com/jet/equinox/pull/87) and [the Equinox QuickStart](https://github.com/jet/equinox/blob/master/README.md#quickstart) for instructions.

# `Propulsion.Kafka`

## Kafka and event sourcing

The term 'event sourcing' is frequently used in proximity to Kafka. It's important to note that [despite the regular stream of articles that appear to suggest to the contrary](https://github.com/oskardudycz/EventSourcing.NetCore#this-is-not-event-sourcing), the fundamental features of Kafka don't align with the requirements as well as some messaging would have you believe. 

## Using Kafka to compensate for read amplification effects of the CosmosDB ChangeFeed

As noted in the [Effect of ChangeFeed on Request Charges](https://github.com/jet/propulsion/blob/master/DOCUMENTATION.md#effect-of-changefeed-on-request-charges) section, in some systems the RU consumption load placed on a given Container can be significant. One way of reducing that effect is to have a single consumer replicate the events onto a Kafka Topic, and have Reactors and/or Publishers consume the events from that source instead.

Propulsion provides components that enable implementing such a strategy:
- A publisher that efficiently publishes events in a canonical format ('Kafka StreamSpans') (see the `proProjector` template), with stateful de-duplication of events (important given the fact that the bulk of appends involve an update to the Tip document, and the current form of the changefeed does not intrinsically expose the before and after states)
- A consumer component that consumes and decodes the 'Kafka StreamSpans' for use by a `StreamsProjector`

It's important to consider deferring the running projections "over a longer wire" until the last responsible moment given:
- making the availability and performance of your Reactions and Publishing contingent on the availability and performance of your Kafka cluster should not be considered lightly (introducing another component alongside the event store intrinsically reduces the achievable SLA of the system as a whole)
- the typical enterprise in-house deployment of Kafka will have a retention/purging policy that rules out having a topic perpetually host a full copy of all events in the store. This implies that retraversing the events in your event store becomes a special case that involves either:
  
    - reading them out of band from the store (which will typically involve code changes and/or introducing complexity to enable switching to the out-of-band reading)
    - re-publishing the historic events (potentially problematic if other subscribers can't efficiently ignore and/or idempotently handle the replayed events)

Resources:
- the [Apache Kafka intro docs](https://kafka.apache.org/intro) provide a good overview of the primary use cases it's intended for and the high level components.
- [low level documentation of the client settings](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md)
- [thorough free book](https://www.confluent.io/wp-content/uploads/confluent-kafka-definitive-guide-complete.pdf)
- [medium post covering some high level structures that Jet explored in this space](https://medium.com/@eulerfx/scaling-event-sourcing-at-jet-9c873cac33b8).


