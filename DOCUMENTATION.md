# Documentation

Please refer to the [FAQ](README.md#FAQ), [README.md](README.md) and the [Issues](https://github.com/jet/propulsion/issues) for background info on what's outstanding (aside from there being lots of room for more and better docs).

# Background reading

In general, the primary background information is covered in the [Background Reading section of Equinox's Documentation](https://github.com/jet/equinox/blob/master/DOCUMENTATION.md#background-reading).

- _Enterprise Integration Patterns, Hohpe/Woolf, 2004_: Timeless patterns compendium on messaging, routing, integration and more. It's naturally missing some aspects regarding serverlessness, streaming and the like.
- _Designing Data Intensive Applications, Kleppmann, 2015_: Timeless book on infrastructure and asynchrony in distributed systems, and much more. Arguably should be on the Equinox Background reading list also.
- [Projections Explained- DDD Europe 2020](https://www.youtube.com/watch?v=b2kSlDcAcps) 1h intermediate level backgrounder talk on projection systems by [@yreynhout](https://github.com/yreynhout)
- [Events, Workflows, Sagas? Keep Your Event-driven Architecture Sane - Lutz Huehnken](https://www.youtube.com/watch?v=Uv1GOrZWpBM): Good talk orienting one re high level patterns of managing workflows in an event driven context  

- **Your link here** - Please add notable materials that helped you on your journey here via PRs!

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

![Propulsion c4model.com Context Diagram](http://www.plantuml.com/plantuml/proxy?cache=no&src=https://raw.github.com/jet/propulsion/master/diagrams/context.puml&fmt=svg)

## Container diagram: Ingesters

![Propulsion c4model.com Container Diagram: Ingesters](http://www.plantuml.com/plantuml/proxy?cache=no&src=https://raw.github.com/jet/propulsion/master/diagrams/IngestersContainer.puml&fmt=svg)

## Container diagram: Publishers

![Propulsion c4model.com Container Diagram: Publishers](http://www.plantuml.com/plantuml/proxy?cache=no&src=https://raw.github.com/jet/propulsion/master/diagrams/PublishersContainer.puml&fmt=svg)

## Container diagram: Reactors

![Propulsion c4model.com Container Diagram: Reactors](http://www.plantuml.com/plantuml/proxy?cache=no&src=https://raw.github.com/jet/propulsion/master/diagrams/ReactorsContainer.puml&fmt=svg)

# The `Propulsion.Streams` Programming Model for Projections, Reactions and Workflows

`Propulsion.Streams` provides a programming model to manage running of _Handlers_ in a manner that optimises for the following:
- isolating the handlers from the client libraries of a given Event Store (the Handler is triggered via a 'Sink' that manages accepts incoming events and checkpointing of progress in a Store-specific manner relevant to the hosting environment in which your handler will be run).
- providing a clean approach to the testing of Reaction logic with and without involving your actual Event Store (the `MemoryStoreProjector` component is a key part of that story).
- getting observability on messages handling and processing. Propulsion exposes diagnostic and telemetry information that enables effective monitoring. That's essential for tuning your Handlers for the production-ready system.

## Overview of running projections with Propulsion

### Glossary

#### Tranches

A Propulsion source can optionally split it's reading into multiple independent _Tranches_ (a tranche is a portion/division of a pool, it literally means slice in french). Each tranche is read (including reading up to a specified number of batches) and checkpointed independently. The internal Submitter component takes a batch from each Tranche in turn to ensure fair distribution of work across all Tranches that have work to be processed.

### Supported Inputs

Propulsion provides for processing events from the following sources:

1. `Propulsion.CosmosStore`: Lightly wraps the Change Feed Processor library within the `Microsoft.Azure.Cosmos` client, which provides a load balanced leasing system (like Kafka), enabling multiple instances of a processor with the same Processor Name (Consumer Group Name) to compete (there can be as many active processors as there are physical partitions; the client API indicates the Partition Identity via the Context's `LeaseToken`).
   - Each physical Partition maps to a Tranche, so processing still honors stream concurrency limits, even if the CFP library assigns more than one partition to a given instance.
   - The CFP library provides for estimation of the Lag of documents to be processed between the current checkpoint position and the tail of each partition, which can surfaced as a metric.
   - Over time, as a Container splits into more physical partitions (either due to assigning > 10K RU per physical partition, or by exceeding the 50GB/partition limit), more Partitions will arise.
   - Checkpoints are maintained by the CFP library within an auxiliary (by convention, a sister container with the name`{container}-aux`) CosmosDB container (the document containing the checkpoint doubles as a lease management target)
2. `Propulsion.DynamoStore`: Reads from an Index Table maintained by a `Propulsion.DynamoStore.Indexer` Lambda
    - In the current implementation, the Indexer feeds all items into a single Tranche within its index (extending it to support multiple Tranches based on a hash of the stream identifier would not be difficult).
    - If an index was to present multiple tranches, the Reader is ready to handle them as is (although there's no support within the source for load balancing (splitting reads by Stream Name hash and then assigning shards across all active instances) across multiple processor instances).
3. `Propulsion.EventStoreDb`: Uses the EventStoreDb gRPC based API to pull events from the `$all` stream.
   - In the current implementation, there's no support within the source for load balancing (splitting reads by Stream Name hash and then assigning shards across all active instances) across multiple processor instances
   - In the current implementation, there's no support for surfacing lag metrics on a continual basis (the reader does report it initially)
   - There are facilities for storing checkpoints in CosmosStore, DynamoStore, Postgres, SQL Server. There is not presently a [checkpoint store implementation that maintains the checkpoints EventStoreDb itself at present](https://github.com/jet/propulsion/issues/8).
4. `Propulsion.Feed`: Provides for reading from an arbitrary upstream system. Such a system might present an ATOM-like feed, but equally can be a periodic ingestion of a dataset from a pool of data that is not incrementally readable (such as a Table in a data warehouse). 
    - A Feed can be represented as multiple Tranches, with processing balancing across them all (e.g. each tenant of an upstream system can be independently read and checkpointed, with new tranches added over time).
    - In the current implementation, there's no support for surfacing lag metrics.
    - There are facilities for storing checkpoints in CosmosStore, DynamoStore, Postgres, SQL Server.

### _Source pipeline_

'Source' refers to a set of components that comprise the store-specific 'consumer' side of the processing. It encompasses:
   a. `Source`: the top level component
      - maintains a reference to the `Sink`, into which the `Ingester`s will feed batches of events
      - controls the set of Tranches from which events will be consumed (for a store like EventStore, there's only a single Tranche representing the `'$all'` stream; for CosmosStore, there's a Tranche per physical partition in the Container etc)
      - spins up a `Reader` and an `Ingester` per Tranche
      - wrapped as a `Pipeline` that can be used to `Stop` the processing and/or `AwaitCompletion`
   b. `Reader`: responsible for obtaining the input events and passing them to the _ingester_ (one per Tranche)
   c. `Monitor`: exposes the current read and commit positions achieved for each Tranche, independent of whether the commit of that progress has been completed (one per Source pipeline)

### _Sink pipeline_

'Sink' refers to the end of the processing pipeline that's not specific to a Store. It's primary role is to drive _handler_ invocations based incoming batches of events from the 'Source'. It encompasses: 

   a. `Ingester`: responsible for limiting the maximum read-ahead per Tranche (one per Tranche)
   b. `ProgressWriter`: used by the `Ingester` to hold, and periodically commit updates to the checkpoint for any progress that has been made (one per Tranche)
   c. `Scheduler`: takes batches from the Ingester, buffering them. Continually feeds items to the `Dispatcher`, reporting latencies and outcomes (to `Stats`) and batch completion (to `Ingester`)
   d. `Dispatcher`: handles keeping up to the desired amount of concurrent handlers in flight at any time
   e. `Stats`: processes Handler invocation outcomes, maintaining statistics for periodic emission and/or forwarding to a metrics sinks such as `Prometheus` (can be customised to gather Domain-relevant statistics identifying the nature of the Reactions processing taking place in addition to generic invocation latency, stream counts and category distributions etc)
   h. `Handler`s: caller-supplied function that's passed a stream identifier (a StreamName:- Category + Id pair) and the span of waiting events for that stream (typically a single event, but can grow in catch-up scenarios, retries, or multiple events being written in close succession)

### Ordering

Event ordering guarantees are a key consideration in running projections or reactions. The origin of the events will intrinsically dictate the upper bound of ordering guarantees possible. Some examples:
- EventStore, SqlStreamStore: these establish a stable order as each event is written (within streams, but also across all streams). The '`$all`' stream and related APIs preserve this order
- CosmosDb: the ChangeFeed guarantees to present Items from logical partitions in order of when they were touched (added/updated). While the relative order of writes across streams happens to be stable when there is one physical partition, that's an irrelevant implementation detail that's not useful for building anything other than a throwaway demo.
- DynamoDb: While DynamoDb streams establishes and preserves the order of each change, that's subject to the 24h retention period limit. The DynamoStore.Indexer and reader all-but preserve that ordering (Note that the indexer does not currently ensure to preserve order _within_ any DDB Streams batch being indexed).
- MessageDb: Events technically have a global order but the store does not expose a `$all` stream equivalent. It does provide a way to read a given category in commit order (internally, appends to a category are serialized to ensure no writes can be missed). Note that The Propulsion reader logic pulls categories independently, without any attempt to correlate across categories (furthermore the fact, noted below, that scheduling is at stream level also removes any guarantees in terms of relative processing order within a category).
- MemoryStore: `MemoryStore`, and `Propulsion.MemoryStore` explicitly guarantees that notifications of write for a given stream are processed (and propagated into the Scheduler) in strict order of those writes (all Propulsion Sources are expected to guarantee that Stream level ordering). 

While the above factors are significant in how the Propulsion Sources are implemented, **the critical thing to understand is that `Propulsion.Streams` makes no attempt to preserve any ordering beyond the individual stream**.

The primary reason for this is that it would imply that invocation of handlers cannot be concurrent. That would be a major impediment to throughput when rate limiting and/or other bottlenecks impose non-uniform effects on handler latency (you'd ideally continue processing on streams that are not currently impacted by rate limiting or latency spikes, working ahead on the basis that the impeded stream's issues will eventually abate).

The other significant opportunity one leaves behind if you don't admit concurrency into your projections is that you can never split or load balance the processing across multiple consumers (Propulsion does not presently implement any explicit sharding support, although when using `Propulsion.CosmosStore`, the underlying Change Feed Processor in the Microsoft SDK implements automated balancing of physical partition leases across all competing instances).

While doing the simplest thing possible to realise any aspect of a system's operation is absolutely the correct starting point for any design, it's also important to consider whether such solutions might impose restrictions that may later be difficult or costly to unravel:
- re-traversing of all events to build a new version of a read model becomes less viable if the time to do that is hardwired to be limited by the serial processing of the items
- while grouping multiple operations into a single SQL batch in order to increase throughput can be effective, it should be noted that such a scheme does not generalise well to the e.g. third party API calls, or writes to document stores etc that do not present such facilities. The other thing that should be noted is that in general, batching will tend to increase lock escalation and hence the amount of contention and risk of deadlocks.
- re-running processing in disaster recovery or amelioration scenarios will have restricted throughput (especially in cases where idempotent reprocessing might mainly involve relatively low cost operations; the lowered individual latency may be dwarfed by the per-call overheads in a way that the original serial processing of the requests might not have surfaced). It's easy to disregard such aspects as nice-to-haves when growing a system from scratch; the point is that there's nothing intrinsic about projection processing that mandates serial processing, and ruling out the opportunity for concurrent processing of streams can result in a system where powerful operational and deployment approaches are ruled out as a result of picking an Easy option over a more Simple one.
- following the strict order of event writes across streams precludes grouping event processing at the stream level (unless you work in a batched manner). Being able to read ahead, collating events from future batches (while still honoring the batch order with regard to checkpointing etc) afford handlers the ability to process multiple events for a single stream as a group (equally, it allows a handler to determine that a given stream has already reached a particular write position, enabling the ingestion process to discard future redundant reads on the basis as processing catches up) 

### Relationships between data established via projections or reactions

The preceding section lays out why building projections that assume a global order of events and then traversing them serially can be harmful to throughput and make a system harder to operate and/or deploy (and, frequently, the complexity of the processing grows beyond the superficially simple initial logic as you've applied batching and other optimizations).

Nonetheless, there will invariably be parent-child relationships within data and it's processing. When analyzed, these group into at least the following buckets:
- foreign key relationships reflecting data dependencies that are necessary to serve queries from a read model
- data that feeds into an overall data processing flow that is reliant on that dependent data being in place for a given piece of processing to yield a correct or meaningful result

Where the parent and child information can live within the same stream, the stream level ordering guarantee can be used to guard against 'child' data being rendered or operated on without its parent being present directly.

#### Explicit Process Managers

In more complex cases, the projection will need to hide or buffer data until such time as the dependency has been fulfilled. At its most general, this is a [Process Manager](https://www.enterpriseintegrationpatterns.com/patterns/messaging/ProcessManager.html). It's important to validate any presumed requirements about being able to render a given read model the instant a child piece of data is ingested into the model; often having a LEFT JOIN in a SELECT exclude the child from a list until such time as the parent has been ingested can be perfectly reasonable regardless of one's desire to have a perfect unified model with all possible foreign keys applied in order to guarantee a watertight model of the entire enterprise (this should be considered alongside other aspects such as whether the projection work is asynchronous, or preemptively carried out prior to reporting completion of a request)

### Phased processing

A key technique in simplifying systems as a whole is to consider whether batching and/or workflows can be managed asynchronously. If we consider the case where a system needs to report completed batches of shipment boxes within a large warehouse:
- a large number of shipments may be being prepared; we can't batch them until we know that all the required items within have been obtained (or deemed unobtainable) and the box sealed
- at peak processing time (or where packing is robotized), there can be deluges of shipments being marked complete. We don't want to impose a variable latency on the turnaround of processing for the person or robot doing the picking to be able to commence it's next activity
- batches need to fulfil size restrictions
- shipments must be included in exactly one batch, no matter how many times the Completed is processed
- batches have their own processing lifecycle that starts subsequent to their being filled 

One way of modelling constraints such as this, is to have a Reactor concurrently handle Shipment Completed notifications, with the grouping of shipments into a given batch (while guaranteeing not to exceed te batch size limit) via a single Decision rather than a storm of concurrent ones. When the reactor responsible for inserting the completed shipments into a batch has  completed its work, it can declare the Batch `Closed`, and the next phase of the cycle (post-processing the Batch prior to transmission) can commence (as a separated piece of Reactor logic, potentially implemented as a separate Reactor Service)

Of course, it's always possible to map such as process to an equivalent set of serial operations on a complete SQL relational model. However implicitly arriving at a model for this processing by ad-hoc SQL operations that assume serial operations is rarely going to yield consistent performance. In some cases, the lack of predictable performance might be tolerable; however the absence of a model that allows one to reason about the behavior of the system as a whole is likely to result in an unmaintainable or difficult to operate system. In conclusion: its critical not to treat the design and implementation of reactions processing as a lesser activity where ease of getting 'something' running trumps all.

### Designing events for projection

- egregious fields to keep simple things simple
- Store in the derived data
- Add at the end, just in time

### Testing Projections

#### Unit testing projections

- No Propulsion required, but MemoryStore can help

#### MemoryStore Projector; deterministic projections integration tests

- `Propulsion.MemoryStore`; example of deterministic waits in a test

#### Generalising MemoryStore Projector tests to hit a concrete store

- Pointers to `proHotel` things

### Transactions

#### Contention with overlapping actors

#### Watchdogs

### Reading your Writes and consistency

When processing based on the a Store's change feed / notification mechanism, there are a number of factors in play:
- balancing straight forward code in the case of a single event on a single stream versus efficiently managing how to catch up when there's a backlog of a significant number of events~~~~
- handling at least once delivery of messages (typically under error conditions, but there are other corner cases too). A primary example would be the case where a significant amount of processing has been completed, but the checkpoint had not yet been committed at the time the host process was torn down; in such a case, an entire (potentially large) batch of events may be re-processed. In such a case, the handling needs to be idempotent (no bad side effects in terms of incorrect or unstable data or duplicate actions; no inordinate overloading of capacity compared to the happy path). 
- when reading without strong consistency, an event observed on a feed may not yet be visible (have propagated to) the node that you query to establish the state of an aggregate (see Reading Your Writes)

Depending on the nature of the processing you're doing, your use of the events will vary:
- performing specific work based solely on the event and/or its payload (e.g., a `Created` event may result in `INSERT`ing a new item into a list)
- treating the events as 'shoulder tap' regarding a given stream; in other words, the nature of the processing is such that whenever events are appended to a given stream, there's a generic piece of processing that should be triggered. Often the processing would not be simpler or more efficient if it inspected the prompting event(s). (e.g. if you were expected to publish a Summary Event per event observed to some external feed, then the only event that does not require inspection of state derived from preceding events would be a Creation event)

#### Mitigations for not being able to Read Your Writes

The following approaches can be used to cater for cases where it can't be guaranteed that
[the read performed by a handler will 'see' the prompting event](https://en.wikipedia.org/wiki/Consistency_model#Read-your-writes_consistency)
(paraphrasing, it's strongly recommended to read
[articles such as this on _eventual consistency_](https://www.allthingsdistributed.com/2007/12/eventually_consistent.html)
or the _Designing Data Intensive Applications_ book):
- Ensure that the read is guaranteed to be from from the cluster's Leader (e.g., a Leader connection in EventStoreDb, the `requireLeader` flag for MessageDb, requesting a 'consistent read' on DynamoDb) or (for a store with Session Consistency) is contingent on the session token being used by the feed reader (e.g. in CosmosDb, using the same `CosmosClient` to ensure the session tokens are synchronized)
- Perform a pre-flight check when reading, based on the `Index` of the newest event passed to the handler. In such a case, it may make sense to back off for a small period, before reporting failure to handle the event (by throwing an exception). The Handler will be re-invoked for another attempt, with a better chance of the event being reflected in the read. In this case, one can safely report `SpanResult.AllProcessed` (or `PartiallyProcessed` if you wish to defer some work due to the backlog of events triggering too much work to perform in a single invocation)
- Perform the processing on a 'shoulder tap' basis: First, load the stream's state, performing any required reactions. Then report the Version attained for the stream (based on the Index of the last event processed) by yielding a `SpanResult.OverrideWritePosition`. In this case, one of following edge cases may result:
    - The handler saw a version prior to the prompting event. For example, if a Create event (`Index = 0`) is relayed, but reading does not yield any events (the replica in question is behind the node from which the feed obtained its state). In this case, the Handler can simply yield `SpanResult.OverrideWritePosition`, which will cause the event to be retained in the input buffer (and most likely, a fresh invocation for that same stream will immediately be dispatched)
    - The Handler saw a Version fresher than the prompting event. For example: if a Create (`Index = 0`) is immediately followed by an Update (`Index = 1`), the handler can yield `SpanResult.OverrideWritePosition 2` to reflect the fact that the next event that's of interest will be event `Index = 2`. Regardless of whether Event 1 arrived while the handler was processing Event 0, or whether it arrives some time afterwards, the event will be dropped from the events pending for that Stream's Handler.

### Consistency in the face of at least once delivery and re-traversal of events

In the general case, events from a feed get de-duplicated, and each event should be seen exactly once. However, this cannot be assumed; ultimately any handler needs to be ready to deal with redelivery of any predecessor event on a stream. In the worst case, that means that immediately after `Index = 4` has been processed, a restart may deliver events `Index = 3` and `Index = 4` as the next span of events within tens of milliseconds.

At the other end of the redelivery spectrum, we have full replays. For instance, it's not uncommon to want to either re-traverse an entire set of events (e.g. if some field was not being stored in the derived data but suddenly becomes relevant), or one may opt to rewind to an earlier checkpoint to trigger re-processing (a downstream processor reports having restored a two hour old back resulting in data loss, which could be resolved by rewinding 3 hours and relying on the idempotency guarantees of their APIs).

A related scenario that often presents itself after a system has been running for some time is the desire to add an entirely new (or significantly revised) read model. In such as case, being able to traverse a large number of events efficiently is of value (being able to provision a tweaked read model in hours rather than days has significant leverage).

### For Read Models, Always Expand and Contract

The safest way to manage extending or tweaking a read model is always to go through the [ParallelChange pattern](https://martinfowler.com/bliki/ParallelChange.html):
- define the tables/entities required, and/or any additional fields or indices. Roll out any schema changes. If you're using a schemaless datastore, this step may not be relevant; perhaps the only thing new is that your documents will now be named `ItemSummary2-{itemId}`
- configure a new consumer group to walk the data and provision the new read model.
- when that's completed, switch the read logic to use the new data.
- at a later point in time, you can TRUNCATE the outgoing read model (to reclaim storage space) before eventually removing it entirely.

True, following such a checklist might feel like overkill in some cases (where a quick `ALTER TABLE` might have done the job). But looking for a shortcut is also passing up an opportunity to practice as you play -- in any business critical system (or one with large amounts of data) hacks are less likely to even be viable.

### Versioning read models over time

One aspect to call out is that it's easy to underestimate the frequency at which such re-processing is required and/or is the best approach to apply; both during the initial development phase of a system (where processing may only have happened in pre-production environment so a quick `TRUNCATE TABLE` will cure it all) or for a short time (where a quick online `UPDATE` or `SELECT INTO` can pragmatically address a need). Two pieces of advice arise from this:
- SQL databases are by far the most commonly used read model stores for good reason - they're malleable (you whack in a join or add an index or two and/or the above mentioned `TRUNCATE TABLE`, `SELECT INTO` tricks will take you a long way), and they provide Good Enough performance and scalability for the vast majority of scenarios (you are not Google).
- it's important to practice how you play with regard to such situations. Use the opportunity to sharpen you and your teams thinking and communication with regard to how to handle such situations rather than cutting corners eery time. That muscle memory will pay back quicker than you think; always sweeping them under the rug (only ever doing hacks) can turn you into an author of one of those sad "what the Internet didn't tell you about Event Sourcing" articles in short order.

In the long run, getting used to dealing with re-traversal scenarios by building handlers to provision fresh adjacent read models is worthwhile. It also a skill that generalises better - a random document store is unlikely to match the full power of a `SELECT INTO`, but ultimately they may be a better overall solution for your read models (`Equinox.CosmosStore` and `Equinox.DynamoStore` also offer powerful `RollingState` modes that can simplify such processing).

In short, it's strongly recommended to at least go through the thought exercise of considering how you'd revise or extend a read model in a way that works when you have a terabyte of data or a billion items in your read model every time you do a 'little tweak' in a SQL read model.  

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

## Using Kafka to reduce RU amplification effects of the CosmosDB ChangeFeed

As noted in the [Effect of ChangeFeed on Request Charges](https://github.com/jet/propulsion/blob/master/DOCUMENTATION.md#effect-of-changefeed-on-request-charges) section, in some systems the RU consumption load placed on a given Container can be significant. One way of reducing that effect is to have a single consumer replicate the events onto a Kafka Topic, and have Reactors and/or Publishers consume the events from that source instead.

Propulsion provides components that enable implementing such a strategy:
- A publisher that efficiently publishes events in a canonical format ('Kafka StreamSpans') (see the `proProjector` template), with stateful de-duplication of events (important given the fact that the bulk of appends involve an update to the Tip document, and the current form of the changefeed does not intrinsically expose the before and after states)
- A consumer component that consumes and decodes the 'Kafka StreamSpans' for use by a `StreamsSink`

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
