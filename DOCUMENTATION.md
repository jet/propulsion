# Documentation

Please refer to the [FAQ](README.md#FAQ), [README.md](README.md) and the [Issues](https://github.com/jet/propulsion/issues) for background info on what's outstanding (aside from there being lots of room for more and better docs).

# Background reading

In general, the primary background information is covered in the [Background Reading section of Equinox's Documentation](https://github.com/jet/equinox/blob/master/DOCUMENTATION.md#background-reading).

- _Enterprise Integration Patterns, Hohpe/Woolf, 2004_: Timeless patterns compendium on messaging, routing, integration and more. It's naturally missing some aspects regarding serverlessness, streaming and the like.
- _Designing Data Intensive Applications, Kleppmann, 2015_: Timeless book on infrastructure and asynchrony in distributed systems, and much more. Arguably should be on the Equinox Background reading list also.
- [Projections Explained- DDD Europe 2020](https://www.youtube.com/watch?v=b2kSlDcAcps) 1h intermediate level backgrounder talk on projection systems by [@yreynhout](https://github.com/yreynhout)
- [Events, Workflows, Sagas? Keep Your Event-driven Architecture Sane - Lutz Huehnken](https://www.youtube.com/watch?v=Uv1GOrZWpBM): Good talk orienting one re high level patterns of managing workflows in an event driven context  

- **Your link here** - Please add notable materials that helped you on your journey here via PRs!

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

# Glossary

There's a [glossary of terms in the Equinox Documentation](https://github.com/jet/equinox/blob/master/DOCUMENTATION.md#glossary). Familiarity with those aspects is a prerequisite for delving into the topic of Projections and Reactions.

| Term                    | Description
|-------------------------|---
| Consumer Group          | Name used to identify a set of checkpoint positions for each Tranche of a Source
| Consumer Load Balancing | Built in lease-based allocation of Partitions to distribute load across instances of a Processor based on a common Consumer Group Name e.g. the broker managed system in Kafka, the Change Feed Processor library within the `Microsoft.Azure.Cosmos` Client
| Batch                   | Group of Events from a Source. Typically includes a Checkpoint callback that's invoked when all events have been handled
| Category                | Group of Streams for a Source matching `{categooryName}-{streamId}` pattern. MessageDB exposes a Feed per Category
| DynamoStore Index       | An `Equinox.DynamoStore` containing a sequence of (Stream,Index,Event Type) entries referencing Events in a Store.<br/>Written by a `Propulsion.DynamoStore.Indexer` Lambda. Can be split into Partitions
| Event                   | An Event from a Stream, obtained from a 
| Feed                    | Incrementally readable portion of a Source that affords a way to represent a Position within that as a durable Checkpoint, with Events being appended at the tail<br/>e.g. the EventStoreDb `$all` stream, an ATOM feed over HTTP, the content of a Physical Partition of a CosmosDb Container
| Handler                 | Function that implements the specific logic that the Processor is providing. Multiple concurrent invocations (Maximum Concurrency: see Processor Parallelism). It is guaranteed that no Handler invocation can be passed the same Stream concurrently.
| Partition               | Autonomous part of a store or feed<br/>e.g. a Physical partition in CosmosDB, a partition of a DynamoStore Index, a partition of a Kafka topic, a shard of a [Sharded](https://www.digitalocean.com/community/tutorials/understanding-database-sharding) database, etc.
| Processor               | Software system wired to a Source with a common Consumer Group Name.<br/>e.g. in the CosmosDB Client, a Change Feed Processor Consumer specifies a `ProcessorName`
| Processor Instance      | Configured Source and Sink ready to handle batches of events for a given Consumer Group Name
| Processor Parallelism   | Upper limit for concurrent Handler invocations for a Processor Instance
| Source                  | Umbrella term for the datasource-specific part of a Processor that obtains batches of origin data from Feeds
| Sink                    | Umbrella term for the datasource-neutral part of a Processor that accepts Batches of Events from the Source, and calls the Checkpointing callbacks when all of the Batch's Events have been handled
| Stream                  | Named ordered sequence of Events as yielded by a Source
| Tranche                 | Checkpointable sequence of Batches supplied from a Source<br/>CosmosDb Change Feed: mapped 1:1 based on Physical Partitions of the CosmosDb Container<br/>DynamoStore: mapped 1:1 from Partitions of a DynamoStore Index<br/>MessageDb: Mapped from each Category relevant to a given Processor's needs

## Propulsion Source Elements

In Propulsion, 'Source' refers to a set of components that comprise the store-specific 'consumer' side of the Processor Pipeline. Per input source, there is generally a `Propulsion.<Input>.<Input>Source` type which wires up the lower level pieces (See [Input Sources](#supported-input-sources) for examples and details).

A Source is supplied with a reference to the Sink that it is to supply Batches to.

It encompasses:

| Component      | Description
|----------------|---
| Source         | Top level component that starts and monitors the (potentially multiple) Readers.<br/>Provides a `Pipeline` object which can be used by the hosting progream to `Stop` the processing and/or `AwaitCompletion`.
| Reader         | Manages reading from a Feed, passing Batches to an Ingester (one per active Tranche)
| Ingester       | A Source maintains one of these per active Reader. Controls periodic checkpointing. Limits the maximum number of Batches that can be Read Ahead
| ProgressWriter | used internally by the `Ingester` to buffer (and periodically commit) updates to the checkpointed position to reflect progress that has been achieved for that Tranche of the Source by this Consumer Group
| Monitor        | Exposes the current read and commit positions for each Tranche within the Source<br/>Enables tests can await completion of processing without having to continually poll to (indirectly) infer whether some desired side effect has been triggered.

## Propulsion Sink Elements

In Propulsion, 'Sink' refers to the store-neutral part of the Processor Pipeline, which consumes the input Batches and Schedules the invocation of Handlers based on events emanating from the 'Source'.

A Sink is fed by a Source.

| Component   | Description
|-------------|---
| Submitter   | Buffers Tranches supplied by Ingesters. Passes Batches to the Scheduler on a round-robin basis to ensure each Tranche of input is serviced equally
| Scheduler   | Loops continually, pioritising Streams to supply to the Dispatcher.<br/>Reports outcome and metrics for Dispatched requests to Stats.<br/>Reports completion of Batches to the Ingester that supplied them [Ingester](#propulsion-source-elements) that supplied it.
| Stats       | Collates Handler invocation outcomes, maintaining statistics for periodic emission and/or forwarding to a metrics sinks such as `Prometheus`<br/>Gathers generic handler latency statistics, stream counts and category distributions<br/>Can be customised to gather Domain-relevant statistics conveying high level Reactor activity.
| Dispatcher  | Loops continually, dispatching Stream Queues to Handlers via the Thread Pool.<br/>Responsible for limiting the maximum number of in-flight Handler invocations to the configured Processor Parallelism limit  
| Handler     | caller-supplied function that's passed a stream name (a Category Name + Aggregate Id pair) and the span of buffered events for that stream<br/>Typically this will be a single event, but this enables optimal processing in catch-up scenarios, when retries are required, or where multiple events are written to the same Stream in close succession.

# The `Propulsion.Streams` Programming Model for Projections, Reactions and Workflows

Propulsion provides a pluggable set of components that enable you to implement high performance, resilient and observable event processing pipeline for Reaction, Ingestion and Publishing pipelines as part of an Event Sourced system.

Each such pipeline manages a related set of follow-on activities. Examples:
- running reactive workflows triggered by user actions (which in turn may trigger further reactions as ripple effects)
- maintaining Read Models based on (or just triggerred by) the Events
- continually synchronising/publishing information for/into partner systems

At a high level, a Propulsion pipeline covers these core responsibilities:
- The **Source** coordinates reading of Events from an event store's notification mechanisms with a defined read ahead limit (which go by various monikers such as subscriptions, CosmosDB change feed, change data capture, DynamoDB streams etc)
- The **Sink** manages (asynchronously) buffering events, scheduling and monitoring the activities of the **Handler**s triggered by the Events, gathering and emitting metrics, balancing processing fairly over all active input Tranches
- The Source and Sink coordinate to asynchronously **Checkpoint**ing progress based on batch completion notifications from the Sink side.
  - A persistent position is maintained per Consumer Group for each Tranche supplied from the Source.
  - The checkpointing is configurable in a manner appropriate to the source in question (for example, you might be reading events from EventStoreDb, but maintain the checkpoints alongside a Read Model that you maintain in CosmosDb or DynamoDb).
  - The `propulsion checkpoint` tool can be used to inspect or override the checkpoint position associated with a specific source+tranche+consumer group

## Features

- encapsulates interaction with specific event store client libraries on an opt-in basis (there are no lowest common denominator abstractions). Upgrading underlying client libraries for a given Reactor becomes a matter of altering `PackageReference`s to Propulsion packages, rather than directly coupling to a specific lower level event store client library.
- provides a clean approach to the testing of Reaction logic with and without involving your actual event store. Propulsion provides a `MemoryStoreProjector` component that, together with `Equinox.MemoryStore` enables a fully deterministic in memory processing pipeline (including handling reactions and waiting for them to complete), without going out of process. See the [Testing](#testing) section).
- provides observability of message handling and processing. Propulsion exposes diagnostic and telemetry information that enables effective monitoring and alerting. That's essential for tuning and troubleshooting your Handlers in a production system.

## Supported Input sources

Propulsion provides for processing events from the following sources:

1. `Propulsion.CosmosStore`: Lightly wraps the Change Feed Processor library within the `Microsoft.Azure.Cosmos` client, which provides a load balanced leasing system (comparable to Kafka consumers). This enables multiple running instances of a Processor bearing the same Consumer Group Name to parallelize processing using the [competing consumers pattern](https://www.enterpriseintegrationpatterns.com/patterns/messaging/CompetingConsumers.html). There can be as many active processors as physical partitions.
   - Each physical Partition of the CosmosDb Container maps to an input Tranche. The configured Processor Parallelism limit governs the maximum number of Streams for which work is being processed at any time (even if the Change Feed Processor library assigns more than one partition to a given Processor Instance).
     - (Internally, when the API supplies a batch of Items/Documents, it indicates the Partition Id via the Context's `LeaseToken` property).
   - Over time, as a Container splits into more physical Partitions (either due to assigning > 10K RU per physical partition, or as the data size approaches the 50GB/partition limit).
   - The Change Feed Processor library can monitor and present metrics about the processing. It estimates the processing Lag to indicate the distance between the current checkpoint position and the tail of each partition. (These lags can be exposed as Prometheus metrics if desired).
   - The Change Feed Processor library maintains checkpoints within an 'auxiliary container'. By convention, that's a sibling Container with the name`{container}-aux`. The document containing the checkpoint position also doubles for managing the current owner of the lease for that partition (Load balancing is achieved either by consumers self-assigning expired leases, or forcibly taking over a lease from a consumer that is currently assigned more than one partition).
2. `Propulsion.DynamoStore`: Reads from an Index Table fed and maintained by a `Propulsion.DynamoStore.Indexer` Lambda.
    - the Source runs a Reader loop for each Partition of the DynamoStore Index, each mapping to an individual input Tranche. (Currently the Indexer is hardcoded to only feed into a single partition.)
    - The `Propulsion.DynamoStore.Notifier` mechanism can be used to distribute the processing over as many Lambda instances as there are DynamoStore Index Partitions (via SQS FIFO triggers) without redundant reprocessing of notifications (each Lambda invocation is triggered by a token specific to a single DynamoStore Index Partition)
    - Note `DynamoStoreSource` does not (yet) implement load balancing, so multiple Processor Instances would typically result in notifications being redundantly reprocessed (as opposed to `CosmosStore`, which, as noted above, provides that directly via the Change Feed Processor's Consumer Load Balancing mechanism).
3. `Propulsion.EventStoreDb`: Uses the EventStoreDb gRPC based API to pull events from the `$all` stream.
   - does not yet implement load balancing; the assumption is that you'll run a single instance of your Processor. 
   - In the current implementation, there's no support for surfacing lag metrics on a continual basis (the Reader logs the lag on startup, but unlike for `CosmosStore`, there is no logic to e.g. log it every minute and/or feed it to Prometheus)
   - Propulsion provides you out of the box checkpoint storage in CosmosStore, DynamoStore, Postgres and SQL Server. There is not presently a [checkpoint store implementation that maintains the checkpoints in EventStoreDb itself at present](https://github.com/jet/propulsion/issues/8).
4. `Propulsion.MessageDb`: Uses the MessageDb stored procedures to concurrently consume from a specified list of Categories within a MessageDb event store.
   - a Reader loop providing a Tranche of events is established per nominated Category
   - does not yet implement load balancing; the assumption is that you'll run a single instance of your Processor.
   - does no (yet) compute or emit metrics representing the processing lag (number of incoming events due to be processed)
   - Propulsion provides you out of the box checkpoint storage for CosmosStore, DynamoStore, Postgres and SQL Server. In general you'll want to store the checkpoints alongside your Read Model.
5. `Propulsion.SqlStreamStore`: Uses the `SqlStreamStore` libraries to consume from MySql, Postgres or SqlServer.
    - a single Reader loop pulls events from the Store (Each `SqlStreamStore` implementation presents a single unified stream analogous to the EventStoreDb `'$all'` stream)
    - does not yet implement load balancing; the assumption is that you'll run a single instance of your Processor.
    - does no (yet) compute or emit metrics representing the processing lag (number of incoming events due to be processed)
    - Propulsion provides you out of the box checkpoint storage for CosmosStore, DynamoStore, Postgres and SQL Server. In general you'll want to store the checkpoints alongside your Read Model.
6. `Propulsion.Feed`: Provides for reading from an arbitrary upstream system using custom wiring under your control.
   - The most direct mechanism, for a source that presents an ATOM-like checkpointable Feed involves just supplying a function that either
     - reads a page forward from a specified `Position`
     - provides an `IAsyncEnumerable&lt;Batch&gt;` that walks forward from a specified checkpoint `Position`
    - A Feed can be represented as multiple Tranches. It balances processing across them. For instance, each tenant of an upstream system can be independently read and checkpointed, with new tranches added over time.
    - In the current implementation, there's no support for exposing lag metrics (Logs show the read position and whether the tail has been reached, but not the lag).
    - Propulsion provides you out of the box checkpoint storage for CosmosStore, DynamoStore, Postgres, SQL Server.
7. `Propulsion.Feed.PeriodicSource`: Provides for periodic scraping of a source that does not present a checkpointable (and thus incrementally readable) Feed. An example would be a SQL Data Warehouse which regularly has arbitrary changes applied to it in a way that does not lend itself to Change Data Capture or any other such mechanism.
   - Internally, the Source generates a synthetic Checkpoint Position based on when the  last complete traversal commenced. A re-traversal of the full input data is triggered per Tranche when when a specified interval has elapsed since the data for that Tranche was last ingested.
   - In all other aspects, it's equivalent to the general `Propulsion.Feed` Source; see the preceding section for details.

## Ordering guarantees

Event ordering guarantees are a key consideration in running projections or reactions. The origin of the events will intrinsically dictate the upper bound of ordering guarantees possible. Some examples:
- EventStore, SqlStreamStore: these establish a stable order as each event is written (within streams, but also across all streams). The '`$all`' stream and related APIs preserve this order
- CosmosDb: the ChangeFeed guarantees to present Items from _logical_ partitions (i.e. Streams) in order of when they were touched (added/updated). While the relative order of writes across streams happens to be stable when there is one physical partition, that's irrelevant for building anything other than a throwaway demo.
- DynamoDb: While DynamoDb Streams establishes and preserves the order of each change for a given Table, this information is subject to the 24h retention period limit. The `Propulsion.DynamoStore.Indexer` and its Reader all-but preserve that ordering.
- MessageDb: Events technically have a global order but the store does not expose a `$all` stream equivalent. It does provide a way to read a given category in commit order (internally, appends to a category are serialized to ensure no writes can be missed). Note that the Propulsion reader logic pulls categories independently, without any attempt to correlate across them.
- MemoryStore: `MemoryStore`, and `Propulsion.MemoryStore` explicitly guarantee that notifications of writes _for a given stream_ can be processed (and propagated into the Sink) in strict order of those writes (all Propulsion Sources are expected to guarantee that same Stream level ordering). 

While the above factors are significant in how the Propulsion Sources are implemented, **the critical thing to understand is that `Propulsion.Streams` makes no attempt to preserve any ordering beyond the individual stream**.

The primary reason for this is that it would imply that the invocation of handlers could never be concurrent. Such a restriction would be a major impediment to throughput when rate limiting and/or other bottlenecks impose non-uniform effects on handler latency (you'd ideally continue processing on streams that are not presently impacted by rate limiting or latency spikes, working ahead on the basis that the impeded stream's issues will eventually abate).

The other significant opportunity that's left on the table if you don't admit concurrency into the picture is that you can never split or [load balance the processing across multiple Processor Instances](#distributing-processing). 

So, while doing the simplest thing possible to realise any aspect of a system's operation is absolutely the correct starting point for any design, it's also important to consider whether such solutions might impose restrictions that may later be difficult or costly to unravel:
- re-traversing all events to build a new version of a read model becomes less viable if the time to do that is hardwired to be limited by the serial processing of the items
- while grouping multiple operations into a single SQL batch in order to increase throughput can be effective, it should be noted that such a scheme does not generalise well e.g. third party API calls, conditional updates to document stores etc tend to not provide for batched updates in the same way that a SQL store does. Even where transactional batches are viable it should be noted that batching will tend to cause lock escalation, increasing contention and the risk of deadlocks.
- re-running processing in disaster recovery or amelioration scenarios will have restricted throughput (especially in cases where idempotent reprocessing might involve relatively low cost operations; the lowered individual latency may be dwarfed by the per-call overheads in a way that the initial processing of the requests might not have surfaced). It's easy to disregard such aspects as nice-to-haves when growing a system from scratch; the point is that there's nothing intrinsic about projection processing that mandates serial processing, and ruling out the opportunity for concurrent processing of streams can result in a system where powerful operational and deployment approaches are ruled out by picking an Easy option over a Simple one.

The bottom line is that following the strict order of event writes across streams precludes grouping event processing at the stream level (unless you work in a batched manner). Being able to read ahead, collating events from future batches (while still honoring the batch order with regard to checkpointing etc) affords handlers the ability to process multiple events for a single stream together. Working at the stream level also enables a handler to provide feedback that a given stream has already reached a particular write position, enabling the ingestion process to skip future redundant work as it catches up.

### Relationships between data established via projections or reactions

The preceding section lays out why building projections that assume a global order of events and then traversing them serially can be harmful to throughput while also tending to make operations and/or deployment more problematic. Not only that, frequently the complexity of the processing grows beyond the superficially simple initial logic as you implement batching and other such optimizations.

Nonetheless, there will invariably be parent-child relationships within data and it's processing. These tend to group into at least the following buckets:
- foreign key relationships reflecting data dependencies intrinsic to being able to serve queries from a read model
- data that feeds into an overall data processing flow which is reliant on that dependent data being in place to yield a correct or meaningful result

The ideal/trivial situation is of course to manage both the Parent and Child information within a single stream; in such cases the stream level ordering guarantee will be sufficient to guard against 'child' data being rendered or operated on without its parent being present directly.

The following sections describe strategies that can be applied where this is not possible.

#### Explicit Process Managers

In more complex cases, the projection will need to hide or buffer data until such time as the dependency has been fulfilled. At its most general, this is a [Process Manager](https://www.enterpriseintegrationpatterns.com/patterns/messaging/ProcessManager.html). 

Equally, before assuming that a Process Manager is warranted, spend the time to validate any 'requirements' you are assuming about being able to render a given read model the instant a child piece of data is ingested into the model; often having a `LEFT JOIN` in a `SELECT` to exclude the child from a list until the parent data has been ingested may be perfectly acceptable. Yes, in spite of one's desire to have your Read Model double as unified enterprise data model with all conceivable foreign keys in place.

### Phased processing

A key technique in simplifying an overall system is to consider whether batching and/or workflows can be managed asynchronously loosely connected decoupled workflows. If we consider the case where a system needs to report completed batches of shipment boxes within a large warehouse:
- a large number of shipments may be in the process of being prepared (picked); we can't thus batch shipments them until we know that all the required items within have been obtained (or deemed unobtainable) and the box sealed
- at peak processing time (or where picking is robotized), there can be deluges of events representing shipments being marked complete. We don't want to impose a variable latency on the turnaround time for a person or robot doing the picking before it is able to commence it's next activity.
- batches need to fulfill size restrictions
- shipments must be included in exactly one batch, no matter how many times the Completed event for a Shipment is processed
- batches have their own processing lifecycle that can only commence when the full set of Shipments that the Batch is composed of is known and can be frozen.

One way to handle constraints such as this is to have a Processor concurrently handle Shipment Completed notifications:
- Initially, each handler can do some independent work at the shipment level.
- Once this independent work has completed, grouping Shipments into a Batch (while guaranteeing not to exceed te batch size limit) can converge as a single Decision rather than a potential storm of concurrent ones.
- Finally, once, the Reactor responsible for inserting the completed shipments into a batch has noted that the batch is full, it can declare the Batch `Closed`
- This `Closed` event can then be used to trigger the next phase of the cycle (post-processing the Batch prior to transmission). In many cases, it will make sense to have the next phase be an entirely separated Processor)

Of course, it's always possible to map any such chain of processes to an equivalent set of serial operations on a more closely coupled SQL relational model. The thing you want to avoid is stumbling over time into a rats nest by a sequence of individually reasonable ad-hoc SQL operations that assume serial operations. Such an ad hoc system is unlikely to end up being either scalable or easy to maintain. In some cases, the lack of predictable performance might be tolerable; however the absence of a model that allows one to reason about the behavior of the system as a whole is likely to result in an unmaintainable or difficult to operate system. In conclusion: its critical not to treat the design and implementation of reactions processing as a lesser activity where ease of getting 'something' running trumps all.

### Proactive reaction processing 

In general, decoupling Read Model maintenance Processes from Transactional processing activities per CQRS conventions is a good default.

Its important to also recognize times when preemptively carrying out some of the reactive processing as part of the write flow can be a part of providing a more user friendly solution.

In some cases, the write workflow can make a best effort attempt at carrying out the reactive work (but degrading gracefully under load etc by limiting the number of retry attempts and/or a timeout), while the Reactions Processor still provides the strong guarantee that any requirements that were not fulfilled proactively are guaranteed to complete eventually.

## Distributing Processing

When running multiple Processor Instances, there's the opportunity to have pending work for the Consumer Group be shared across all active instances by filtering the Streams for which events are fed into the Ingester. There are two primary ways in which this can be achieved:
- Use Consumer Load Balancing to balance assignment of source Partitions to Processor Instances. Currently supported by Cosmos and Kafka sources.
   - (Note this implies that the number of Processor Instances performing work is restricted to the number of underlying partitions)
- Use a Lambda SQS FIFO Source Mapping together with the `Propulsion.DynamoStore.Notifier` to map each DynamoStore Index Partition's activity to a single running Lambda instance for that Partition.
   - In this case, the maximum number of concurrently active Processor Instances is limited by the number of Partitions that the `Propulsion.DynamoStore.Indexer` Lambda has been configured to shard the DynamoStore Index into.
- (not currently implemented) Have the Reader shard the data into N Tranches for a Source's Feeds/Partitions based on the Stream Name. This would involve:
   - Implement a way to configure the number of shards that each instance's Reader is to split each Batch (i.e. all Sources need to agree that `$all` content is to be split into Tranches 0, 1, 2 based on a common hashing function)
   - Implement a leasing system akin to that implemented in the CosmosDb Change Feed Processor library to manage balanced assignment of Tranches to Processor Instances. As leases are assigned and revoked, the Source needs to run a Reader and Ingester per assigmnent, that reads the underlying data and forwards the relevant data solely for the batches assigned to the specific Processor Instance in question

## Consistency, Reading your Writes

When Processing reactions based on a Store's change feed / notification mechanism, there are many factors to be considered:
- balancing straight forward logic for the base case of a single event on a single stream with the demands of efficiently catch up when there's a backlog of a significant number of events at hand
- handling at least once delivery of messages (typically under error conditions, but there are other corner cases too). A primary example would be the case where a significant amount of processing has been completed, but the progress had not yet been committed at the time the Processor Instance was torn down; in such a case, an entire (potentially large) batch of events may be re-processed. Regardless of the cause, the handling needs to cope by processing in an idempotent fashion (no bad side effects in terms of incorrect or unstable data or duplicate actions; processing time and resource consumption should reduce compared to the initial processing of the event). 
- when reading from a store without strong consistency, an event observed on a feed may not yet be visible (have propagated to) the node that you query to establish the state of the aggregate (see Reading Your Writes)

Depending on the nature of your Processor's purpose, your use of the events will vary:
- performing specific work based solely on the event type and/or its payload without having to first look up data within your Read Model (e.g., a `Created` event may result in `INSERT`ing a new item into a list)
- treating the events as 'shoulder tap' regarding a given stream. In this mode, the nature of the processing may be that whenever any event is appended to a given stream, there's a generic piece of processing that should be triggered. In many such cases, the actual event(s) that prompted the work may be almost relevant, e.g. if you were expected to publish a Summary Event to an external feed per set of changes observed, then the only event that does not require derivation of a state derived from the complete set of preceding events would be a Creation event.

### Mitigations for not being able to Read Your Writes

The following approaches can be used to cater for cases where it can't be guaranteed that
[the read performed by a handler will 'see' the prompting event](https://en.wikipedia.org/wiki/Consistency_model#Read-your-writes_consistency)
(paraphrasing, it's strongly recommended to read
[articles such as this on _eventual consistency_](https://www.allthingsdistributed.com/2007/12/eventually_consistent.html)
or the _Designing Data Intensive Applications_ book):
1. Ensure that the read is guaranteed to be consistent by employing a technique relevant to the store in question, e.g.
   - EventStoreDb: a Leader connection
   - MessageDb: use the `requireLeader` flag
   - DynamoDb: requesting a 'consistent read'
   - CosmosDb: when using Session Consistency, require that reads are contingent on the session token being used by the feed reader. This can be achieved by using the same `CosmosClient` to ensure the session tokens are synchronized.
2. Perform a pre-flight check when reading, based on the `Index` of the newest event passed to the handler. In such a case, it may make sense to back off for a small period, before reporting failure to handle the event (by throwing an exception). The Handler will be re-invoked for another attempt, with a better chance of the event being reflected in the read.
   - Once such a pre-flight check has been carried out, one can safely report `SpanResult.AllProcessed` (or `PartiallyProcessed` if you wish to defer some work due to the backlog of events triggering too much work to perform in a single invocation)
3. Perform the processing on a 'shoulder tap' basis.
   - First, load the stream's state, performing any required reactions.
   - Then report the Version attained for the stream (based on the Index of the last event processed) by yielding a `SpanResult.OverrideWritePosition`.
   - In this case, one of following edge cases may result:
     - _The handler saw a version prior to the prompting event_. For example, if a Create event (`Index = 0`) is relayed, but reading does not yield any events (the replica in question is behind the node from which the feed obtained its state). In this case, the Handler can simply yield `SpanResult.OverrideWritePosition`, which will cause the event to be retained in the input buffer (and most likely, a fresh invocation for that same stream will immediately be dispatched)
     - _The Handler saw a Version fresher than the prompting event_. For example: if a Create (`Index = 0`) is immediately followed by an Update (`Index = 1`), the handler can yield `SpanResult.OverrideWritePosition 2` to reflect the fact that the next event that's of interest will be event `Index = 2`. Regardless of whether Event 1 arrived while the handler was processing Event 0, or whether it arrives some time afterwards, the event will be dropped from the events pending for that Stream's Handler.

### Consistency in the face of at least once delivery and re-traversal of events

In the general case, events from a feed get de-duplicated, and each event should be seen exactly once. However, this cannot be assumed; ultimately any handler needs to be ready to deal with redelivery of any predecessor event on a stream. In the worst case, that means that immediately after `Index = 4` has been processed, a restart may deliver events `Index = 3` and `Index = 4` as the next span of events within tens of milliseconds.

At the other end of the redelivery spectrum, we have full replays. For instance, it's not uncommon to want to either re-traverse an entire set of events (e.g. if some field was not being stored in the derived data but suddenly becomes relevant), or one may opt to rewind to an earlier checkpoint to trigger re-processing (a downstream processor reports having restored a two hour old back resulting in data loss, which could be resolved by rewinding 3 hours and relying on the idempotency guarantees of their APIs).

A related scenario that often presents itself after a system has been running for some time is the desire to add an entirely new (or significantly revised) read model. In such as case, being able to traverse a large number of events efficiently is of value (being able to provision a tweaked read model in hours rather than days has significant leverage).

## For Read Models, default to 'Expand and Contract'

The safest way to manage extending or tweaking a read model is always to go through the [ParallelChange pattern](https://martinfowler.com/bliki/ParallelChange.html):
- define the tables/entities required, and/or any additional fields or indices. Roll out any schema changes. If you're using a schemaless datastore, this step may not be relevant; perhaps the only thing new is that your documents will now be named `ItemSummary2-{itemId}`
- configure a new Processor with a different Consumer Group to walk the data and provision the new read model.
- when that's completed, switch the read logic to use the new model.
- at a later point in time, you can TRUNCATE the outgoing read model (to reclaim storage space) before eventually removing it entirely.

True, following such a checklist might feel like overkill in some cases (where a quick `ALTER TABLE` might have done the job). But looking for a shortcut is also passing up an opportunity to practice as you play -- in any business critical system (or one with large amounts of data) hacks are less likely to even be viable.

### Versi~~~~oning read models over time

One aspect to call out is that it's easy to underestimate the frequency at which a full re-traversal is required and/or is simply the easiest approach to apply given a non-empty read model store; both during the initial development phase of a system (where processing may only have happened in pre-production environment so a quick `TRUNCATE TABLE` will cure it all) or for a short time (where a quick online `UPDATE` or `SELECT INTO` can pragmatically address a need). Two pieces of advice arise from this:
- SQL databases are by far the most commonly used read model stores for good reason - they're malleable (you whack in a join or add an index or two and/or the above mentioned `TRUNCATE TABLE`, `SELECT INTO` tricks will take you a long way), and they provide Good Enough performance and scalability for the vast majority of scenarios (you are not Google).
- it's important to practice how you play with regard to such situations. Use the opportunity to sharpen you and your team's thinking and communication with regard to how to handle such situations rather than cutting corners every time. That muscle memory will pay back far quicker than you think; always sweeping them under the rug (only ever doing hacks) can turn you into the next author of one of those sad "what the Internet didn't tell you about Event Sourcing" articles in short order.

In the long run, getting used to dealing with re-traversal scenarios by building handlers to provision fresh adjacent read models is worthwhile. It also a skill that generalises better - a random document store is unlikely to match the full power of a `SELECT INTO`, but ultimately they may be a better overall solution for your read models (`Equinox.CosmosStore` and `Equinox.DynamoStore` also offer powerful `RollingState` modes that can simplify such processing).

In short, it's strongly recommended to at least go through the thought exercise of considering how you'd revise or extend a read model in a way that works when you have a terabyte of data or a billion items in your read model every time you do a 'little tweak' in a SQL read model.  

## TODO

### Designing events for projection

- egregious fields to keep simple things simple
- Store in the derived data
- Add at the end, just in time

### Testing

#### Unit testing projections

- No Propulsion required, but MemoryStore can help

#### MemoryStore Projector; deterministic projections integration tests

- `Propulsion.MemoryStore`; example of deterministic waits in a test

#### Generalising MemoryStore Projector tests to hit a concrete store

- Pointers to `proHotel` things

### Transactions

#### Contention with overlapping actors

#### Watchdogs

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
