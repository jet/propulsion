# Changelog

The repo is versioned based on [SemVer 2.0](https://semver.org/spec/v2.0.0.html) using the tiny-but-mighty [MinVer](https://github.com/adamralph/minver) from [@adamralph](https://github.com/adamralph). [See here](https://github.com/adamralph/minver#how-it-works) for more information on how it works.

All notable changes to this project will be documented in this file. The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

The `Unreleased` section name is replaced by the expected version of next release. A stable version's log contains all changes between that version and the previous stable version (can duplicate the prereleases logs).

## [Unreleased]

### Added

- Streams Scheduler: Tune memory consumption re write positions of inactive streams [#94](https://github.com/jet/propulsion/pull/94) 
- Streams Scheduler: Add `purgeInterval` to reclaim memory used by write positions of inactive streams [#95](https://github.com/jet/propulsion/pull/95) 

### Changed
### Removed
### Fixed

<a name="2.10.0-rc2"></a>
## [2.10.0-rc2] - 2021-01-27

### Added

- `Propulsion.Prometheus`: Add Buffer, Cpu and Handler latency metrics to `Propulsion.Streams` Scheduler [#93](https://github.com/jet/propulsion/pull/93) 

### Changed

- Add `<RollForward>Major</RollForward>` for `Propulsion.Tool` [#92](https://github.com/jet/propulsion/pull/92)

### Fixed

- Replaced numeric field names with strings in latency percentiles message [#93](https://github.com/jet/propulsion/pull/93)

<a name="2.10.0-rc1"></a>
## [2.10.0-rc1] - 2020-12-03

### Added

- `Propulsion.Cosmos`: `prometheus-net` integration [#91](https://github.com/jet/propulsion/pull/91)

### Changed

- `Kafka`: Replace defaulting of `Producer`'s `acks` defaulting to `Leader` with mandatory argument [#89](https://github.com/jet/propulsion/pull/89)

<a name="2.9.1"></a>
## [2.9.1] - 2020-11-09

### Fixed

- `Cosmos.CosmosPruner`: Fixed off-by-one error that renders pruning inert [#88](https://github.com/jet/propulsion/pull/88)

<a name="2.9.0"></a>
## [2.9.0] - 2020-11-04

### Added

- Exposed optional `maxEvents` and `maxBytes` arguments for `CosmosSink` [#84](https://github.com/jet/propulsion/pull/84)

### Changed

- `Cosmos`: Tweak logging in `CosmosSink` [#85](https://github.com/jet/propulsion/pull/85)

<a name="2.9.0-rc2"></a>
## [2.9.0-rc2] - 2020-09-08

### Changed

- Consistently exposed optional `idleDelay` and `pumpInterval` arguments for all Projectors and Ingesters [#81](https://github.com/jet/propulsion/pull/81)

### Fixed

- `Kafka.StreamsProducerSink`: Changed bad default `idleDelay` from 0ms to 1ms [#82](https://github.com/jet/propulsion/pull/82)

<a name="2.9.0-rc1"></a>
## [2.9.0-rc1] - 2020-08-31

### Added

- `Cosmos`: Add `CosmosPruner` [#80](https://github.com/jet/propulsion/pull/80)

### Fixed

- Corrected Cosmos + Stream Sink error logging [#79](https://github.com/jet/propulsion/pull/79)

<a name="2.8.1"></a>
## [2.8.1] - 2020-08-04

### Added

- `Kafka.Producer`: Expose `acks`, `linger`, `compression` options [#78](https://github.com/jet/propulsion/pull/78)

### Changed

- `Kafka`: Adjust default linger to 5ms [#78](https://github.com/jet/propulsion/pull/78)

### Fixed

- Apply [Corrected FsKafka Poll loop](https://github.com/jet/FsKafka/pull/70) when over `maxInFlightBytes` threshold as per `FsKafka` v `1.5.0` [#77](https://github.com/jet/propulsion/pull/77)

<a name="2.8.0"></a>
## [2.8.0] - 2020-07-27

### Changed

- `Kafka`: Targets [`FsKafka`/`FsKafka0` (and `Confluent.Kafka`) v `1.5.0`](https://github.com/jet/FsKafka/blob/master/CHANGELOG.md#1.5.0) [#75](https://github.com/jet/propulsion/pull/75)

<a name="2.7.1"></a>
## [2.7.1] - 2020-07-27

### Fixed

- Fix Projector hang when using >1 Streams Scheduler [#74](https://github.com/jet/propulsion/pull/74) :pray: [@fnipo](https://github.com/fnipo)

<a name="2.7.0"></a>
## [2.7.0] - 2020-06-09

### Added

- `Propulsion.SqlStreamStore`: Reader and checkpoint implementation for SqlStreamStore [#72](https://github.com/jet/propulsion/pull/72) :pray: [@scrwtp](https://github.com/scrwtp)

<a name="2.6.0"></a>
## [2.6.0] - 2020-05-20

### Added

- `Kafka`: Add `Propulsion.Kafka.ConsumeResultContext` and default `StreamNameSequenceGenerator.ConsumeResultToStreamEvent` [#70](https://github.com/jet/propulsion/pull/70)

### Changed

- `Kafka`: Targets [`FsKafka`/`FsKafka0` (and `Confluent.Kafka`) v `1.4.3`](https://github.com/jet/FsKafka/blob/master/CHANGELOG.md#1.4.3) [#69](https://github.com/jet/propulsion/pull/69)

<a name="2.5.2"></a>
## [2.5.2] - 2020-05-16

### Fixed

- `Kafka`: Targets `FsKafka`/`FsKafka0` v `1.5.0-beta.2` to resolve `TypeLoadException` when using `1.5.0-beta.1`

<a name="2.5.1"></a>
## [2.5.1] - 2020-05-14 (Unlisted/broken : :point_up:)

### Fixed

- `EventStoreSource`: Fixed `obj` -> `int` type discrepancy re [#63](https://github.com/jet/propulsion/pull/63)

<a name="2.5.0"></a>
## [2.5.0] - 2020-05-13 (Unlisted/broken :point_up:)

### Changed

- `Kafka`: Targets [`FsKafka`/`FsKafka0` v `1.4.2`](https://github.com/jet/FsKafka/blob/master/CHANGELOG.md#1.4.2) [#67](https://github.com/jet/propulsion/pull/67)

### Removed

- `Propulsion.Kafka0` Some `Propulsion.Kafka0`-namespaced shimming elements are now found in the `FsKafka` namespace in `FsKafka0` [#67](https://github.com/jet/propulsion/pull/67)
- `Propulsion.Kafka`: `KafkaMonitor` is now found in the `FsKafka` namespace in `FsKafka`/FsKafka0` (NOTE: integration tests continue to live in this repo) [#67](https://github.com/jet/propulsion/pull/67)

### Fixed

- `Kafka`: Change buffer grouping to include `Topic` alongside `PartitionId` - existing implementation did not guarantee marking progress where consuming from more than one Topic concurrently [#63](https://github.com/jet/propulsion/pull/63)

<a name="2.4.3"></a>
## [2.4.3] - 2020-04-27

### Changed

- Revised `handle` function signatures in `Propulsion.Sync.StreamsSync` and `Propulsion.Streams.StreamsProjector` to use `Propulsion.Streams.SpanResult` representing Write Position updates [#62](https://github.com/jet/propulsion/pull/62)

<a name="2.4.2"></a>
## [2.4.2] - 2020-04-27

### Changed

- Mark `Scheduler.Stats` abstract, reorder/name `statsInterval` arguments [#61](https://github.com/jet/propulsion/pull/61)

<a name="2.4.1"></a>
## [2.4.1] - 2020-04-25

### Changed

- Added abstract `HandleExn` method to `type Stats`; removed defaulting of `stats` arguments in almost all cases [#60](https://github.com/jet/propulsion/pull/60)

<a name="2.4.0"></a>
## [2.4.0] - 2020-04-24

### Added

- BREAKING: Revised `handle` function signatures in `Propulsion.Kafka.StreamsConsumer` and `Propulsion.Streams.StreamsProjector` to include a `Propulsion.Streams.SpanResult` representing Write Position updates [#59](https://github.com/jet/propulsion/pull/59)

### Changed

- Removed egregious `int64` from stats handler signatures in `Propulsion.Streams.Scheduling.StreamSchedulerStats` and `Projector.Stats` [#59](https://github.com/jet/propulsion/pull/59)
- Renamed `Streams.Sync.StreamsSyncStats` and `Streams.Scheduling.StreamSchedulerStats` to `Stats` for consistency [#59](https://github.com/jet/propulsion/pull/59)

<a name="2.3.0"></a>
## [2.3.0] - 2020-04-22

### Added

- `Propulsion.Kafka`: `StreamNameSequenceGenerator.ConsumeResultToStreamEvent` provides a set of helpers for mapping from Kafka `ConsumeResult`s to a `StreamEvent` for processing [#56](https://github.com/jet/propulsion/pull/56)
- `Propulsion.Kafka.StreamsConsumer.Start`: now has a `consumeResultToStreamEvents : ConsumeResult<_, _> -> Propulsion.Streams.StreamEvent<_> seq` parameter (which can be implemented via `StreamNameSequenceGenerator.ConsumeResultToStreamEvent`) [#56](https://github.com/jet/propulsion/pull/56)
- BREAKING: Moved pubternal `Propulsion.Kafka.Core.StreamKeyEventSequencer` to public `Propulsion.Kafka.StreamNameSequenceGenerator` [#56](https://github.com/jet/propulsion/pull/56)

### Changed

- BREAKING: Moved `Propulsion.Kafka.StreamsConsumer`'s Start methods to `Propulsion.Core.StreamsConsumer` (i.e. need to prefix `StreamsConsumer` to migrate) [#56](https://github.com/jet/propulsion/pull/56)
- BREAKING: `Propulsion.Kafka.Core.StreamKeyEventSequencer.ToStreamEvent` becomes `Propulsion.Kafka.StreamNameSequenceGenerator.KeyValueToStreamEvent` [#56](https://github.com/jet/propulsion/pull/56)

<a name="2.2.0"></a>
## [2.2.0] - 2020-04-18

### Added

- `Propulsion.Kafka`/`Kafka0`: Exposed `Produce` API providing ability to customize the `Confluent.Kafka` API used
- `Propulsion.Kafka`: Added `Produce` API admitting a raw `Message`
- `Propulsion.Kafka`: Added `Produce` API admitting optional `headers`

### Changed

- `Kafka`: Targets [`FsKafka` v `1.4.1`](https://github.com/jet/FsKafka/blob/master/CHANGELOG.md#1.4.1)
- `Propulsion.Kafka`/`Kafka0`: Replaced `ProduceAsync` API with `Produce` that no longer yields a `DeliveryReport` (this functionality can be replicated via an alternate overload)
- `EventStore`: Updated `CheckpointSeries` to adhere more closely to naming per [standardized naming](https://github.com/jet/dotnet-templates/pull/54) [#51](https://github.com/jet/propulsion/pull/51)

<a name="2.1.1"></a>
## [2.1.1] - 2020-03-11

### Added

- `StreamsSyncStats`: Added missing `HandleOk`

<a name="2.1.0"></a>
## 2.1.0 - 2020-03-11

### Added

- `StreamsProducerSink`: Added overload to facilitate `prepare` make production of an output `option`al and admitting processing of Stats [#50](https://github.com/jet/propulsion/pull/50)

### Changed

- `StreamsSync`: Add `* 'outcome` to `handle` function signature [#50](https://github.com/jet/propulsion/pull/50)
- Update to `3.1.101` SDK
- Retarget `netcoreapp2.1` apps to `netcoreapp3.1` (not least to make tool traverse proxies on Windows)

<a name="2.0.0"></a>
## [2.0.0] - 2020-02-19

### Changed

- Target `FsCodec`.* v `2.0.0`, `Equinox`.* v `2.0.0`

<a name="2.0.0-rc3"></a>
## [2.0.0-rc3] - 2020-02-08

### Changed

- Extend `StreamsProjector` overloads to enable simplified overriding of Write Position

<a name="2.0.0-rc2"></a>
## [2.0.0-rc2] - 2020-02-04

### Fixed

- `Kafka0`: Change `"compression.type"` to `"compression.codec"` to correctly support `Confluent.Kafka` v `0.11.3`

<a name="2.0.0-rc1"></a>
## [2.0.0-rc1] - 2020-01-31

### Changed

- Target `FsCodec`.* v `2.0.0`, `Equinox`.* v `2.0.0-rc9` [#47](https://github.com/jet/propulsion/pull/47)
- Use `FsCodec.StreamName` to replace string names; remove need for `categorize` functions [#47](https://github.com/jet/propulsion/pull/47)
- Updated `EventStore.Checkpoint` to adhere to standard naming/layout (`Folds`->`Fold`)

<a name="1.5.0"></a>
## [1.5.0] - 2019-12-06

### Changed

- `Tool`: Change switches (without arguments) to upper case
- `Kafka`: Targets [`FsKafka` v `1.3.0`](https://github.com/jet/FsKafka/blob/master/CHANGELOG.md#1.3.0)
- `Cosmos`: Targets `Microsoft.Azure.DocumentDB.ChangeFeedProcessor` v `2.2.8`
- `EventStore`: Targets `TypeShape` v `8.0.1`
- `Tool`: Uses `Argu` v `6.0.0`
- Update to `Microsoft.SourceLink.GitHub` v `1.0.0`

<a name="1.4.0"></a>
## [1.4.0] - 2019-11-14

### Changed

- Targets `Equinox` v `2.0.0-rc8`, `FsCodec` v `1.2.1`
- `Cosmos`: Retarget to specify stores as `DocumentClient` [#40](https://github.com/jet/propulsion/pull/40) :pray: [@Kelvin4702](https://github.com/kelvin4702)

<a name="1.3.2"></a>
## [1.3.2] - 2019-11-13

### Fixed

- `StreamKeyEventSequencer`: Handle `null` keys [#43](https://github.com/jet/propulsion/pull/43) :pray: [@nosman](https://github.com/nosman)
- `EventStore.Checkpoint`: Fix to actually write `Checkpointed` events [#44](https://github.com/jet/propulsion/pull/44)

<a name="1.3.1"></a>
## [1.3.1] - 2019-11-12

### Changed

- Add `-g` to `ConsumerGroupName` for consistency with [dotnet-templates#37](https://github.com/jet/dotnet-templates/pull/37)

### Fixed

- EventStore: Handle `minBatchSize` < 128
- EventStore: Inhibit redundant checkpoint writing as originally intended in [`1.0.1`](#1.0.1)

<a name="1.3.0"></a>
## [1.3.0] - 2019-10-17

### Changed

- Targets `Equinox` v `2.0.0-rc7`, `FsCodec` v `1.0.0`
- Updated MinVer to `2.0.0`

<a name="1.2.1"></a>
## [1.2.1] - 2019-10-11

### Added

- `.Kafka.Core.StreamKeyEventSequencer` - helper to synthesize a stream index in the event of a source not providing it intrinsically.

### Changed

- `.Kafka`: Targeted [`Jet.ConfluentKafka.FSharp` v `1.2.0`](https://github.com/jet/Jet.ConfluentKafka.FSharp/blob/master/CHANGELOG.md#1.2.0)
- rename `offsetCommitInterval` to `autoCommitInterval` to match similar change in `Jet.ConfluentKafka.FSharp` v `1.2.0`
- Generalize `Checkpoints.Folds.transmute` to be directly usable [#35](https://github.com/jet/propulsion/pull/35)
- Updated MinVer to `2.0.0-rc.1`

### Fixed

- `NullReferenceException` when handling `null` keys/values in Kafka messages [#37](https://github.com/jet/propulsion/pull/37) :pray: [@jgardella](https://github.com/jgardella)

<a name="1.2.0"></a>
## [1.2.0] - 2019-09-15

### Added

- honored `maxBatchSize` in Kafka Consumers [#31](https://github.com/jet/propulsion/pull/31)
- `offsetCommitInterval` to `Propulsion.Kafka0`'s `KafkaConsumerConfig`
- `maximizeOffsetWriting` to `StreamsConsumer` in order to give maximum effect to `maxBatchSize` limit implemented in [#31](https://github.com/jet/propulsion/pull/31)
- test suite for `StreamsConsumer` [#32](https://github.com/jet/propulsion/pull/32)
- `BatchesConsumer`: support for custom stream based batch scheduling algorithms [#29](https://github.com/jet/propulsion/pull/29)

### Changed

- Use `IIndexedEvent` in lieu of `IIEvent` + `index` in `StreamSpan` and `StreamEvent` [#28](https://github.com/jet/propulsion/pull/28)
- Shorten `Rendered*.parse*` to `Rendered*.parse` [#28](https://github.com/jet/propulsion/pull/28)
- Updated MinVer to `2.0.0-alpha.2`

<a name="1.1.1"></a>
## [1.1.1] - 2019-09-07

### Changed

- Targeted `Equinox`.* v `2.0.0-rc6`, `FsCodec`.* v `1.0.0-rc2` [#27](https://github.com/jet/propulsion/pull/27)

<a name="1.1.0"></a>
## [1.1.0] - 2019-08-30

### Added

- Rebased on `FcCodec` to enable cleaner interop with `Equinox`. Includes removing redundant Converters and helpers [#26](https://github.com/jet/propulsion/pull/26)
- Targeted `Equinox`.* v `2.0.0-rc5` [#26](https://github.com/jet/propulsion/pull/26)

<a name="1.0.1"></a>
## [1.0.1] - 2019-08-26

### Added

- `EventStore`: Switched  `Checkpoints` to correctly only log one event per hour using Equinox `RollingUnfolds`/`transmute` mechanism
- `Kafka`/`Kafka0`: Added `KafkaMonitor` based on [Burrow](https://github.com/linkedin/Burrow) [#12](https://github.com/jet/propulsion/pull/12) :pray: [@jgardella](https://github.com/jgardella) 
- Added overloads, `Codec.RenderedSummary` and `Propulsion.Streams.Sync` to support `dotnet new proSummaryProjector/Consumer` [#23](https://github.com/jet/propulsion/pull/23)

### Changed

- Targeted `Equinox`.* v `2.0.0-rc3` [#22](https://github.com/jet/propulsion/pull/22)
- Targeted `Equinox`.* v `2.0.0-rc4`

<a name="1.0.1-rc9"></a>
## [1.0.1-rc9] - 2019-08-09

### Added

- `Propulsion.Tool`: `initAux` (now `init`) and `project` facilities moved from Equinox [#17](https://github.com/jet/propulsion/pull/17)
- `Propulsion.Tool`: `initAux` and `project` facilities moved from Equinox [#17](https://github.com/jet/propulsion/pull/17)

### Changed

- Targeted `Equinox`.* v `2.0.0-rc2`

### Fixed

- Resolve `Propulsion.Kafka0` conflicts with `Jet.ConfluentKafka.fsharp` v0 [#19](https://github.com/jet/propulsion/pull/19)

<a name="1.0.1-rc8"></a>
## [1.0.1-rc8] - 2019-07-05

### Changed

- `Kafka`/`Kafka0`: Tuned stream producer params

<a name="1.0.1-rc7"></a>
## [1.0.1-rc7] - 2019-07-04

### Added

- `Kafka`: Log `Producer` exceptions
- `Kafka`: `StreamsProducerSink` Enable control of `maxEvents`, `maxBytes`

### Fixed

- `Propulsion`/`Propulsion.Kafka`/`Propulsion.Kafka0`: Removed `IEvent` on `StreamEvent`, `IEnumerable<IEvent>` on `StreamEventSpan`

<a name="1.0.1-rc6"></a>
## [1.0.1-rc6] - 2019-07-03

### Changed

- `Kafka`/`Kafka0`: Rename `Producers` to `Producer`, add deprecation not to [renamed] `degreeOfParallelism` parameter

### Fixed

- `Kafka0` - added missing error check on produce [#14](https://github.com/jet/propulsion/pull/14)

<a name="1.0.1-rc5"></a>
## [1.0.1-rc5] - 2019-07-02

### Added

- `Propulsion`: Implement `IEvent` on `StreamEvent`, `IEnumerable<IEvent>` on `StreamEventSpan`
- `Propulsion.Kafka`: Implemented `IEnumerable<IEvent>` on `RenderedSpan`
- `Propulsion.Kafka`: Added `Parse`, `parseStreamEvents` helpers to `RenderedSpan`

<a name="1.0.1-rc4"></a>
## [1.0.1-rc4] - 2019-07-01

### Changed

- `.Kafka`: Targeted `Jet.ConfluentKafka.FSharp` v `1.1.0`

### Fixed

- Idling logic bug [#13](https://github.com/jet/propulsion/pull/13)
- `EventStoreSource`: Gorging -> Tailing transition [#10](https://github.com/jet/propulsion/issues/10) [#13](https://github.com/jet/propulsion/pull/13)

<a name="1.0.1-rc3"></a>
## [1.0.1-rc3] - 2019-06-18

### Added

- `Core`: Average Streams latency measurements/loggging for `StreamsConsumer` [#3](https://github.com/jet/propulsion/pull/3)
- `Kafka`: `customize` option for `ParallelConsumer` and `StreamsConsumer`'s `.Create` methods [#3](https://github.com/jet/propulsion/pull/3)
- `Kafka`: `producerParallelism` option [#3](https://github.com/jet/propulsion/pull/3)
- `Kafka0`: Provides source-compatibility with `Propulsion.Kafka` targeting `Jet.ConfluentKafka.FSharp` v `0.9.1` / `Confluent.Kafka` v `1.0.1` [#4](https://github.com/jet/propulsion/pull/4)
- `Kafka`/`Kafka0`: `Producers` - Common Kafka producer wrapper with metrics [#9](https://github.com/jet/propulsion/pull/9)
- `Kafka`/`Kafka0`: `StreamsConsumerStats` - Consumer outcome / statistics / logging support [#9](https://github.com/jet/propulsion/pull/9)

### Changed

- `Propulsion.Cosmos`: Tidied Cosmos ingester lag breakdown
- Moved `RenderedSpan` et al to `Propulsion.Codec.NewtonsoftJson` [#5](https://github.com/jet/propulsion/pull/5)
- Targeted `Jet.ConfluentKafka.FSharp` v `1.0.1` [#3](https://github.com/jet/propulsion/pull/3)
- Targeted `Equinox`.* v `2.0.0-rc1` [#7](https://github.com/jet/propulsion/pull/7)

<a name="1.0.1-rc2"></a>
## [1.0.1-rc2] - 2019-06-07

### Added

- `Propulsion.EventStore.EventStoreSource` (productized from `Equinox.Templates`'s `eqxsync`) [#1](https://github.com/jet/propulsion/pull/1)

### Changed

- Targets `Microsoft.Azure.DocumentDB.ChangeFeedProcessor` v `2.2.7`, which includes critical lease management improvements

<a name="1.0.1-rc1"></a>
## [1.0.1-rc1] - 2019-06-03

### Added

- `Propulsion.Kafka.Codec.RenderedSpan` (nee `Equinox.Projection.Codec.RenderedSpan`, which is deprecated and is being removed)
- `Propulsion.EventStore`, `Propulsion.Cosmos` (productized from `Equinox.Templates`'s `eqxsync` and `eqxprojector`)

### Changed

- Targets `Jet.ConfluentKafka.FSharp` v `1.0.1`

<a name="1.0.0-rc13"></a>
## [1.0.0-rc13] - 2019-06-01

### Added

- `StreamsConsumer` and `StreamsProducer` [#35](https://github.com/jet/Jet.ConfluentKafka.FSharp/pull/35)
- `ParallelProducer` [#36](https://github.com/jet/Jet.ConfluentKafka.FSharp/pull/36)

### Changed

- Split reusable components of `ParallelConsumer` out into independent `Propulsion` and `Propulsion.Kafka` libraries [#34](https://github.com/jet/Jet.ConfluentKafka.FSharp/pull/34)

## 1.0.0-rc12 - 2019-05-31

### Fixed

- Significant tuning / throughput improvements for `ParallelConsumer` 

## 1.0.0-rc11 - 2019-05-27

### Added

- `ParallelConsumer` [#33](https://github.com/jet/Jet.ConfluentKafka.FSharp/pull/33)

## squashed prior to initial relevant commit

[Unreleased]: https://github.com/jet/propulsion/compare/2.10.0-rc2...HEAD
[2.10.0-rc2]: https://github.com/jet/propulsion/compare/2.10.0-rc1...2.10.0-rc2
[2.10.0-rc1]: https://github.com/jet/propulsion/compare/2.9.1...2.10.0-rc1
[2.9.1]: https://github.com/jet/propulsion/compare/2.9.0...2.9.1
[2.9.0]: https://github.com/jet/propulsion/compare/2.9.0-rc2...2.9.0
[2.9.0-rc2]: https://github.com/jet/propulsion/compare/2.9.0-rc1...2.9.0-rc2
[2.9.0-rc1]: https://github.com/jet/propulsion/compare/2.8.1...2.9.0-rc1
[2.8.1]: https://github.com/jet/propulsion/compare/2.8.0...2.8.1
[2.8.0]: https://github.com/jet/propulsion/compare/2.7.1...2.8.0
[2.7.1]: https://github.com/jet/propulsion/compare/2.7.0...2.7.1
[2.7.0]: https://github.com/jet/propulsion/compare/2.6.0...2.7.0
[2.6.0]: https://github.com/jet/propulsion/compare/2.5.2...2.6.0
[2.5.2]: https://github.com/jet/propulsion/compare/2.5.1...2.5.2
[2.5.1]: https://github.com/jet/propulsion/compare/2.5.0...2.5.1
[2.5.0]: https://github.com/jet/propulsion/compare/2.4.3...2.5.0
[2.4.3]: https://github.com/jet/propulsion/compare/2.4.2...2.4.3
[2.4.2]: https://github.com/jet/propulsion/compare/2.4.1...2.4.2
[2.4.1]: https://github.com/jet/propulsion/compare/2.4.0...2.4.1
[2.4.0]: https://github.com/jet/propulsion/compare/2.3.0...2.4.0
[2.3.0]: https://github.com/jet/propulsion/compare/2.2.0...2.3.0
[2.2.0]: https://github.com/jet/propulsion/compare/2.1.1...2.2.0
[2.1.1]: https://github.com/jet/propulsion/compare/2.0.0...2.1.1
[2.0.0]: https://github.com/jet/propulsion/compare/2.0.0-rc3...2.0.0
[2.0.0-rc3]: https://github.com/jet/propulsion/compare/2.0.0-rc2...2.0.0-rc3
[2.0.0-rc2]: https://github.com/jet/propulsion/compare/2.0.0-rc1...2.0.0-rc2
[2.0.0-rc1]: https://github.com/jet/propulsion/compare/1.5.0...2.0.0-rc1
[1.5.0]: https://github.com/jet/propulsion/compare/1.4.0...1.5.0
[1.4.0]: https://github.com/jet/propulsion/compare/1.3.2...1.4.0
[1.3.2]: https://github.com/jet/propulsion/compare/1.3.1...1.3.2
[1.3.1]: https://github.com/jet/propulsion/compare/1.3.0...1.3.1
[1.3.0]: https://github.com/jet/propulsion/compare/1.2.1...1.3.0
[1.2.1]: https://github.com/jet/propulsion/compare/1.2.0...1.2.1
[1.2.0]: https://github.com/jet/propulsion/compare/1.1.1...1.2.0
[1.1.1]: https://github.com/jet/propulsion/compare/1.1.0...1.1.1
[1.1.0]: https://github.com/jet/propulsion/compare/1.0.1...1.1.0
[1.0.1]: https://github.com/jet/propulsion/compare/1.0.1-rc9...1.0.1
[1.0.1-rc9]: https://github.com/jet/propulsion/compare/1.0.1-rc8...1.0.1-rc9
[1.0.1-rc8]: https://github.com/jet/propulsion/compare/1.0.1-rc7...1.0.1-rc8
[1.0.1-rc7]: https://github.com/jet/propulsion/compare/1.0.1-rc6...1.0.1-rc7
[1.0.1-rc6]: https://github.com/jet/propulsion/compare/1.0.1-rc5...1.0.1-rc6
[1.0.1-rc5]: https://github.com/jet/propulsion/compare/1.0.1-rc4...1.0.1-rc5
[1.0.1-rc4]: https://github.com/jet/propulsion/compare/1.0.1-rc3...1.0.1-rc4
[1.0.1-rc3]: https://github.com/jet/propulsion/compare/1.0.1-rc2...1.0.1-rc3
[1.0.1-rc2]: https://github.com/jet/propulsion/compare/1.0.1-rc1...1.0.1-rc2
[1.0.1-rc1]: https://github.com/jet/propulsion/compare/1.0.0-rc13...1.0.1-rc1
[1.0.0-rc13]: https://github.com/jet/propulsion/compare/d2caf9a007a137994e91ab709c87eb29fe32489b...1.0.0-rc13

(Stripped down repo for history purposes, see [`master` branch of Jet.ConfluentKafka.FSharp for complete history prior to 1.0.0-rc13](https://github.com/jet/Jet.ConfluentKafka.FSharp/blob/master/CHANGELOG.md))