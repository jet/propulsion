# Changelog

The repo is versioned based on [SemVer 2.0](https://semver.org/spec/v2.0.0.html) using the tiny-but-mighty [MinVer](https://github.com/adamralph/minver) from [@adamralph](https://github.com/adamralph). [See here](https://github.com/adamralph/minver#how-it-works) for more information on how it works.

All notable changes to this project will be documented in this file. The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

The `Unreleased` section name is replaced by the expected version of next release. A stable version's log contains all changes between that version and the previous stable version (can duplicate the prereleases logs).

## [Unreleased]

### Added

- Average Streams latency measurements/loggging for `StreamsConsumer`
- `customize` option for `ParallelConsumer` and `StreamsConsumer`'s `.Create` methods

### Changed

- Tidied Cosmos ingester lag breakdown

### Removed
### Fixed

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

(Stripped down repo for history purposes, see [`master` branch of Jet.ConfluentKafka.FSharp for complete history prior to 1.0.0-rc13](https://github.com/jet/Jet.ConfluentKafka.FSharp/blob/master/CHANGELOG.md))

[Unreleased]: https://github.com/jet/propulsion/compare/1.0.1-rc2...HEAD
[1.0.1-rc2]: https://github.com/jet/propulsion/compare/1.0.1-rc1...1.0.1-rc2
[1.0.1-rc1]: https://github.com/jet/propulsion/compare/1.0.0-rc13...1.0.1-rc1
[1.0.0-rc13]: https://github.com/jet/propulsion/compare/d2caf9a007a137994e91ab709c87eb29fe32489b...1.0.0-rc13