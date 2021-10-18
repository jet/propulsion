// This file implements a Serilog Sink `LogSink` that publishes metric values to Prometheus.
namespace Propulsion.Feed.Prometheus

open Propulsion.Feed

[<AutoOpen>]
module private Impl =

    let baseName stat = "propulsion_feed_" + stat
    let baseDesc desc = "Propulsion Feed: Source " + desc
    let trancheLabels = [| "source"; "tranche" |]
    let [<Literal>] secondsStat = "_seconds"
    let [<Literal>] latencyDesc = " latency"

module private Gauge =

    let private make (config : Prometheus.GaugeConfiguration) name desc =
        let gauge = Prometheus.Metrics.CreateGauge(name, desc, config)
        fun tagValues (source, tranche) value ->
            let labelValues = Array.append tagValues [| SourceId.toString source; TrancheId.toString tranche |]
            gauge.WithLabels(labelValues).Set(value)

    let create (tagNames, tagValues) stat desc =
        let config = Prometheus.GaugeConfiguration(LabelNames = Array.append tagNames trancheLabels)
        make config (baseName stat) (baseDesc desc) tagValues

module private Counter =

    let private make (config : Prometheus.CounterConfiguration) name desc =
        let ctr = Prometheus.Metrics.CreateCounter(name, desc, config)
        fun tagValues (source, tranche) value ->
            let labelValues = Array.append tagValues [| SourceId.toString source; TrancheId.toString tranche |]
            ctr.WithLabels(labelValues).Inc(value)

    let create (tagNames, tagValues) stat desc =
        let config = Prometheus.CounterConfiguration(LabelNames = Array.append tagNames trancheLabels)
        make config (baseName stat) (baseDesc desc) tagValues

module private Summary =

    let private create (config : Prometheus.SummaryConfiguration) name desc =
        let summary = Prometheus.Metrics.CreateSummary(name, desc, config)
        fun tagValues (source, tranche) value ->
            let labelValues = Array.append tagValues [| SourceId.toString source; TrancheId.toString tranche |]
            summary.WithLabels(labelValues).Observe(value)

    let private objectives =
        [|
            0.50, 0.05 // Between 45th and 55th percentile
            0.95, 0.01 // Between 94th and 96th percentile
            0.99, 0.01 // Between 100th and 98th percentile
        |] |> Array.map Prometheus.QuantileEpsilonPair

    let private create' statSuffix descSuffix (tagNames, tagValues) stat desc =
        let config =
            Prometheus.SummaryConfiguration(Objectives = objectives, LabelNames = Array.append tagNames trancheLabels, MaxAge = System.TimeSpan.FromMinutes 1.)

        create config (baseName stat + statSuffix) (baseDesc desc + descSuffix) tagValues

    let latency = create' secondsStat latencyDesc

module private Histogram =

    let private create (config : Prometheus.HistogramConfiguration) name desc =
        let histogram = Prometheus.Metrics.CreateHistogram(name, desc, config)
        fun tagValues (source, tranche) value ->
            let labelValues = Array.append tagValues [| SourceId.toString source; TrancheId.toString tranche |]
            histogram.WithLabels(labelValues).Observe(value)

    let private create' buckets statSuffix descSuffix (tagNames, tagValues) stat desc =
        let config = Prometheus.HistogramConfiguration(Buckets = buckets, LabelNames = Array.append tagNames trancheLabels)
        create config (baseName stat + statSuffix) (baseDesc desc + descSuffix) tagValues

    let latencyBuckets = [| 0.0005; 0.001; 0.002; 0.004; 0.008; 0.016; 0.5; 1.; 2.; 4.; 8.; 16. |]
    let latency = create' latencyBuckets secondsStat latencyDesc

open Propulsion.Feed.Internal.Log

/// <summary>An ILogEventSink that publishes to Prometheus</summary>
/// <param name="customTags">Custom tags to annotate the metric we're publishing where such tag manipulation cannot better be achieved via the Prometheus scraper config.</param>
type LogSink(customTags: seq<string * string>) =

    let tags = Array.ofSeq customTags |> Array.unzip

    let observeReadLatencyHis = Histogram.latency   tags "read"             "Read"
    let observeReadLatencySum = Summary.latency     tags "read_summary"     "Read"
    let observeIngestLatHis =   Histogram.latency   tags "ingest"           "Ingest"
    let observeIngestLatSum =   Summary.latency     tags "ingest_summary"   "Ingest"
    let observeToken =          Gauge.create        tags "position_token"   "Feed Token of most recent Page observed"
    let observeIngestQueue =    Gauge.create        tags "ingest_queue"     "Ingest queue length"
    let observePageCount =      Counter.create      tags "pages_total"      "Observed page count"
    let observeItemCount =      Counter.create      tags "items_total"      "Observed item count"

    let observeRead (m : ReadMetric) =
        let group = m.source, m.tranche
        let latS, readPages, readItems = m.latency.TotalSeconds, float m.pages, float m.items
        if m.token.HasValue then observeToken group (float m.token.Value)
        observePageCount group readPages
        observeItemCount group readItems
        observeReadLatencyHis group latS
        observeReadLatencySum group latS
        let ingestLatS, ingestQueueLen = m.ingestLatency.TotalSeconds, float m.ingestQueued
        observeIngestLatHis group ingestLatS
        observeIngestLatSum group ingestLatS
        observeIngestQueue group ingestQueueLen

    interface Serilog.Core.ILogEventSink with
        member _.Emit logEvent = logEvent |> function
            | MetricEvent cm -> cm |> function
                | Metric.Read m -> observeRead m
            | _ -> ()
