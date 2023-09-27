// This file implements a Serilog Sink `LogSink` that publishes metric values to Prometheus.

namespace Propulsion.CosmosStore.Prometheus

[<AutoOpen>]
module private Impl =

    let baseName stat = "propulsion_changefeed_" + stat
    let baseDesc desc = "Propulsion CosmosDB: ChangeFeed " + desc
    let groupLabels = [| "db"; "con"; "group" |]
    let rangeLabels = [| "db"; "con"; "group"; "rangeId" |]
    let [<Literal>] secondsStat = "_seconds"
    let [<Literal>] rusStat = "_rus"
    let [<Literal>] latencyDesc = " latency"
    let [<Literal>] chargeDesc = " charge"

    let append = Array.append

module private Gauge =

    let private make (config: Prometheus.GaugeConfiguration) name desc =
        let gauge = Prometheus.Metrics.CreateGauge(name, desc, config)
        fun tagValues (db, con, group, range) value ->
            let labelValues = append tagValues [| db; con; group; range |]
            gauge.WithLabels(labelValues).Set(value)

    let create (tagNames, tagValues) stat desc =
        let config = Prometheus.GaugeConfiguration(LabelNames = append tagNames rangeLabels)
        make config (baseName stat) (baseDesc desc) tagValues

module private Counter =

    let private make (config: Prometheus.CounterConfiguration) name desc =
        let ctr = Prometheus.Metrics.CreateCounter(name, desc, config)
        fun tagValues (db, con, group, range) value ->
            let labelValues = append tagValues [| db; con; group; range |]
            ctr.WithLabels(labelValues).Inc(value)

    let create (tagNames, tagValues) stat desc =
        let config = Prometheus.CounterConfiguration(LabelNames = append tagNames rangeLabels)
        make config (baseName stat) (baseDesc desc) tagValues

module private Summary =

    let private create (config: Prometheus.SummaryConfiguration) name desc =
        let summary = Prometheus.Metrics.CreateSummary(name, desc, config)
        fun tagValues (db, con, group) value ->
            let labelValues = append tagValues [| db; con; group; |]
            summary.WithLabels(labelValues).Observe(value)

    let private objectives =
        [|
            0.50, 0.05 // Between 45th and 55th percentile
            0.95, 0.01 // Between 94th and 96th percentile
            0.99, 0.01 // Between 100th and 98th percentile
        |] |> Array.map Prometheus.QuantileEpsilonPair

    let private create' statSuffix descSuffix (tagNames, tagValues) stat desc =
        let config =
            Prometheus.SummaryConfiguration(Objectives = objectives, LabelNames = append tagNames groupLabels, MaxAge = System.TimeSpan.FromMinutes 1.)

        create config (baseName stat + statSuffix) (baseDesc desc + descSuffix) tagValues


    let latency = create' secondsStat latencyDesc
    let charge = create' rusStat chargeDesc

module private Histogram =

    let private create (config: Prometheus.HistogramConfiguration) name desc =
        let histogram = Prometheus.Metrics.CreateHistogram(name, desc, config)
        fun tagValues (db, con, group) value ->
            let labelValues = append tagValues [| db; con; group; |]
            histogram.WithLabels(labelValues).Observe(value)

    let private create' buckets statSuffix descSuffix (tagNames, tagValues) stat desc =
        let config = Prometheus.HistogramConfiguration(Buckets = buckets, LabelNames = append tagNames groupLabels)
        create config (baseName stat + statSuffix) (baseDesc desc + descSuffix) tagValues

    let latencyBuckets = [| 0.0005; 0.001; 0.002; 0.004; 0.008; 0.016; 0.5; 1.; 2.; 4.; 8.; 16. |]
    let ruBuckets = Prometheus.Histogram.ExponentialBuckets(1., 2., 9)

    let latency = create' latencyBuckets secondsStat latencyDesc
    let charge = create' ruBuckets rusStat chargeDesc

open Propulsion.CosmosStore.Log

/// <summary>An ILogEventSink that publishes to Prometheus</summary>
/// <param name="customTags">Custom tags to annotate the metric we're publishing where such tag manipulation cannot better be achieved via the Prometheus scraper config.</param>
type LogSink(customTags: seq<string * string>) =

    let tags = Array.ofSeq customTags |> Array.unzip

    (* Group level metrics *)

    let observeReadLatencyHis = Histogram.latency   tags "read"            "Read"
    let observeReadLatencySum = Summary.latency     tags "read_summary"    "Read"
    let observeReadChargeHis =  Histogram.charge    tags "read"            "Read"
    let observeReadChargeSum =  Summary.charge      tags "read_summary"    "Read"
    let observeIngestLatHis =   Histogram.latency   tags "ingest"          "Ingest"
    let observeIngestLatSum =   Summary.latency     tags "ingest_summary"  "Ingest"

    (* Group+Range level metrics *)

    let observeRangeToken =     Gauge.create        tags "position_token"  "Feed Token of most recent Batch observed" // read
    let observeRangeAge =       Gauge.create        tags "age_seconds"     "Age of most recent Batch observed" // read
    let observeRangeLag =       Gauge.create        tags "lag_documents"   "Unread documents queued for this Range" // read
    let observeIngestQueue =    Gauge.create        tags "ingest_queue"    "Ingest queue length"
    let observeRangeDocCount =  Counter.create      tags "documents_total" "Observed document count" // read
    let observeRangeRu =        Counter.create      tags "ru_total"        "Observed batch read Request Charges" // read

    let observeRead (m: ReadMetric) =
        let range = m.database, m.container, m.group, string m.rangeId
        let ageS, latS, readDocs, token = m.age.TotalSeconds, m.latency.TotalSeconds, float m.docs, float m.token
        observeRangeToken range token
        observeRangeAge range ageS
        observeRangeDocCount range readDocs
        observeRangeRu range m.rc
        let group = m.database, m.container, m.group
        observeReadLatencyHis group latS
        observeReadLatencySum group latS
        observeReadChargeHis group m.rc
        observeReadChargeSum group m.rc
        let ingestLatS, ingestQueueLen = m.ingestLatency.TotalSeconds, float m.ingestQueued
        observeIngestLatHis group ingestLatS
        observeIngestLatSum group ingestLatS
        observeIngestQueue range ingestQueueLen

    let observeLag (m: LagMetric) =
        for rangeId, lag in m.rangeLags do
            let range = m.database, m.container, m.group, string rangeId
            observeRangeLag range (float lag)

    interface Serilog.Core.ILogEventSink with
        member _.Emit logEvent = logEvent |> function
            | MetricEvent cm -> cm |> function
                | Metric.Read m -> observeRead m
                | Metric.Lag m -> observeLag m
            | _ -> ()
