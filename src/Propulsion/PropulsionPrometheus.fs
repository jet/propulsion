// This file implements a Serilog Sink `LogSink` that publishes metric values to Prometheus.
namespace Propulsion.Prometheus

[<AutoOpen>]
module private Impl =

    let baseName stat = "propulsion_scheduler_" + stat
    let baseDesc desc = "Propulsion Scheduler " + desc
    let groupLabels = [| "group"; "state" |]
    let activityLabels = [| "group"; "activity" |]
    let latencyLabels = [| "group"; "kind" |]

    let [<Literal>] secondsStat = "_seconds"
    let [<Literal>] latencyDesc = " latency"

    let append = Array.append

module private Gauge =

    let private make (config : Prometheus.GaugeConfiguration) name desc =
        let gauge = Prometheus.Metrics.CreateGauge(name, desc, config)
        fun tagValues group state value ->
            let labelValues = append tagValues [| group; state |]
            gauge.WithLabels(labelValues).Set(value)

    let create (tagNames, tagValues) stat desc =
        let config = Prometheus.GaugeConfiguration(LabelNames = append tagNames groupLabels)
        make config (baseName stat) (baseDesc desc) tagValues

module private Counter =

    let private make (config : Prometheus.CounterConfiguration) name desc =
        let counter = Prometheus.Metrics.CreateCounter(name, desc, config)
        fun tagValues group activity value ->
            let labelValues = append tagValues [| group; activity |]
            counter.WithLabels(labelValues).Inc(value)

    let create (tagNames, tagValues) stat desc =
        let config = Prometheus.CounterConfiguration(LabelNames = append tagNames activityLabels)
        make config (baseName stat) (baseDesc desc) tagValues

module private Summary =

    let private create (config : Prometheus.SummaryConfiguration) name desc  =
        let summary = Prometheus.Metrics.CreateSummary(name, desc, config)
        fun tagValues (group, kind) value ->
            let labelValues = append tagValues [| group; kind |]
            summary.WithLabels(labelValues).Observe(value)

    let private objectives =
           [|
               0.50, 0.05 // Between 45th and 55th percentile
               0.95, 0.01 // Between 94th and 96th percentile
               0.99, 0.01 // Between 100th and 98th percentile
           |] |> Array.map Prometheus.QuantileEpsilonPair

    let latency (tagNames, tagValues) stat desc =
        let config =
            let labelValues = append tagNames latencyLabels
            Prometheus.SummaryConfiguration(Objectives = objectives, LabelNames = labelValues, MaxAge = System.TimeSpan.FromMinutes 1.)
        create config (baseName stat + secondsStat) (baseDesc desc + latencyDesc) tagValues

module private Histogram =

    let private create (config : Prometheus.HistogramConfiguration) name desc =
        let histogram = Prometheus.Metrics.CreateHistogram(name, desc, config)
        fun tagValues (group, kind) value ->
            let labelValues = append tagValues [| group; kind |]
            histogram.WithLabels(labelValues).Observe(value)

    let private sBuckets =
        Prometheus.Histogram.ExponentialBuckets(0.001, 2., 16) // 1ms .. 64s

    let latency (tagNames, tagValues) stat desc =
        let config = Prometheus.HistogramConfiguration(Buckets = sBuckets, LabelNames = append tagNames latencyLabels)
        create config (baseName stat + secondsStat) (baseDesc desc + latencyDesc) tagValues

open Propulsion.Streams.Log

/// <summary>An ILogEventSink that publishes to Prometheus</summary>
/// <param name="customTags">Custom tags to annotate the metric we're publishing where such tag manipulation cannot better be achieved via the Prometheus scraper config.</param>
/// <param name="group">ChangeFeedProcessor <c>processorName</c>.</param>
type LogSink(customTags: seq<string * string>, group: string) =

    let tags = Array.ofSeq customTags |> Array.unzip

    let observeCats =    Gauge.create      tags "cats"            "Current categories"
    let observeStreams = Gauge.create      tags "streams"         "Current streams"
    let observeEvents =  Gauge.create      tags "events"          "Current events"
    let observeBytes =   Gauge.create      tags "bytes"           "Current bytes"

    let observeCpu =     Counter.create    tags "cpu"             "Processing Time Breakdown"

    let observeLatSum =  Summary.latency   tags "handler_summary" "Handler action"
    let observeLatHis =  Histogram.latency tags "handler"         "Handler action"

    let observeState ctx state (m : BufferMetric) =
        observeCats ctx state (float m.cats)
        observeStreams ctx state (float m.streams)
        observeEvents ctx state (float m.events)
        observeBytes ctx state (float m.bytes)

    let observeState = observeState group
    let observeCpu = observeCpu group
    let observeLatency kind latenciesS =
        for v in latenciesS do
           observeLatSum (group, kind) v
           observeLatHis (group, kind) v

    interface Serilog.Core.ILogEventSink with
        member _.Emit logEvent = logEvent |> function
            | MetricEvent e -> e |> function
                | Metric.BufferReport m ->
                    observeState "ingesting" m
                | Metric.SchedulerStateReport (synced, busyStats, readyStats, bufferingStats, malformedStats) ->
                    observeStreams group "synced" (float synced)
                    observeState "active" busyStats
                    observeState "ready" readyStats
                    observeState "buffering" bufferingStats
                    observeState "malformed" malformedStats
                | Metric.SchedulerCpu (merge, ingest, dispatch, results, stats) ->
                    observeCpu "merge" merge.TotalSeconds
                    observeCpu "ingest" ingest.TotalSeconds
                    observeCpu "dispatch" dispatch.TotalSeconds
                    observeCpu "results" results.TotalSeconds
                    observeCpu "stats" stats.TotalSeconds
                | Metric.AttemptLatencies (kind, latenciesS) ->
                    observeLatency kind latenciesS
            | _ -> ()
