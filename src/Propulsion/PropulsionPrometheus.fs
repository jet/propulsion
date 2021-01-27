namespace Propulsion.Prometheus

module private Impl =

    let baseName stat = "propulsion_scheduler_" + stat
    let baseDesc desc = "Propulsion Scheduler " + desc
    let groupLabels = [| "app"; "group"; "state" |]
    let activityLabels = [| "app"; "group"; "activity" |]
    let latencyLabels = [| "app"; "group"; "kind" |]

module private Gauge =

    let private mk (cfg : Prometheus.GaugeConfiguration) name help =
        let g = Prometheus.Metrics.CreateGauge(name, help, cfg)
        fun app group state v -> g.WithLabels(app, group, state).Set(v)
    let private config = Prometheus.GaugeConfiguration(LabelNames = Impl.groupLabels)
    let create stat desc = mk config (Impl.baseName stat) (Impl.baseDesc desc)

module private Counter =

    let private mk (cfg : Prometheus.CounterConfiguration) name desc =
        let c = Prometheus.Metrics.CreateCounter(name, desc, cfg)
        fun app group activity v -> c.WithLabels(app, group, activity).Inc(v)
    let private config = Prometheus.CounterConfiguration(LabelNames = Impl.activityLabels)
    let create stat desc =
        mk config (Impl.baseName stat) (Impl.baseDesc desc)

module private Summary =

    let private create (cfg : Prometheus.SummaryConfiguration) name desc  =
        let s = Prometheus.Metrics.CreateSummary(name, desc, cfg)
        fun app (group, kind) v -> s.WithLabels(app, group, kind).Observe(v)
    let config =
        let inline qep q e = Prometheus.QuantileEpsilonPair(q, e)
        let objectives = [| qep 0.50 0.05; qep 0.95 0.01; qep 0.99 0.01 |]
        Prometheus.SummaryConfiguration(Objectives = objectives, LabelNames = Impl.latencyLabels, MaxAge = System.TimeSpan.FromMinutes 1.)
    let latency stat desc = create config (Impl.baseName stat + "_seconds") (Impl.baseDesc desc + " latency")

module private Histogram =

    let private create (cfg : Prometheus.HistogramConfiguration) name desc =
        let h = Prometheus.Metrics.CreateHistogram(name, desc, cfg)
        fun app (group, kind) v -> h.WithLabels(app, group, kind).Observe(v)
    let private sHistogram =
        let sBuckets = Prometheus.Histogram.ExponentialBuckets(0.001, 2., 16) // 1ms .. 64s
        let sCfg = Prometheus.HistogramConfiguration(Buckets = sBuckets, LabelNames = Impl.latencyLabels)
        create sCfg
    let latency stat desc = sHistogram (Impl.baseName stat + "_seconds") (Impl.baseDesc desc + " latency")

open Propulsion.Streams.Log

module private Stats =

    let observeCats =    Gauge.create      "cats"            "Current categories"
    let observeStreams = Gauge.create      "streams"         "Current streams"
    let observeEvents =  Gauge.create      "events"          "Current events"
    let observeBytes =   Gauge.create      "bytes"           "Current bytes"

    let observeCpu =     Counter.create    "cpu"             "Processing Time Breakdown"

    let observeLatSum =  Summary.latency   "handler_summary" "Handler action"
    let observeLatHis =  Histogram.latency "handler"         "Handler action"

    let observeState app ctx state (m : BufferMetric) =
        observeCats app ctx state (float m.cats)
        observeStreams app ctx state (float m.streams)
        observeEvents app ctx state (float m.events)
        observeBytes app ctx state (float m.bytes)

type LogSink(app, group) =

    let observeState = Stats.observeState app group
    let observeCpu = Stats.observeCpu app group
    let observeLatency kind latenciesS =
        for v in latenciesS do
            Stats.observeLatSum app (group, kind) v
            Stats.observeLatHis app (group, kind) v

    interface Serilog.Core.ILogEventSink with
        member __.Emit logEvent = logEvent |> function
            | MetricEvent cm -> cm |> function
                | Metric.BufferReport m ->
                    observeState "ingesting" m
                | Metric.SchedulerStateReport (synced, busyStats, readyStats, bufferingStats, malformedStats) ->
                    Stats.observeStreams app group "synced" (float synced)
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
