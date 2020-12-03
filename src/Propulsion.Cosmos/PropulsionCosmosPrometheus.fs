namespace Propulsion.Cosmos.Prometheus

module private Impl =

    let baseName stat = "propulsion_changefeed_" + stat
    let baseDesc desc = "Propulsion CosmosDB: ChangeFeed " + desc
    let groupLabels = [| "app"; "db"; "con"; "group" |]
    let rangeLabels = [| "app"; "db"; "con"; "group"; "rangeId" |]

module private Gauge =

    let private mk (cfg : Prometheus.GaugeConfiguration) name help =
        let g = Prometheus.Metrics.CreateGauge(name, help, cfg)
        fun app (db, con, group, range) v -> g.WithLabels(app, db, con, group, range).Set(v)
    let private config = Prometheus.GaugeConfiguration(LabelNames = Impl.rangeLabels)
    let create stat desc = mk config (Impl.baseName stat) (Impl.baseDesc desc)

module private Counter =

    let private mk (cfg : Prometheus.CounterConfiguration) name desc =
        let c = Prometheus.Metrics.CreateCounter(name, desc, cfg)
        fun app (db, con, group, range) v -> c.WithLabels(app, db, con, group, range).Inc(v)
    let private config = Prometheus.CounterConfiguration(LabelNames = Impl.rangeLabels)
    let create stat desc =
        mk config (Impl.baseName stat) (Impl.baseDesc desc)

module private Summary =

    let private create (cfg : Prometheus.SummaryConfiguration) name desc  =
        let s = Prometheus.Metrics.CreateSummary(name, desc, cfg)
        fun app (db, con, group) v -> s.WithLabels(app, db, con, group).Observe(v)
    let config =
        let inline qep q e = Prometheus.QuantileEpsilonPair(q, e)
        let objectives = [| qep 0.50 0.05; qep 0.95 0.01; qep 0.99 0.01 |]
        Prometheus.SummaryConfiguration(Objectives = objectives, LabelNames = Impl.groupLabels, MaxAge = System.TimeSpan.FromMinutes 1.)
    let latency stat desc = create config (Impl.baseName stat + "_seconds") (Impl.baseDesc desc + " latency")
    let charge stat desc = create config (Impl.baseName stat + "_ru") (Impl.baseDesc desc + " charge")

module private Histogram =

    let private create (cfg : Prometheus.HistogramConfiguration) name desc =
        let h = Prometheus.Metrics.CreateHistogram(name, desc, cfg)
        fun app (db, con, group) v -> h.WithLabels(app, db, con, group).Observe(v)
    // Given we also have summary metrics with equivalent labels, we focus the bucketing on LAN latencies
    let private sHistogram =
        let sBuckets = [| 0.0005; 0.001; 0.002; 0.004; 0.008; 0.016; 0.5; 1.; 2.; 4.; 8.; 16. |]
        let sCfg = Prometheus.HistogramConfiguration(Buckets = sBuckets, LabelNames = Impl.groupLabels)
        create sCfg
    let private ruHistogram =
        let ruBuckets = Prometheus.Histogram.ExponentialBuckets(1., 2., 9) // 1 .. 256
        let ruCfg = Prometheus.HistogramConfiguration(Buckets = ruBuckets, LabelNames = Impl.groupLabels)
        create ruCfg
    let latency stat desc = sHistogram (Impl.baseName stat + "_seconds") (Impl.baseDesc desc + " latency")
    let charge stat desc = ruHistogram (Impl.baseName stat + "_ru") (Impl.baseDesc desc + " charge")

open Propulsion.Cosmos.Log

module private Stats =

    (* Group level metrics *)

    let observeReadLatencyHis = Histogram.latency    "read"            "Read"
    let observeReadLatencySum = Summary.latency      "read_summary"    "Read"
    let observeReadChargeHis =  Histogram.charge     "read"            "Read"
    let observeReadChargeSum =  Summary.charge       "read_summary"    "Read"
    let observeIngestLatHis =   Histogram.latency    "ingest"          "Ingest"
    let observeIngestLatSum =   Summary.latency      "ingest_summary"  "Ingest"

    (* Group+Range level metrics *)

    let observeRangeToken =     Gauge.create         "position_token"  "Feed Token of most recent Batch observed" // read
    let observeRangeAge =       Gauge.create         "age_seconds"     "Age of most recent Batch observed" // read
    let observeRangeLag =       Gauge.create         "lag_documents"   "Unread documents queued for this Range" // read
    let observeIngestQueue =    Gauge.create         "ingest_queue"    "Ingest queue length"
    let observeRangeDocCount =  Counter.create       "documents_total" "Observed document count" // read
    let observeRangeRu =        Counter.create       "ru_total"        "Observed batch read Request Charges" // read

    let observeRead app (m : ReadMetric) =
        let range = m.database, m.container, m.group, string m.rangeId
        let ageS, latS, readDocs, token = m.age.TotalSeconds, m.latency.TotalSeconds, float m.docs, float m.token
        observeRangeToken app range token
        observeRangeAge app range ageS
        observeRangeDocCount app range readDocs
        observeRangeRu app range m.rc
        let group = m.database, m.container, m.group
        observeReadLatencyHis app group latS
        observeReadLatencySum app group latS
        observeReadChargeHis app group m.rc
        observeReadChargeSum app group m.rc
        let ingestLatS, ingestQueueLen = m.ingestLatency.TotalSeconds, float m.ingestQueued
        observeIngestLatHis app group ingestLatS
        observeIngestLatSum app group ingestLatS
        observeIngestQueue app range ingestQueueLen

    let observeLag app (m : LagMetric) =
        for rangeId, lag in m.rangeLags do
            let range = m.database, m.container, m.group, string rangeId
            observeRangeLag app range (float lag)

type LogSink(app) =
    interface Serilog.Core.ILogEventSink with
        member __.Emit logEvent = logEvent |> function
            | MetricEvent cm -> cm |> function
                | Metric.Read m -> Stats.observeRead app m
                | Metric.Lag m -> Stats.observeLag app m
            | _ -> ()
