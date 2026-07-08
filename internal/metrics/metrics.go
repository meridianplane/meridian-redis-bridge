// Package metrics provides Prometheus metrics, health checks, and pprof.
package metrics

import (
	"net/http"
	"net/http/pprof"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Metrics struct {
	RespConnections      prometheus.Gauge
	RespConnectionsTotal prometheus.Counter
	RespDispatches       *prometheus.CounterVec
	RespDispatchLatency  *prometheus.HistogramVec
	RespErrors           *prometheus.CounterVec

	WALAppendTotal   prometheus.Counter
	WALAppendLatency prometheus.Histogram
	WALCurrentSeq    prometheus.Gauge
	WALSegmentCount  prometheus.Gauge
	WALReadBatches   prometheus.Counter
	WALReadEntries   prometheus.Counter

	ReplicationSubscribers    prometheus.Gauge
	ReplicationStreamBatches  prometheus.Counter
	ReplicationStreamEntries  prometheus.Counter
	ReplicationForwards       prometheus.Counter
	ReplicationAcks           prometheus.Counter
	ReplicationLagHighest     prometheus.Gauge

	FollowerLag            prometheus.Gauge
	FollowerEntriesApplied prometheus.Counter
	FollowerBatchesApplied prometheus.Counter
	FollowerReconnects     prometheus.Counter

	BackendRequests      prometheus.Counter
	BackendErrors        prometheus.Counter
	BackendLatency       prometheus.Histogram
	BackendHealthy       prometheus.Gauge

	ForwardRequests prometheus.Counter
	ForwardErrors   prometheus.Counter
	ForwardLatency  prometheus.Histogram

	FullSyncInProgress prometheus.Gauge
	FullSyncTotal      prometheus.Counter
	FullSyncDuration   prometheus.Histogram

	Info *prometheus.GaugeVec

	FollowerLastAckTime *prometheus.GaugeVec
	WALOldestSegmentAge prometheus.Gauge
}

func New(cluster, node string) *Metrics {
	reg := prometheus.WrapRegistererWith(prometheus.Labels{
		"meridian_cluster": cluster,
		"meridian_node":    node,
	}, prometheus.DefaultRegisterer)

	m := &Metrics{}

	m.RespConnections = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "meridian_resp_connections", Help: "Active RESP client connections.",
	})
	m.RespConnectionsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "meridian_resp_connections_total", Help: "Total RESP connections accepted.",
	})
	m.RespDispatches = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "meridian_resp_dispatches_total", Help: "Commands dispatched by route.",
	}, []string{"route"})
	m.RespDispatchLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "meridian_resp_dispatch_seconds", Help: "Dispatch latency by route.",
		Buckets: prometheus.DefBuckets,
	}, []string{"route"})
	m.RespErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "meridian_resp_errors_total", Help: "RESP-layer errors by kind.",
	}, []string{"kind"})

	m.WALAppendTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "meridian_wal_append_total", Help: "Total WAL entries appended.",
	})
	m.WALAppendLatency = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "meridian_wal_append_seconds", Help: "WAL append latency.",
		Buckets: prometheus.ExponentialBuckets(0.0001, 2, 12),
	})
	m.WALCurrentSeq = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "meridian_wal_current_seq", Help: "Current WAL sequence number.",
	})
	m.WALSegmentCount = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "meridian_wal_segment_count", Help: "Number of WAL segments.",
	})
	m.WALReadBatches = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "meridian_wal_read_batches_total", Help: "WAL read batches served.",
	})
	m.WALReadEntries = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "meridian_wal_read_entries_total", Help: "WAL entries served to subscribers.",
	})

	m.ReplicationSubscribers = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "meridian_replication_subscribers", Help: "Active gRPC subscribers.",
	})
	m.ReplicationStreamBatches = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "meridian_replication_stream_batches_total", Help: "SyncBatch messages sent.",
	})
	m.ReplicationStreamEntries = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "meridian_replication_stream_entries_total", Help: "WAL entries streamed.",
	})
	m.ReplicationForwards = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "meridian_replication_forwards_total", Help: "Forward gRPC requests received.",
	})
	m.ReplicationAcks = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "meridian_replication_acks_total", Help: "Ack gRPC requests received.",
	})
	m.ReplicationLagHighest = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "meridian_replication_lag_highest", Help: "Highest replication lag across followers.",
	})

	m.Info = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "meridian_info",
		Help: "Instance information (always 1).",
	}, []string{"cluster", "name", "role"})

	m.FullSyncInProgress = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "meridian_full_sync_in_progress", Help: "Full syncs currently running.",
	})
	m.FullSyncTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "meridian_full_sync_total", Help: "Total full syncs completed.",
	})
	m.FullSyncDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "meridian_full_sync_duration_seconds", Help: "Full sync completion duration.",
		Buckets: prometheus.ExponentialBuckets(1, 2, 12),
	})

	m.FollowerLag = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "meridian_follower_lag", Help: "Follower replication lag.",
	})
	m.FollowerEntriesApplied = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "meridian_follower_entries_applied_total", Help: "WAL entries applied by follower.",
	})
	m.FollowerBatchesApplied = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "meridian_follower_batches_applied_total", Help: "Batches applied by follower.",
	})
	m.FollowerReconnects = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "meridian_follower_reconnects_total", Help: "Follower reconnect attempts.",
	})
	m.FollowerLastAckTime = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "meridian_follower_last_ack_timestamp_seconds", Help: "Last ack timestamp per follower.",
	}, []string{"follower_id"})

	m.WALOldestSegmentAge = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "meridian_wal_oldest_segment_age_seconds", Help: "Age of the oldest WAL segment.",
	})

	m.BackendRequests = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "meridian_backend_requests_total", Help: "Backend Redis requests.",
	})
	m.BackendErrors = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "meridian_backend_errors_total", Help: "Backend Redis errors.",
	})
	m.BackendLatency = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "meridian_backend_seconds", Help: "Backend Redis request latency.",
		Buckets: prometheus.DefBuckets,
	})
	m.BackendHealthy = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "meridian_backend_healthy", Help: "Backend Redis reachability.",
	})

	m.ForwardRequests = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "meridian_forward_requests_total", Help: "Write-forward requests sent.",
	})
	m.ForwardErrors = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "meridian_forward_errors_total", Help: "Write-forward request errors.",
	})
	m.ForwardLatency = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "meridian_forward_seconds", Help: "Write-forward latency.",
		Buckets: prometheus.DefBuckets,
	})

	for _, c := range []prometheus.Collector{
		m.Info,
		m.RespConnections, m.RespConnectionsTotal, m.RespDispatches, m.RespDispatchLatency, m.RespErrors,
		m.WALAppendTotal, m.WALAppendLatency, m.WALCurrentSeq, m.WALSegmentCount,
		m.WALReadBatches, m.WALReadEntries, m.WALOldestSegmentAge,
		m.ReplicationSubscribers, m.ReplicationStreamBatches, m.ReplicationStreamEntries,
		m.ReplicationForwards, m.ReplicationAcks, m.ReplicationLagHighest,
		m.FullSyncInProgress, m.FullSyncTotal, m.FullSyncDuration,
		m.FollowerLag, m.FollowerEntriesApplied, m.FollowerBatchesApplied, m.FollowerReconnects,
		m.FollowerLastAckTime,
		m.BackendRequests, m.BackendErrors, m.BackendLatency, m.BackendHealthy,
		m.ForwardRequests, m.ForwardErrors, m.ForwardLatency,
	} {
		reg.MustRegister(c)
	}
	reg.MustRegister(collectors.NewGoCollector())
	reg.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	reg.MustRegister(collectors.NewBuildInfoCollector())
	return m
}

func HTTPHandler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ready"))
	})
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	return mux
}


// --- Recorder methods (nil-safe) ---

func (m *Metrics) RecordWALAppend(seq uint64, latency time.Duration) {
	if m == nil { return }
	m.WALAppendTotal.Inc()
	m.WALAppendLatency.Observe(latency.Seconds())
	m.WALCurrentSeq.Set(float64(seq))
}

func (m *Metrics) RecordWALRead(batches, entries int) {
	if m == nil { return }
	m.WALReadBatches.Add(float64(batches))
	m.WALReadEntries.Add(float64(entries))
}

func (m *Metrics) RecordBackend(latency time.Duration, isError bool) {
	if m == nil { return }
	m.BackendRequests.Inc()
	m.BackendLatency.Observe(latency.Seconds())
	if isError { m.BackendErrors.Inc() }
}

func (m *Metrics) RecordDispatch(route string, latency time.Duration) {
	if m == nil { return }
	m.RespDispatches.WithLabelValues(route).Inc()
	m.RespDispatchLatency.WithLabelValues(route).Observe(latency.Seconds())
}

func (m *Metrics) RecordForward(latency time.Duration, isError bool) {
	if m == nil { return }
	m.ForwardRequests.Inc()
	m.ForwardLatency.Observe(latency.Seconds())
	if isError { m.ForwardErrors.Inc() }
}

func (m *Metrics) RecordSubscribeStart() {
	if m == nil { return }
	m.ReplicationSubscribers.Inc()
}

func (m *Metrics) RecordSubscribeEnd() {
	if m == nil { return }
	m.ReplicationSubscribers.Dec()
}

func (m *Metrics) RecordStreamBatch(entries int) {
	if m == nil { return }
	m.ReplicationStreamBatches.Inc()
	m.ReplicationStreamEntries.Add(float64(entries))
}

func (m *Metrics) RecordStreamForward() {
	if m == nil { return }
	m.ReplicationForwards.Inc()
}

func (m *Metrics) RecordAck(lag uint64) {
	if m == nil { return }
	m.ReplicationAcks.Inc()
	m.ReplicationLagHighest.Set(float64(lag))
}

func (m *Metrics) RecordFollowerApplied(batches, entries int) {
	if m == nil { return }
	m.FollowerBatchesApplied.Add(float64(batches))
	m.FollowerEntriesApplied.Add(float64(entries))
}

func (m *Metrics) RecordFollowerReconnect() {
	if m == nil { return }
	m.FollowerReconnects.Inc()
}

func (m *Metrics) RecordRespOpen() {
	if m == nil { return }
	m.RespConnections.Inc()
	m.RespConnectionsTotal.Inc()
}

func (m *Metrics) RecordRespClose() {
	if m == nil { return }
	m.RespConnections.Dec()
}

func (m *Metrics) RecordFullSyncStart() {
	if m == nil { return }
	m.FullSyncInProgress.Inc()
}

func (m *Metrics) RecordFullSyncEnd(dur time.Duration) {
	if m == nil { return }
	m.FullSyncInProgress.Dec()
	m.FullSyncTotal.Inc()
	m.FullSyncDuration.Observe(dur.Seconds())
}

func (m *Metrics) RecordAckTime(followerID string) {
	if m == nil { return }
	m.FollowerLastAckTime.WithLabelValues(followerID).Set(float64(time.Now().Unix()))
}

func (m *Metrics) RecordOldestSegmentAge(age time.Duration) {
	if m == nil { return }
	m.WALOldestSegmentAge.Set(age.Seconds())
}
