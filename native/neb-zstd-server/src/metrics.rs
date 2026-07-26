//! Operational metrics for neb-zstd-server, exposed as a Prometheus text
//! exposition endpoint (`GET /metrics`) for VictoriaMetrics' vmagent (or any
//! Prometheus-compatible scraper).
//!
//! Everything is std-only and lock-free: counters/gauges are atomics shared
//! by all serve workers, and rendering happens on scrape. The HTTP server is
//! a minimal hand-rolled listener good for exactly one route.

use std::fmt::Write as _;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// Request operations, as label values and array indices. "invalid" covers
/// requests whose header could not be parsed or whose op is not recognized.
pub const OPS: [&str; 5] = ["compress", "decompress", "oneshot", "reset", "invalid"];
pub const OP_COMPRESS_IDX: usize = 0;
pub const OP_DECOMPRESS_IDX: usize = 1;
pub const OP_ONESHOT_IDX: usize = 2;
pub const OP_RESET_IDX: usize = 3;
pub const OP_INVALID_IDX: usize = 4;

/// Request outcomes, as label values and array indices.
pub const STATUSES: [&str; 4] = ["ok", "bad_request", "zstd_error", "too_large"];
pub const STATUS_OK_IDX: usize = 0;
pub const STATUS_BAD_REQUEST_IDX: usize = 1;
pub const STATUS_ZSTD_ERROR_IDX: usize = 2;
pub const STATUS_TOO_LARGE_IDX: usize = 3;

/// Reasons an endpoint is dropped (fixed label set).
pub const DROP_REASONS: [&str; 4] = ["transport", "hard_cap", "wrap", "hello"];
/// Reasons a connection context is evicted (fixed label set).
pub const EVICT_REASONS: [&str; 2] = ["gc", "endpoint"];

/// Duration histogram bucket bounds, seconds (50µs ..= 50ms). Buckets are
/// cumulative, as Prometheus expects.
const DURATION_BUCKETS: [f64; 10] = [
    0.00005, 0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05,
];

/// All metric state. Cheap to share: `Arc<Metrics>` is cloned into every
/// serve worker.
pub struct Metrics {
    endpoints: AtomicI64,
    contexts: AtomicI64,
    connections_accepted: AtomicU64,
    connections_dropped: [AtomicU64; DROP_REASONS.len()],
    contexts_evicted: [AtomicU64; EVICT_REASONS.len()],
    requests: [[AtomicU64; STATUSES.len()]; OPS.len()],
    request_bytes_in: [AtomicU64; OPS.len()],
    request_bytes_out: [AtomicU64; OPS.len()],
    duration_buckets: [[AtomicU64; DURATION_BUCKETS.len()]; OPS.len()],
    duration_sum_nanos: [AtomicU64; OPS.len()],
    duration_count: [AtomicU64; OPS.len()],
}

impl Metrics {
    pub fn new() -> Self {
        Self {
            endpoints: AtomicI64::new(0),
            contexts: AtomicI64::new(0),
            connections_accepted: AtomicU64::new(0),
            connections_dropped: Default::default(),
            contexts_evicted: Default::default(),
            requests: Default::default(),
            request_bytes_in: Default::default(),
            request_bytes_out: Default::default(),
            duration_buckets: Default::default(),
            duration_sum_nanos: Default::default(),
            duration_count: Default::default(),
        }
    }

    pub fn endpoint_accepted(&self) {
        self.connections_accepted.fetch_add(1, Ordering::Relaxed);
        self.endpoints.fetch_add(1, Ordering::Relaxed);
    }

    pub fn endpoint_dropped(&self, reason_idx: usize) {
        self.connections_dropped[reason_idx].fetch_add(1, Ordering::Relaxed);
        self.endpoints.fetch_sub(1, Ordering::Relaxed);
    }

    /// An endpoint that failed before being fully accepted (e.g. the HELLO
    /// announcement could not be sent): count it without touching the gauge,
    /// since it was never registered as live.
    pub fn connection_failed_before_accept(&self, reason_idx: usize) {
        self.connections_dropped[reason_idx].fetch_add(1, Ordering::Relaxed);
    }

    pub fn contexts_added(&self, n: i64) {
        self.contexts.fetch_add(n, Ordering::Relaxed);
    }

    /// Record `n` contexts evicted for `reason_idx` (also lowers the gauge).
    pub fn contexts_evicted(&self, reason_idx: usize, n: u64) {
        if n == 0 {
            return;
        }
        self.contexts_evicted[reason_idx].fetch_add(n, Ordering::Relaxed);
        self.contexts.fetch_sub(n as i64, Ordering::Relaxed);
    }

    /// Record one answered request: outcome, payload sizes, and the time the
    /// actual zstd work took.
    pub fn request(
        &self,
        op_idx: usize,
        status_idx: usize,
        bytes_in: usize,
        bytes_out: usize,
        dur: Duration,
    ) {
        self.requests[op_idx][status_idx].fetch_add(1, Ordering::Relaxed);
        self.request_bytes_in[op_idx].fetch_add(bytes_in as u64, Ordering::Relaxed);
        self.request_bytes_out[op_idx].fetch_add(bytes_out as u64, Ordering::Relaxed);
        let secs = dur.as_secs_f64();
        for (i, bound) in DURATION_BUCKETS.iter().enumerate() {
            if secs <= *bound {
                self.duration_buckets[op_idx][i].fetch_add(1, Ordering::Relaxed);
            }
        }
        self.duration_sum_nanos[op_idx].fetch_add(dur.as_nanos() as u64, Ordering::Relaxed);
        self.duration_count[op_idx].fetch_add(1, Ordering::Relaxed);
    }

    /// Render the Prometheus text exposition format.
    pub fn render(&self) -> String {
        let mut out = String::with_capacity(4096);
        let relaxed = Ordering::Relaxed;

        out.push_str("# HELP neb_zstd_endpoints Live UCX endpoints (client slots).\n");
        out.push_str("# TYPE neb_zstd_endpoints gauge\n");
        let _ = writeln!(out, "neb_zstd_endpoints {}", self.endpoints.load(relaxed));

        out.push_str("# HELP neb_zstd_contexts Live per-connection zstd contexts.\n");
        out.push_str("# TYPE neb_zstd_contexts gauge\n");
        let _ = writeln!(out, "neb_zstd_contexts {}", self.contexts.load(relaxed));

        out.push_str(
            "# HELP neb_zstd_connections_accepted_total Endpoints accepted and announced.\n",
        );
        out.push_str("# TYPE neb_zstd_connections_accepted_total counter\n");
        let _ = writeln!(
            out,
            "neb_zstd_connections_accepted_total {}",
            self.connections_accepted.load(relaxed)
        );

        out.push_str("# HELP neb_zstd_connections_dropped_total Endpoints dropped, by reason.\n");
        out.push_str("# TYPE neb_zstd_connections_dropped_total counter\n");
        for (i, reason) in DROP_REASONS.iter().enumerate() {
            let _ = writeln!(
                out,
                "neb_zstd_connections_dropped_total{{reason=\"{reason}\"}} {}",
                self.connections_dropped[i].load(relaxed)
            );
        }

        out.push_str(
            "# HELP neb_zstd_contexts_evicted_total Connection contexts evicted, by reason.\n",
        );
        out.push_str("# TYPE neb_zstd_contexts_evicted_total counter\n");
        for (i, reason) in EVICT_REASONS.iter().enumerate() {
            let _ = writeln!(
                out,
                "neb_zstd_contexts_evicted_total{{reason=\"{reason}\"}} {}",
                self.contexts_evicted[i].load(relaxed)
            );
        }

        out.push_str("# HELP neb_zstd_requests_total Requests answered, by op and status.\n");
        out.push_str("# TYPE neb_zstd_requests_total counter\n");
        for (op_idx, op) in OPS.iter().enumerate() {
            for (status_idx, status) in STATUSES.iter().enumerate() {
                let _ = writeln!(
                    out,
                    "neb_zstd_requests_total{{op=\"{op}\",status=\"{status}\"}} {}",
                    self.requests[op_idx][status_idx].load(relaxed)
                );
            }
        }

        out.push_str("# HELP neb_zstd_request_bytes_total Payload bytes processed. Compression ratio = out/in of op=\"compress\".\n");
        out.push_str("# TYPE neb_zstd_request_bytes_total counter\n");
        for (op_idx, op) in OPS.iter().enumerate() {
            let _ = writeln!(
                out,
                "neb_zstd_request_bytes_total{{op=\"{op}\",direction=\"in\"}} {}",
                self.request_bytes_in[op_idx].load(relaxed)
            );
            let _ = writeln!(
                out,
                "neb_zstd_request_bytes_total{{op=\"{op}\",direction=\"out\"}} {}",
                self.request_bytes_out[op_idx].load(relaxed)
            );
        }

        out.push_str(
            "# HELP neb_zstd_request_duration_seconds Time spent on the actual zstd work, by op.\n",
        );
        out.push_str("# TYPE neb_zstd_request_duration_seconds histogram\n");
        for (op_idx, op) in OPS.iter().enumerate() {
            for (i, bound) in DURATION_BUCKETS.iter().enumerate() {
                let _ = writeln!(
                    out,
                    "neb_zstd_request_duration_seconds_bucket{{op=\"{op}\",le=\"{bound}\"}} {}",
                    self.duration_buckets[op_idx][i].load(relaxed)
                );
            }
            let _ = writeln!(
                out,
                "neb_zstd_request_duration_seconds_bucket{{op=\"{op}\",le=\"+Inf\"}} {}",
                self.duration_count[op_idx].load(relaxed)
            );
            let _ = writeln!(
                out,
                "neb_zstd_request_duration_seconds_sum{{op=\"{op}\"}} {}",
                self.duration_sum_nanos[op_idx].load(relaxed) as f64 / 1e9
            );
            let _ = writeln!(
                out,
                "neb_zstd_request_duration_seconds_count{{op=\"{op}\"}} {}",
                self.duration_count[op_idx].load(relaxed)
            );
        }

        out
    }
}

/// Spawn the metrics HTTP listener on its own thread. Serves `GET /metrics`
/// in Prometheus text format; everything else gets 404. Runs until the
/// process exits.
pub fn spawn_http(metrics: Arc<Metrics>, addr: SocketAddr) -> std::io::Result<()> {
    let listener = TcpListener::bind(addr)?;
    std::thread::Builder::new()
        .name("neb-metrics".to_string())
        .spawn(move || {
            for stream in listener.incoming() {
                match stream {
                    Ok(stream) => {
                        let metrics = Arc::clone(&metrics);
                        // One thread per scrape: scrapes are rare (interval
                        // seconds) and rendering is sub-millisecond.
                        std::thread::spawn(move || {
                            let _ = handle_connection(stream, &metrics);
                        });
                    }
                    Err(e) => {
                        eprintln!("[neb-metrics] accept failed: {e}");
                    }
                }
            }
        })?;
    Ok(())
}

fn handle_connection(mut stream: TcpStream, metrics: &Metrics) -> std::io::Result<()> {
    stream.set_read_timeout(Some(Duration::from_secs(5)))?;
    stream.set_write_timeout(Some(Duration::from_secs(5)))?;

    // Read the request head; 8 KiB is plenty for any GET.
    let mut head = Vec::with_capacity(1024);
    let mut buf = [0u8; 1024];
    while !head.windows(4).any(|w| w == b"\r\n\r\n") && head.len() < 8192 {
        let n = stream.read(&mut buf)?;
        if n == 0 {
            break;
        }
        head.extend_from_slice(&buf[..n]);
    }
    let request_line = head
        .split(|&b| b == b'\n')
        .next()
        .map(|l| String::from_utf8_lossy(l).trim().to_string())
        .unwrap_or_default();
    let path = request_line.split_whitespace().nth(1).unwrap_or("");

    let (status, content_type, body) = if request_line.starts_with("GET ") && path == "/metrics" {
        (
            "200 OK",
            "text/plain; version=0.0.4; charset=utf-8",
            metrics.render(),
        )
    } else {
        (
            "404 Not Found",
            "text/plain; charset=utf-8",
            "not found\n".to_string(),
        )
    };
    let response = format!(
        "HTTP/1.1 {status}\r\ncontent-type: {content_type}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
        body.len(),
        body
    );
    stream.write_all(response.as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn render_contains_all_families() {
        let m = Metrics::new();
        m.endpoint_accepted();
        m.endpoint_dropped(0);
        m.contexts_added(3);
        m.contexts_evicted(0, 1);
        m.request(
            OP_COMPRESS_IDX,
            STATUS_OK_IDX,
            1000,
            100,
            Duration::from_micros(300),
        );
        m.request(
            OP_COMPRESS_IDX,
            STATUS_ZSTD_ERROR_IDX,
            10,
            0,
            Duration::from_millis(10),
        );
        let text = m.render();
        assert!(text.contains("neb_zstd_endpoints 0"));
        assert!(text.contains("neb_zstd_contexts 2"));
        assert!(text.contains("neb_zstd_connections_accepted_total 1"));
        assert!(text.contains("neb_zstd_connections_dropped_total{reason=\"transport\"} 1"));
        assert!(text.contains("neb_zstd_contexts_evicted_total{reason=\"gc\"} 1"));
        assert!(text.contains("neb_zstd_requests_total{op=\"compress\",status=\"ok\"} 1"));
        assert!(text.contains("neb_zstd_requests_total{op=\"compress\",status=\"zstd_error\"} 1"));
        assert!(
            text.contains("neb_zstd_request_bytes_total{op=\"compress\",direction=\"in\"} 1010")
        );
        assert!(
            text.contains("neb_zstd_request_bytes_total{op=\"compress\",direction=\"out\"} 100")
        );
        // 300µs falls into le=0.0005 and above only; cumulative semantics.
        assert!(text
            .contains("neb_zstd_request_duration_seconds_bucket{op=\"compress\",le=\"0.0001\"} 0"));
        assert!(text
            .contains("neb_zstd_request_duration_seconds_bucket{op=\"compress\",le=\"0.0005\"} 1"));
        assert!(text
            .contains("neb_zstd_request_duration_seconds_bucket{op=\"compress\",le=\"+Inf\"} 2"));
        assert!(text.contains("neb_zstd_request_duration_seconds_count{op=\"compress\"} 2"));
    }
}
