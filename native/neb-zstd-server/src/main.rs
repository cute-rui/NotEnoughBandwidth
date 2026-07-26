//! Remote zstd offload server for NotEnoughBandwidth.
//!
//! Accepts UCX client-server connections (one per game client), keeps the
//! per-game-connection streaming zstd contexts in memory, and answers
//! tag-matched requests (compress / decompress / oneshot / reset) over RDMA.
//!
//! Threading model:
//! - the main thread owns the accept worker (`UCS_THREAD_MODE_SINGLE`): it
//!   drives listener events and hands connection requests to the worker
//!   threads, round-robin;
//! - each worker thread owns one UCX worker (`UCS_THREAD_MODE_SINGLE`), the
//!   endpoints it accepted, and every zstd context created on behalf of those
//!   endpoints. Contexts are `!Send` and never cross threads: the client pins
//!   a game connection (`conn_id`) to one endpoint, so all of its requests
//!   land on the worker that owns the endpoint.

use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

mod metrics;

use metrics::{
    Metrics, OP_COMPRESS_IDX, OP_DECOMPRESS_IDX, OP_INVALID_IDX, OP_ONESHOT_IDX, OP_RESET_IDX,
    STATUS_BAD_REQUEST_IDX, STATUS_OK_IDX, STATUS_TOO_LARGE_IDX, STATUS_ZSTD_ERROR_IDX,
};

use neb_offload_core::{
    compress_bound, conn_id_of, ep_id_of, hello_tag, response_message, response_tag,
    CompressStream, DecompressStream, Hello, RequestHeader, OP_COMPRESS, OP_COMPRESS_ONESHOT,
    OP_DECOMPRESS, OP_RESET, REQUEST_HEADER_LEN, STATUS_BAD_REQUEST, STATUS_MESSAGE_TOO_LARGE,
    STATUS_OK, STATUS_ZSTD_ERROR, TAG_RESP_BIT,
};
use ucx_ffi::raw;
use ucx_ffi::ucp::{ConnRequest, Context, Endpoint, Worker};

/// Lightweight log line prefixed with the current thread's name.
macro_rules! log {
    ($($arg:tt)*) => {{
        let thread = std::thread::current();
        println!("[{}] {}", thread.name().unwrap_or("main"), format!($($arg)*));
    }};
}

const DEFAULT_LISTEN: &str = "0.0.0.0:19999";
const DEFAULT_METRICS_LISTEN: &str = "0.0.0.0:9100";
const DEFAULT_LEVEL: i32 = 3;
const DEFAULT_WINDOW_LOG: u32 = 23;
const DEFAULT_MAX_PAYLOAD: usize = 8 * 1024 * 1024;
const DEFAULT_GC_SECS: u64 = 600;

/// Absolute ceiling for one wire message and for `raw_size` allocations;
/// anything beyond is treated as a broken/hostile peer. `--max-payload` is
/// validated to stay below this.
const HARD_CAP: usize = 64 * 1024 * 1024;

const HELLO_TIMEOUT: Duration = Duration::from_secs(5);
const RECV_TIMEOUT: Duration = Duration::from_secs(30);
const SEND_TIMEOUT: Duration = Duration::from_secs(30);
/// Idle nap of the serve loop (spec: ~50µs).
const IDLE_SLEEP: Duration = Duration::from_micros(50);
/// Idle nap of the accept loop (spec: ~100µs).
const ACCEPT_IDLE_SLEEP: Duration = Duration::from_micros(100);
/// How often the idle-context sweep runs.
const GC_INTERVAL: Duration = Duration::from_secs(5);

#[derive(Clone, Copy)]
struct Config {
    listen: SocketAddr,
    /// Prometheus metrics endpoint; `None` disables the HTTP listener.
    metrics_listen: Option<SocketAddr>,
    threads: usize,
    level: i32,
    window_log: u32,
    max_payload: usize,
    gc_secs: u64,
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.iter().any(|a| a == "--help" || a == "-h") {
        print_usage();
        return;
    }
    let cfg = match parse_args(&args) {
        Ok(cfg) => cfg,
        Err(msg) => {
            eprintln!("neb-zstd-server: {msg}");
            eprintln!("try --help for usage");
            std::process::exit(2);
        }
    };
    if let Err(e) = run(cfg) {
        log!("fatal: {e}");
        std::process::exit(1);
    }
}

fn print_usage() {
    println!(
        "neb-zstd-server — remote zstd offload server for NotEnoughBandwidth (UCX/RoCE)

USAGE:
    neb-zstd-server [OPTIONS]

OPTIONS:
    --listen ADDR:PORT   Listen address (default: {DEFAULT_LISTEN})
    --metrics-listen ADDR:PORT
                         Prometheus metrics endpoint serving GET /metrics
                         (default: {DEFAULT_METRICS_LISTEN}, "off" to disable)
    --threads N          Serve worker threads (default: available parallelism)
    --level N            zstd compression level, 1..=22 (default: {DEFAULT_LEVEL})
    --window-log N       zstd window log, 21..=25 (default: {DEFAULT_WINDOW_LOG})
    --max-payload BYTES  Largest accepted request payload (default: {DEFAULT_MAX_PAYLOAD},
                         hard cap {HARD_CAP})
    --gc-secs SECONDS    Drop per-connection contexts idle this long (default: {DEFAULT_GC_SECS})
    --help               Print this help"
    );
}

fn default_threads() -> usize {
    std::thread::available_parallelism()
        .map(NonZeroUsize::get)
        .unwrap_or(4)
}

fn parse_val<T: std::str::FromStr>(flag: &str, value: &str) -> Result<T, String> {
    value
        .parse::<T>()
        .map_err(|_| format!("invalid value '{value}' for {flag}"))
}

fn parse_args(args: &[String]) -> Result<Config, String> {
    let mut cfg = Config {
        listen: parse_val("--listen", DEFAULT_LISTEN).expect("built-in default must parse"),
        metrics_listen: Some(
            parse_val("--metrics-listen", DEFAULT_METRICS_LISTEN)
                .expect("built-in default must parse"),
        ),
        threads: default_threads(),
        level: DEFAULT_LEVEL,
        window_log: DEFAULT_WINDOW_LOG,
        max_payload: DEFAULT_MAX_PAYLOAD,
        gc_secs: DEFAULT_GC_SECS,
    };
    let mut i = 0;
    while i < args.len() {
        let flag = args[i].as_str();
        let value = args
            .get(i + 1)
            .ok_or_else(|| format!("missing value for {flag}"))?;
        match flag {
            "--listen" => cfg.listen = parse_val(flag, value)?,
            "--metrics-listen" => {
                cfg.metrics_listen = if value.eq_ignore_ascii_case("off") {
                    None
                } else {
                    Some(parse_val(flag, value)?)
                };
            }
            "--threads" => {
                cfg.threads = parse_val(flag, value)?;
                if cfg.threads == 0 {
                    return Err("--threads must be >= 1".into());
                }
            }
            "--level" => {
                cfg.level = parse_val(flag, value)?;
                if !(1..=22).contains(&cfg.level) {
                    return Err("--level must be in 1..=22".into());
                }
            }
            "--window-log" => {
                cfg.window_log = parse_val(flag, value)?;
                if !(21..=25).contains(&cfg.window_log) {
                    return Err("--window-log must be in 21..=25".into());
                }
            }
            "--max-payload" => {
                cfg.max_payload = parse_val(flag, value)?;
                if cfg.max_payload == 0 || cfg.max_payload > HARD_CAP {
                    return Err(format!("--max-payload must be in 1..={HARD_CAP}"));
                }
            }
            "--gc-secs" => {
                cfg.gc_secs = parse_val(flag, value)?;
                if cfg.gc_secs == 0 {
                    return Err("--gc-secs must be >= 1".into());
                }
            }
            other => return Err(format!("unknown option '{other}'")),
        }
        i += 2;
    }
    Ok(cfg)
}

fn run(cfg: Config) -> Result<(), String> {
    let context = Context::new().map_err(|e| format!("failed to initialize UCX: {e}"))?;

    let metrics = Arc::new(Metrics::new());
    if let Some(addr) = cfg.metrics_listen {
        metrics::spawn_http(Arc::clone(&metrics), addr)
            .map_err(|e| format!("failed to bind metrics endpoint on {addr}: {e}"))?;
        log!("metrics on http://{addr}/metrics");
    }

    // One accept queue per worker thread; the listener callback pushes
    // incoming connection requests into them round-robin.
    let queues: Arc<Vec<Mutex<VecDeque<ConnRequest>>>> = Arc::new(
        (0..cfg.threads)
            .map(|_| Mutex::new(VecDeque::new()))
            .collect(),
    );

    // Spawn the serve worker threads. Each creates its own UCX worker and
    // owns everything it accepts for its whole lifetime.
    for i in 0..cfg.threads {
        let context = Arc::clone(&context);
        let queues = Arc::clone(&queues);
        let metrics = Arc::clone(&metrics);
        std::thread::Builder::new()
            .name(format!("neb-worker-{i}"))
            .spawn(move || {
                let worker = match context.worker(raw::ucs_thread_mode_t::UCS_THREAD_MODE_SINGLE) {
                    Ok(worker) => worker,
                    Err(e) => {
                        log!("failed to create UCX worker: {e}");
                        std::process::exit(1);
                    }
                };
                WorkerState::new(worker, cfg, metrics).serve_loop(&queues[i]);
            })
            .map_err(|e| format!("failed to spawn worker thread {i}: {e}"))?;
    }

    // The accept worker stays on the main thread; it only drives listener
    // events and forwards connection requests.
    let accept_worker = context
        .worker(raw::ucs_thread_mode_t::UCS_THREAD_MODE_SINGLE)
        .map_err(|e| format!("failed to create accept worker: {e}"))?;

    let mut next = 0usize;
    let accept_queues = Arc::clone(&queues);
    let _listener = accept_worker
        .listen(
            &cfg.listen,
            Box::new(move |req: ConnRequest| {
                let i = next;
                next = (next + 1) % accept_queues.len();
                accept_queues[i].lock().unwrap().push_back(req);
            }),
        )
        .map_err(|e| format!("failed to listen on {}: {e}", cfg.listen))?;

    log!(
        "listening on {}, {} worker thread(s), level {}, window-log {}, max-payload {} bytes, gc-secs {}",
        cfg.listen, cfg.threads, cfg.level, cfg.window_log, cfg.max_payload, cfg.gc_secs
    );

    // No SIGINT handling on purpose: all zstd contexts are in-memory only and
    // there is no state to flush on shutdown, so SIGKILL is a safe way to
    // stop this process.
    loop {
        if accept_worker.progress() == 0 {
            std::thread::sleep(ACCEPT_IDLE_SLEEP);
        }
    }
}

/// Per-game-connection zstd state, pinned to the worker thread that owns the
/// endpoint. Both halves are lazy: a connection that only ever compresses
/// never pays for a decompression context.
struct ConnCtx {
    cctx: Option<CompressStream>,
    dctx: Option<DecompressStream>,
    last_used: Instant,
}

impl ConnCtx {
    fn new() -> Self {
        Self {
            cctx: None,
            dctx: None,
            last_used: Instant::now(),
        }
    }
}

/// Everything one serve worker thread owns.
struct WorkerState {
    worker: Worker,
    cfg: Config,
    metrics: Arc<Metrics>,
    /// Live endpoints by their server-assigned id.
    endpoints: HashMap<u16, Endpoint>,
    /// Per-connection zstd contexts by (endpoint id, game connection id).
    ctxs: HashMap<(u16, u32), ConnCtx>,
    /// Next endpoint id to hand out; wraps after 2^16 accepts on this thread.
    next_ep_id: u16,
    /// Shared stateless compressor for OP_COMPRESS_ONESHOT: it produces
    /// complete frames and keeps no inter-message state, so one context
    /// serves every connection on this thread.
    oneshot: Option<CompressStream>,
    last_gc: Instant,
}

impl WorkerState {
    fn new(worker: Worker, cfg: Config, metrics: Arc<Metrics>) -> Self {
        Self {
            worker,
            cfg,
            metrics,
            endpoints: HashMap::new(),
            ctxs: HashMap::new(),
            next_ep_id: 0,
            oneshot: None,
            last_gc: Instant::now(),
        }
    }

    fn serve_loop(&mut self, queue: &Mutex<VecDeque<ConnRequest>>) -> ! {
        loop {
            let mut did_work = self.accept_pending(queue);
            // Requests carry the resp bit clear; probe for any of them.
            if let Some((tag, len)) = self.worker.tag_probe(0, TAG_RESP_BIT) {
                did_work = true;
                self.handle_message(tag, len);
            }
            self.gc();
            if !did_work {
                // Nothing to do: pump the network a couple more times, then
                // nap briefly so an idle worker does not burn a core.
                let mut progressed = 0;
                for _ in 0..2 {
                    progressed += self.worker.progress();
                }
                if progressed == 0 {
                    std::thread::sleep(IDLE_SLEEP);
                }
            }
        }
    }

    /// Take every queued connection request: create an endpoint on this
    /// worker, announce ourselves with a HELLO, and register it. Failures
    /// drop the endpoint (force-close on drop) and do not stop the drain.
    fn accept_pending(&mut self, queue: &Mutex<VecDeque<ConnRequest>>) -> bool {
        let mut did_work = false;
        loop {
            let req = queue.lock().unwrap().pop_front();
            let Some(req) = req else { break };
            did_work = true;
            let ep = match self.worker.accept(req) {
                Ok(ep) => ep,
                Err(e) => {
                    log!("failed to accept connection request: {e}");
                    continue;
                }
            };
            let ep_id = self.alloc_ep_id();
            let hello = Hello {
                level: self.cfg.level as u8,
                window_log: self.cfg.window_log as u8,
                magicless: true,
                max_payload: self.cfg.max_payload as u32,
            };
            let announce = hello.encode();
            match self
                .worker
                .tag_send(&ep, hello_tag(ep_id), &announce, HELLO_TIMEOUT)
            {
                Ok(()) => {
                    self.endpoints.insert(ep_id, ep);
                    self.metrics.endpoint_accepted();
                    log!("accepted endpoint {ep_id} ({} live)", self.endpoints.len());
                }
                Err(e) => {
                    log!("HELLO failed on endpoint {ep_id}: {e}");
                    self.metrics.connection_failed_before_accept(3 /* hello */);
                    // `ep` goes out of scope here and is force-closed.
                }
            }
        }
        did_work
    }

    /// Assign an endpoint id from this thread's counter. After 2^16 accepts
    /// the id may still be in use; force-drop any stale state under it.
    fn alloc_ep_id(&mut self) -> u16 {
        let id = self.next_ep_id;
        self.next_ep_id = self.next_ep_id.wrapping_add(1);
        self.drop_endpoint(id, "endpoint id wrap-around", 2 /* wrap */);
        id
    }

    /// Force-close an endpoint (via `Endpoint::drop`) and purge every
    /// connection context pinned to it — after a transport error those
    /// contexts are unrecoverable anyway. `reason_idx` indexes
    /// `metrics::DROP_REASONS`.
    fn drop_endpoint(&mut self, ep_id: u16, reason: &str, reason_idx: usize) {
        if self.endpoints.remove(&ep_id).is_some() {
            log!("dropping endpoint {ep_id}: {reason}");
            self.metrics.endpoint_dropped(reason_idx);
        }
        let before = self.ctxs.len();
        self.ctxs.retain(|&(e, _), _| e != ep_id);
        let purged = before - self.ctxs.len();
        if purged > 0 {
            log!("purged {purged} context(s) of endpoint {ep_id}");
            self.metrics
                .contexts_evicted(1 /* endpoint */, purged as u64);
        }
    }

    /// Receive one probed request, dispatch it, and send the response. Any
    /// transport error on an endpoint kills it (see [`Self::drop_endpoint`]).
    fn handle_message(&mut self, tag: u64, len: usize) {
        let ep_id = ep_id_of(tag);

        if len > HARD_CAP {
            // Way beyond anything legitimate. Consume the message with a
            // truncating receive (UCX discards the remainder) so it is not
            // re-probed forever, then kill the endpoint.
            let mut sink = [0u8; 1];
            let _ = self.worker.tag_recv(tag, u64::MAX, &mut sink, RECV_TIMEOUT);
            log!("endpoint {ep_id} sent a {len}-byte message (hard cap {HARD_CAP}), dropping it");
            self.drop_endpoint(ep_id, "message beyond hard cap", 1 /* hard_cap */);
            return;
        }

        // The message was already probed, so this returns immediately unless
        // the transport is sick.
        let mut buf = vec![0u8; len];
        if let Err(e) = self.worker.tag_recv(tag, u64::MAX, &mut buf, RECV_TIMEOUT) {
            log!("receive on endpoint {ep_id} failed: {e}");
            self.drop_endpoint(ep_id, "transport error on receive", 0 /* transport */);
            return;
        }

        if !self.endpoints.contains_key(&ep_id) {
            // The endpoint is gone (dropped earlier); the message is consumed
            // and there is nobody left to answer.
            log!("discarding request for unknown endpoint {ep_id}");
            return;
        }

        let (status, payload) = if len < REQUEST_HEADER_LEN {
            self.metrics.request(
                OP_INVALID_IDX,
                STATUS_BAD_REQUEST_IDX,
                len,
                0,
                Duration::ZERO,
            );
            (STATUS_BAD_REQUEST, Vec::new())
        } else if len > self.cfg.max_payload + REQUEST_HEADER_LEN {
            self.metrics
                .request(OP_INVALID_IDX, STATUS_TOO_LARGE_IDX, len, 0, Duration::ZERO);
            (STATUS_MESSAGE_TOO_LARGE, Vec::new())
        } else {
            self.dispatch(tag, &buf)
        };

        let msg = response_message(status, &payload);
        let result = match self.endpoints.get(&ep_id) {
            Some(ep) => self
                .worker
                .tag_send(ep, response_tag(tag), &msg, SEND_TIMEOUT),
            None => return,
        };
        if let Err(e) = result {
            log!("send on endpoint {ep_id} failed: {e}");
            self.drop_endpoint(ep_id, "transport error on send", 0 /* transport */);
        }
    }

    /// Execute one fully-received request, record its metrics, and return
    /// the response status and payload. Only called with
    /// `buf.len() >= REQUEST_HEADER_LEN` and a payload within the configured
    /// limit.
    fn dispatch(&mut self, tag: u64, buf: &[u8]) -> (i32, Vec<u8>) {
        let start = Instant::now();
        let (op_idx, status_idx, bytes_in, (status, payload)) = self.dispatch_inner(tag, buf);
        self.metrics
            .request(op_idx, status_idx, bytes_in, payload.len(), start.elapsed());
        (status, payload)
    }

    /// The actual work of [`Self::dispatch`], additionally reporting the op,
    /// outcome and input size as metrics indices.
    fn dispatch_inner(&mut self, tag: u64, buf: &[u8]) -> (usize, usize, usize, (i32, Vec<u8>)) {
        /// Map a wire status code to its metrics index.
        fn status_idx(code: i32) -> usize {
            match code {
                STATUS_OK => STATUS_OK_IDX,
                STATUS_ZSTD_ERROR => STATUS_ZSTD_ERROR_IDX,
                STATUS_MESSAGE_TOO_LARGE => STATUS_TOO_LARGE_IDX,
                _ => STATUS_BAD_REQUEST_IDX,
            }
        }

        let key = (ep_id_of(tag), conn_id_of(tag));
        let header = match RequestHeader::decode(buf) {
            Ok(h) => h,
            Err(_) => {
                return (
                    OP_INVALID_IDX,
                    STATUS_BAD_REQUEST_IDX,
                    buf.len(),
                    (STATUS_BAD_REQUEST, Vec::new()),
                );
            }
        };
        let payload = &buf[REQUEST_HEADER_LEN..];
        if header.payload_len as usize != payload.len() {
            log!(
                "endpoint {} conn {}: header payload_len {} != message body {}",
                key.0,
                key.1,
                header.payload_len,
                payload.len()
            );
            return (
                OP_INVALID_IDX,
                STATUS_BAD_REQUEST_IDX,
                buf.len(),
                (STATUS_BAD_REQUEST, Vec::new()),
            );
        }

        match header.op {
            OP_COMPRESS => {
                let created = !self.ctxs.contains_key(&key);
                let ctx = self.ctxs.entry(key).or_insert_with(ConnCtx::new);
                if created {
                    self.metrics.contexts_added(1);
                }
                ctx.last_used = Instant::now();
                if ctx.cctx.is_none() {
                    match CompressStream::new(self.cfg.level, self.cfg.window_log) {
                        Ok(cctx) => ctx.cctx = Some(cctx),
                        Err(e) => {
                            log!("cannot create compress context: {e}");
                            return (
                                OP_COMPRESS_IDX,
                                STATUS_ZSTD_ERROR_IDX,
                                payload.len(),
                                (STATUS_ZSTD_ERROR, Vec::new()),
                            );
                        }
                    }
                }
                let cctx = ctx.cctx.as_mut().unwrap();
                let mut out = vec![0u8; compress_bound(payload.len())];
                let result = match cctx.compress(payload, &mut out) {
                    Ok(n) => {
                        out.truncate(n);
                        (STATUS_OK, out)
                    }
                    Err(e) => {
                        log!("compress failed on conn {}: {e}", key.1);
                        (STATUS_ZSTD_ERROR, Vec::new())
                    }
                };
                (OP_COMPRESS_IDX, status_idx(result.0), payload.len(), result)
            }
            OP_COMPRESS_ONESHOT => {
                if self.oneshot.is_none() {
                    match CompressStream::new(self.cfg.level, self.cfg.window_log) {
                        Ok(cctx) => self.oneshot = Some(cctx),
                        Err(e) => {
                            log!("cannot create oneshot compress context: {e}");
                            return (
                                OP_ONESHOT_IDX,
                                STATUS_ZSTD_ERROR_IDX,
                                payload.len(),
                                (STATUS_ZSTD_ERROR, Vec::new()),
                            );
                        }
                    }
                }
                let cctx = self.oneshot.as_mut().unwrap();
                let mut out = vec![0u8; compress_bound(payload.len())];
                let result = match cctx.compress_oneshot(payload, &mut out) {
                    Ok(n) => {
                        out.truncate(n);
                        (STATUS_OK, out)
                    }
                    Err(e) => {
                        log!("oneshot compress failed: {e}");
                        (STATUS_ZSTD_ERROR, Vec::new())
                    }
                };
                (OP_ONESHOT_IDX, status_idx(result.0), payload.len(), result)
            }
            OP_DECOMPRESS => {
                let raw_size = header.raw_size as usize;
                // raw_size comes from the wire and decides an allocation;
                // refuse absurd sizes before trusting it.
                if raw_size > HARD_CAP {
                    return (
                        OP_DECOMPRESS_IDX,
                        STATUS_TOO_LARGE_IDX,
                        payload.len(),
                        (STATUS_MESSAGE_TOO_LARGE, Vec::new()),
                    );
                }
                let created = !self.ctxs.contains_key(&key);
                let ctx = self.ctxs.entry(key).or_insert_with(ConnCtx::new);
                if created {
                    self.metrics.contexts_added(1);
                }
                ctx.last_used = Instant::now();
                if ctx.dctx.is_none() {
                    match DecompressStream::new() {
                        Ok(dctx) => ctx.dctx = Some(dctx),
                        Err(e) => {
                            log!("cannot create decompress context: {e}");
                            return (
                                OP_DECOMPRESS_IDX,
                                STATUS_ZSTD_ERROR_IDX,
                                payload.len(),
                                (STATUS_ZSTD_ERROR, Vec::new()),
                            );
                        }
                    }
                }
                let dctx = ctx.dctx.as_mut().unwrap();
                let mut out = vec![0u8; raw_size];
                let result = match dctx.decompress_exact(payload, &mut out) {
                    Ok(()) => (STATUS_OK, out),
                    Err(e) => {
                        log!("decompress failed on conn {}: {e}", key.1);
                        (STATUS_ZSTD_ERROR, Vec::new())
                    }
                };
                (
                    OP_DECOMPRESS_IDX,
                    status_idx(result.0),
                    payload.len(),
                    result,
                )
            }
            OP_RESET => {
                // Idempotent: the client sends this when a game connection
                // closes, whether or not we still hold state for it.
                if self.ctxs.remove(&key).is_some() {
                    self.metrics.contexts_added(-1);
                }
                (
                    OP_RESET_IDX,
                    STATUS_OK_IDX,
                    payload.len(),
                    (STATUS_OK, Vec::new()),
                )
            }
            op => {
                log!("endpoint {} conn {}: unknown op {op}", key.0, key.1);
                (
                    OP_INVALID_IDX,
                    STATUS_BAD_REQUEST_IDX,
                    payload.len(),
                    (STATUS_BAD_REQUEST, Vec::new()),
                )
            }
        }
    }

    /// Drop contexts idle for longer than `--gc-secs` (game connections can
    /// die without OP_RESET when the client crashes). Runs about every 5 s.
    fn gc(&mut self) {
        if self.last_gc.elapsed() < GC_INTERVAL {
            return;
        }
        self.last_gc = Instant::now();
        let max_idle = Duration::from_secs(self.cfg.gc_secs);
        let before = self.ctxs.len();
        self.ctxs.retain(|&(ep_id, conn_id), ctx| {
            let idle = ctx.last_used.elapsed();
            if idle > max_idle {
                log!("gc: conn {conn_id} on endpoint {ep_id} idle for {idle:?}, dropping context");
                false
            } else {
                true
            }
        });
        if self.ctxs.len() < before {
            log!(
                "gc: dropped {} idle context(s), {} live",
                before - self.ctxs.len(),
                self.ctxs.len()
            );
            self.metrics
                .contexts_evicted(0 /* gc */, (before - self.ctxs.len()) as u64);
        }
    }
}
