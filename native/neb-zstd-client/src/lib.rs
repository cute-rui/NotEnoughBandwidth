//! Client library for the NEB remote zstd offload service, loaded by the JVM
//! through the FFM API (the mod binds the exported `neb_*` symbols).
//!
//! The zstd streaming contexts live on the remote server; this library is a
//! thread-safe transport client speaking the tag-matched UCX protocol defined
//! in `neb-offload-core`:
//!
//! - one process-wide UCX [`Context`], created lazily on the first `neb_open`
//! - per client `workers` slots, each with its own worker and at most one
//!   endpoint to the server
//! - connection pinning: every game connection (`conn_id`) is routed to
//!   `slots[conn_id % slots.len()]` and the slot's conn mutex is held for the
//!   whole request/response round trip. This keeps the requests of one
//!   connection ordered and the server-side zstd contexts pinned to a single
//!   endpoint/worker pair.
//!
//! C ABI (signatures exactly as bound by the Java side):
//!
//! ```c
//! long neb_open(const char* addr, int workers, int level, int window_log, int max_payload);
//! int neb_compress(long handle, unsigned int conn_id, const void* src, int src_len, void* dst, int dst_cap);
//! int neb_compress_oneshot(long handle, unsigned int conn_id, const void* src, int src_len, void* dst, int dst_cap);
//! int neb_decompress(long handle, unsigned int conn_id, const void* src, int src_len, void* dst, int raw_size);
//! int neb_reset_conn(long handle, unsigned int conn_id);
//! void neb_close(long handle);
//! ```
//!
//! Error convention: server statuses (`STATUS_*`, -1..=-5) pass through to
//! the caller unchanged; client-side failures report the `ERR_*` codes
//! (-100..=-103) defined below. Every exported body is wrapped in
//! `catch_unwind`: a panic must never cross the FFI boundary, it is reported
//! as `STATUS_INTERNAL_ERROR` instead.

use std::ffi::CStr;
use std::net::{SocketAddr, ToSocketAddrs};
use std::os::raw::{c_char, c_int, c_long, c_uint, c_void};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;
use std::{ptr, slice};

use neb_offload_core::protocol::{
    ep_id_of, make_request_tag, response_tag, Hello, RequestHeader, ResponseHeader, HELLO_CONN_ID,
    HELLO_LEN, HELLO_MASK, OP_COMPRESS, OP_COMPRESS_ONESHOT, OP_DECOMPRESS, OP_RESET,
    REQUEST_HEADER_LEN, RESPONSE_HEADER_LEN, STATUS_INTERNAL_ERROR, STATUS_MESSAGE_TOO_LARGE,
    STATUS_OK, STATUS_PARAM_MISMATCH, TAG_RESP_BIT,
};
use ucx_ffi::raw;
use ucx_ffi::ucp::{Context, Endpoint, Error as UcxError, Worker};

/// UCX transport failure (the slot's endpoint is dropped; the next operation
/// reconnects from scratch).
const ERR_TRANSPORT: c_int = -100;
/// An operation did not complete within [`IO_TIMEOUT`].
const ERR_TIMEOUT: c_int = -101;
/// Bad handle, pointer or length argument.
const ERR_INVALID_ARG: c_int = -102;
/// No connection to the server (connect or handshake failed).
const ERR_NOT_CONNECTED: c_int = -103;

/// Per-operation I/O timeout (handshake, send, receive).
const IO_TIMEOUT: Duration = Duration::from_secs(10);
/// Lower bound of the response buffer, so small replies need no exact size
/// estimate from the caller.
const MIN_RESPONSE_BUF: usize = 16 * 1024;

/// Process-wide UCX context. Created on first use and intentionally never
/// destroyed: workers and endpoints reference it, so running `ucp_cleanup`
/// while a client handle could still exist would risk use-after-free.
static CONTEXT: OnceLock<Arc<Context>> = OnceLock::new();

fn global_context() -> Result<Arc<Context>, ClientError> {
    if let Some(ctx) = CONTEXT.get() {
        return Ok(Arc::clone(ctx));
    }
    let ctx = Context::new().map_err(|_| ClientError::NotConnected)?;
    // Racing threads may both create a context; keep the first, drop ours.
    let _ = CONTEXT.set(ctx);
    Ok(Arc::clone(CONTEXT.get().expect("context just set")))
}

/// Internal error type; every variant maps to exactly one ABI error code.
enum ClientError {
    /// Server-reported status, passed through to the caller unchanged.
    Status(c_int),
    /// UCX failure on an established endpoint.
    Transport,
    /// An operation exceeded [`IO_TIMEOUT`].
    Timeout,
    /// Connect/handshake phase failure.
    NotConnected,
}

impl ClientError {
    fn code(&self) -> c_int {
        match self {
            ClientError::Status(s) => *s,
            ClientError::Transport => ERR_TRANSPORT,
            ClientError::Timeout => ERR_TIMEOUT,
            ClientError::NotConnected => ERR_NOT_CONNECTED,
        }
    }

    /// Whether the endpoint must be dropped after this error: the request may
    /// have been (partially) processed server-side, so the connection's zstd
    /// stream state can no longer be trusted. The next operation reconnects.
    fn drops_endpoint(&self) -> bool {
        matches!(self, ClientError::Transport | ClientError::Timeout)
    }
}

/// Map a ucx-ffi error to the timeout/transport distinction. ucx-ffi reports
/// a timeout as a status-less error (the request was cancelled locally),
/// while real transport failures carry a UCX status code.
fn map_io_error(e: UcxError) -> ClientError {
    if e.status.is_none() && e.message.contains("timed out") {
        ClientError::Timeout
    } else {
        ClientError::Transport
    }
}

/// Offload client handle; created by `neb_open`, reclaimed by `neb_close`.
struct Client {
    slots: Vec<Arc<Slot>>,
    addr: SocketAddr,
    level: i32,
    window_log: u32,
    max_payload: usize,
    timeout: Duration,
}

/// One UCX worker plus its (single) server endpoint. A slot owns the game
/// connections pinned to it; the conn mutex serializes their round trips.
struct Slot {
    worker: Worker,
    conn: Mutex<SlotConn>,
}

struct SlotConn {
    ep: Option<Endpoint>,
    /// Endpoint id assigned by the server in the HELLO handshake; carried in
    /// the tag of every request sent over this endpoint.
    ep_id: u16,
}

// Workers are created with UCS_THREAD_MODE_SERIALIZED (UCX serializes
// concurrent calls internally) and every access is additionally serialized by
// the slot's conn mutex, so sharing a slot across threads is sound.
unsafe impl Sync for Slot {}

impl Drop for Slot {
    fn drop(&mut self) {
        // Fields drop in declaration order, so `worker` would be destroyed
        // before the endpoint stored in `conn`, while Endpoint::drop pumps
        // that worker's progress. Force-close the endpoint explicitly while
        // the worker is still alive.
        if let Ok(conn) = self.conn.get_mut() {
            conn.ep = None;
        }
    }
}

impl Client {
    /// Connect the slot's endpoint if needed and perform the HELLO handshake.
    /// On success `conn.ep`/`conn.ep_id` are set; on failure `conn.ep` stays
    /// `None` so the next operation retries from scratch.
    fn ensure_connected(&self, slot: &Slot, conn: &mut SlotConn) -> Result<(), ClientError> {
        if conn.ep.is_some() {
            return Ok(());
        }
        let ep = slot
            .worker
            .connect(&self.addr)
            .map_err(|_| ClientError::NotConnected)?;

        // The server announces itself with a HELLO message on every accepted
        // endpoint; the ep_id in its sender tag is ours from now on. The mask
        // matches the response bit and the reserved HELLO conn_id with any
        // ep_id, since the id is not known yet.
        let mut buf = [0u8; HELLO_LEN];
        let (sender_tag, len) = slot
            .worker
            .tag_recv(
                TAG_RESP_BIT | HELLO_CONN_ID as u64,
                HELLO_MASK,
                &mut buf,
                self.timeout,
            )
            .map_err(|e| match map_io_error(e) {
                ClientError::Timeout => ClientError::Timeout,
                _ => ClientError::NotConnected,
            })?;
        let hello = Hello::decode(&buf[..len]).map_err(|_| ClientError::NotConnected)?;
        if i32::from(hello.level) != self.level
            || u32::from(hello.window_log) != self.window_log
            || !hello.magicless
            || hello.max_payload as usize != self.max_payload
        {
            // The server would produce frames the mod cannot decode (or
            // reject messages the mod sends); refuse the endpoint.
            return Err(ClientError::Status(STATUS_PARAM_MISMATCH));
        }
        conn.ep_id = ep_id_of(sender_tag);
        conn.ep = Some(ep);
        Ok(())
    }

    /// Wire-level request/response exchange on the slot's established
    /// endpoint. Must be called with the slot's conn mutex held; the caller
    /// drops the endpoint when the returned error demands it.
    fn round_trip(
        &self,
        slot: &Slot,
        conn: &SlotConn,
        conn_id: u32,
        op: u8,
        payload: &[u8],
        raw_size: u32,
        expected_out_len: usize,
    ) -> Result<Vec<u8>, ClientError> {
        let ep = conn.ep.as_ref().ok_or(ClientError::NotConnected)?;
        let req_tag = make_request_tag(conn.ep_id, conn_id);
        let header = RequestHeader {
            op,
            raw_size,
            payload_len: payload.len() as u32,
        };
        let mut request = Vec::with_capacity(REQUEST_HEADER_LEN + payload.len());
        request.extend_from_slice(&header.encode());
        request.extend_from_slice(payload);
        slot.worker
            .tag_send(ep, req_tag, &request, self.timeout)
            .map_err(map_io_error)?;

        let mut buf = vec![0u8; (RESPONSE_HEADER_LEN + expected_out_len).max(MIN_RESPONSE_BUF)];
        let (_, len) = slot
            .worker
            .tag_recv(response_tag(req_tag), u64::MAX, &mut buf, self.timeout)
            .map_err(map_io_error)?;
        if len < RESPONSE_HEADER_LEN {
            return Err(ClientError::Transport);
        }
        let resp = ResponseHeader::decode(&buf[..RESPONSE_HEADER_LEN])
            .map_err(|_| ClientError::Transport)?;
        if resp.status != STATUS_OK {
            return Err(ClientError::Status(resp.status));
        }
        if resp.payload_len as usize != len - RESPONSE_HEADER_LEN {
            // Framing desync; the stream state can no longer be trusted.
            return Err(ClientError::Transport);
        }
        Ok(buf[RESPONSE_HEADER_LEN..len].to_vec())
    }

    /// Run one offload operation: pin the connection to its slot, hold the
    /// slot's conn mutex for the entire round trip (ordering guarantee), and
    /// reconnect on demand. Transport errors drop the endpoint so the next
    /// operation reconnects.
    fn operation(
        &self,
        conn_id: u32,
        op: u8,
        payload: &[u8],
        raw_size: u32,
        expected_out_len: usize,
    ) -> Result<Vec<u8>, ClientError> {
        let slot = &self.slots[conn_id as usize % self.slots.len()];
        let mut conn = slot.conn.lock().unwrap_or_else(|p| p.into_inner());
        self.ensure_connected(slot, &mut conn)?;
        match self.round_trip(
            slot,
            &conn,
            conn_id,
            op,
            payload,
            raw_size,
            expected_out_len,
        ) {
            Ok(resp) => Ok(resp),
            Err(e) => {
                if e.drops_endpoint() {
                    conn.ep = None;
                }
                Err(e)
            }
        }
    }
}

// ---------------------------------------------------------------------------
// C ABI. Every body is wrapped in catch_unwind; a panic is reported as
// STATUS_INTERNAL_ERROR and never unwinds into the JVM.

/// Rebuild the client reference for a handle; `None` for 0 (the failure
/// sentinel returned by `neb_open`). The handle must be alive, i.e. not yet
/// passed to `neb_close`.
fn client_ref(handle: c_long) -> Option<&'static Client> {
    if handle == 0 {
        None
    } else {
        Some(unsafe { &*(handle as *const Client) })
    }
}

/// Open an offload client. `addr` is a NUL-terminated "host:port" string.
/// Returns an opaque handle, or 0 on any failure.
#[no_mangle]
pub extern "C" fn neb_open(
    addr: *const c_char,
    workers: c_int,
    level: c_int,
    window_log: c_int,
    max_payload: c_int,
) -> c_long {
    match catch_unwind(AssertUnwindSafe(|| {
        neb_open_impl(addr, workers, level, window_log, max_payload)
    })) {
        Ok(Some(handle)) => handle,
        _ => 0,
    }
}

fn neb_open_impl(
    addr: *const c_char,
    workers: c_int,
    level: c_int,
    window_log: c_int,
    max_payload: c_int,
) -> Option<c_long> {
    if addr.is_null() {
        return None;
    }
    // Safety: the caller passes a valid NUL-terminated string.
    let addr = unsafe { CStr::from_ptr(addr) }.to_str().ok()?;
    // Accepts numeric "host:port" as well as hostnames (first resolved).
    let addr = addr.to_socket_addrs().ok()?.next()?;

    // Mirror the mod's configuration constraints: zstd levels 1..=22,
    // explicit window log 21..=25, positive payload limit.
    if !(1..=22).contains(&level) || !(21..=25).contains(&window_log) || max_payload <= 0 {
        return None;
    }
    let workers = workers.clamp(1, 64) as usize;

    let context = global_context().ok()?;
    let mut slots = Vec::with_capacity(workers);
    for _ in 0..workers {
        let worker = context
            .worker(raw::ucs_thread_mode_t::UCS_THREAD_MODE_SERIALIZED)
            .ok()?;
        slots.push(Arc::new(Slot {
            worker,
            conn: Mutex::new(SlotConn { ep: None, ep_id: 0 }),
        }));
    }
    let client = Client {
        slots,
        addr,
        level,
        window_log: window_log as u32,
        max_payload: max_payload as usize,
        timeout: IO_TIMEOUT,
    };
    // Eagerly connect and handshake every slot, so neb_open fails loudly here
    // instead of surfacing connect errors on the first packet.
    for slot in &client.slots {
        let mut conn = slot.conn.lock().ok()?;
        client.ensure_connected(slot, &mut conn).ok()?;
    }
    Some(Box::into_raw(Box::new(client)) as c_long)
}

/// Streaming compress one aggregated message on the server-side context of
/// `conn_id`. Returns the number of bytes written to `dst`, or <0 on error.
#[no_mangle]
pub extern "C" fn neb_compress(
    handle: c_long,
    conn_id: c_uint,
    src: *const c_void,
    src_len: c_int,
    dst: *mut c_void,
    dst_cap: c_int,
) -> c_int {
    catch_unwind(AssertUnwindSafe(|| {
        compress_impl(handle, conn_id, src, src_len, dst, dst_cap, OP_COMPRESS)
    }))
    .unwrap_or(STATUS_INTERNAL_ERROR)
}

/// Stateless one-shot compress producing a complete frame (for connections
/// whose server-side context reuse is disabled). Same returns as
/// [`neb_compress`].
#[no_mangle]
pub extern "C" fn neb_compress_oneshot(
    handle: c_long,
    conn_id: c_uint,
    src: *const c_void,
    src_len: c_int,
    dst: *mut c_void,
    dst_cap: c_int,
) -> c_int {
    catch_unwind(AssertUnwindSafe(|| {
        compress_impl(
            handle,
            conn_id,
            src,
            src_len,
            dst,
            dst_cap,
            OP_COMPRESS_ONESHOT,
        )
    }))
    .unwrap_or(STATUS_INTERNAL_ERROR)
}

fn compress_impl(
    handle: c_long,
    conn_id: c_uint,
    src: *const c_void,
    src_len: c_int,
    dst: *mut c_void,
    dst_cap: c_int,
    op: u8,
) -> c_int {
    let Some(client) = client_ref(handle) else {
        return ERR_INVALID_ARG;
    };
    if src_len < 0
        || dst_cap < 0
        || (src_len > 0 && src.is_null())
        || (dst_cap > 0 && dst.is_null())
    {
        return ERR_INVALID_ARG;
    }
    let src_len = src_len as usize;
    if src_len > client.max_payload {
        return STATUS_MESSAGE_TOO_LARGE;
    }
    // Safety: the caller guarantees src/dst point to src_len/dst_cap
    // readable/writable bytes; a zero length uses an empty slice instead of
    // the (possibly null) pointer.
    let input: &[u8] = if src_len == 0 {
        &[]
    } else {
        unsafe { slice::from_raw_parts(src as *const u8, src_len) }
    };
    let out = match client.operation(conn_id, op, input, 0, dst_cap as usize) {
        Ok(out) => out,
        Err(e) => return e.code(),
    };
    if out.len() > dst_cap as usize {
        // The reply does not fit the caller's buffer: the request was already
        // processed server-side, so report failure instead of truncating (the
        // Java side must treat the connection as broken).
        return ERR_INVALID_ARG;
    }
    if !out.is_empty() {
        unsafe { ptr::copy_nonoverlapping(out.as_ptr(), dst as *mut u8, out.len()) };
    }
    out.len() as c_int
}

/// Streaming decompress one message on the server-side context of `conn_id`.
/// The server produces exactly `raw_size` bytes (from the S varint of the
/// aggregation wire format). Returns 0 on success, <0 on error.
#[no_mangle]
pub extern "C" fn neb_decompress(
    handle: c_long,
    conn_id: c_uint,
    src: *const c_void,
    src_len: c_int,
    dst: *mut c_void,
    raw_size: c_int,
) -> c_int {
    catch_unwind(AssertUnwindSafe(|| {
        decompress_impl(handle, conn_id, src, src_len, dst, raw_size)
    }))
    .unwrap_or(STATUS_INTERNAL_ERROR)
}

fn decompress_impl(
    handle: c_long,
    conn_id: c_uint,
    src: *const c_void,
    src_len: c_int,
    dst: *mut c_void,
    raw_size: c_int,
) -> c_int {
    let Some(client) = client_ref(handle) else {
        return ERR_INVALID_ARG;
    };
    if src_len < 0
        || raw_size < 0
        || (src_len > 0 && src.is_null())
        || (raw_size > 0 && dst.is_null())
    {
        return ERR_INVALID_ARG;
    }
    let src_len = src_len as usize;
    if src_len > client.max_payload {
        return STATUS_MESSAGE_TOO_LARGE;
    }
    // Safety: same caller contract as compress_impl.
    let input: &[u8] = if src_len == 0 {
        &[]
    } else {
        unsafe { slice::from_raw_parts(src as *const u8, src_len) }
    };
    let out = match client.operation(
        conn_id,
        OP_DECOMPRESS,
        input,
        raw_size as u32,
        raw_size as usize,
    ) {
        Ok(out) => out,
        Err(e) => return e.code(),
    };
    if out.len() != raw_size as usize {
        // The protocol guarantees exactly raw_size bytes back; anything else
        // means the two sides desynced (e.g. after an uncoordinated reset).
        return ERR_TRANSPORT;
    }
    if raw_size > 0 {
        unsafe { ptr::copy_nonoverlapping(out.as_ptr(), dst as *mut u8, raw_size as usize) };
    }
    0
}

/// Drop the server-side zstd contexts of `conn_id` (the game connection was
/// closed or the streams are being reset). Returns 0 on success, <0 on error.
#[no_mangle]
pub extern "C" fn neb_reset_conn(handle: c_long, conn_id: c_uint) -> c_int {
    catch_unwind(AssertUnwindSafe(|| {
        let Some(client) = client_ref(handle) else {
            return ERR_INVALID_ARG;
        };
        match client.operation(conn_id, OP_RESET, &[], 0, 0) {
            Ok(_) => 0,
            Err(e) => e.code(),
        }
    }))
    .unwrap_or(STATUS_INTERNAL_ERROR)
}

/// Close a client handle. Idempotent on 0 (the failure sentinel).
#[no_mangle]
pub extern "C" fn neb_close(handle: c_long) {
    let _ = catch_unwind(AssertUnwindSafe(|| {
        if handle != 0 {
            // Rebuild the box and drop it: endpoints force-close (see
            // Slot::drop), workers are destroyed; the global context stays
            // alive for the process lifetime.
            drop(unsafe { Box::from_raw(handle as *mut Client) });
        }
    }));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn null_handle_is_invalid() {
        assert_eq!(
            neb_compress(0, 0, ptr::null(), 0, ptr::null_mut(), 0),
            ERR_INVALID_ARG
        );
        assert_eq!(
            neb_compress_oneshot(0, 0, ptr::null(), 0, ptr::null_mut(), 0),
            ERR_INVALID_ARG
        );
        assert_eq!(
            neb_decompress(0, 0, ptr::null(), 0, ptr::null_mut(), 0),
            ERR_INVALID_ARG
        );
        assert_eq!(neb_reset_conn(0, 0), ERR_INVALID_ARG);
    }

    #[test]
    fn open_rejects_bad_arguments_without_touching_ucx() {
        // Null address.
        assert_eq!(neb_open(ptr::null(), 4, 3, 23, 8 * 1024 * 1024), 0);
        // Malformed address (no port).
        let bad = b"not-an-address\0";
        assert_eq!(neb_open(bad.as_ptr() as *const c_char, 4, 3, 23, 1024), 0);
        // Out-of-range zstd parameters with a valid address: rejected during
        // validation, before any UCX call or connection attempt.
        let addr = b"127.0.0.1:1\0".as_ptr() as *const c_char;
        assert_eq!(neb_open(addr, 4, 99, 23, 1024), 0);
        assert_eq!(neb_open(addr, 4, 3, 10, 1024), 0);
        assert_eq!(neb_open(addr, 4, 3, 23, 0), 0);
    }

    #[test]
    fn close_accepts_null_handle() {
        neb_close(0); // must be a no-op, not a crash
    }
}
