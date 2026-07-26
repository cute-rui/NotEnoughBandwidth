//! Minimal safe-ish wrapper over the UCX UCP API, covering exactly what the
//! NEB offload service needs:
//!
//! - client-server connection flow ([`Worker::listen`] / [`Worker::connect`]
//!   / [`Worker::accept`])
//! - blocking tag-matched send/receive with timeout ([`Worker::tag_send`],
//!   [`Worker::tag_recv`], [`Worker::tag_probe`])
//!
//! Threading model: a [`Worker`] may be moved across threads but must be
//! *driven* by one thread at a time (callers serialize externally; the client
//! library requests `UCS_THREAD_MODE_SERIALIZED`, the server
//! `UCS_THREAD_MODE_SINGLE`).
//!
//! API notes (verified against UCX master `ucp.h`/`ucp_def.h`):
//! - the legacy `ucp_tag_send_nb`/`ucp_tag_recv_nb` were removed from UCX
//!   master; the `nbx` variants exist since UCX 1.10 and are used everywhere
//! - `ucp_dt_make_contig(1)` == `(1 << UCP_DATATYPE_SHIFT) | UCP_DATATYPE_CONTIG`
//!   == 8 (shift is 3, contiguous class is 0)

use std::cell::Cell;
use std::error::Error as StdError;
use std::ffi::{c_void, CStr};
use std::fmt::{Display, Formatter};
use std::net::SocketAddr;
use std::ptr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::raw;

/// `ucp_dt_make_contig(1)` — see module docs.
const DT_CONTIG_BYTE: raw::ucp_datatype_t = (1u64 << 3) | 0;

#[derive(Debug)]
pub struct Error {
    pub status: Option<i32>,
    pub message: String,
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self.status {
            Some(s) => write!(f, "{} (ucx status {})", self.message, s),
            None => f.write_str(&self.message),
        }
    }
}

impl StdError for Error {}

fn status_error(op: &str, status: raw::ucs_status_t) -> Error {
    let text = unsafe { CStr::from_ptr(raw::ucs_status_string(status)) };
    Error {
        status: Some(status as i32),
        message: format!("{} failed: {}", op, text.to_string_lossy()),
    }
}

/// Status pointers returned by nbx functions encode error statuses as small
/// negative values; recover the enum. bindgen gives `ucs_status_t` an 8-bit
/// repr (its values span UCS_OK..UCS_ERR_LAST, i.e. 1..=-100, which fits i8),
/// so transmute through i8.
fn ptr_to_status(ptr: *mut c_void) -> raw::ucs_status_t {
    unsafe { std::mem::transmute::<i8, raw::ucs_status_t>(ptr as i8) }
}

fn is_err_ptr(ptr: *mut c_void) -> bool {
    (ptr as isize) < 0 && (ptr as isize) >= raw::ucs_status_t::UCS_ERR_LAST as isize
}

fn plain_error(msg: impl Into<String>) -> Error {
    Error {
        status: None,
        message: msg.into(),
    }
}

/// Convert a `SocketAddr` into an owned `sockaddr` pair for `ucs_sock_addr_t`.
struct SockAddrHolder {
    storage: Box<raw::sockaddr_storage>,
    addrlen: u32,
}

impl SockAddrHolder {
    fn new(addr: &SocketAddr) -> Self {
        let mut storage: Box<raw::sockaddr_storage> = Box::new(unsafe { std::mem::zeroed() });
        let addrlen = match addr {
            SocketAddr::V4(v4) => {
                let sa = unsafe { &mut *(storage.as_mut() as *mut _ as *mut libc::sockaddr_in) };
                sa.sin_family = libc::AF_INET as u16;
                sa.sin_port = v4.port().to_be();
                sa.sin_addr = libc::in_addr {
                    s_addr: u32::from_ne_bytes(v4.ip().octets()),
                };
                std::mem::size_of::<libc::sockaddr_in>() as u32
            }
            SocketAddr::V6(v6) => {
                let sa = unsafe { &mut *(storage.as_mut() as *mut _ as *mut libc::sockaddr_in6) };
                sa.sin6_family = libc::AF_INET6 as u16;
                sa.sin6_port = v6.port().to_be();
                sa.sin6_flowinfo = v6.flowinfo();
                sa.sin6_scope_id = v6.scope_id();
                sa.sin6_addr = libc::in6_addr {
                    s6_addr: v6.ip().octets(),
                };
                std::mem::size_of::<libc::sockaddr_in6>() as u32
            }
        };
        Self { storage, addrlen }
    }

    fn as_ucs_sock_addr(&self) -> raw::ucs_sock_addr_t {
        raw::ucs_sock_addr_t {
            addr: self.storage.as_ref() as *const raw::sockaddr_storage as *const raw::sockaddr,
            addrlen: self.addrlen,
        }
    }
}

/// Completion state shared between an nbx callback and [`Worker::wait`].
/// Lives on the caller's stack; [`Worker::wait`] guarantees the callback has
/// fired (even after a timeout, via cancel) before it returns.
struct OpState {
    done: AtomicBool,
    status: Cell<raw::ucs_status_t>,
}

impl OpState {
    fn new() -> Self {
        Self {
            done: AtomicBool::new(false),
            status: Cell::new(raw::ucs_status_t::UCS_OK),
        }
    }
}

unsafe extern "C" fn op_completed_cb(
    _request: *mut c_void,
    status: raw::ucs_status_t,
    user_data: *mut c_void,
) {
    let state = &*(user_data as *const OpState);
    state.status.set(status);
    state.done.store(true, Ordering::Release);
}

unsafe extern "C" fn recv_completed_cb(
    _request: *mut c_void,
    status: raw::ucs_status_t,
    _tag_info: *const raw::ucp_tag_recv_info_t,
    user_data: *mut c_void,
) {
    op_completed_cb(_request, status, user_data);
}

pub struct Context {
    handle: raw::ucp_context_h,
}

/// `ucp_context` is documented as thread-safe for worker creation; each
/// `Worker` enforces its own threading discipline.
unsafe impl Send for Context {}
unsafe impl Sync for Context {}

impl Context {
    pub fn new() -> Result<Arc<Self>, Error> {
        unsafe {
            let mut config: *mut raw::ucp_config_t = ptr::null_mut();
            let status = raw::ucp_config_read(ptr::null(), ptr::null(), &mut config);
            if status != raw::ucs_status_t::UCS_OK {
                return Err(status_error("ucp_config_read", status));
            }
            let mut params: raw::ucp_params_t = std::mem::zeroed();
            params.field_mask = raw::ucp_params_field::UCP_PARAM_FIELD_FEATURES as u64;
            params.features = raw::ucp_feature::UCP_FEATURE_TAG as u64;
            let mut handle: raw::ucp_context_h = ptr::null_mut();
            // ucp_init is a `static inline` wrapper in ucp.h since UCX 1.15
            // (bindgen cannot bind it); call the real entry point directly,
            // exactly as the inline does.
            let status = raw::ucp_init_version(
                raw::UCP_API_MAJOR as _,
                raw::UCP_API_MINOR as _,
                &params,
                config,
                &mut handle,
            );
            raw::ucp_config_release(config);
            if status != raw::ucs_status_t::UCS_OK {
                return Err(status_error("ucp_init", status));
            }
            Ok(Arc::new(Self { handle }))
        }
    }

    pub fn worker(self: &Arc<Self>, thread_mode: raw::ucs_thread_mode_t) -> Result<Worker, Error> {
        unsafe {
            let mut params: raw::ucp_worker_params_t = std::mem::zeroed();
            params.field_mask =
                raw::ucp_worker_params_field::UCP_WORKER_PARAM_FIELD_THREAD_MODE as u64;
            params.thread_mode = thread_mode;
            let mut handle: raw::ucp_worker_h = ptr::null_mut();
            let status = raw::ucp_worker_create(self.handle, &params, &mut handle);
            if status != raw::ucs_status_t::UCS_OK {
                return Err(status_error("ucp_worker_create", status));
            }
            Ok(Worker {
                handle,
                context: Arc::clone(self),
            })
        }
    }
}

impl Drop for Context {
    fn drop(&mut self) {
        unsafe { raw::ucp_cleanup(self.handle) }
    }
}

pub struct Worker {
    handle: raw::ucp_worker_h,
    #[allow(dead_code)]
    context: Arc<Context>,
}

unsafe impl Send for Worker {}

impl Worker {
    /// Drive communication progress; returns the number of events handled.
    pub fn progress(&self) -> u32 {
        unsafe { raw::ucp_worker_progress(self.handle) as u32 }
    }

    /// Start listening for client connections. `on_conn` is invoked from
    /// inside [`progress`](Self::progress) on this worker, so the listening
    /// worker must be progressed regularly (the server does this on its
    /// accept thread).
    pub fn listen(
        &self,
        addr: &SocketAddr,
        on_conn: Box<dyn FnMut(ConnRequest) + Send>,
    ) -> Result<Listener, Error> {
        let holder = SockAddrHolder::new(addr);
        let cb: Box<Box<dyn FnMut(ConnRequest) + Send>> = Box::new(on_conn);
        let cb_raw = Box::into_raw(cb);
        unsafe {
            let mut params: raw::ucp_listener_params_t = std::mem::zeroed();
            params.field_mask = raw::ucp_listener_params_field::UCP_LISTENER_PARAM_FIELD_SOCK_ADDR
                as u64
                | raw::ucp_listener_params_field::UCP_LISTENER_PARAM_FIELD_CONN_HANDLER as u64;
            params.sockaddr = holder.as_ucs_sock_addr();
            params.conn_handler = raw::ucp_listener_conn_handler_t {
                cb: Some(conn_request_trampoline),
                arg: cb_raw as *mut c_void,
            };
            let mut handle: raw::ucp_listener_h = ptr::null_mut();
            let status = raw::ucp_listener_create(self.handle, &params, &mut handle);
            if status != raw::ucs_status_t::UCS_OK {
                drop(Box::from_raw(cb_raw));
                return Err(status_error("ucp_listener_create", status));
            }
            Ok(Listener {
                handle,
                _callback: Box::from_raw(cb_raw),
            })
        }
    }

    /// Connect to a listening server (client side). The endpoint is usable
    /// immediately; connection establishment proceeds in the background and
    /// any failure surfaces as an error on the first in-flight operation.
    pub fn connect(&self, addr: &SocketAddr) -> Result<Endpoint, Error> {
        let holder = SockAddrHolder::new(addr);
        unsafe {
            let mut params: raw::ucp_ep_params_t = std::mem::zeroed();
            params.field_mask = raw::ucp_ep_params_field::UCP_EP_PARAM_FIELD_SOCK_ADDR as u64
                | raw::ucp_ep_params_field::UCP_EP_PARAM_FIELD_FLAGS as u64;
            params.flags = raw::ucp_ep_params_flags_field::UCP_EP_PARAMS_FLAGS_CLIENT_SERVER as u32;
            params.sockaddr = holder.as_ucs_sock_addr();
            self.create_ep(&params)
        }
    }

    /// Accept an incoming connection request (server side), creating the
    /// endpoint on *this* worker. UCX allows creating the endpoint on any
    /// worker of the same context as long as the caller synchronizes access —
    /// the server hands requests to worker threads through a channel.
    pub fn accept(&self, request: ConnRequest) -> Result<Endpoint, Error> {
        unsafe {
            let mut params: raw::ucp_ep_params_t = std::mem::zeroed();
            params.field_mask = raw::ucp_ep_params_field::UCP_EP_PARAM_FIELD_CONN_REQUEST as u64;
            params.conn_request = request.0;
            self.create_ep(&params)
        }
    }

    unsafe fn create_ep(&self, params: &raw::ucp_ep_params_t) -> Result<Endpoint, Error> {
        let mut handle: raw::ucp_ep_h = ptr::null_mut();
        let status = raw::ucp_ep_create(self.handle, params, &mut handle);
        if status != raw::ucs_status_t::UCS_OK {
            return Err(status_error("ucp_ep_create", status));
        }
        Ok(Endpoint {
            handle,
            worker: self.handle,
        })
    }

    /// Probe for an already-received tag message without consuming it.
    /// Returns `(sender_tag, length)` when a matching message is present.
    /// Must be followed by [`tag_recv`](Self::tag_recv) to actually take it.
    pub fn tag_probe(&self, tag: u64, tag_mask: u64) -> Option<(u64, usize)> {
        unsafe {
            let mut info: raw::ucp_tag_recv_info_t = std::mem::zeroed();
            let msg = raw::ucp_tag_probe_nb(self.handle, tag, tag_mask, 0, &mut info);
            if msg.is_null() {
                None
            } else {
                Some((info.sender_tag, info.length))
            }
        }
    }

    /// Blocking tagged receive. Fills `buf` with the whole message and
    /// returns `(sender_tag, received_len)`. Fails if the message is larger
    /// than `buf`, on transport error, or on `timeout` (the request is then
    /// cancelled and drained before returning).
    pub fn tag_recv(
        &self,
        tag: u64,
        tag_mask: u64,
        buf: &mut [u8],
        timeout: Duration,
    ) -> Result<(u64, usize), Error> {
        let state = OpState::new();
        let mut info: raw::ucp_tag_recv_info_t = unsafe { std::mem::zeroed() };
        let mut params: raw::ucp_request_param_t = unsafe { std::mem::zeroed() };
        params.op_attr_mask = raw::ucp_op_attr_t::UCP_OP_ATTR_FIELD_CALLBACK as u32
            | raw::ucp_op_attr_t::UCP_OP_ATTR_FIELD_USER_DATA as u32
            | raw::ucp_op_attr_t::UCP_OP_ATTR_FIELD_DATATYPE as u32
            | raw::ucp_op_attr_t::UCP_OP_ATTR_FIELD_RECV_INFO as u32;
        params.cb.recv = Some(recv_completed_cb);
        params.datatype = DT_CONTIG_BYTE;
        params.user_data = &state as *const OpState as *mut c_void;
        params.recv_info.tag_info = &mut info;

        let request = unsafe {
            raw::ucp_tag_recv_nbx(
                self.handle,
                buf.as_mut_ptr() as *mut c_void,
                buf.len(),
                tag,
                tag_mask,
                &params,
            )
        };
        self.wait("ucp_tag_recv_nbx", request, &state, timeout)?;
        if info.length > buf.len() {
            return Err(plain_error(format!(
                "received message ({} bytes) larger than buffer ({} bytes)",
                info.length,
                buf.len()
            )));
        }
        Ok((info.sender_tag, info.length))
    }

    /// Blocking tagged send. Returns once the buffer has been consumed by the
    /// transport, i.e. `buf` is reusable on return.
    pub fn tag_send(
        &self,
        ep: &Endpoint,
        tag: u64,
        buf: &[u8],
        timeout: Duration,
    ) -> Result<(), Error> {
        let state = OpState::new();
        let mut params: raw::ucp_request_param_t = unsafe { std::mem::zeroed() };
        params.op_attr_mask = raw::ucp_op_attr_t::UCP_OP_ATTR_FIELD_CALLBACK as u32
            | raw::ucp_op_attr_t::UCP_OP_ATTR_FIELD_USER_DATA as u32
            | raw::ucp_op_attr_t::UCP_OP_ATTR_FIELD_DATATYPE as u32;
        params.cb.send = Some(op_completed_cb);
        params.datatype = DT_CONTIG_BYTE;
        params.user_data = &state as *const OpState as *mut c_void;

        let request = unsafe {
            raw::ucp_tag_send_nbx(
                ep.handle,
                buf.as_ptr() as *const c_void,
                buf.len(),
                tag,
                &params,
            )
        };
        self.wait("ucp_tag_send_nbx", request, &state, timeout)
    }

    /// Shared completion pump for nbx operations: drives progress until the
    /// operation's callback has fired. On timeout the request is cancelled
    /// and the pump keeps running until the callback fired, so the on-stack
    /// `OpState` is never referenced after return.
    fn wait(
        &self,
        op: &str,
        request: *mut c_void,
        state: &OpState,
        timeout: Duration,
    ) -> Result<(), Error> {
        if request.is_null() {
            // Immediate completion.
            return Ok(());
        }
        if is_err_ptr(request) {
            return Err(status_error(op, ptr_to_status(request)));
        }
        let deadline = Instant::now() + timeout;
        let mut timed_out = false;
        loop {
            self.progress();
            if state.done.load(Ordering::Acquire) {
                break;
            }
            if !timed_out && Instant::now() >= deadline {
                timed_out = true;
                unsafe { raw::ucp_request_cancel(self.handle, request) };
            }
        }
        unsafe { raw::ucp_request_free(request) };
        if timed_out {
            return Err(plain_error(format!("{} timed out", op)));
        }
        let status = state.status.get();
        if status != raw::ucs_status_t::UCS_OK {
            return Err(status_error(op, status));
        }
        Ok(())
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        unsafe { raw::ucp_worker_destroy(self.handle) }
    }
}

/// Handle of an incoming connection request; pass to [`Worker::accept`].
pub struct ConnRequest(raw::ucp_conn_request_h);
unsafe impl Send for ConnRequest {}

pub struct Listener {
    handle: raw::ucp_listener_h,
    /// Keeps the boxed Rust callback alive for the listener's lifetime.
    _callback: Box<Box<dyn FnMut(ConnRequest) + Send>>,
}

unsafe impl Send for Listener {}

impl Drop for Listener {
    fn drop(&mut self) {
        unsafe { raw::ucp_listener_destroy(self.handle) }
    }
}

unsafe extern "C" fn conn_request_trampoline(request: raw::ucp_conn_request_h, arg: *mut c_void) {
    let callback = &mut *(arg as *mut Box<dyn FnMut(ConnRequest) + Send>);
    callback(ConnRequest(request));
}

pub struct Endpoint {
    handle: raw::ucp_ep_h,
    worker: raw::ucp_worker_h,
}

unsafe impl Send for Endpoint {}

impl Drop for Endpoint {
    fn drop(&mut self) {
        // Force-close: releases the endpoint without waiting for the peer;
        // outstanding operations complete with UCS_ERR_CANCELED.
        unsafe {
            let mut params: raw::ucp_request_param_t = std::mem::zeroed();
            params.op_attr_mask = raw::ucp_op_attr_t::UCP_OP_ATTR_FIELD_FLAGS as u32;
            params.flags = raw::ucp_ep_close_flags_t::UCP_EP_CLOSE_FLAG_FORCE as u32;
            let request = raw::ucp_ep_close_nbx(self.handle, &params);
            if request.is_null() || is_err_ptr(request) {
                return;
            }
            // Pump until the close completes; bounded so a wedged transport
            // cannot hang shutdown (the request is leaked then, which is
            // acceptable on a dying endpoint).
            let deadline = Instant::now() + Duration::from_secs(2);
            loop {
                raw::ucp_worker_progress(self.worker);
                let status = raw::ucp_request_check_status(request);
                if status != raw::ucs_status_t::UCS_INPROGRESS || Instant::now() >= deadline {
                    break;
                }
            }
            raw::ucp_request_free(request);
        }
    }
}
