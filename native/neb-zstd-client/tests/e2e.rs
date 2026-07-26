//! End-to-end test: a real UCX client ↔ server round trip.
//!
//! Skipped unless `NEB_E2E=1` is set (CI sets it; the dev machine has no UCX).
//! CI selects non-RDMA transports with `UCX_TLS=tcp,self`, since runners have
//! no RDMA NIC — this exercises the full protocol and both binaries, leaving
//! only the RDMA-specific transport paths to manual testing on real hardware.
//!
//! Requires `neb-zstd-server` already built into the workspace target dir
//! (`cargo build --workspace` first).

use std::io::{BufRead, BufReader};
use std::net::TcpListener;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::time::Duration;

use neb_offload_core::compress_bound;
use neb_zstd_client::*;

const MAX_PAYLOAD: i32 = 8 * 1024 * 1024;
const ZSTD_MAGIC: [u8; 4] = 0xFD2FB528u32.to_le_bytes();

struct ServerGuard(Child);

impl Drop for ServerGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn server_binary() -> PathBuf {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let debug = manifest.join("../target/debug/neb-zstd-server");
    assert!(
        debug.exists(),
        "neb-zstd-server not found at {debug:?}; run `cargo build --workspace` first"
    );
    debug.canonicalize().unwrap()
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// Spawn the server and block until it prints its "listening on" line.
fn start_server(port: u16) -> ServerGuard {
    let mut child = Command::new(server_binary())
        .args(["--listen", &format!("127.0.0.1:{port}"), "--threads", "2"])
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("failed to spawn neb-zstd-server");
    let stdout = child.stdout.take().unwrap();
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        for line in BufReader::new(stdout).lines() {
            let Ok(line) = line else { break };
            // Forward server logs so CI output shows server-side failures.
            eprintln!("[server] {line}");
            if line.contains("listening on") {
                let _ = tx.send(());
            }
        }
    });
    rx.recv_timeout(Duration::from_secs(15))
        .expect("server did not report listening within 15s");
    ServerGuard(child)
}

fn c_string(s: &str) -> Vec<u8> {
    let mut v = s.as_bytes().to_vec();
    v.push(0);
    v
}

/// Repetitive, highly compressible sample resembling aggregated packet data.
fn sample(n_records: usize) -> Vec<u8> {
    let mut v = Vec::new();
    for i in 0..n_records {
        v.extend_from_slice(
            format!("packet-{{\"id\":{},\"payload\":\"abcdefghij\"}}", i % 7).as_bytes(),
        );
    }
    v
}

fn compress(handle: i64, conn: u32, raw: &[u8]) -> Vec<u8> {
    let mut dst = vec![0u8; compress_bound(raw.len())];
    let n = neb_compress(
        handle,
        conn,
        raw.as_ptr() as *const _,
        raw.len() as i32,
        dst.as_mut_ptr() as *mut _,
        dst.len() as i32,
    );
    assert!(n >= 0, "neb_compress failed with {n}");
    dst.truncate(n as usize);
    dst
}

fn decompress(handle: i64, conn: u32, compressed: &[u8], raw_size: usize) -> Vec<u8> {
    let mut dst = vec![0u8; raw_size];
    let rc = neb_decompress(
        handle,
        conn,
        compressed.as_ptr() as *const _,
        compressed.len() as i32,
        dst.as_mut_ptr() as *mut _,
        raw_size as i32,
    );
    assert_eq!(rc, 0, "neb_decompress failed with {rc}");
    dst
}

#[test]
fn end_to_end_round_trip() {
    if std::env::var("NEB_E2E").as_deref() != Ok("1") {
        eprintln!("skipped: set NEB_E2E=1 to run the UCX end-to-end test");
        return;
    }

    let port = free_port();
    let _server = start_server(port);
    let addr = c_string(&format!("127.0.0.1:{port}"));

    // Parameter handshake: a mismatched zstd level must be refused at open.
    let bad = neb_open(addr.as_ptr() as *const _, 1, 5, 23, MAX_PAYLOAD);
    assert_eq!(bad, 0, "handshake must reject mismatched zstd level");

    let handle = neb_open(addr.as_ptr() as *const _, 2, 3, 23, MAX_PAYLOAD);
    assert_ne!(handle, 0, "neb_open failed");

    let raw_a = sample(2000);
    let raw_b = sample(1500);

    // --- conn 7: streaming compress with context reuse over the wire ---
    let c1 = compress(handle, 7, &raw_a);
    assert_ne!(&c1[..4], &ZSTD_MAGIC, "frames must be magicless");
    let c2 = compress(handle, 7, &raw_a);
    assert!(
        c2.len() < c1.len() / 2,
        "streaming context reuse should shrink the repeated message: {} -> {}",
        c1.len(),
        c2.len()
    );
    // The same connection's streaming decoder reads both in order.
    assert_eq!(decompress(handle, 7, &c1, raw_a.len()), raw_a);
    assert_eq!(decompress(handle, 7, &c2, raw_a.len()), raw_a);

    // --- conn 9: one-shot stateless frames through a fresh decoder ---
    let mut dst = vec![0u8; compress_bound(raw_b.len())];
    let n = neb_compress_oneshot(
        handle,
        9,
        raw_b.as_ptr() as *const _,
        raw_b.len() as i32,
        dst.as_mut_ptr() as *mut _,
        dst.len() as i32,
    );
    assert!(n >= 0, "neb_compress_oneshot failed with {n}");
    assert_eq!(
        decompress(handle, 9, &dst[..n as usize], raw_b.len()),
        raw_b
    );

    // --- reset drops server-side contexts ---
    assert_eq!(neb_reset_conn(handle, 7), 0, "neb_reset_conn failed");

    neb_close(handle);
}
