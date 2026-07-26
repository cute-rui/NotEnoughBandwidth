//! Streaming zstd contexts mirroring the exact semantics of the mod's Java
//! side (`Context.java`, zstd-jni):
//!
//! - magicless frames (`ZSTD_c_format = ZSTD_f_zstd1_magicless`)
//! - no content-size flag (`ZSTD_c_contentSizeFlag = 0`)
//! - explicit window log (`ZSTD_c_windowLog = 21..=25`)
//! - one streaming context per game connection, `ZSTD_e_flush` after every
//!   message so the peer can decode it independently, while the sliding
//!   window keeps history across messages (this is what gives NEB its
//!   compression ratio)
//! - decompression uses a streaming context too (`ZSTD_d_format` magicless);
//!   one-shot compressed messages are complete frames, which a streaming
//!   decoder accepts transparently.

use std::error::Error;
use std::ffi::CStr;
use std::fmt::{Display, Formatter};

use zstd_sys::*;

#[derive(Debug)]
pub struct ZstdError(String);

impl Display for ZstdError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl Error for ZstdError {}

fn check(code: usize) -> Result<usize, ZstdError> {
    if unsafe { ZSTD_isError(code) } != 0 {
        let name = unsafe { CStr::from_ptr(ZSTD_getErrorName(code)) };
        Err(ZstdError(name.to_string_lossy().into_owned()))
    } else {
        Ok(code)
    }
}

/// Maximum compressed size for `src_size` bytes.
pub fn compress_bound(src_size: usize) -> usize {
    unsafe { ZSTD_compressBound(src_size) as usize }
}

/// Per-connection streaming compression context. Not `Send` on purpose: the
/// server pins every connection to the worker thread that accepted its
/// endpoint, so contexts never cross threads.
pub struct CompressStream {
    cctx: *mut ZSTD_CCtx,
}

impl CompressStream {
    pub fn new(level: i32, window_log: u32) -> Result<Self, ZstdError> {
        let cctx = unsafe { ZSTD_createCCtx() };
        if cctx.is_null() {
            return Err(ZstdError("ZSTD_createCCtx failed".into()));
        }
        let result = (|| {
            check(unsafe {
                ZSTD_CCtx_setParameter(cctx, ZSTD_cParameter::ZSTD_c_compressionLevel, level)
            })?;
            check(unsafe {
                ZSTD_CCtx_setParameter(
                    cctx,
                    ZSTD_cParameter::ZSTD_c_windowLog,
                    window_log as std::ffi::c_int,
                )
            })?;
            check(unsafe {
                ZSTD_CCtx_setParameter(
                    cctx,
                    // ZSTD_c_format (stable alias of ZSTD_c_experimentalParam2)
                    ZSTD_cParameter::ZSTD_c_experimentalParam2,
                    ZSTD_format_e::ZSTD_f_zstd1_magicless as std::ffi::c_int,
                )
            })?;
            check(unsafe {
                ZSTD_CCtx_setParameter(cctx, ZSTD_cParameter::ZSTD_c_contentSizeFlag, 0)
            })?;
            Ok(())
        })();
        if let Err(e) = result {
            unsafe { ZSTD_freeCCtx(cctx) };
            return Err(e);
        }
        Ok(Self { cctx })
    }

    /// Compress one aggregated message, flushing afterwards so the peer's
    /// streaming decoder can decode it immediately. `out` must be at least
    /// `compress_bound(input.len())` bytes; returns the number of bytes written.
    pub fn compress(&mut self, input: &[u8], out: &mut [u8]) -> Result<usize, ZstdError> {
        let mut in_buf = ZSTD_inBuffer {
            src: input.as_ptr() as *const std::ffi::c_void,
            size: input.len(),
            pos: 0,
        };
        let mut out_buf = ZSTD_outBuffer {
            dst: out.as_mut_ptr() as *mut std::ffi::c_void,
            size: out.len(),
            pos: 0,
        };
        // e_flush guarantees: all input consumed AND everything so far flushed
        // to output (return value 0) before we are done.
        loop {
            let remaining = check(unsafe {
                ZSTD_compressStream2(
                    self.cctx,
                    &mut out_buf,
                    &mut in_buf,
                    ZSTD_EndDirective::ZSTD_e_flush,
                )
            })? as usize;
            if in_buf.pos == in_buf.size && remaining == 0 {
                break;
            }
            if out_buf.pos == out_buf.size {
                return Err(ZstdError("output buffer too small".into()));
            }
        }
        Ok(out_buf.pos as usize)
    }

    /// One-shot stateless compress producing a complete frame (used for
    /// connections with context reuse disabled). Safe to call repeatedly on
    /// the same context: ZSTD_compress2 keeps no inter-frame state.
    pub fn compress_oneshot(&mut self, input: &[u8], out: &mut [u8]) -> Result<usize, ZstdError> {
        let written = check(unsafe {
            ZSTD_compress2(
                self.cctx,
                out.as_mut_ptr() as *mut std::ffi::c_void,
                out.len(),
                input.as_ptr() as *const std::ffi::c_void,
                input.len(),
            )
        })?;
        Ok(written as usize)
    }

    /// Start a new stream, dropping all history. The peer's decompressor must
    /// be reset too, otherwise the streams desync.
    pub fn reset(&mut self) -> Result<(), ZstdError> {
        check(unsafe { ZSTD_CCtx_reset(self.cctx, ZSTD_ResetDirective::ZSTD_reset_session_only) })?;
        Ok(())
    }
}

impl Drop for CompressStream {
    fn drop(&mut self) {
        unsafe { ZSTD_freeCCtx(self.cctx) };
    }
}

/// Per-connection streaming decompression context.
pub struct DecompressStream {
    dctx: *mut ZSTD_DCtx,
}

impl DecompressStream {
    pub fn new() -> Result<Self, ZstdError> {
        let dctx = unsafe { ZSTD_createDCtx() };
        if dctx.is_null() {
            return Err(ZstdError("ZSTD_createDCtx failed".into()));
        }
        let result = check(unsafe {
            ZSTD_DCtx_setParameter(
                dctx,
                // ZSTD_d_format (stable alias of ZSTD_d_experimentalParam1)
                ZSTD_dParameter::ZSTD_d_experimentalParam1,
                ZSTD_format_e::ZSTD_f_zstd1_magicless as std::ffi::c_int,
            )
        });
        if let Err(e) = result {
            unsafe { ZSTD_freeDCtx(dctx) };
            return Err(e);
        }
        Ok(Self { dctx })
    }

    /// Decompress one message produced by [`CompressStream::compress`].
    /// `out` must be exactly the expected raw size; the produced size is
    /// returned and must equal it (a mismatch means the two sides desynced,
    /// e.g. after an uncoordinated reset).
    pub fn decompress(&mut self, input: &[u8], out: &mut [u8]) -> Result<usize, ZstdError> {
        let mut in_buf = ZSTD_inBuffer {
            src: input.as_ptr() as *const std::ffi::c_void,
            size: input.len(),
            pos: 0,
        };
        let mut out_buf = ZSTD_outBuffer {
            dst: out.as_mut_ptr() as *mut std::ffi::c_void,
            size: out.len(),
            pos: 0,
        };
        while in_buf.pos < in_buf.size {
            check(unsafe { ZSTD_decompressStream(self.dctx, &mut out_buf, &mut in_buf) })?;
            if in_buf.pos < in_buf.size && out_buf.pos == out_buf.size {
                return Err(ZstdError("output buffer too small".into()));
            }
        }
        Ok(out_buf.pos as usize)
    }

    /// [`decompress`](Self::decompress) with the exact-size semantics the
    /// offload protocol requires: the request carries the expected raw size,
    /// so anything but an exact fill of `out` is a desync and an error.
    pub fn decompress_exact(&mut self, input: &[u8], out: &mut [u8]) -> Result<(), ZstdError> {
        let produced = self.decompress(input, out)?;
        if produced != out.len() {
            return Err(ZstdError(format!(
                "decompressed size mismatch: expected {}, got {} (stream desync)",
                out.len(),
                produced
            )));
        }
        // ZSTD_decompressStream may have consumed the whole input into its
        // internal buffer even after the output filled, silently withholding
        // decompressed bytes that would corrupt the next message. Probe with
        // empty input: any further output means the frame produced more than
        // the expected raw size.
        let mut probe = [0u8; 1];
        let mut in_buf = ZSTD_inBuffer {
            src: std::ptr::null(),
            size: 0,
            pos: 0,
        };
        let mut out_buf = ZSTD_outBuffer {
            dst: probe.as_mut_ptr() as *mut std::ffi::c_void,
            size: 1,
            pos: 0,
        };
        check(unsafe { ZSTD_decompressStream(self.dctx, &mut out_buf, &mut in_buf) })?;
        if out_buf.pos != 0 {
            return Err(ZstdError(
                "decompressed data beyond expected size (stream desync)".into(),
            ));
        }
        Ok(())
    }

    pub fn reset(&mut self) -> Result<(), ZstdError> {
        check(unsafe { ZSTD_DCtx_reset(self.dctx, ZSTD_ResetDirective::ZSTD_reset_session_only) })?;
        Ok(())
    }
}

impl Drop for DecompressStream {
    fn drop(&mut self) {
        unsafe { ZSTD_freeDCtx(self.dctx) };
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ZSTD_MAGIC: [u8; 4] = 0xFD2FB528u32.to_le_bytes();

    fn sample_data(n: usize) -> Vec<u8> {
        // Repetitive but not trivial data, similar to serialized game packets.
        let mut v = Vec::with_capacity(n * 64);
        for i in 0..n {
            v.extend_from_slice(
                format!("packet-{{\"id\":{},\"payload\":\"abcdefghij\"}}", i % 7).as_bytes(),
            );
        }
        v
    }

    #[test]
    fn roundtrip_single_message() {
        let raw = sample_data(50);
        let mut cctx = CompressStream::new(3, 23).unwrap();
        let mut compressed = vec![0u8; compress_bound(raw.len())];
        let len = cctx.compress(&raw, &mut compressed).unwrap();
        compressed.truncate(len);

        let mut dctx = DecompressStream::new().unwrap();
        let mut out = vec![0u8; raw.len()];
        let produced = dctx.decompress(&compressed, &mut out).unwrap();
        assert_eq!(produced, raw.len());
        assert_eq!(out, raw);
    }

    #[test]
    fn produces_magicless_frames() {
        let raw = sample_data(10);
        let mut cctx = CompressStream::new(3, 23).unwrap();
        let mut compressed = vec![0u8; compress_bound(raw.len())];
        let len = cctx.compress(&raw, &mut compressed).unwrap();
        assert_ne!(&compressed[..4], &ZSTD_MAGIC, "frame must be magicless");
    }

    /// Deterministic pseudo-random bytes (xorshift), so messages share a large
    /// incompressible base — the realistic case where one-shot compression
    /// cannot help but a reused window can.
    fn prng_bytes(seed: u64, n: usize) -> Vec<u8> {
        let mut state = seed.max(1);
        (0..n)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                (state & 0xff) as u8
            })
            .collect()
    }

    #[test]
    fn streaming_context_reuse_improves_ratio() {
        // Two messages share a 4KB random base (e.g. repeated structures of
        // consecutive aggregated packets) and differ only in a short suffix.
        let base = prng_bytes(42, 4096);
        let mut msg1 = base.clone();
        msg1.extend_from_slice(b"first-message-suffix");
        let mut msg2 = base.clone();
        msg2.extend_from_slice(b"other-message-suffix!");

        let mut cctx = CompressStream::new(3, 23).unwrap();
        let mut dctx = DecompressStream::new().unwrap();

        let mut buf1 = vec![0u8; compress_bound(msg1.len())];
        let len1 = cctx.compress(&msg1, &mut buf1).unwrap();
        let mut buf2 = vec![0u8; compress_bound(msg2.len())];
        let len2 = cctx.compress(&msg2, &mut buf2).unwrap();

        let mut oneshot = CompressStream::new(3, 23).unwrap();
        let mut buf3 = vec![0u8; compress_bound(msg2.len())];
        let len3 = oneshot.compress_oneshot(&msg2, &mut buf3).unwrap();

        assert!(
            len2 < len3 / 4,
            "context reuse should shrink the repeated message: streamed {} vs oneshot {}",
            len2,
            len3
        );

        // Both messages decode correctly through one streaming decoder.
        let mut out = vec![0u8; msg1.len()];
        assert_eq!(
            dctx.decompress(&buf1[..len1], &mut out).unwrap(),
            msg1.len()
        );
        assert_eq!(out, msg1);
        let mut out2 = vec![0u8; msg2.len()];
        assert_eq!(
            dctx.decompress(&buf2[..len2], &mut out2).unwrap(),
            msg2.len()
        );
        assert_eq!(out2, msg2);
    }

    #[test]
    fn oneshot_frames_decode_through_streaming_decoder() {
        // Mirrors the playersDoNotUseContext path: every message is a complete
        // frame, decoded by a streaming DCtx (this is what the Java side does).
        let mut oneshot = CompressStream::new(3, 21).unwrap();
        let mut dctx = DecompressStream::new().unwrap();
        for i in 0..3 {
            let raw = sample_data(20 + i);
            let mut buf = vec![0u8; compress_bound(raw.len())];
            let len = oneshot.compress_oneshot(&raw, &mut buf).unwrap();
            let mut out = vec![0u8; raw.len()];
            assert_eq!(dctx.decompress(&buf[..len], &mut out).unwrap(), raw.len());
            assert_eq!(out, raw);
        }
    }

    #[test]
    fn paired_reset_keeps_stream_working() {
        let raw = sample_data(30);
        let mut cctx = CompressStream::new(3, 23).unwrap();
        let mut dctx = DecompressStream::new().unwrap();

        let mut buf = vec![0u8; compress_bound(raw.len())];
        let len = cctx.compress(&raw, &mut buf).unwrap();
        let mut out = vec![0u8; raw.len()];
        dctx.decompress(&buf[..len], &mut out).unwrap();

        cctx.reset().unwrap();
        dctx.reset().unwrap();

        let len = cctx.compress(&raw, &mut buf).unwrap();
        let mut out = vec![0u8; raw.len()];
        let produced = dctx.decompress(&buf[..len], &mut out).unwrap();
        assert_eq!(produced, raw.len());
        assert_eq!(out, raw);
    }

    #[test]
    fn output_size_mismatch_is_detected() {
        // Simulates desync: the protocol carries the expected raw size, and
        // decompress_exact must fail loudly instead of silently truncating
        // (a short read would also poison the streaming window history).
        let raw = sample_data(80);
        let mut cctx = CompressStream::new(3, 23).unwrap();
        let mut compressed = vec![0u8; compress_bound(raw.len())];
        let len = cctx.compress(&raw, &mut compressed).unwrap();

        let mut dctx = DecompressStream::new().unwrap();
        let mut short = vec![0u8; raw.len() / 2];
        assert!(dctx
            .decompress_exact(&compressed[..len], &mut short)
            .is_err());

        // And the exact-size path succeeds.
        let mut dctx2 = DecompressStream::new().unwrap();
        let mut exact = vec![0u8; raw.len()];
        dctx2
            .decompress_exact(&compressed[..len], &mut exact)
            .unwrap();
        assert_eq!(exact, raw);
    }
}
