//! Streaming zstd contexts mirroring the exact semantics of the mod's Java
//! side (`Context.java`, zstd-jni):
//!
//! - magicless frames (`ZSTD_c_format = ZSTD_f_zstd1_magicless`)
//! - no content-size flag (`ZSTD_c_contentSizeFlag = 0`)
//! - explicit window log (`ZSTD_c_windowLog = 21..=25`)
//! - one streaming context per game connection, flushed after every message
//!   so the peer can decode it immediately, while the sliding window keeps
//!   history across messages (this is what gives NEB its compression ratio)
//! - decompression uses a streaming context too; one-shot compressed messages
//!   are complete frames, which a streaming decoder accepts transparently
//!
//! Implemented on the high-level `zstd` crate (`stream::raw` for the
//! streaming contexts, `bulk` for one-shot); the magicless `Format` parameter
//! requires the crate's `experimental` feature.

use std::error::Error;
use std::fmt::{Display, Formatter};

use zstd::stream::raw::{Decoder, Encoder, InBuffer, Operation, OutBuffer};
use zstd::zstd_safe::{CParameter, DParameter, FrameFormat};

#[derive(Debug)]
pub struct ZstdError(String);

impl Display for ZstdError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl Error for ZstdError {}

fn map_err(op: &str, e: std::io::Error) -> ZstdError {
    ZstdError(format!("{op}: {e}"))
}

/// Maximum compressed size for `src_size` bytes.
pub fn compress_bound(src_size: usize) -> usize {
    zstd::zstd_safe::compress_bound(src_size)
}

/// Per-connection streaming compression context. Not `Send` on purpose: the
/// server pins every connection to the worker thread that accepted its
/// endpoint, so contexts never cross threads.
pub struct CompressStream {
    encoder: Encoder<'static>,
    level: i32,
    window_log: u32,
}

impl CompressStream {
    pub fn new(level: i32, window_log: u32) -> Result<Self, ZstdError> {
        Ok(Self {
            encoder: Self::build_encoder(level, window_log)?,
            level,
            window_log,
        })
    }

    fn build_encoder(level: i32, window_log: u32) -> Result<Encoder<'static>, ZstdError> {
        let mut encoder = Encoder::new(level).map_err(|e| map_err("create encoder", e))?;
        encoder
            .set_parameter(CParameter::WindowLog(window_log))
            .map_err(|e| map_err("set windowLog", e))?;
        encoder
            .set_parameter(CParameter::Format(FrameFormat::Magicless))
            .map_err(|e| map_err("set magicless format", e))?;
        encoder
            .set_parameter(CParameter::ContentSizeFlag(false))
            .map_err(|e| map_err("disable content size", e))?;
        Ok(encoder)
    }

    /// Compress one aggregated message, flushing afterwards so the peer's
    /// streaming decoder can decode it immediately. `out` must be at least
    /// `compress_bound(input.len())` bytes; returns the number of bytes written.
    pub fn compress(&mut self, input: &[u8], out: &mut [u8]) -> Result<usize, ZstdError> {
        let mut in_buf = InBuffer::around(input);
        let mut out_buf = OutBuffer::around(out);
        // Feed all input...
        while in_buf.pos() < in_buf.src.len() {
            self.encoder
                .run(&mut in_buf, &mut out_buf)
                .map_err(|e| map_err("compress", e))?;
            if in_buf.pos() < in_buf.src.len() && out_buf.pos() == out_buf.capacity() {
                return Err(ZstdError("output buffer too small".into()));
            }
        }
        // ...then flush until everything so far is decodable by the peer.
        loop {
            let remaining = self
                .encoder
                .flush(&mut out_buf)
                .map_err(|e| map_err("flush", e))?;
            if remaining == 0 {
                break;
            }
            if out_buf.pos() == out_buf.capacity() {
                return Err(ZstdError("output buffer too small".into()));
            }
        }
        Ok(out_buf.pos())
    }

    /// One-shot stateless compress producing a complete frame (used for
    /// connections with context reuse disabled). A fresh bulk compressor is
    /// used per call, so no state carries over between messages.
    pub fn compress_oneshot(&mut self, input: &[u8], out: &mut [u8]) -> Result<usize, ZstdError> {
        let mut compressor =
            zstd::bulk::Compressor::new(self.level).map_err(|e| map_err("create compressor", e))?;
        compressor
            .set_parameter(CParameter::WindowLog(self.window_log))
            .map_err(|e| map_err("set windowLog", e))?;
        compressor
            .set_parameter(CParameter::Format(FrameFormat::Magicless))
            .map_err(|e| map_err("set magicless format", e))?;
        compressor
            .set_parameter(CParameter::ContentSizeFlag(false))
            .map_err(|e| map_err("disable content size", e))?;
        compressor
            .compress_to_buffer(input, out)
            .map_err(|e| map_err("oneshot compress", e))
    }

    /// Start a new stream, dropping all history. The peer's decompressor must
    /// be reset too, otherwise the streams desync.
    pub fn reset(&mut self) -> Result<(), ZstdError> {
        self.encoder = Self::build_encoder(self.level, self.window_log)?;
        Ok(())
    }
}

/// Per-connection streaming decompression context.
pub struct DecompressStream {
    decoder: Decoder<'static>,
}

impl DecompressStream {
    pub fn new() -> Result<Self, ZstdError> {
        Ok(Self {
            decoder: Self::build_decoder()?,
        })
    }

    fn build_decoder() -> Result<Decoder<'static>, ZstdError> {
        let mut decoder = Decoder::new().map_err(|e| map_err("create decoder", e))?;
        decoder
            .set_parameter(DParameter::Format(FrameFormat::Magicless))
            .map_err(|e| map_err("set magicless format", e))?;
        Ok(decoder)
    }

    /// Decompress one message produced by [`CompressStream::compress`].
    /// `out` must be exactly the expected raw size; the produced size is
    /// returned and must equal it (a mismatch means the two sides desynced,
    /// e.g. after an uncoordinated reset).
    pub fn decompress(&mut self, input: &[u8], out: &mut [u8]) -> Result<usize, ZstdError> {
        let mut in_buf = InBuffer::around(input);
        let mut out_buf = OutBuffer::around(out);
        // Run at least once, even with empty input: decompress_exact relies on
        // an empty-input run to flush bytes withheld in the decoder.
        loop {
            self.decoder
                .run(&mut in_buf, &mut out_buf)
                .map_err(|e| map_err("decompress", e))?;
            if in_buf.pos() >= in_buf.src.len() {
                break;
            }
            if out_buf.pos() == out_buf.capacity() {
                return Err(ZstdError("output buffer too small".into()));
            }
        }
        Ok(out_buf.pos())
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
        // The decoder may have consumed the whole input into its internal
        // buffer even after the output filled, silently withholding
        // decompressed bytes that would corrupt the next message. Probe with
        // empty input: any further output means the frame produced more than
        // the expected raw size.
        let mut probe = [0u8; 1];
        let withheld = self.decompress(&[], &mut probe)?;
        if withheld != 0 {
            return Err(ZstdError(
                "decompressed data beyond expected size (stream desync)".into(),
            ));
        }
        Ok(())
    }

    pub fn reset(&mut self) -> Result<(), ZstdError> {
        self.decoder = Self::build_decoder()?;
        Ok(())
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
        assert!(len >= 4, "magicless frame should still have content");
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
