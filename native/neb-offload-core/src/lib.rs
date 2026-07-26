//! Shared, transport-independent pieces of the NEB remote zstd offload
//! service: the wire protocol and the streaming zstd contexts that mirror the
//! mod's zstd-jni usage.

pub mod protocol;
pub mod zstream;

pub use protocol::*;
pub use zstream::{compress_bound, CompressStream, DecompressStream, ZstdError};
