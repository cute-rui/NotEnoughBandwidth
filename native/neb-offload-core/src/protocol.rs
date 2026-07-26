//! Wire protocol for the NEB remote zstd offload service.
//!
//! The protocol is transport-agnostic. The transport (UCX tag matching) is
//! responsible for delivering, with every request, the 64-bit connection id of
//! the game connection the request belongs to, and for routing the response
//! back to the requesting endpoint. Everything else travels in the payload.
//!
//! Request (header 12 bytes + payload):
//! ```text
//! ┌---------┬---------┬-----------┬----------------┬----------------┬===========···
//! │ op: u8  │ rsv: u8 │ rsv: u16  │ raw_size: u32  │ len: u32       │ payload
//! └---------┴---------┴-----------┴----------------┴----------------┴===========···
//! ```
//! - `raw_size` is only used by OP_DECOMPRESS (expected decompressed size,
//!   taken from the S varint of the NEB aggregation wire format).
//! - OP_HELLO carries a [`Hello`] payload and is used by the server to
//!   announce itself right after accepting an endpoint (see server code).
//!
//! Response (header 8 bytes + payload):
//! ```text
//! ┌-------------┬----------------┬===========···
//! │ status: i32 │ len: u32       │ payload
//! └-------------┴----------------┴===========···
//! ```

use std::io;

pub const REQUEST_HEADER_LEN: usize = 12;
pub const RESPONSE_HEADER_LEN: usize = 8;

/// Server announces itself on a fresh endpoint. Not a request op.
pub const OP_HELLO: u8 = 0;
/// Stateful streaming compress on the per-connection context (ZSTD_e_flush per call).
pub const OP_COMPRESS: u8 = 1;
/// Stateful streaming decompress on the per-connection context.
pub const OP_DECOMPRESS: u8 = 2;
/// One-shot stateless compress (for connections whose context reuse is disabled,
/// see `playersDoNotUseContext` in the mod config).
pub const OP_COMPRESS_ONESHOT: u8 = 3;
/// Drop the per-connection contexts on the server (connection closed).
pub const OP_RESET: u8 = 4;

pub const STATUS_OK: i32 = 0;
pub const STATUS_BAD_REQUEST: i32 = -1;
pub const STATUS_ZSTD_ERROR: i32 = -2;
pub const STATUS_PARAM_MISMATCH: i32 = -3;
pub const STATUS_MESSAGE_TOO_LARGE: i32 = -4;
pub const STATUS_INTERNAL_ERROR: i32 = -5;

// ---------------------------------------------------------------------------
// UCX tag layout: [ep_id: 16][reserved: 15][resp: 1][conn_id: 32]
//
// The tag carries the routing information, so the message payload does not
// have to. `ep_id` is assigned by the server when it accepts an endpoint and
// announced to the client in the HELLO message; the client pins every game
// connection (`conn_id`) to one endpoint, so all requests of a connection
// land on the same server worker and contexts stay pinned to it.
pub const TAG_RESP_BIT: u64 = 1 << 32;
/// Reserved conn_id used by the server-initiated HELLO message.
pub const HELLO_CONN_ID: u32 = u32::MAX;
/// Mask matching a HELLO message regardless of the (unknown yet) ep_id.
pub const HELLO_MASK: u64 = TAG_RESP_BIT | u32::MAX as u64;

pub fn make_request_tag(ep_id: u16, conn_id: u32) -> u64 {
    ((ep_id as u64) << 48) | conn_id as u64
}

pub fn response_tag(request_tag: u64) -> u64 {
    request_tag | TAG_RESP_BIT
}

pub fn hello_tag(ep_id: u16) -> u64 {
    make_request_tag(ep_id, HELLO_CONN_ID) | TAG_RESP_BIT
}

pub fn ep_id_of(tag: u64) -> u16 {
    (tag >> 48) as u16
}

pub fn conn_id_of(tag: u64) -> u32 {
    tag as u32
}

pub fn is_request_tag(tag: u64) -> bool {
    tag & TAG_RESP_BIT == 0
}

/// Magic for the HELLO payload, "NEB1".
pub const HELLO_MAGIC: u32 = 0x4E45_4231;
pub const HELLO_LEN: usize = 16;

pub const FLAG_MAGICLESS: u8 = 1;

/// Server hello announced on every accepted endpoint. The client must verify
/// that the parameters match its own configuration before using the endpoint;
/// a mismatch means the two sides would produce incompatible zstd frames.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Hello {
    pub level: u8,
    pub window_log: u8,
    pub magicless: bool,
    /// Largest request payload the server accepts, in bytes.
    pub max_payload: u32,
}

impl Hello {
    pub fn encode(&self) -> [u8; HELLO_LEN] {
        let mut b = [0u8; HELLO_LEN];
        b[0..4].copy_from_slice(&HELLO_MAGIC.to_le_bytes());
        b[4] = self.level;
        b[5] = self.window_log;
        b[6] = if self.magicless { FLAG_MAGICLESS } else { 0 };
        b[8..12].copy_from_slice(&self.max_payload.to_le_bytes());
        b
    }

    pub fn decode(buf: &[u8]) -> Result<Self, io::Error> {
        if buf.len() < HELLO_LEN {
            return Err(invalid("hello payload too short"));
        }
        let magic = u32::from_le_bytes(buf[0..4].try_into().unwrap());
        if magic != HELLO_MAGIC {
            return Err(invalid("bad hello magic"));
        }
        Ok(Self {
            level: buf[4],
            window_log: buf[5],
            magicless: buf[6] & FLAG_MAGICLESS != 0,
            max_payload: u32::from_le_bytes(buf[8..12].try_into().unwrap()),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RequestHeader {
    pub op: u8,
    pub raw_size: u32,
    pub payload_len: u32,
}

impl RequestHeader {
    pub fn encode(&self) -> [u8; REQUEST_HEADER_LEN] {
        let mut b = [0u8; REQUEST_HEADER_LEN];
        b[0] = self.op;
        b[4..8].copy_from_slice(&self.raw_size.to_le_bytes());
        b[8..12].copy_from_slice(&self.payload_len.to_le_bytes());
        b
    }

    pub fn decode(buf: &[u8]) -> Result<Self, io::Error> {
        if buf.len() < REQUEST_HEADER_LEN {
            return Err(invalid("request header too short"));
        }
        Ok(Self {
            op: buf[0],
            raw_size: u32::from_le_bytes(buf[4..8].try_into().unwrap()),
            payload_len: u32::from_le_bytes(buf[8..12].try_into().unwrap()),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResponseHeader {
    pub status: i32,
    pub payload_len: u32,
}

impl ResponseHeader {
    pub fn encode(&self) -> [u8; RESPONSE_HEADER_LEN] {
        let mut b = [0u8; RESPONSE_HEADER_LEN];
        b[0..4].copy_from_slice(&self.status.to_le_bytes());
        b[4..8].copy_from_slice(&self.payload_len.to_le_bytes());
        b
    }

    pub fn decode(buf: &[u8]) -> Result<Self, io::Error> {
        if buf.len() < RESPONSE_HEADER_LEN {
            return Err(invalid("response header too short"));
        }
        Ok(Self {
            status: i32::from_le_bytes(buf[0..4].try_into().unwrap()),
            payload_len: u32::from_le_bytes(buf[4..8].try_into().unwrap()),
        })
    }
}

/// Build a full response message (header + payload).
pub fn response_message(status: i32, payload: &[u8]) -> Vec<u8> {
    let header = ResponseHeader {
        status,
        payload_len: payload.len() as u32,
    };
    let mut out = Vec::with_capacity(RESPONSE_HEADER_LEN + payload.len());
    out.extend_from_slice(&header.encode());
    out.extend_from_slice(payload);
    out
}

fn invalid(msg: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hello_roundtrip() {
        let hello = Hello {
            level: 3,
            window_log: 23,
            magicless: true,
            max_payload: 8 * 1024 * 1024,
        };
        let decoded = Hello::decode(&hello.encode()).unwrap();
        assert_eq!(hello, decoded);
    }

    #[test]
    fn hello_rejects_bad_magic() {
        let mut buf = Hello {
            level: 3,
            window_log: 23,
            magicless: true,
            max_payload: 1024,
        }
        .encode();
        buf[0] = 0;
        assert!(Hello::decode(&buf).is_err());
    }

    #[test]
    fn request_header_roundtrip() {
        let h = RequestHeader {
            op: OP_DECOMPRESS,
            raw_size: 12345,
            payload_len: 678,
        };
        assert_eq!(h, RequestHeader::decode(&h.encode()).unwrap());
    }

    #[test]
    fn response_message_layout() {
        let msg = response_message(STATUS_OK, &[1, 2, 3]);
        assert_eq!(msg.len(), RESPONSE_HEADER_LEN + 3);
        let h = ResponseHeader::decode(&msg).unwrap();
        assert_eq!(h.status, STATUS_OK);
        assert_eq!(h.payload_len, 3);
        assert_eq!(&msg[RESPONSE_HEADER_LEN..], &[1, 2, 3]);
    }
}
