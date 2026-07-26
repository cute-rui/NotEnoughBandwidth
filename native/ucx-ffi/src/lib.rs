//! Raw UCX UCP bindings plus a minimal safe wrapper used by the NEB offload
//! client and server. Only the tag-matching subset of UCP is wrapped.

#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(dead_code)]

pub mod raw {
    include!(concat!(env!("OUT_DIR"), "/bindings.rs"));
}

pub mod ucp;
