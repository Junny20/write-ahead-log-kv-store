// The gRPC surface: a client-facing Kv service, an internal Raft service, and the peer
// client that implements raft::Transport. The schema is in proto/kv.proto, compiled by
// build.rs into pb below. Everything here is thin glue between protobuf and the raft
// core's plain structs.

pub mod service;

// Code generated from proto/kv.proto by tonic-build (see build.rs).
pub mod pb {
    tonic::include_proto!("waldkv");
}

pub use service::{KvService, PeerTransport, RaftService};
