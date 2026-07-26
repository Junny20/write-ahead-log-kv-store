// wal-kv: a key/value store on top of a write-ahead log and Raft.
//
// Layers, each in its own module. Each only depends on the ones below it:
//   wal    - the durable append-only log
//   store  - the state machine (a map) plus snapshots
//   raft   - election, replication, leader lease
//   rpc    - gRPC schema and services
//   config - node id, peers, timeouts
//
// The WAL stores opaque bytes and knows nothing about Raft; the raft layer
// serialises its LogEntry values into it.

#![forbid(unsafe_code)]

pub mod config;
pub mod raft;
pub mod rpc;
pub mod store;
pub mod wal;

pub use config::{Config, Peer};

use thiserror::Error;

pub type Result<T, E = Error> = std::result::Result<T, E>;

// One error type for the whole crate so `?` works across layers.
#[derive(Debug, Error)]
pub enum Error {
    #[error("i/o error: {0}")]
    Io(#[from] std::io::Error),

    #[error("serialization error: {0}")]
    Encode(#[from] bincode::Error),

    // A CRC failure somewhere other than the tail, i.e. real on-disk corruption
    // rather than a torn write we can recover from.
    #[error("corruption detected: {0}")]
    Corruption(String),

    #[error("wal error: {0}")]
    Wal(String),

    #[error("raft error: {0}")]
    Raft(String),

    #[error("configuration error: {0}")]
    Config(String),

    #[error("transport error: {0}")]
    Transport(String),
}

impl Error {
    pub fn raft(msg: impl Into<String>) -> Self {
        Error::Raft(msg.into())
    }

    pub fn wal(msg: impl Into<String>) -> Self {
        Error::Wal(msg.into())
    }
}
