// Shared helpers for the raft unit tests (cfg(test) only).

use std::net::SocketAddr;

use super::{NodeId, RaftCore};
use crate::store::SnapshotMeta;
use crate::wal::Log;
use crate::{Config, Peer};

// A single-node core backed by a throwaway data dir.
pub fn single_node_core() -> RaftCore {
    core_with_peers(1, Vec::new())
}

// A core for node `id` with the given peers. The temp dir is left in place (into_path)
// so the core's open WAL handles stay valid for the test.
pub fn core_with_peers(id: NodeId, peers: Vec<NodeId>) -> RaftCore {
    let dir = tempfile::tempdir().unwrap().into_path();
    let addr: SocketAddr = format!("127.0.0.1:{}", 7000 + id).parse().unwrap();

    let cfg = Config::new(id, addr, &dir).with_peers(
        peers
            .iter()
            .map(|&p| Peer { id: p, addr: format!("127.0.0.1:{}", 7000 + p) })
            .collect(),
    );

    let log = Log::open(cfg.wal_dir(), cfg.segment_max_bytes).unwrap();
    RaftCore::restore(&cfg, SnapshotMeta::default(), log).unwrap()
}
