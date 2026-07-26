// Partition behaviour: a lone node that is the whole cluster stays available, while a
// node cut off from its quorum refuses to make progress. The two sides of the CAP
// trade-off: a majority keeps serving; a minority blocks writes rather than diverge.

use std::net::SocketAddr;
use std::path::Path;

use tempfile::tempdir;
use wal_kv::raft::{RaftCore, Role};
use wal_kv::store::{Command, SnapshotMeta};
use wal_kv::wal::Log;
use wal_kv::{Config, Peer};

fn build_core(id: u64, peers: &[u64], dir: &Path) -> RaftCore {
    let addr: SocketAddr = format!("127.0.0.1:{}", 9100 + id).parse().unwrap();
    let data = dir.join(format!("n{id}"));
    let cfg = Config::new(id, addr, &data).with_peers(
        peers
            .iter()
            .map(|&p| Peer { id: p, addr: format!("127.0.0.1:{}", 9100 + p) })
            .collect(),
    );
    let log = Log::open(cfg.wal_dir(), cfg.segment_max_bytes).unwrap();
    RaftCore::restore(&cfg, SnapshotMeta::default(), log).unwrap()
}

#[test]
fn single_node_cluster_stays_available() {
    let dir = tempdir().unwrap();
    let mut node = build_core(1, &[], dir.path());

    // A single-node cluster has a quorum of one: it can elect itself with no peers.
    node.become_candidate().unwrap();
    assert_eq!(node.quorum(), 1);
    node.become_leader();
    assert_eq!(node.role(), Role::Leader);

    // Writes commit immediately because the leader alone is a majority.
    let entry = node
        .append_command(Command::Put { key: b"k".to_vec(), value: b"v".to_vec() })
        .unwrap();
    node.sync().unwrap();
    node.advance_commit_index();
    assert_eq!(node.commit_index(), entry.index);

    let applied = node.take_committed();
    assert_eq!(applied.len(), 1);
    assert_eq!(applied[0].command, Command::Put { key: b"k".to_vec(), value: b"v".to_vec() });
}

#[test]
fn minority_partition_cannot_commit() {
    let dir = tempdir().unwrap();
    // Node 1 believes in a three-node cluster but is partitioned from 2 and 3.
    let mut node = build_core(1, &[2, 3], dir.path());

    node.become_candidate().unwrap();
    // With no reachable peers it collects only its own vote - short of the quorum of 2 -
    // so it never becomes leader.
    assert_eq!(node.quorum(), 2);
    assert_ne!(node.role(), Role::Leader);

    // And because it is not the leader, it will not accept client writes: safety over
    // availability on the minority side of a partition.
    assert!(
        node.append_command(Command::Noop).is_err(),
        "a non-leader must reject proposals",
    );
    assert_eq!(node.commit_index(), 0, "nothing may commit without a quorum");
}
