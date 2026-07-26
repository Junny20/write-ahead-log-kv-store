// Multi-node agreement: an entry accepted by a quorum commits and reaches followers.
// Instead of three async nodes over gRPC (timing-dependent), this drives three RaftCores
// by hand and passes RPCs between them synchronously, so the consensus logic (votes, log
// matching, the commit rule) is tested deterministically.

use std::net::SocketAddr;
use std::path::Path;

use tempfile::tempdir;
use wal_kv::raft::{RaftCore, Role};
use wal_kv::store::{Command, SnapshotMeta};
use wal_kv::wal::Log;
use wal_kv::{Config, Peer};

// Build a fresh core for `id` that knows about `peers`, rooted under `dir`.
fn build_core(id: u64, peers: &[u64], dir: &Path) -> RaftCore {
    let addr: SocketAddr = format!("127.0.0.1:{}", 9000 + id).parse().unwrap();
    let data = dir.join(format!("n{id}"));
    let cfg = Config::new(id, addr, &data).with_peers(
        peers
            .iter()
            .map(|&p| Peer { id: p, addr: format!("127.0.0.1:{}", 9000 + p) })
            .collect(),
    );
    let log = Log::open(cfg.wal_dir(), cfg.segment_max_bytes).unwrap();
    RaftCore::restore(&cfg, SnapshotMeta::default(), log).unwrap()
}

// One round of AppendEntries leader -> follower, feeding the reply back (what the driver
// does per tick).
fn replicate_once(leader: &mut RaftCore, follower: &mut RaftCore, follower_id: u64) {
    let args = leader.build_append_entries(follower_id);
    let last_sent = args.prev_log_index + args.entries.len() as u64;
    let reply = follower.handle_append_entries(args).unwrap();
    leader.on_append_entries_reply(follower_id, last_sent, &reply);
}

#[test]
fn quorum_commit_and_replication() {
    let dir = tempdir().unwrap();
    let mut n1 = build_core(1, &[2, 3], dir.path());
    let mut n2 = build_core(2, &[1, 3], dir.path());
    let mut n3 = build_core(3, &[1, 2], dir.path());

    // --- Election: n1 campaigns and wins 2 of 3 votes (itself + one peer). ---
    let vote = n1.become_candidate().unwrap();
    let r2 = n2.handle_request_vote(vote.clone()).unwrap();
    let r3 = n3.handle_request_vote(vote.clone()).unwrap();
    assert!(r2.vote_granted && r3.vote_granted, "up-to-date candidate should win votes");
    n1.become_leader();
    assert_eq!(n1.role(), Role::Leader);

    // --- Replication: propose a write and push it to both followers. ---
    let entry = n1
        .append_command(Command::Put { key: b"color".to_vec(), value: b"blue".to_vec() })
        .unwrap();
    n1.sync().unwrap();

    replicate_once(&mut n1, &mut n2, 2);
    replicate_once(&mut n1, &mut n3, 3);

    // A majority now stores the entry, so the leader commits it.
    assert_eq!(n1.commit_index(), entry.index, "entry replicated to a quorum must commit");
    assert_eq!(n2.term_at(entry.index), Some(entry.term));
    assert_eq!(n3.term_at(entry.index), Some(entry.term));

    // --- Commit propagation: a second round carries the leader commit to followers. ---
    replicate_once(&mut n1, &mut n2, 2);
    replicate_once(&mut n1, &mut n3, 3);
    assert_eq!(n2.commit_index(), entry.index);
    assert_eq!(n3.commit_index(), entry.index);

    // Each node applies the committed entry to its own state machine.
    for node in [&mut n1, &mut n2, &mut n3] {
        let applied = node.take_committed();
        assert_eq!(applied.len(), 1);
        assert_eq!(applied[0].command, Command::Put { key: b"color".to_vec(), value: b"blue".to_vec() });
    }
}

#[test]
fn stale_follower_log_is_repaired() {
    let dir = tempdir().unwrap();
    let mut leader = build_core(1, &[2], dir.path());
    let mut follower = build_core(2, &[1], dir.path());

    // Leader accumulates several entries in term 1.
    leader.become_candidate().unwrap();
    leader.become_leader();
    for v in [b"a".to_vec(), b"b".to_vec(), b"c".to_vec()] {
        leader.append_command(Command::Put { key: v.clone(), value: v }).unwrap();
    }
    leader.sync().unwrap();

    // The follower starts empty; a few AppendEntries rounds should backfill it. The
    // leader backs `next_index` off on rejection and retries until prev matches.
    for _ in 0..5 {
        replicate_once(&mut leader, &mut follower, 2);
    }

    assert_eq!(follower.last_log_index(), leader.last_log_index());
    assert_eq!(leader.commit_index(), leader.last_log_index());
}
