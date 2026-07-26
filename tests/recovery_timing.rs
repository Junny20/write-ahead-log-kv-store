// Cold-start recovery timing: rebuild the state machine two ways and print both.
//   replay   - reopen the WAL and re-apply every entry
//   snapshot - load a snapshot taken at the same point
// This is the "snapshotting cut recovery from Xs to Yms" number.
//
// Run:  cargo test --release --test recovery_timing -- --nocapture
// Size: set WALKV_ENTRIES (default 200000).

use std::time::Instant;

use tempfile::tempdir;
use wal_kv::store::{snapshot, Command, MemTable, SnapshotMeta};
use wal_kv::wal::Log;

#[test]
fn snapshot_vs_replay_recovery() {
    let n: u64 = std::env::var("WALKV_ENTRIES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(200_000);

    let dir = tempdir().unwrap();
    let wal_dir = dir.path().join("wal");
    let snap_dir = dir.path().join("snapshots");

    // Build a WAL of N Put commands plus a memtable holding the same state.
    let mut table = MemTable::new();
    {
        let mut log = Log::open(&wal_dir, 64 << 20).unwrap();
        for i in 1..=n {
            let cmd = Command::Put { key: i.to_le_bytes().to_vec(), value: vec![0u8; 32] };
            log.append(&bincode::serialize(&cmd).unwrap()).unwrap();
            table.apply(i, &cmd);
        }
        log.sync().unwrap();
    }

    // Recovery via full WAL replay.
    let t0 = Instant::now();
    let mut replayed = MemTable::new();
    {
        let log = Log::open(&wal_dir, 64 << 20).unwrap();
        for (idx, bytes) in log.read_all().unwrap() {
            let cmd: Command = bincode::deserialize(&bytes).unwrap();
            replayed.apply(idx, &cmd);
        }
    }
    let replay = t0.elapsed();
    assert_eq!(replayed.last_applied(), n);

    // Recovery via snapshot load.
    snapshot::write(
        &snap_dir,
        SnapshotMeta { last_included_index: n, last_included_term: 1 },
        &table,
    )
    .unwrap();
    let t1 = Instant::now();
    let (_meta, loaded) = snapshot::load_latest(&snap_dir).unwrap().unwrap();
    let snap = t1.elapsed();
    assert_eq!(loaded.last_applied(), n);

    println!("recovery of {n} entries: replay = {replay:?}, snapshot = {snap:?}");
}
