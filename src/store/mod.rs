// The store: the state machine plus its snapshot policy. Raft drives it - it calls
// apply as entries commit, checks should_snapshot, and on yes calls snapshot and then
// compacts the WAL prefix. The store knows nothing about networking or consensus.

pub mod memtable;
pub mod snapshot;

use std::path::PathBuf;

pub use memtable::{Command, MemTable};
pub use snapshot::SnapshotMeta;

use crate::{Config, Result};

/// The state machine and its snapshot lifecycle.
#[derive(Debug)]
pub struct Store {
    /// Directory holding snapshot files.
    snapshot_dir: PathBuf,
    memtable: MemTable,
    snapshot_threshold: u64,
    last_snapshot: SnapshotMeta,
}

impl Store {
    // Open the store, loading the newest snapshot if any. It's caught up only to the
    // snapshot; the caller replays later committed WAL entries via apply.
    pub fn open(cfg: &Config) -> Result<Self> {
        let snapshot_dir = cfg.snapshot_dir();
        let (last_snapshot, memtable) = match snapshot::load_latest(&snapshot_dir)? {
            Some((meta, table)) => (meta, table),
            None => (SnapshotMeta::default(), MemTable::new()),
        };
        Ok(Store {
            snapshot_dir,
            memtable,
            snapshot_threshold: cfg.snapshot_threshold,
            last_snapshot,
        })
    }

    /// Apply the committed command at `index`. Idempotent for already-applied indices.
    pub fn apply(&mut self, index: u64, command: &Command) {
        self.memtable.apply(index, command);
    }

    /// Read a key from the current committed state.
    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.memtable.get(key)
    }

    /// Index of the last applied command.
    pub fn last_applied(&self) -> u64 {
        self.memtable.last_applied()
    }

    /// Metadata of the most recent durable snapshot.
    pub fn last_snapshot(&self) -> SnapshotMeta {
        self.last_snapshot
    }

    /// Number of live keys.
    pub fn len(&self) -> usize {
        self.memtable.len()
    }

    /// Whether the store is empty.
    pub fn is_empty(&self) -> bool {
        self.memtable.is_empty()
    }

    /// Whether enough has been applied since the last snapshot to warrant a new one.
    pub fn should_snapshot(&self) -> bool {
        self.last_applied()
            .saturating_sub(self.last_snapshot.last_included_index)
            >= self.snapshot_threshold
    }

    // Take a durable snapshot at the current applied index. last_included_term is the
    // term of that entry (supplied by Raft). Returns the meta so the caller can truncate
    // the WAL prefix.
    pub fn snapshot(&mut self, last_included_term: u64) -> Result<SnapshotMeta> {
        let meta = SnapshotMeta {
            last_included_index: self.memtable.last_applied(),
            last_included_term,
        };
        snapshot::write(&self.snapshot_dir, meta, &self.memtable)?;
        self.last_snapshot = meta;
        Ok(meta)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;
    use tempfile::tempdir;

    fn config(dir: &std::path::Path) -> Config {
        let addr: SocketAddr = "127.0.0.1:6001".parse().unwrap();
        let mut cfg = Config::new(1, addr, dir);
        cfg.snapshot_threshold = 3;
        cfg
    }

    #[test]
    fn apply_snapshot_reopen() {
        let dir = tempdir().unwrap();
        {
            let mut store = Store::open(&config(dir.path())).unwrap();
            store.apply(1, &Command::Put { key: b"a".to_vec(), value: b"1".to_vec() });
            store.apply(2, &Command::Put { key: b"b".to_vec(), value: b"2".to_vec() });
            store.apply(3, &Command::Put { key: b"c".to_vec(), value: b"3".to_vec() });
            assert!(store.should_snapshot());
            store.snapshot(1).unwrap();
        }
        // Reopen: the snapshot alone should restore all applied state.
        let store = Store::open(&config(dir.path())).unwrap();
        assert_eq!(store.last_applied(), 3);
        assert_eq!(store.get(b"a"), Some(b"1".to_vec()));
        assert_eq!(store.get(b"c"), Some(b"3".to_vec()));
    }
}
