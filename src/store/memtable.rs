// The state machine: an ordered in-memory key/value map applied from committed
// commands. It tracks last_applied so replaying an already-applied index is a no-op,
// which keeps recovery (snapshot + WAL replay) idempotent.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

// A command: the unit Raft replicates and commits. Noop lets a new leader commit an
// entry in its own term (to advance the commit index); it does nothing to the map.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Command {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
    Noop,
}

/// An ordered in-memory key/value map plus the index of the last command folded in.
#[derive(Debug, Default, Clone)]
pub struct MemTable {
    map: BTreeMap<Vec<u8>, Vec<u8>>,
    last_applied: u64,
}

impl MemTable {
    /// An empty state machine at index 0.
    pub fn new() -> Self {
        Self::default()
    }

    /// Rebuild a memtable from a snapshot's contents.
    pub fn from_parts(map: BTreeMap<Vec<u8>, Vec<u8>>, last_applied: u64) -> Self {
        MemTable { map, last_applied }
    }

    // Apply the command at index (expected to be last_applied + 1). An already-applied
    // index is ignored, so replaying the tail of the WAL on recovery is safe.
    pub fn apply(&mut self, index: u64, command: &Command) {
        if index <= self.last_applied {
            return; // already applied - idempotent replay
        }
        debug_assert_eq!(
            index,
            self.last_applied + 1,
            "state machine applied out of order (gap in the committed stream)",
        );

        match command {
            Command::Put { key, value } => {
                self.map.insert(key.clone(), value.clone());
            }
            Command::Delete { key } => {
                self.map.remove(key);
            }
            Command::Noop => {}
        }
        self.last_applied = index;
    }

    /// Look up a key.
    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.map.get(key).cloned()
    }

    /// Number of keys currently stored.
    pub fn len(&self) -> usize {
        self.map.len()
    }

    /// Whether the map is empty.
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    /// Index of the last command applied.
    pub fn last_applied(&self) -> u64 {
        self.last_applied
    }

    /// Borrow the underlying map, e.g. to serialise a snapshot.
    pub fn map(&self) -> &BTreeMap<Vec<u8>, Vec<u8>> {
        &self.map
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn put_get_delete() {
        let mut m = MemTable::new();
        m.apply(1, &Command::Put { key: b"a".to_vec(), value: b"1".to_vec() });
        m.apply(2, &Command::Put { key: b"b".to_vec(), value: b"2".to_vec() });
        assert_eq!(m.get(b"a"), Some(b"1".to_vec()));
        assert_eq!(m.last_applied(), 2);

        m.apply(3, &Command::Delete { key: b"a".to_vec() });
        assert_eq!(m.get(b"a"), None);
        assert_eq!(m.len(), 1);
    }

    #[test]
    fn replay_is_idempotent() {
        let mut m = MemTable::new();
        m.apply(1, &Command::Put { key: b"k".to_vec(), value: b"v".to_vec() });
        // Replaying an already-applied index must not double-apply or advance.
        m.apply(1, &Command::Put { key: b"k".to_vec(), value: b"OTHER".to_vec() });
        assert_eq!(m.get(b"k"), Some(b"v".to_vec()));
        assert_eq!(m.last_applied(), 1);
    }

    #[test]
    fn noop_only_advances_index() {
        let mut m = MemTable::new();
        m.apply(1, &Command::Noop);
        assert_eq!(m.last_applied(), 1);
        assert!(m.is_empty());
    }
}
