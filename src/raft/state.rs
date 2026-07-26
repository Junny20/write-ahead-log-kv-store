// Core Raft types and the state that must survive a crash. Persistent state must be
// durable before answering any RPC; volatile state is rebuilt on restart. Here: the
// id/term/index aliases and Role, LogEntry (stored in the WAL and replicated), and
// HardState (current_term + voted_for). The log entries live in the WAL, not here.

use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::store::Command;
use crate::Result;

pub type NodeId = u64; // stable, cluster-unique node id
pub type Term = u64; // monotonically increasing; the protocol's logical clock
pub type LogIndex = u64; // 1-based position in the log

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    Follower,  // replicates from a leader, votes in elections
    Candidate, // campaigning for votes
    Leader,    // accepts writes and drives replication
}

// One entry in the replicated log. term and index are stored inline so an entry is
// self-describing on the wire, in the WAL, or in memory.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogEntry {
    pub term: Term,
    pub index: LogIndex,
    pub command: Command,
}

// Durable Raft metadata, flushed before any RPC is answered. Losing it across a crash
// would let a node vote twice in a term or forget its term - both break safety - so it
// is persisted atomically with fsync.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct HardState {
    pub current_term: Term,
    pub voted_for: Option<NodeId>,
}

impl HardState {
    // Load hard state, or the default (term 0, no vote) if the file doesn't exist.
    pub fn load(path: &Path) -> Result<Self> {
        match fs::read(path) {
            Ok(bytes) => Ok(bincode::deserialize(&bytes)?),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(HardState::default()),
            Err(e) => Err(e.into()),
        }
    }

    // Persist atomically: temp file -> fsync -> rename -> fsync parent dir.
    pub fn persist(&self, path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let bytes = bincode::serialize(self)?;

        let tmp = tmp_path(path);
        {
            let mut f = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&tmp)?;
            f.write_all(&bytes)?;
            f.sync_all()?;
        }
        fs::rename(&tmp, path)?;
        if let Some(parent) = path.parent() {
            File::open(parent)?.sync_all()?;
        }
        Ok(())
    }
}

fn tmp_path(path: &Path) -> PathBuf {
    let mut s = path.as_os_str().to_owned();
    s.push(".tmp");
    PathBuf::from(s)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn hard_state_roundtrip() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("hard_state");

        // Missing file loads as default.
        assert_eq!(HardState::load(&path).unwrap(), HardState::default());

        let hs = HardState { current_term: 7, voted_for: Some(3) };
        hs.persist(&path).unwrap();
        assert_eq!(HardState::load(&path).unwrap(), hs);

        // Overwrites cleanly.
        let hs2 = HardState { current_term: 8, voted_for: None };
        hs2.persist(&path).unwrap();
        assert_eq!(HardState::load(&path).unwrap(), hs2);
    }

    #[test]
    fn log_entry_is_serializable() {
        let e = LogEntry {
            term: 2,
            index: 5,
            command: Command::Put { key: b"k".to_vec(), value: b"v".to_vec() },
        };
        let bytes = bincode::serialize(&e).unwrap();
        let back: LogEntry = bincode::deserialize(&bytes).unwrap();
        assert_eq!(e, back);
    }
}
