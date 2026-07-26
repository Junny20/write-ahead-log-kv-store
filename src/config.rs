// Node configuration: id, peers, data dir, and the Raft timing knobs.
// Build it with Config::new or load from TOML with Config::from_file; either way
// Config::validate rejects bad setups.

use std::net::SocketAddr;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::Deserialize;

use crate::raft::state::NodeId;
use crate::{Error, Result};

// A peer: its id and the address its Raft service listens on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Peer {
    pub id: NodeId,
    pub addr: String, // host:port
}

#[derive(Debug, Clone)]
pub struct Config {
    pub id: NodeId, // must be non-zero
    pub listen_addr: SocketAddr,
    pub peers: Vec<Peer>, // every other node
    pub data_dir: PathBuf, // holds WAL segments, snapshots, hard state
    pub heartbeat_interval: Duration,
    // A follower that hears nothing for a random value in this range starts an
    // election. The low end must be well above heartbeat_interval.
    pub election_timeout: Range<Duration>,
    pub segment_max_bytes: u64, // roll to a new WAL segment past this size
    pub snapshot_threshold: u64, // snapshot after this many applied entries
}

impl Config {
    // Build a single-node config with defaults. Add peers with with_peers.
    pub fn new(id: NodeId, listen_addr: SocketAddr, data_dir: impl Into<PathBuf>) -> Self {
        Config {
            id,
            listen_addr,
            peers: Vec::new(),
            data_dir: data_dir.into(),
            heartbeat_interval: Duration::from_millis(50),
            election_timeout: Duration::from_millis(150)..Duration::from_millis(300),
            segment_max_bytes: 64 * 1024 * 1024,
            snapshot_threshold: 10_000,
        }
    }

    /// Replace the peer set, returning `self` for chaining.
    pub fn with_peers(mut self, peers: Vec<Peer>) -> Self {
        self.peers = peers;
        self
    }

    // Load configuration from a TOML file (timing fields in milliseconds).
    pub fn from_file(path: impl AsRef<Path>) -> Result<Self> {
        let text = std::fs::read_to_string(path.as_ref())?;
        let raw: FileConfig =
            toml::from_str(&text).map_err(|e| Error::Config(format!("invalid TOML: {e}")))?;
        raw.into_config()
    }

    /// Directory holding the WAL segments.
    pub fn wal_dir(&self) -> PathBuf {
        self.data_dir.join("wal")
    }

    /// Directory holding snapshot files.
    pub fn snapshot_dir(&self) -> PathBuf {
        self.data_dir.join("snapshots")
    }

    /// File holding the durable Raft hard state (`current_term`, `voted_for`).
    pub fn hard_state_path(&self) -> PathBuf {
        self.data_dir.join("hard_state")
    }

    /// The number of nodes in the cluster, including this one.
    pub fn cluster_size(&self) -> usize {
        self.peers.len() + 1
    }

    /// The size of a majority quorum.
    pub fn quorum(&self) -> usize {
        self.cluster_size() / 2 + 1
    }

    /// Reject configurations that would misbehave at runtime.
    pub fn validate(&self) -> Result<()> {
        if self.id == 0 {
            return Err(Error::Config("node id must be non-zero".into()));
        }
        if self.election_timeout.start <= self.heartbeat_interval {
            return Err(Error::Config(
                "election timeout must be larger than the heartbeat interval".into(),
            ));
        }
        if self.election_timeout.start >= self.election_timeout.end {
            return Err(Error::Config(
                "election timeout range must be non-empty (start < end)".into(),
            ));
        }
        let mut ids: Vec<NodeId> = self.peers.iter().map(|p| p.id).collect();
        ids.push(self.id);
        ids.sort_unstable();
        if ids.windows(2).any(|w| w[0] == w[1]) {
            return Err(Error::Config("duplicate node id in cluster".into()));
        }
        Ok(())
    }
}

// TOML form: timing fields are in milliseconds. Converted into Config on load.
#[derive(Debug, Deserialize)]
pub struct FileConfig {
    pub id: NodeId,
    pub listen_addr: String,
    #[serde(default)]
    pub peers: Vec<RawPeer>,
    pub data_dir: PathBuf,
    #[serde(default = "default_heartbeat_ms")]
    pub heartbeat_ms: u64,
    #[serde(default = "default_election_min_ms")]
    pub election_min_ms: u64,
    #[serde(default = "default_election_max_ms")]
    pub election_max_ms: u64,
    #[serde(default = "default_segment_max_bytes")]
    pub segment_max_bytes: u64,
    #[serde(default = "default_snapshot_threshold")]
    pub snapshot_threshold: u64,
}

// TOML form of a peer.
#[derive(Debug, Deserialize)]
pub struct RawPeer {
    pub id: NodeId,
    pub addr: String,
}

impl FileConfig {
    fn into_config(self) -> Result<Config> {
        let listen_addr = self
            .listen_addr
            .parse::<SocketAddr>()
            .map_err(|e| Error::Config(format!("invalid listen_addr: {e}")))?;
        let peers = self
            .peers
            .into_iter()
            .map(|p| Peer { id: p.id, addr: p.addr })
            .collect();
        let cfg = Config {
            id: self.id,
            listen_addr,
            peers,
            data_dir: self.data_dir,
            heartbeat_interval: Duration::from_millis(self.heartbeat_ms),
            election_timeout: Duration::from_millis(self.election_min_ms)
                ..Duration::from_millis(self.election_max_ms),
            segment_max_bytes: self.segment_max_bytes,
            snapshot_threshold: self.snapshot_threshold,
        };
        cfg.validate()?;
        Ok(cfg)
    }
}

fn default_heartbeat_ms() -> u64 {
    50
}
fn default_election_min_ms() -> u64 {
    150
}
fn default_election_max_ms() -> u64 {
    300
}
fn default_segment_max_bytes() -> u64 {
    64 * 1024 * 1024
}
fn default_snapshot_threshold() -> u64 {
    10_000
}

#[cfg(test)]
mod tests {
    use super::*;

    fn addr() -> SocketAddr {
        "127.0.0.1:6001".parse().unwrap()
    }

    #[test]
    fn defaults_are_valid() {
        let cfg = Config::new(1, addr(), "/tmp/wal-kv");
        cfg.validate().expect("default config should validate");
        assert_eq!(cfg.cluster_size(), 1);
        assert_eq!(cfg.quorum(), 1);
    }

    #[test]
    fn quorum_scales_with_peers() {
        let cfg = Config::new(1, addr(), "/tmp/wal-kv").with_peers(vec![
            Peer { id: 2, addr: "127.0.0.1:6002".into() },
            Peer { id: 3, addr: "127.0.0.1:6003".into() },
        ]);
        assert_eq!(cfg.cluster_size(), 3);
        assert_eq!(cfg.quorum(), 2);
    }

    #[test]
    fn rejects_zero_id_and_bad_timeouts() {
        let mut cfg = Config::new(0, addr(), "/tmp/wal-kv");
        assert!(cfg.validate().is_err());
        cfg.id = 1;
        cfg.election_timeout = Duration::from_millis(10)..Duration::from_millis(20);
        assert!(cfg.validate().is_err(), "election timeout below heartbeat must be rejected");
    }

    #[test]
    fn rejects_duplicate_ids() {
        let cfg = Config::new(1, addr(), "/tmp/wal-kv")
            .with_peers(vec![Peer { id: 1, addr: "127.0.0.1:6002".into() }]);
        assert!(cfg.validate().is_err());
    }
}
