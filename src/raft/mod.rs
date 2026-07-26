// Raft consensus: leader election, log replication, linearizable reads.
//
// Split into a passive core and an active driver:
//   RaftCore - holds all state and makes every decision as a plain sync method (handle a
//     RequestVote, append entries, advance commit, ...). It never touches the network or
//     clock, so it's easy to unit-test. This is the complete part.
//   Raft - the async driver: owns the timers and Transport, ticks the core, fans RPCs
//     out to peers, and feeds committed entries into the Store. A few corners are TODO.

pub mod election;
pub mod lease;
pub mod replication;
pub mod state;

#[cfg(test)]
pub(crate) mod test_support;

pub use election::{RequestVoteArgs, RequestVoteReply};
pub use lease::LeaderLease;
pub use replication::{AppendEntriesArgs, AppendEntriesReply};
pub use state::{HardState, LogEntry, LogIndex, NodeId, Role, Term};

use std::collections::HashMap;
use std::ops::Range;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use tokio::sync::{watch, Mutex};
use tracing::{debug, info, warn};

use crate::store::{Command, Store};
use crate::wal::Log;
use crate::{Config, Error, Result};

// Outbound RPC to peers. Abstracted so the driver can use a real gRPC client or an
// in-memory mock in tests.
#[async_trait]
pub trait Transport: Send + Sync + 'static {
    async fn request_vote(&self, to: NodeId, args: RequestVoteArgs) -> Result<RequestVoteReply>;
    async fn append_entries(
        &self,
        to: NodeId,
        args: AppendEntriesArgs,
    ) -> Result<AppendEntriesReply>;
}

// All Raft state for one node, plus the logic that mutates it. Fields are module-
// private; the election/replication/lease submodules extend impl RaftCore and use them
// directly (a child module can see its ancestor's private items).
#[derive(Debug)]
pub struct RaftCore {
    // ---- identity & configuration ----
    id: NodeId,
    peers: Vec<NodeId>,
    cluster_size: usize,
    election_timeout: Range<Duration>,

    // ---- persistent state ----
    hard: HardState,
    hard_path: std::path::PathBuf,
    log: Log, // durable backing; `entries` mirrors it above the snapshot
    entries: Vec<LogEntry>, // every entry with index > snapshot.last_included_index
    snapshot: crate::store::SnapshotMeta,

    // ---- volatile state (all servers) ----
    role: Role,
    commit_index: LogIndex,
    last_applied: LogIndex,
    leader_id: Option<NodeId>,
    election_deadline: Instant,

    // ---- volatile state (leader only) ----
    next_index: HashMap<NodeId, LogIndex>,
    match_index: HashMap<NodeId, LogIndex>,
    lease: LeaderLease,
}

impl RaftCore {
    // Rebuild a node's Raft state from durable storage. Entries still in the WAL above
    // the snapshot are loaded into memory; commit/applied start at the snapshot (a safe
    // lower bound the node re-learns from the leader).
    pub fn restore(
        cfg: &Config,
        snapshot: crate::store::SnapshotMeta,
        log: Log,
    ) -> Result<Self> {
        let hard_path = cfg.hard_state_path();
        let hard = HardState::load(&hard_path)?;

        // Reconstruct in-memory entries above the snapshot from the WAL.
        let mut entries = Vec::new();
        for (index, bytes) in log.read_all()? {
            if index <= snapshot.last_included_index {
                continue; // already folded into the snapshot
            }
            let entry: LogEntry = bincode::deserialize(&bytes)?;
            debug_assert_eq!(entry.index, index, "WAL index disagrees with entry index");
            entries.push(entry);
        }

        let peers: Vec<NodeId> = cfg.peers.iter().map(|p| p.id).collect();
        let lease_duration = cfg.election_timeout.start / 2; // safely shorter than an election
        let mut core = RaftCore {
            id: cfg.id,
            peers,
            cluster_size: cfg.cluster_size(),
            election_timeout: cfg.election_timeout.clone(),
            hard,
            hard_path,
            log,
            entries,
            snapshot,
            role: Role::Follower,
            commit_index: snapshot.last_included_index,
            last_applied: snapshot.last_included_index,
            leader_id: None,
            election_deadline: Instant::now(),
            next_index: HashMap::new(),
            match_index: HashMap::new(),
            lease: LeaderLease::new(lease_duration),
        };
        core.reset_election_timer();
        Ok(core)
    }

    // ---- identity / status accessors ----

    /// This node's id.
    pub fn id(&self) -> NodeId {
        self.id
    }
    /// Current role.
    pub fn role(&self) -> Role {
        self.role
    }
    /// Whether this node currently believes it is leader.
    pub fn is_leader(&self) -> bool {
        self.role == Role::Leader
    }
    /// Current term.
    pub fn current_term(&self) -> Term {
        self.hard.current_term
    }
    /// Highest index known to be committed.
    pub fn commit_index(&self) -> LogIndex {
        self.commit_index
    }
    /// The leader this node last heard from, if any.
    pub fn leader_id(&self) -> Option<NodeId> {
        self.leader_id
    }
    /// Peer ids (everyone but this node).
    pub fn peers(&self) -> &[NodeId] {
        &self.peers
    }
    /// Majority quorum size.
    pub fn quorum(&self) -> usize {
        self.cluster_size / 2 + 1
    }

    // ---- log helpers ----

    /// Index of the first entry held in memory (`snapshot.last_included_index + 1`).
    fn base_index(&self) -> LogIndex {
        self.snapshot.last_included_index + 1
    }

    /// Index of the last entry in the log (snapshot index if the tail is empty).
    pub fn last_log_index(&self) -> LogIndex {
        match self.entries.last() {
            Some(e) => e.index,
            None => self.snapshot.last_included_index,
        }
    }

    /// Term of the last entry (snapshot term if the tail is empty).
    pub fn last_log_term(&self) -> Term {
        match self.entries.last() {
            Some(e) => e.term,
            None => self.snapshot.last_included_term,
        }
    }

    /// Term of the entry at `index`, if known.
    pub fn term_at(&self, index: LogIndex) -> Option<Term> {
        if index == self.snapshot.last_included_index {
            return Some(self.snapshot.last_included_term);
        }
        if index < self.base_index() {
            return None; // compacted away
        }
        self.entries
            .get((index - self.base_index()) as usize)
            .map(|e| e.term)
    }

    /// Borrow the entry at `index`, if present in memory.
    pub fn entry_at(&self, index: LogIndex) -> Option<&LogEntry> {
        if index < self.base_index() {
            return None;
        }
        self.entries.get((index - self.base_index()) as usize)
    }

    /// Clone every entry with index `>= from` (what a leader ships to a lagging peer).
    pub fn entries_from(&self, from: LogIndex) -> Vec<LogEntry> {
        if from < self.base_index() {
            // Peer needs data we've compacted; a real implementation would send an
            // InstallSnapshot here. TODO: snapshot streaming.
            return self.entries.clone();
        }
        let start = (from - self.base_index()) as usize;
        self.entries
            .get(start..)
            .map(|slice| slice.to_vec())
            .unwrap_or_default()
    }

    // Append a fresh command as leader, returning the entry. Caller must sync() for
    // durability.
    pub fn append_command(&mut self, command: Command) -> Result<LogEntry> {
        if self.role != Role::Leader {
            return Err(Error::raft("only the leader may append commands"));
        }
        let index = self.last_log_index() + 1;
        let entry = LogEntry { term: self.hard.current_term, index, command };
        let bytes = bincode::serialize(&entry)?;
        let wal_index = self.log.append(&bytes)?;
        debug_assert_eq!(wal_index, index, "WAL and Raft indices diverged");
        self.entries.push(entry.clone());
        Ok(entry)
    }

    /// Flush the WAL, making all appended entries durable.
    pub fn sync(&mut self) -> Result<()> {
        self.log.sync()
    }

    // ---- persistence ----

    // Persist HardState. Called whenever current_term or voted_for changes, before the
    // corresponding RPC reply is sent.
    fn persist_hard(&mut self) -> Result<()> {
        self.hard.persist(&self.hard_path)
    }

    // ---- commit / apply ----

    // Return newly-committed-but-unapplied entries and advance last_applied. The driver
    // applies them to the Store (kept separate so the core stays store-free).
    pub fn take_committed(&mut self) -> Vec<LogEntry> {
        let mut out = Vec::new();
        while self.last_applied < self.commit_index {
            let next = self.last_applied + 1;
            match self.entry_at(next) {
                Some(entry) => {
                    out.push(entry.clone());
                    self.last_applied = next;
                }
                None => break, // not in memory yet (e.g. awaiting snapshot install)
            }
        }
        out
    }

    // Compact: drop entries and WAL records at or below meta.last_included_index.
    pub fn compact(&mut self, meta: crate::store::SnapshotMeta) -> Result<()> {
        if meta.last_included_index <= self.snapshot.last_included_index {
            return Ok(());
        }
        // Drop in-memory entries the snapshot now covers.
        self.entries
            .retain(|e| e.index > meta.last_included_index);
        // Compact the WAL prefix and update our snapshot marker.
        self.log.truncate_prefix(meta.last_included_index + 1)?;
        self.snapshot = meta;
        Ok(())
    }
}

// ---- driver ----

// The async driver that turns a RaftCore into a running node. Owns the shared core and
// store, the peer Transport, and a watch channel that publishes the commit index so
// waiting client requests wake when their entry commits.
#[derive(Clone)]
pub struct Raft {
    core: Arc<Mutex<RaftCore>>,
    store: Arc<Mutex<Store>>,
    transport: Arc<dyn Transport>,
    commit_tx: watch::Sender<LogIndex>,
    heartbeat_interval: Duration,
}

impl Raft {
    // Open all durable state for cfg and assemble a driver. Returns the driver plus a
    // watch::Receiver of the commit index (for awaiting commits).
    pub fn bootstrap(
        cfg: &Config,
        transport: Arc<dyn Transport>,
    ) -> Result<(Self, watch::Receiver<LogIndex>)> {
        let store = Store::open(cfg)?;
        let log = Log::open(cfg.wal_dir(), cfg.segment_max_bytes)?;
        let core = RaftCore::restore(cfg, store.last_snapshot(), log)?;

        let (commit_tx, commit_rx) = watch::channel(core.commit_index());
        let raft = Raft {
            core: Arc::new(Mutex::new(core)),
            store: Arc::new(Mutex::new(store)),
            transport,
            commit_tx,
            heartbeat_interval: cfg.heartbeat_interval,
        };
        Ok((raft, commit_rx))
    }

    // Shared handle to the core (the RPC layer uses it to serve Raft RPCs).
    pub fn core(&self) -> Arc<Mutex<RaftCore>> {
        Arc::clone(&self.core)
    }

    // Shared handle to the store (the RPC layer uses it to serve local reads).
    pub fn store(&self) -> Arc<Mutex<Store>> {
        Arc::clone(&self.store)
    }

    // Run the node forever: drive timers, replicate, apply committed entries. Ticks at a
    // fraction of the heartbeat interval so timeouts are checked promptly.
    pub async fn run(self) {
        let tick = (self.heartbeat_interval / 2).max(Duration::from_millis(5));
        let mut ticker = tokio::time::interval(tick);
        loop {
            ticker.tick().await;
            if let Err(e) = self.tick().await {
                warn!(error = %e, "raft tick failed");
            }
        }
    }

    /// One iteration of the driver loop.
    async fn tick(&self) -> Result<()> {
        let action = {
            let core = self.core.lock().await;
            if core.is_leader() {
                Action::Replicate
            } else if core.election_timed_out(Instant::now()) {
                Action::Elect
            } else {
                Action::Idle
            }
        };

        match action {
            Action::Elect => self.start_election().await?,
            Action::Replicate => self.broadcast_append_entries().await?,
            Action::Idle => {}
        }

        self.apply_committed().await
    }

    // Campaign for leadership: bump the term, vote for self, gather votes.
    async fn start_election(&self) -> Result<()> {
        let (args, term) = {
            let mut core = self.core.lock().await;
            let args = core.become_candidate()?;
            info!(node = core.id(), term = args.term, "starting election");
            (args.clone(), core.current_term())
        };

        let peers: Vec<NodeId> = {
            let core = self.core.lock().await;
            core.peers().to_vec()
        };

        // Fan RequestVote out to every peer concurrently.
        let mut votes = 1usize; // we voted for ourselves
        let mut tasks = futures_unordered(peers.into_iter().map(|peer| {
            let transport = Arc::clone(&self.transport);
            let args = args.clone();
            async move { transport.request_vote(peer, args).await }
        }));

        while let Some(result) = tasks.next().await {
            let reply = match result {
                Ok(reply) => reply,
                Err(e) => {
                    debug!(error = %e, "request_vote to peer failed");
                    continue;
                }
            };

            let mut core = self.core.lock().await;
            // A higher term seen in a reply means we lost the race; step down.
            if reply.term > core.current_term() {
                core.become_follower(reply.term, None)?;
                return Ok(());
            }
            // Ignore stale replies from a prior campaign.
            if core.role() != Role::Candidate || core.current_term() != term {
                return Ok(());
            }
            if reply.vote_granted {
                votes += 1;
                if votes >= core.quorum() {
                    core.become_leader();
                    info!(node = core.id(), term, "won election");
                    // Commit a no-op in our term so we can advance the commit index.
                    // TODO: also kick an immediate heartbeat round here.
                    core.append_command(Command::Noop)?;
                    core.sync()?;
                    core.advance_commit_index();
                    return Ok(());
                }
            }
        }
        Ok(())
    }

    // Send AppendEntries (heartbeat or real entries) to every peer and fold in replies.
    async fn broadcast_append_entries(&self) -> Result<()> {
        let (peers, term) = {
            let core = self.core.lock().await;
            (core.peers().to_vec(), core.current_term())
        };
        let mut acks = 1usize; // the leader counts for its own lease

        let mut tasks = futures_unordered(peers.into_iter().map(|peer| {
            let core = Arc::clone(&self.core);
            let transport = Arc::clone(&self.transport);
            async move {
                let args = {
                    let core = core.lock().await;
                    core.build_append_entries(peer)
                };
                let last_sent = args.prev_log_index + args.entries.len() as u64;
                let reply = transport.append_entries(peer, args).await;
                (peer, last_sent, reply)
            }
        }));

        while let Some((peer, last_sent, result)) = tasks.next().await {
            let reply = match result {
                Ok(reply) => reply,
                Err(e) => {
                    debug!(peer, error = %e, "append_entries failed");
                    continue;
                }
            };
            let mut core = self.core.lock().await;
            if reply.term > core.current_term() {
                core.become_follower(reply.term, None)?;
                return Ok(());
            }
            if !core.is_leader() || core.current_term() != term {
                return Ok(());
            }
            core.on_append_entries_reply(peer, last_sent, &reply);
            if reply.success {
                acks += 1;
            }
        }

        // A heartbeat round acknowledged by a quorum renews the leader's read lease.
        if acks >= {
            let core = self.core.lock().await;
            core.quorum()
        } {
            self.core.lock().await.renew_lease(Instant::now());
        }
        Ok(())
    }

    // Apply committed entries to the store, snapshot if warranted, and publish the new
    // commit index to waiters.
    async fn apply_committed(&self) -> Result<()> {
        let committed = {
            let mut core = self.core.lock().await;
            core.take_committed()
        };
        if committed.is_empty() {
            return Ok(());
        }

        {
            let mut store = self.store.lock().await;
            for entry in &committed {
                store.apply(entry.index, &entry.command);
            }

            // Snapshot + compact when the store says it is time.
            if store.should_snapshot() {
                let applied = store.last_applied();
                let term = {
                    let core = self.core.lock().await;
                    core.term_at(applied).unwrap_or(0)
                };
                let meta = store.snapshot(term)?;
                self.core.lock().await.compact(meta)?;
                debug!(index = meta.last_included_index, "took snapshot and compacted WAL");
            }
        }

        let latest = committed.last().unwrap().index;
        let _ = self.commit_tx.send(latest);
        Ok(())
    }

    // Propose a command (leader only). Returns the assigned index; the caller awaits
    // commit via the watch::Receiver from bootstrap.
    pub async fn propose(&self, command: Command) -> Result<LogIndex> {
        let index = {
            let mut core = self.core.lock().await;
            if !core.is_leader() {
                return Err(Error::raft("not leader"));
            }
            let entry = core.append_command(command)?;
            core.sync()?;
            // Single-node clusters commit immediately; multi-node relies on the next
            // replication round.
            core.advance_commit_index();
            entry.index
        };
        // Nudge replication so followers see the new entry without waiting a full tick.
        let _ = self.broadcast_append_entries().await;
        self.apply_committed().await?;
        Ok(index)
    }
}

// What tick decided while holding the lock (so the lock isn't held across the await).
enum Action {
    Elect,
    Replicate,
    Idle,
}

// A tiny local shim so we don't pull in the `futures` crate just for `FuturesUnordered`.
// It collects futures and drives them concurrently on the tokio runtime.
use futures_shim::futures_unordered;

impl std::fmt::Debug for Raft {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Raft")
            .field("heartbeat_interval", &self.heartbeat_interval)
            .finish_non_exhaustive()
    }
}

// Minimal concurrent-future collection on tokio's JoinSet, so the driver can await many
// peer RPCs at once without an extra dependency.
mod futures_shim {
    use std::future::Future;
    use tokio::task::JoinSet;

    // A set of concurrently-running futures; drain outputs via next().
    pub struct FuturesUnordered<T> {
        set: JoinSet<T>,
    }

    // Build a FuturesUnordered from an iterator of futures.
    pub fn futures_unordered<T, F, I>(iter: I) -> FuturesUnordered<T>
    where
        T: Send + 'static,
        F: Future<Output = T> + Send + 'static,
        I: IntoIterator<Item = F>,
    {
        let mut set = JoinSet::new();
        for fut in iter {
            set.spawn(fut);
        }
        FuturesUnordered { set }
    }

    impl<T: Send + 'static> FuturesUnordered<T> {
        // Await the next completed future's output, or None when all are done. A
        // panicked task is skipped.
        pub async fn next(&mut self) -> Option<T> {
            loop {
                match self.set.join_next().await {
                    Some(Ok(v)) => return Some(v),
                    Some(Err(_join_err)) => continue, // task panicked; ignore
                    None => return None,
                }
            }
        }
    }
}
