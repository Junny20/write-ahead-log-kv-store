// Log replication: both sides of AppendEntries, the next/match index bookkeeping, and
// the commit rule. These methods extend RaftCore. The follower (handle_append_entries)
// accepts entries only if the preceding entry matches, truncates any divergent suffix,
// and fsyncs before acking. The leader advances the commit index only for an entry in
// its own term - the condition that keeps committed entries from being overwritten.

use super::{LogEntry, LogIndex, NodeId, RaftCore, Role, Term};
use crate::Result;

// AppendEntries RPC (also a heartbeat when entries is empty).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AppendEntriesArgs {
    pub term: Term,
    pub leader_id: NodeId,
    pub prev_log_index: LogIndex, // entry just before `entries`
    pub prev_log_term: Term,
    pub entries: Vec<LogEntry>, // contiguous from prev_log_index + 1
    pub leader_commit: LogIndex,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AppendEntriesReply {
    pub term: Term,
    pub success: bool, // follower had a matching prev_log_index/term
    // On failure, a hint so the leader backs next_index up quickly. Zero when unused.
    pub conflict_index: LogIndex,
}

impl RaftCore {
    // Build the AppendEntries to send to peer given its next_index.
    pub fn build_append_entries(&self, peer: NodeId) -> AppendEntriesArgs {
        let next = *self
            .next_index
            .get(&peer)
            .unwrap_or(&(self.last_log_index() + 1));
        let prev_log_index = next.saturating_sub(1);
        let prev_log_term = self
            .term_at(prev_log_index)
            .unwrap_or(self.snapshot.last_included_term);

        AppendEntriesArgs {
            term: self.hard.current_term,
            leader_id: self.id,
            prev_log_index,
            prev_log_term,
            entries: self.entries_from(next),
            leader_commit: self.commit_index,
        }
    }

    // Handle an incoming AppendEntries.
    pub fn handle_append_entries(
        &mut self,
        args: AppendEntriesArgs,
    ) -> Result<AppendEntriesReply> {
        // 1. Reject an out-of-date leader.
        if args.term < self.hard.current_term {
            return Ok(self.reject(0));
        }

        // 2. Recognise `args.leader_id` as leader for this term and reset our timer.
        if args.term > self.hard.current_term {
            self.become_follower(args.term, Some(args.leader_id))?;
        } else {
            self.role = Role::Follower;
            self.leader_id = Some(args.leader_id);
        }
        self.reset_election_timer();

        // 3. Log-matching check: we must already hold `prev_log_index` with the right
        //    term. If our log is too short, tell the leader where to resume.
        if args.prev_log_index > self.last_log_index() {
            return Ok(self.reject(self.last_log_index() + 1));
        }
        if let Some(local_term) = self.term_at(args.prev_log_index) {
            if local_term != args.prev_log_term {
                // Divergence at prev; ask the leader to retry from here.
                return Ok(self.reject(args.prev_log_index));
            }
        }
        // (If `prev_log_index` is below our snapshot we can't check the term; it is
        //  already covered by the snapshot, so we accept. TODO: verify via snapshot term.)

        // 4. Splice in the new entries, truncating any conflicting suffix first.
        for entry in &args.entries {
            if entry.index < self.base_index() {
                continue; // already covered by our snapshot
            }
            match self.term_at(entry.index) {
                Some(t) if t == entry.term => continue, // identical entry already present
                Some(_) => {
                    // Same index, different term: our tail diverges. Drop it and append.
                    self.truncate_from(entry.index)?;
                    self.append_replicated(entry)?;
                }
                None => self.append_replicated(entry)?,
            }
        }

        // 5. Durability before acknowledgement - an ack implies these entries survive a
        //    crash on this node.
        self.sync()?;

        // 6. Adopt the leader's commit index (bounded by what we actually hold).
        if args.leader_commit > self.commit_index {
            self.commit_index = args.leader_commit.min(self.last_log_index());
        }

        Ok(AppendEntriesReply {
            term: self.hard.current_term,
            success: true,
            conflict_index: 0,
        })
    }

    // Fold a peer's reply into leader bookkeeping. last_sent is the highest index we
    // told the peer to store (prev_log_index + entries.len()).
    pub fn on_append_entries_reply(
        &mut self,
        peer: NodeId,
        last_sent: LogIndex,
        reply: &AppendEntriesReply,
    ) {
        if self.role != Role::Leader {
            return;
        }
        if reply.success {
            self.match_index.insert(peer, last_sent);
            self.next_index.insert(peer, last_sent + 1);
            self.advance_commit_index();
        } else {
            // Back `next_index` up toward the follower's hint so the next round resumes
            // near the divergence rather than crawling one index at a time.
            let current = *self.next_index.get(&peer).unwrap_or(&1);
            let hint = if reply.conflict_index > 0 {
                reply.conflict_index
            } else {
                current.saturating_sub(1)
            };
            let backed_off = hint.min(current.saturating_sub(1)).max(1);
            self.next_index.insert(peer, backed_off);
        }
    }

    // Advance the commit index to the highest entry on a majority, but only if it's from
    // the current term (the commit-safety rule).
    pub fn advance_commit_index(&mut self) {
        // Collect match indices: each peer's, plus our own log end.
        let mut matches: Vec<LogIndex> = self
            .peers
            .iter()
            .map(|p| *self.match_index.get(p).unwrap_or(&0))
            .collect();
        matches.push(self.last_log_index());
        matches.sort_unstable_by(|a, b| b.cmp(a)); // descending

        // The value a majority has reached is at position `quorum - 1`.
        let candidate = matches[self.quorum() - 1];
        if candidate > self.commit_index && self.term_at(candidate) == Some(self.hard.current_term)
        {
            self.commit_index = candidate;
        }
    }

    // ---- internal helpers ----

    // Drop every entry (in memory and the WAL) with index >= index.
    fn truncate_from(&mut self, index: LogIndex) -> Result<()> {
        self.entries.retain(|e| e.index < index);
        self.log.truncate_suffix(index.saturating_sub(1))?;
        Ok(())
    }

    // Append a replicated entry to memory and the WAL (must extend contiguously).
    fn append_replicated(&mut self, entry: &LogEntry) -> Result<()> {
        debug_assert_eq!(
            entry.index,
            self.last_log_index() + 1,
            "replicated append left a gap in the log",
        );
        let bytes = bincode::serialize(entry)?;
        let wal_index = self.log.append(&bytes)?;
        debug_assert_eq!(wal_index, entry.index);
        self.entries.push(entry.clone());
        Ok(())
    }

    // Build a failed reply with a conflict_index hint.
    fn reject(&self, conflict_index: LogIndex) -> AppendEntriesReply {
        AppendEntriesReply {
            term: self.hard.current_term,
            success: false,
            conflict_index,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::test_support::single_node_core;
    use crate::store::Command;

    fn entry(term: Term, index: LogIndex) -> LogEntry {
        LogEntry { term, index, command: Command::Noop }
    }

    #[test]
    fn accepts_matching_append_and_commits() {
        let mut core = single_node_core();
        // Pretend a leader in term 1 sends us two entries from an empty log.
        let reply = core
            .handle_append_entries(AppendEntriesArgs {
                term: 1,
                leader_id: 2,
                prev_log_index: 0,
                prev_log_term: 0,
                entries: vec![entry(1, 1), entry(1, 2)],
                leader_commit: 2,
            })
            .unwrap();
        assert!(reply.success);
        assert_eq!(core.last_log_index(), 2);
        assert_eq!(core.commit_index(), 2);
    }

    #[test]
    fn rejects_gap_and_hints_next_index() {
        let mut core = single_node_core();
        let reply = core
            .handle_append_entries(AppendEntriesArgs {
                term: 1,
                leader_id: 2,
                prev_log_index: 5, // we have nothing yet
                prev_log_term: 1,
                entries: vec![entry(1, 6)],
                leader_commit: 0,
            })
            .unwrap();
        assert!(!reply.success);
        assert_eq!(reply.conflict_index, 1, "hint should point at our log end + 1");
    }

    #[test]
    fn conflicting_suffix_is_truncated() {
        let mut core = single_node_core();
        // First accept entries in term 1.
        core.handle_append_entries(AppendEntriesArgs {
            term: 1,
            leader_id: 2,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![entry(1, 1), entry(1, 2), entry(1, 3)],
            leader_commit: 0,
        })
        .unwrap();
        // A new leader in term 2 overwrites from index 2.
        let reply = core
            .handle_append_entries(AppendEntriesArgs {
                term: 2,
                leader_id: 3,
                prev_log_index: 1,
                prev_log_term: 1,
                entries: vec![entry(2, 2)],
                leader_commit: 0,
            })
            .unwrap();
        assert!(reply.success);
        assert_eq!(core.last_log_index(), 2);
        assert_eq!(core.term_at(2), Some(2), "index 2 should now be term 2");
    }
}
