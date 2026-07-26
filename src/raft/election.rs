// Leader election: timers, role transitions, and both sides of RequestVote. These
// methods extend RaftCore. handle_request_vote follows the Raft rules: reject stale
// terms, step down on newer ones, grant a vote only to a candidate whose log is at
// least as up-to-date as ours, and only once per term (which is why the vote is
// persisted before replying).

use std::time::{Duration, Instant};

use rand::Rng;

use super::{LogIndex, NodeId, RaftCore, Role, Term};
use crate::Result;

// RequestVote RPC.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RequestVoteArgs {
    pub term: Term,
    pub candidate_id: NodeId,
    pub last_log_index: LogIndex,
    pub last_log_term: Term,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RequestVoteReply {
    pub term: Term,
    pub vote_granted: bool,
}

impl RaftCore {
    // Arm the election timer with a fresh random deadline (randomised so split votes
    // are rare - nodes rarely time out at the same instant).
    pub(super) fn reset_election_timer(&mut self) {
        let lo = self.election_timeout.start.as_millis() as u64;
        let hi = self.election_timeout.end.as_millis() as u64;
        let ms = rand::thread_rng().gen_range(lo..hi);
        self.election_deadline = Instant::now() + Duration::from_millis(ms);
    }

    /// Whether the election timer has expired as of `now`.
    pub fn election_timed_out(&self, now: Instant) -> bool {
        now >= self.election_deadline
    }

    // Step down to follower. A newer term is adopted and clears our vote (persisted).
    pub fn become_follower(&mut self, term: Term, leader: Option<NodeId>) -> Result<()> {
        if term > self.hard.current_term {
            self.hard.current_term = term;
            self.hard.voted_for = None;
            self.persist_hard()?;
        }
        self.role = Role::Follower;
        self.leader_id = leader;
        self.lease.invalidate();
        Ok(())
    }

    // Begin an election: bump the term, vote for self, reset the timer, return the args
    // to broadcast.
    pub fn become_candidate(&mut self) -> Result<RequestVoteArgs> {
        self.role = Role::Candidate;
        self.hard.current_term += 1;
        self.hard.voted_for = Some(self.id);
        self.persist_hard()?;

        self.leader_id = None;
        self.reset_election_timer();

        Ok(RequestVoteArgs {
            term: self.hard.current_term,
            candidate_id: self.id,
            last_log_index: self.last_log_index(),
            last_log_term: self.last_log_term(),
        })
    }

    // Assume leadership: init per-peer next = last_log_index + 1, match = 0.
    pub fn become_leader(&mut self) {
        self.role = Role::Leader;
        self.leader_id = Some(self.id);
        let next = self.last_log_index() + 1;
        self.next_index.clear();
        self.match_index.clear();
        for &peer in &self.peers {
            self.next_index.insert(peer, next);
            self.match_index.insert(peer, 0);
        }
    }

    // Handle an incoming RequestVote.
    pub fn handle_request_vote(&mut self, args: RequestVoteArgs) -> Result<RequestVoteReply> {
        // 1. Reject anything from an older term outright.
        if args.term < self.hard.current_term {
            return Ok(RequestVoteReply {
                term: self.hard.current_term,
                vote_granted: false,
            });
        }
        // 2. A newer term means we're behind; step down and adopt it (clearing our vote).
        if args.term > self.hard.current_term {
            self.become_follower(args.term, None)?;
        }

        // 3. Grant iff we haven't voted for someone else this term AND the candidate's
        //    log is at least as up-to-date as ours (compare (term, index) lexically).
        let ours = (self.last_log_term(), self.last_log_index());
        let theirs = (args.last_log_term, args.last_log_index);
        let log_ok = theirs >= ours;
        let free_to_vote = matches!(self.hard.voted_for, None) || self.hard.voted_for == Some(args.candidate_id);

        let vote_granted = log_ok && free_to_vote;
        if vote_granted {
            self.hard.voted_for = Some(args.candidate_id);
            self.persist_hard()?;
            // Granting a vote is a sign of a live candidate; don't immediately time out.
            self.reset_election_timer();
        }

        Ok(RequestVoteReply {
            term: self.hard.current_term,
            vote_granted,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::test_support::single_node_core;

    #[test]
    fn grants_vote_to_up_to_date_candidate() {
        let mut core = single_node_core();
        let reply = core
            .handle_request_vote(RequestVoteArgs {
                term: 1,
                candidate_id: 2,
                last_log_index: 0,
                last_log_term: 0,
            })
            .unwrap();
        assert!(reply.vote_granted);
        assert_eq!(reply.term, 1);
    }

    #[test]
    fn rejects_second_candidate_in_same_term() {
        let mut core = single_node_core();
        core.handle_request_vote(RequestVoteArgs {
            term: 1,
            candidate_id: 2,
            last_log_index: 0,
            last_log_term: 0,
        })
        .unwrap();
        // A different candidate in the same term must be refused.
        let reply = core
            .handle_request_vote(RequestVoteArgs {
                term: 1,
                candidate_id: 3,
                last_log_index: 0,
                last_log_term: 0,
            })
            .unwrap();
        assert!(!reply.vote_granted);
    }

    #[test]
    fn rejects_stale_term() {
        let mut core = single_node_core();
        core.become_candidate().unwrap(); // term -> 1
        core.become_candidate().unwrap(); // term -> 2
        let reply = core
            .handle_request_vote(RequestVoteArgs {
                term: 1,
                candidate_id: 2,
                last_log_index: 0,
                last_log_term: 0,
            })
            .unwrap();
        assert!(!reply.vote_granted);
        assert_eq!(reply.term, 2);
    }
}
