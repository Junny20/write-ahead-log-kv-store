// Leader leases for linearizable reads. Committing a no-op per read is correct but
// costs a round-trip. Instead, when a leader hears from a quorum (e.g. a heartbeat) it
// holds a lease for a fixed duration during which no other leader can exist, so it can
// answer reads from local state and still be linearizable.
//
// This assumes bounded clock drift: the lease must be shorter than the election timeout
// by more than the max clock skew between nodes. We use half the minimum election
// timeout (see RaftCore::restore). If you can't bound skew, don't use lease reads.

use std::time::{Duration, Instant};

use super::RaftCore;

// A time-bounded promise that this node is the sole leader.
#[derive(Clone, Copy, Debug)]
pub struct LeaderLease {
    duration: Duration,
    expiry: Option<Instant>,
}

impl LeaderLease {
    /// Create an un-held lease with the given validity `duration`.
    pub fn new(duration: Duration) -> Self {
        LeaderLease { duration, expiry: None }
    }

    /// Renew the lease as of `now` (call after a quorum acknowledges a heartbeat).
    pub fn renew(&mut self, now: Instant) {
        self.expiry = Some(now + self.duration);
    }

    /// Whether the lease is still valid at `now`.
    pub fn is_valid(&self, now: Instant) -> bool {
        matches!(self.expiry, Some(deadline) if now < deadline)
    }

    /// Drop the lease (e.g. on stepping down).
    pub fn invalidate(&mut self) {
        self.expiry = None;
    }
}

impl RaftCore {
    /// Renew the read lease. No-op unless we are the leader.
    pub fn renew_lease(&mut self, now: Instant) {
        if self.is_leader() {
            self.lease.renew(now);
        }
    }

    // Whether this node may serve a linearizable read locally right now (leader with a
    // valid lease).
    pub fn can_serve_local_read(&self, now: Instant) -> bool {
        self.is_leader() && self.lease.is_valid(now)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lease_validity_tracks_time() {
        let mut lease = LeaderLease::new(Duration::from_millis(100));
        let t0 = Instant::now();
        assert!(!lease.is_valid(t0), "a fresh lease is not held");

        lease.renew(t0);
        assert!(lease.is_valid(t0 + Duration::from_millis(50)));
        assert!(!lease.is_valid(t0 + Duration::from_millis(150)), "expired");

        lease.invalidate();
        assert!(!lease.is_valid(t0));
    }

    #[test]
    fn only_leader_can_serve_local_read() {
        use crate::raft::test_support::single_node_core;
        let mut core = single_node_core();
        let now = Instant::now();
        core.become_leader();
        assert!(!core.can_serve_local_read(now), "no lease held yet");
        core.renew_lease(now);
        assert!(core.can_serve_local_read(now));
    }
}
