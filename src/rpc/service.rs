// tonic service impls and the peer transport. Thin glue: convert protobuf to/from the
// raft core's plain structs and delegate to the core.
//   KvService     - client API. put/delete propose and wait for commit; get reads from
//                   the leader.
//   RaftService   - internal API. request_vote/append_entries call the core handlers.
//   PeerTransport - outbound gRPC client pool implementing Transport.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use tokio::sync::{watch, Mutex};
use tonic::transport::{Channel, Endpoint};
use tonic::{Request, Response, Status};

use super::pb;
use crate::config::Peer;
use crate::raft::{
    AppendEntriesArgs, AppendEntriesReply, LogEntry, LogIndex, NodeId, Raft as RaftHandle,
    RaftCore, RequestVoteArgs, RequestVoteReply, Transport,
};
use crate::store::{Command, Store};
use crate::{Error, Result};

/// How long a `put`/`delete` waits for its entry to commit before giving up.
const COMMIT_TIMEOUT: Duration = Duration::from_secs(5);

// ---------------------------------------------------------------------------
// Client-facing KV service
// ---------------------------------------------------------------------------

// Implements the client Kv gRPC service.
#[derive(Clone)]
pub struct KvService {
    raft: RaftHandle,
    store: Arc<Mutex<Store>>,
    commit_rx: watch::Receiver<LogIndex>,
}

impl KvService {
    // Wrap a running Raft handle. commit_rx (from bootstrap) wakes put/delete when their
    // entry commits.
    pub fn new(raft: RaftHandle, commit_rx: watch::Receiver<LogIndex>) -> Self {
        let store = raft.store();
        KvService { raft, store, commit_rx }
    }

    /// Block until the commit index reaches `index`, or time out.
    async fn wait_for_commit(&self, index: LogIndex) -> std::result::Result<(), Status> {
        let mut rx = self.commit_rx.clone();
        loop {
            if *rx.borrow() >= index {
                return Ok(());
            }
            match tokio::time::timeout(COMMIT_TIMEOUT, rx.changed()).await {
                Ok(Ok(())) => continue,
                Ok(Err(_)) => return Err(Status::internal("commit channel closed")),
                Err(_) => return Err(Status::deadline_exceeded("timed out waiting for commit")),
            }
        }
    }
}

#[tonic::async_trait]
impl pb::kv_server::Kv for KvService {
    async fn get(
        &self,
        request: Request<pb::GetRequest>,
    ) -> std::result::Result<Response<pb::GetResponse>, Status> {
        let key = request.into_inner().key;

        // Only the leader may answer, to stay linearizable. A stricter build would gate
        // this on the lease (can_serve_local_read) or do a read-index round; here we just
        // require leadership and redirect otherwise.
        let (is_leader, leader_hint) = {
            let core = self.raft.core();
            let core = core.lock().await;
            (core.is_leader(), core.leader_id().unwrap_or(0))
        };
        if !is_leader {
            return Ok(Response::new(pb::GetResponse {
                found: false,
                value: Vec::new(),
                leader_hint,
            }));
        }

        let value = {
            let store = self.store.lock().await;
            store.get(&key)
        };
        Ok(Response::new(match value {
            Some(v) => pb::GetResponse { found: true, value: v, leader_hint },
            None => pb::GetResponse { found: false, value: Vec::new(), leader_hint },
        }))
    }

    async fn put(
        &self,
        request: Request<pb::PutRequest>,
    ) -> std::result::Result<Response<pb::PutResponse>, Status> {
        let r = request.into_inner();
        let index = self
            .raft
            .propose(Command::Put { key: r.key, value: r.value })
            .await
            .map_err(status)?;
        self.wait_for_commit(index).await?;
        Ok(Response::new(pb::PutResponse { index }))
    }

    async fn delete(
        &self,
        request: Request<pb::DeleteRequest>,
    ) -> std::result::Result<Response<pb::DeleteResponse>, Status> {
        let r = request.into_inner();
        let index = self
            .raft
            .propose(Command::Delete { key: r.key })
            .await
            .map_err(status)?;
        self.wait_for_commit(index).await?;
        Ok(Response::new(pb::DeleteResponse { index }))
    }
}

// ---------------------------------------------------------------------------
// Internal Raft service
// ---------------------------------------------------------------------------

/// Implements the internal `Raft` gRPC service by delegating to the shared core.
#[derive(Clone)]
pub struct RaftService {
    core: Arc<Mutex<RaftCore>>,
}

impl RaftService {
    // Wrap the shared core.
    pub fn new(core: Arc<Mutex<RaftCore>>) -> Self {
        RaftService { core }
    }
}

#[tonic::async_trait]
impl pb::raft_server::Raft for RaftService {
    async fn request_vote(
        &self,
        request: Request<pb::RequestVoteRequest>,
    ) -> std::result::Result<Response<pb::RequestVoteResponse>, Status> {
        let r = request.into_inner();
        let args = RequestVoteArgs {
            term: r.term,
            candidate_id: r.candidate_id,
            last_log_index: r.last_log_index,
            last_log_term: r.last_log_term,
        };
        let reply = {
            let mut core = self.core.lock().await;
            core.handle_request_vote(args).map_err(status)?
        };
        Ok(Response::new(pb::RequestVoteResponse {
            term: reply.term,
            vote_granted: reply.vote_granted,
        }))
    }

    async fn append_entries(
        &self,
        request: Request<pb::AppendEntriesRequest>,
    ) -> std::result::Result<Response<pb::AppendEntriesResponse>, Status> {
        let r = request.into_inner();
        let mut entries = Vec::with_capacity(r.entries.len());
        for pe in r.entries {
            entries.push(entry_from_pb(pe).map_err(status)?);
        }
        let args = AppendEntriesArgs {
            term: r.term,
            leader_id: r.leader_id,
            prev_log_index: r.prev_log_index,
            prev_log_term: r.prev_log_term,
            entries,
            leader_commit: r.leader_commit,
        };
        let reply = {
            let mut core = self.core.lock().await;
            core.handle_append_entries(args).map_err(status)?
        };
        Ok(Response::new(pb::AppendEntriesResponse {
            term: reply.term,
            success: reply.success,
            conflict_index: reply.conflict_index,
        }))
    }
}

// ---------------------------------------------------------------------------
// Outbound transport
// ---------------------------------------------------------------------------

// A gRPC-backed Transport: a pool of RaftClients keyed by peer id, connected on first
// use and cached (the tonic client is cheap to clone and multiplexes over one HTTP/2
// connection).
pub struct PeerTransport {
    addrs: HashMap<NodeId, String>,
    clients: Mutex<HashMap<NodeId, pb::raft_client::RaftClient<Channel>>>,
}

impl PeerTransport {
    /// Build a transport for the given peer set.
    pub fn new(peers: &[Peer]) -> Self {
        let addrs = peers
            .iter()
            .map(|p| (p.id, normalize_url(&p.addr)))
            .collect();
        PeerTransport { addrs, clients: Mutex::new(HashMap::new()) }
    }

    /// Return a client for `to`, connecting on first use.
    async fn client(
        &self,
        to: NodeId,
    ) -> Result<pb::raft_client::RaftClient<Channel>> {
        let mut guard = self.clients.lock().await;
        if let Some(client) = guard.get(&to) {
            return Ok(client.clone());
        }
        let url = self
            .addrs
            .get(&to)
            .ok_or_else(|| Error::Transport(format!("unknown peer id {to}")))?;
        // from_shared accepts a dynamic address string (connect's convenient form wants a
        // 'static str). Connect lazily and cache the client.
        let endpoint = Endpoint::from_shared(url.clone())
            .map_err(|e| Error::Transport(e.to_string()))?;
        let channel = endpoint
            .connect()
            .await
            .map_err(|e| Error::Transport(e.to_string()))?;
        let client = pb::raft_client::RaftClient::new(channel);
        guard.insert(to, client.clone());
        Ok(client)
    }
}

#[async_trait]
impl Transport for PeerTransport {
    async fn request_vote(
        &self,
        to: NodeId,
        args: RequestVoteArgs,
    ) -> Result<RequestVoteReply> {
        let mut client = self.client(to).await?;
        let resp = client
            .request_vote(pb::RequestVoteRequest {
                term: args.term,
                candidate_id: args.candidate_id,
                last_log_index: args.last_log_index,
                last_log_term: args.last_log_term,
            })
            .await
            .map_err(|e| Error::Transport(e.to_string()))?
            .into_inner();
        Ok(RequestVoteReply { term: resp.term, vote_granted: resp.vote_granted })
    }

    async fn append_entries(
        &self,
        to: NodeId,
        args: AppendEntriesArgs,
    ) -> Result<AppendEntriesReply> {
        let mut client = self.client(to).await?;
        let mut entries = Vec::with_capacity(args.entries.len());
        for e in &args.entries {
            entries.push(entry_to_pb(e)?);
        }
        let resp = client
            .append_entries(pb::AppendEntriesRequest {
                term: args.term,
                leader_id: args.leader_id,
                prev_log_index: args.prev_log_index,
                prev_log_term: args.prev_log_term,
                entries,
                leader_commit: args.leader_commit,
            })
            .await
            .map_err(|e| Error::Transport(e.to_string()))?
            .into_inner();
        Ok(AppendEntriesReply {
            term: resp.term,
            success: resp.success,
            conflict_index: resp.conflict_index,
        })
    }
}

// ---------------------------------------------------------------------------
// Conversions & helpers
// ---------------------------------------------------------------------------

// Encode a LogEntry for the wire (command is bincode-serialised).
fn entry_to_pb(e: &LogEntry) -> Result<pb::LogEntry> {
    Ok(pb::LogEntry {
        term: e.term,
        index: e.index,
        command: bincode::serialize(&e.command)?,
    })
}

// Decode a wire LogEntry back into a raft LogEntry.
fn entry_from_pb(p: pb::LogEntry) -> Result<LogEntry> {
    Ok(LogEntry {
        term: p.term,
        index: p.index,
        command: bincode::deserialize(&p.command)?,
    })
}

// Map a crate Error to a gRPC Status. "not leader" is a precondition failure (retry
// elsewhere); everything else is internal.
fn status(e: Error) -> Status {
    match e {
        Error::Raft(msg) => Status::failed_precondition(msg),
        other => Status::internal(other.to_string()),
    }
}

/// Ensure a peer address carries a scheme tonic can dial.
fn normalize_url(addr: &str) -> String {
    if addr.starts_with("http://") || addr.starts_with("https://") {
        addr.to_string()
    } else {
        format!("http://{addr}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn log_entry_wire_roundtrip() {
        let e = LogEntry {
            term: 3,
            index: 9,
            command: Command::Put { key: b"k".to_vec(), value: b"v".to_vec() },
        };
        let back = entry_from_pb(entry_to_pb(&e).unwrap()).unwrap();
        assert_eq!(e, back);
    }

    #[test]
    fn url_normalisation() {
        assert_eq!(normalize_url("127.0.0.1:6001"), "http://127.0.0.1:6001");
        assert_eq!(normalize_url("http://host:1"), "http://host:1");
    }
}
