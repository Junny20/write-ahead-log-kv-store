// wal-kv node binary. Parses flags (or a TOML config) into a Config, opens durable
// state, and serves the Kv and Raft gRPC services on one address while the Raft driver
// runs in the background.
//
//   wal-kv --id 1 --listen 127.0.0.1:6001 --data-dir ./data/n1 \
//          --peer 2=127.0.0.1:6002 --peer 3=127.0.0.1:6003

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use tonic::transport::Server;
use tracing::info;
use tracing_subscriber::EnvFilter;

use wal_kv::config::Peer;
use wal_kv::raft::state::NodeId;
use wal_kv::raft::Raft;
use wal_kv::rpc::pb::{kv_server::KvServer, raft_server::RaftServer};
use wal_kv::rpc::{KvService, PeerTransport, RaftService};
use wal_kv::Config;

/// Run a single wal-kv node.
#[derive(Debug, Parser)]
#[command(name = "wal-kv", version, about)]
struct Cli {
    /// This node's cluster-unique id (non-zero).
    #[arg(long)]
    id: NodeId,

    /// Address to bind the gRPC server to.
    #[arg(long)]
    listen: SocketAddr,

    /// Directory for this node's WAL, snapshots, and hard state.
    #[arg(long)]
    data_dir: PathBuf,

    /// A peer, as `id=host:port`. Repeat once per other node.
    #[arg(long = "peer", value_parser = parse_peer)]
    peers: Vec<Peer>,

    /// Load configuration from a TOML file instead of the flags above (except this one).
    #[arg(long)]
    config: Option<PathBuf>,
}

fn parse_peer(s: &str) -> Result<Peer, String> {
    let (id, addr) = s
        .split_once('=')
        .ok_or_else(|| format!("expected `id=host:port`, got `{s}`"))?;
    let id: NodeId = id
        .parse()
        .map_err(|_| format!("invalid peer id in `{s}`"))?;
    Ok(Peer { id, addr: addr.to_string() })
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // `RUST_LOG=info` by default; override via the env var.
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .init();

    let cli = Cli::parse();
    let cfg = match &cli.config {
        Some(path) => Config::from_file(path)?,
        None => Config::new(cli.id, cli.listen, cli.data_dir.clone()).with_peers(cli.peers.clone()),
    };
    cfg.validate()?;
    info!(id = cfg.id, listen = %cfg.listen_addr, peers = cfg.peers.len(), "starting node");

    // Assemble the node: transport, Raft driver, and the two gRPC services.
    let transport = Arc::new(PeerTransport::new(&cfg.peers));
    let (raft, commit_rx) = Raft::bootstrap(&cfg, transport)?;

    let kv = KvService::new(raft.clone(), commit_rx);
    let raft_svc = RaftService::new(raft.core());

    // Drive Raft in the background.
    tokio::spawn(raft.run());

    info!(addr = %cfg.listen_addr, "serving Kv + Raft gRPC");
    Server::builder()
        .add_service(KvServer::new(kv))
        .add_service(RaftServer::new(raft_svc))
        .serve(cfg.listen_addr)
        .await?;

    Ok(())
}
