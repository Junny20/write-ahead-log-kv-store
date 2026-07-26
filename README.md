# wal-kv

A small key/value store built on a write-ahead log and Raft replication. A hobby
project for learning how these pieces fit together.

Path of a write:

```
client --gRPC--> rpc --> raft (propose) --> wal (fsync) --> store (apply)
                          |
                          +-- replicate to peers --gRPC--> raft
```

The storage engine (WAL, memtable, snapshots) is unit-tested. The Raft
layer implements the receiver side in full (RequestVote / AppendEntries, log
matching, commit rule, leader lease) plus a driver that ties it together.

## Layout

- `src/wal/` — record framing with CRC, log segments, rotation, recovery
- `src/store/` — in-memory map applied from committed commands, plus snapshots
- `src/raft/` — election, replication, leader lease
- `src/rpc/` — gRPC schema and services
- `src/config.rs` — node id, peers, timeouts

## Build and run

Needs `protoc` on PATH for the gRPC codegen.

```
cargo build

# single node
cargo run -- --id 1 --listen 127.0.0.1:6001 --data-dir ./data/n1

# three nodes (each in its own shell)
cargo run -- --id 1 --listen 127.0.0.1:6001 --data-dir ./data/n1 \
    --peer 2=127.0.0.1:6002 --peer 3=127.0.0.1:6003
```

## Test

```
cargo test
cargo bench
```
