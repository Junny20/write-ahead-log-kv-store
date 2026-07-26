// Crash-restart durability: an acknowledged write is never lost. These tests hit the
// storage engine directly (WAL + store), where durability is enforced - no async Raft
// driver, so they're deterministic. Once sync() returns, the data survives a reopen,
// and a torn write from a simulated crash doesn't corrupt already-acknowledged records.

use std::fs::{self, OpenOptions};
use std::io::Write;
use std::net::SocketAddr;
use std::path::Path;

use tempfile::tempdir;
use wal_kv::store::{Command, Store};
use wal_kv::wal::Log;
use wal_kv::Config;

fn config(dir: &Path) -> Config {
    let addr: SocketAddr = "127.0.0.1:6001".parse().unwrap();
    Config::new(1, addr, dir)
}

#[test]
fn synced_wal_entries_survive_reopen() {
    let dir = tempdir().unwrap();
    let wal_dir = dir.path().join("wal");

    {
        let mut log = Log::open(&wal_dir, 1 << 20).unwrap();
        for i in 1..=100 {
            log.append(format!("entry-{i}").as_bytes()).unwrap();
        }
        log.sync().unwrap(); // acknowledged
    } // "crash": drop everything

    let log = Log::open(&wal_dir, 1 << 20).unwrap();
    assert_eq!(log.last_index(), 100, "no acknowledged entry may be lost");
    let all = log.read_all().unwrap();
    assert_eq!(all.len(), 100);
    assert_eq!(all[41].1, b"entry-42");
}

#[test]
fn torn_tail_does_not_destroy_acknowledged_entries() {
    let dir = tempdir().unwrap();
    let wal_dir = dir.path().join("wal");

    {
        let mut log = Log::open(&wal_dir, 1 << 20).unwrap();
        for i in 1..=10 {
            log.append(format!("entry-{i}").as_bytes()).unwrap();
        }
        log.sync().unwrap();
    }

    // Simulate a crash *during* the next append: garbage bytes land at the tail of the
    // active segment without a valid header/CRC.
    let active = newest_segment(&wal_dir);
    let mut f = OpenOptions::new().append(true).open(&active).unwrap();
    f.write_all(&[0xDE, 0xAD, 0xBE, 0xEF, 0x00]).unwrap();
    f.sync_all().unwrap();

    // Recovery must discard the torn tail and keep every acknowledged entry.
    let log = Log::open(&wal_dir, 1 << 20).unwrap();
    assert_eq!(log.last_index(), 10);
    assert_eq!(log.read_all().unwrap().len(), 10);
}

#[test]
fn store_state_survives_snapshot_and_reopen() {
    let dir = tempdir().unwrap();

    {
        let mut store = Store::open(&config(dir.path())).unwrap();
        for i in 1..=25 {
            store.apply(
                i,
                &Command::Put {
                    key: format!("key-{i}").into_bytes(),
                    value: format!("val-{i}").into_bytes(),
                },
            );
        }
        store.apply(26, &Command::Delete { key: b"key-1".to_vec() });
        store.snapshot(3).unwrap(); // durable checkpoint at index 26
    }

    let store = Store::open(&config(dir.path())).unwrap();
    assert_eq!(store.last_applied(), 26);
    assert_eq!(store.get(b"key-10"), Some(b"val-10".to_vec()));
    assert_eq!(store.get(b"key-1"), None, "the delete must have persisted");
}

// Return the newest (lexically greatest) segment file in wal_dir.
fn newest_segment(wal_dir: &Path) -> std::path::PathBuf {
    fs::read_dir(wal_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .map(|n| n.starts_with("wal-") && n.ends_with(".log"))
                .unwrap_or(false)
        })
        .max()
        .expect("at least one segment file")
}
