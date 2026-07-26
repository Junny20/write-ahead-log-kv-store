// Crash fault-injection: repeatedly write acknowledged records, simulate a crash by
// scribbling garbage on the active segment's tail and truncating at a random offset,
// then reopen and verify every acknowledged write survived. Prints the run count.
//
// Run:   cargo test --release --test fault_injection -- --nocapture
// Count: set WALKV_FAULT_RUNS (default 1000).

use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::Path;

use rand::Rng;
use tempfile::tempdir;
use wal_kv::wal::Log;

fn newest_segment(dir: &Path) -> std::path::PathBuf {
    fs::read_dir(dir)
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
        .expect("a segment file")
}

#[test]
fn crash_faults_preserve_acknowledged_writes() {
    let runs: usize = std::env::var("WALKV_FAULT_RUNS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1000);
    let mut rng = rand::thread_rng();

    for run in 0..runs {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");

        // Write a random number of acknowledged records (append then fsync).
        let acked = rng.gen_range(1..=200usize);
        let expected: Vec<Vec<u8>> =
            (0..acked).map(|i| format!("run{run}-rec{i}").into_bytes()).collect();
        {
            let mut log = Log::open(&wal_dir, 1 << 20).unwrap();
            for r in &expected {
                log.append(r).unwrap();
            }
            log.sync().unwrap(); // acknowledged here
        }

        // Simulate a crash mid-next-write: append random garbage to the active segment
        // and truncate at a random offset at or past the last synced byte.
        {
            let seg = newest_segment(&wal_dir);
            let synced_len = fs::metadata(&seg).unwrap().len();
            let mut f = OpenOptions::new().append(true).open(&seg).unwrap();
            let garbage: Vec<u8> = (0..rng.gen_range(1..64)).map(|_| rng.gen::<u8>()).collect();
            f.write_all(&garbage).unwrap();
            f.sync_all().unwrap();
            let end = fs::metadata(&seg).unwrap().len();
            let cut = rng.gen_range(synced_len..=end);
            OpenOptions::new().write(true).open(&seg).unwrap().set_len(cut).unwrap();
        }

        // Recover and verify no acknowledged write was lost.
        let log = Log::open(&wal_dir, 1 << 20).unwrap();
        let recovered: Vec<Vec<u8>> =
            log.read_all().unwrap().into_iter().map(|(_, p)| p).collect();
        assert!(recovered.len() >= expected.len(), "run {run}: lost acknowledged writes");
        assert_eq!(&recovered[..expected.len()], &expected[..], "run {run}: data mismatch");
    }

    println!("fault-injection: {runs}/{runs} crash-recover runs, no acknowledged-write loss");
}
