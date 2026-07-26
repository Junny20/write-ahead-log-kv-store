// Throughput micro-benchmarks for the storage engine: WAL append + fsync, and folding a
// command into the memtable. Run with `cargo bench`.

use criterion::{criterion_group, criterion_main, BatchSize, Criterion, Throughput};
use tempfile::tempdir;
use wal_kv::store::{Command, MemTable};
use wal_kv::wal::Log;

// Append a record and fsync it - the durability-bound write path.
fn wal_append_sync(c: &mut Criterion) {
    let payload = vec![0u8; 256];
    let mut group = c.benchmark_group("wal");
    group.throughput(Throughput::Bytes(payload.len() as u64));
    group.bench_function("append_and_sync_256B", |b| {
        let dir = tempdir().unwrap();
        let mut log = Log::open(dir.path(), 256 << 20).unwrap();
        b.iter(|| {
            log.append(&payload).unwrap();
            log.sync().unwrap();
        });
    });
    group.finish();
}

// Append many records, fsync once - shows the win from batching fsync.
fn wal_batched_append(c: &mut Criterion) {
    let payload = vec![0u8; 256];
    let mut group = c.benchmark_group("wal");
    group.throughput(Throughput::Bytes(payload.len() as u64 * 1000));
    group.bench_function("append_1000_then_sync", |b| {
        let dir = tempdir().unwrap();
        let mut log = Log::open(dir.path(), 256 << 20).unwrap();
        b.iter(|| {
            for _ in 0..1000 {
                log.append(&payload).unwrap();
            }
            log.sync().unwrap();
        });
    });
    group.finish();
}

// Fold 1000 Puts into a fresh memtable - the apply path.
fn memtable_apply(c: &mut Criterion) {
    c.bench_function("memtable_apply_1000_puts", |b| {
        b.iter_batched(
            MemTable::new,
            |mut table| {
                for i in 0..1000u64 {
                    table.apply(
                        i + 1,
                        &Command::Put { key: i.to_le_bytes().to_vec(), value: vec![7u8; 32] },
                    );
                }
                table
            },
            BatchSize::SmallInput,
        );
    });
}

criterion_group!(benches, wal_append_sync, wal_batched_append, memtable_apply);
criterion_main!(benches);
