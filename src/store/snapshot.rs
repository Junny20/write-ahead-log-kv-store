// Snapshots: a point-in-time dump of the state machine, and the basis for WAL
// compaction. Once written, every WAL record at or below last_included_index can be
// dropped.
//
// Written atomically: temp file -> fsync -> rename -> fsync dir. A reader only ever
// sees a complete snapshot or none.

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use super::memtable::MemTable;
use crate::Result;

// What index/term a snapshot reflects.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotMeta {
    pub last_included_index: u64,
    // Term of the entry at last_included_index; kept for log matching after the
    // entries are compacted away.
    pub last_included_term: u64,
}

// On-disk layout: metadata plus the full map.
#[derive(Serialize, Deserialize)]
struct SnapshotFile {
    meta: SnapshotMeta,
    data: BTreeMap<Vec<u8>, Vec<u8>>,
}

const SNAPSHOT_PREFIX: &str = "snapshot-";
const SNAPSHOT_SUFFIX: &str = ".snap";

// Atomically write table as a snapshot at meta; removes older snapshots afterwards.
pub fn write(dir: &Path, meta: SnapshotMeta, table: &MemTable) -> Result<PathBuf> {
    fs::create_dir_all(dir)?;

    let payload = SnapshotFile { meta, data: table.map().clone() };
    let bytes = bincode::serialize(&payload)?;

    let final_path = snapshot_path(dir, meta.last_included_index);
    let tmp_path = with_extension(&final_path, "tmp");

    // 1. Write the temp file and flush its contents to disk.
    {
        let mut f = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&tmp_path)?;
        f.write_all(&bytes)?;
        f.sync_all()?;
    }

    // 2. Atomically move it into place, then fsync the directory so the rename is
    //    itself durable (otherwise a crash could lose the directory entry).
    fs::rename(&tmp_path, &final_path)?;
    fsync_dir(dir)?;

    // 3. Best-effort removal of superseded snapshots.
    for (index, path) in discover(dir)? {
        if index != meta.last_included_index {
            let _ = fs::remove_file(path);
        }
    }

    Ok(final_path)
}

/// Load the newest snapshot in `dir`, if any, reconstructing the memtable.
pub fn load_latest(dir: &Path) -> Result<Option<(SnapshotMeta, MemTable)>> {
    let Some((_, path)) = discover(dir)?.into_iter().max_by_key(|(idx, _)| *idx) else {
        return Ok(None);
    };
    let bytes = fs::read(&path)?;
    let file: SnapshotFile = bincode::deserialize(&bytes)?;
    let table = MemTable::from_parts(file.data, file.meta.last_included_index);
    Ok(Some((file.meta, table)))
}

fn snapshot_path(dir: &Path, index: u64) -> PathBuf {
    dir.join(format!("{SNAPSHOT_PREFIX}{index:020}{SNAPSHOT_SUFFIX}"))
}

fn with_extension(path: &Path, ext: &str) -> PathBuf {
    let mut s = path.as_os_str().to_owned();
    s.push(".");
    s.push(ext);
    PathBuf::from(s)
}

/// Return `(index, path)` for every snapshot file in `dir`.
fn discover(dir: &Path) -> Result<Vec<(u64, PathBuf)>> {
    let mut out = Vec::new();
    if !dir.exists() {
        return Ok(out);
    }
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        let Some(rest) = name.strip_prefix(SNAPSHOT_PREFIX) else { continue };
        let Some(digits) = rest.strip_suffix(SNAPSHOT_SUFFIX) else { continue };
        if let Ok(index) = digits.parse::<u64>() {
            out.push((index, entry.path()));
        }
    }
    Ok(out)
}

/// `fsync` a directory so a rename/creation within it is durable.
fn fsync_dir(dir: &Path) -> Result<()> {
    let f = File::open(dir)?;
    f.sync_all()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::memtable::Command;
    use tempfile::tempdir;

    #[test]
    fn write_then_load_roundtrips() {
        let dir = tempdir().unwrap();
        let mut table = MemTable::new();
        table.apply(1, &Command::Put { key: b"a".to_vec(), value: b"1".to_vec() });
        table.apply(2, &Command::Put { key: b"b".to_vec(), value: b"2".to_vec() });

        let meta = SnapshotMeta { last_included_index: 2, last_included_term: 1 };
        write(dir.path(), meta, &table).unwrap();

        let (loaded_meta, loaded) = load_latest(dir.path()).unwrap().unwrap();
        assert_eq!(loaded_meta, meta);
        assert_eq!(loaded.get(b"a"), Some(b"1".to_vec()));
        assert_eq!(loaded.get(b"b"), Some(b"2".to_vec()));
        assert_eq!(loaded.last_applied(), 2);
    }

    #[test]
    fn newest_snapshot_wins_and_old_is_pruned() {
        let dir = tempdir().unwrap();
        let mut table = MemTable::new();
        table.apply(1, &Command::Put { key: b"x".to_vec(), value: b"1".to_vec() });
        write(dir.path(), SnapshotMeta { last_included_index: 1, last_included_term: 1 }, &table).unwrap();

        table.apply(2, &Command::Put { key: b"x".to_vec(), value: b"2".to_vec() });
        write(dir.path(), SnapshotMeta { last_included_index: 2, last_included_term: 1 }, &table).unwrap();

        assert_eq!(discover(dir.path()).unwrap().len(), 1, "old snapshot pruned");
        let (meta, loaded) = load_latest(dir.path()).unwrap().unwrap();
        assert_eq!(meta.last_included_index, 2);
        assert_eq!(loaded.get(b"x"), Some(b"2".to_vec()));
    }

    #[test]
    fn empty_dir_loads_nothing() {
        let dir = tempdir().unwrap();
        assert!(load_latest(dir.path()).unwrap().is_none());
    }
}
