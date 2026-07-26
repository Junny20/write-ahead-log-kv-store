// The segmented log: a sequence of segment files forming one 1-based, append-only
// record stream. Segments keep files bounded and make prefix compaction cheap (just
// delete whole files).
//
// Indices start at 1. An empty log has first_index == 1, last_index == 0. first_index
// is the smallest index still present (advances past a snapshot); last_index is the
// highest ever appended. Each segment file name encodes its first index (base_index).

use std::fs;
use std::path::{Path, PathBuf};

use super::segment::Segment;
use crate::{Error, Result};

/// Segment file names look like `wal-00000000000000000001.log`, zero-padded so a plain
/// lexical sort matches numeric order.
const SEGMENT_PREFIX: &str = "wal-";
const SEGMENT_SUFFIX: &str = ".log";

// A segmented, append-only log of opaque byte records. It knows nothing about Raft;
// the raft layer serialises its LogEntry into records.
#[derive(Debug)]
pub struct Log {
    dir: PathBuf,
    max_segment_bytes: u64,
    /// Always non-empty; the last element is the active append target.
    segments: Vec<Segment>,
    first_index: u64,
    last_index: u64,
}

impl Log {
    /// Open (creating if necessary) the log rooted at `dir`.
    ///
    /// Existing segments are recovered in order, discarding a torn tail on the final
    /// segment. If the directory is empty a fresh segment is created.
    pub fn open(dir: impl Into<PathBuf>, max_segment_bytes: u64) -> Result<Self> {
        let dir = dir.into();
        fs::create_dir_all(&dir)?;

        let mut bases = discover_segments(&dir)?;
        bases.sort_unstable();

        let mut segments = Vec::new();
        if bases.is_empty() {
            // Brand-new log: one empty segment starting at index 1.
            segments.push(Segment::create(segment_path(&dir, 1), 1)?);
        } else {
            for base in &bases {
                segments.push(Segment::recover(segment_path(&dir, *base), *base)?);
            }
        }

        let first_index = segments.first().unwrap().base_index();
        // An empty final segment leaves `last_index` one below its base (the log is
        // logically empty up to that point); otherwise it's base + count - 1.
        let last = segments.last().unwrap();
        let last_index = if last.count() == 0 {
            last.base_index() - 1
        } else {
            last.base_index() + last.count() - 1
        };

        Ok(Log {
            dir,
            max_segment_bytes,
            segments,
            first_index,
            last_index,
        })
    }

    /// Append a record and return the index it was assigned.
    ///
    /// Rotates to a fresh segment first if the active one has reached
    /// `max_segment_bytes` (and is non-empty, so a single oversized record still fits).
    pub fn append(&mut self, payload: &[u8]) -> Result<u64> {
        let active = self.segments.last().unwrap();
        if active.len_bytes() >= self.max_segment_bytes && active.count() > 0 {
            let base = self.last_index + 1;
            self.segments
                .push(Segment::create(segment_path(&self.dir, base), base)?);
        }

        self.segments.last_mut().unwrap().append(payload)?;
        self.last_index += 1;
        Ok(self.last_index)
    }

    /// Flush and `fsync` the active segment, making all prior appends durable.
    pub fn sync(&mut self) -> Result<()> {
        self.segments.last_mut().unwrap().sync()
    }

    /// Read every surviving record in order, paired with its index. Used at startup to
    /// rebuild the in-memory Raft log above the latest snapshot.
    pub fn read_all(&self) -> Result<Vec<(u64, Vec<u8>)>> {
        let mut out = Vec::new();
        for seg in &self.segments {
            let mut idx = seg.base_index();
            for rec in seg.iter()? {
                out.push((idx, rec?));
                idx += 1;
            }
        }
        Ok(out)
    }

    // Drop records with index < up_to_index (snapshot compaction). Only whole segments
    // are removed, so first_index may end up a bit below up_to_index. The active
    // segment is never removed.
    pub fn truncate_prefix(&mut self, up_to_index: u64) -> Result<()> {
        while self.segments.len() > 1 {
            let seg = &self.segments[0];
            let seg_last = seg.base_index() + seg.count().max(1) - 1;
            if seg_last < up_to_index {
                let removed = self.segments.remove(0);
                removed.remove()?;
            } else {
                break;
            }
        }
        self.first_index = self.segments.first().unwrap().base_index();
        Ok(())
    }

    // Drop records with index > keep_last (raft conflict resolution, where a new leader
    // overwrites a follower's divergent tail). Whole trailing segments are deleted; the
    // segment straddling keep_last is rewritten in place.
    pub fn truncate_suffix(&mut self, keep_last: u64) -> Result<()> {
        if keep_last >= self.last_index {
            return Ok(());
        }
        if keep_last + 1 < self.first_index {
            return Err(Error::wal(format!(
                "cannot truncate to {keep_last}: below compacted prefix at {}",
                self.first_index
            )));
        }

        // Delete whole trailing segments that begin past the cut point.
        while self.segments.len() > 1 {
            let seg = self.segments.last().unwrap();
            if seg.base_index() > keep_last {
                let removed = self.segments.pop().unwrap();
                removed.remove()?;
            } else {
                break;
            }
        }

        // Rewrite the straddling segment, keeping only records up to `keep_last`.
        let seg = self.segments.last().unwrap();
        let base = seg.base_index();
        let path = seg.path().to_path_buf();

        let mut survivors = Vec::new();
        let mut idx = base;
        for rec in seg.iter()? {
            if idx > keep_last {
                break;
            }
            survivors.push(rec?);
            idx += 1;
        }

        let old = self.segments.pop().unwrap();
        old.remove()?;
        let mut fresh = Segment::create(&path, base)?;
        for payload in &survivors {
            fresh.append(payload)?;
        }
        fresh.sync()?;
        self.segments.push(fresh);

        self.last_index = keep_last;
        Ok(())
    }

    /// Smallest index still present.
    pub fn first_index(&self) -> u64 {
        self.first_index
    }

    /// Highest index ever appended (0 if the log is empty).
    pub fn last_index(&self) -> u64 {
        self.last_index
    }

    /// Number of records currently present.
    pub fn len(&self) -> u64 {
        if self.last_index >= self.first_index {
            self.last_index - self.first_index + 1
        } else {
            0
        }
    }

    /// Whether the log holds no records.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Build the path for the segment whose first record has index `base`.
fn segment_path(dir: &Path, base: u64) -> PathBuf {
    dir.join(format!("{SEGMENT_PREFIX}{base:020}{SEGMENT_SUFFIX}"))
}

/// Return the `base_index` of every segment file found in `dir`.
fn discover_segments(dir: &Path) -> Result<Vec<u64>> {
    let mut bases = Vec::new();
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        let Some(rest) = name.strip_prefix(SEGMENT_PREFIX) else { continue };
        let Some(digits) = rest.strip_suffix(SEGMENT_SUFFIX) else { continue };
        match digits.parse::<u64>() {
            Ok(base) => bases.push(base),
            Err(_) => {
                return Err(Error::wal(format!("malformed segment file name: {name}")))
            }
        }
    }
    Ok(bases)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn payload(i: u64) -> Vec<u8> {
        format!("entry-{i}").into_bytes()
    }

    #[test]
    fn append_assigns_sequential_indices() {
        let dir = tempdir().unwrap();
        let mut log = Log::open(dir.path(), 1 << 20).unwrap();
        assert_eq!(log.last_index(), 0);
        for i in 1..=5 {
            assert_eq!(log.append(&payload(i)).unwrap(), i);
        }
        assert_eq!(log.first_index(), 1);
        assert_eq!(log.last_index(), 5);
        assert_eq!(log.len(), 5);
    }

    #[test]
    fn rotation_creates_multiple_segments() {
        let dir = tempdir().unwrap();
        // Tiny segment size forces frequent rotation.
        let mut log = Log::open(dir.path(), 32).unwrap();
        for i in 1..=20 {
            log.append(&payload(i)).unwrap();
        }
        log.sync().unwrap();
        assert!(discover_segments(dir.path()).unwrap().len() > 1, "expected rotation");

        let all = log.read_all().unwrap();
        assert_eq!(all.len(), 20);
        for (i, (idx, p)) in all.iter().enumerate() {
            assert_eq!(*idx, i as u64 + 1);
            assert_eq!(*p, payload(i as u64 + 1));
        }
    }

    #[test]
    fn reopen_recovers_all_entries() {
        let dir = tempdir().unwrap();
        {
            let mut log = Log::open(dir.path(), 64).unwrap();
            for i in 1..=10 {
                log.append(&payload(i)).unwrap();
            }
            log.sync().unwrap();
        }
        let log = Log::open(dir.path(), 64).unwrap();
        assert_eq!(log.last_index(), 10);
        assert_eq!(log.read_all().unwrap().len(), 10);
    }

    #[test]
    fn truncate_suffix_drops_tail() {
        let dir = tempdir().unwrap();
        let mut log = Log::open(dir.path(), 64).unwrap();
        for i in 1..=10 {
            log.append(&payload(i)).unwrap();
        }
        log.sync().unwrap();
        log.truncate_suffix(6).unwrap();
        assert_eq!(log.last_index(), 6);
        let all = log.read_all().unwrap();
        assert_eq!(all.len(), 6);
        assert_eq!(all.last().unwrap().0, 6);

        // The log is still writable and continues from the new tail.
        assert_eq!(log.append(&payload(7)).unwrap(), 7);
    }

    #[test]
    fn truncate_prefix_advances_first_index() {
        let dir = tempdir().unwrap();
        let mut log = Log::open(dir.path(), 16).unwrap(); // many small segments
        for i in 1..=20 {
            log.append(&payload(i)).unwrap();
        }
        log.sync().unwrap();
        log.truncate_prefix(10).unwrap();
        assert!(log.first_index() <= 10);
        assert_eq!(log.last_index(), 20);
    }
}
