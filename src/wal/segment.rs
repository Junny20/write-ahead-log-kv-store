// One log segment = one append-only file. Appends buffer; sync() flushes and fsyncs.
// On startup, recover() scans the file, drops a torn record at the tail, and refuses
// to open a file whose interior is corrupt.

use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use tracing::warn;

use super::record::{self, RecordRead};
use crate::{Error, Result};

/// One append-only log file.
#[derive(Debug)]
pub struct Segment {
    // Logical index of the first record in this segment (encoded in the file name).
    base_index: u64,
    path: PathBuf,
    writer: BufWriter<File>,
    /// Total bytes on disk (kept in sync with appends so rotation decisions are O(1)).
    bytes: u64,
    /// Number of records currently in the file.
    count: u64,
}

impl Segment {
    // Create a brand-new empty segment. Fails if the file exists; use recover() for an
    // existing one.
    pub fn create(path: impl Into<PathBuf>, base_index: u64) -> Result<Self> {
        let path = path.into();
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path)?;
        Ok(Segment {
            base_index,
            path,
            writer: BufWriter::new(file),
            bytes: 0,
            count: 0,
        })
    }

    // Reopen an existing segment for appending, recovering a possibly-torn tail.
    // A partial record at the end is truncated away. A CRC failure with valid data
    // after it means interior corruption and errors instead.
    pub fn recover(path: impl Into<PathBuf>, base_index: u64) -> Result<Self> {
        let path = path.into();
        let file = OpenOptions::new().read(true).open(&path)?;
        let file_len = file.metadata()?.len();

        let mut reader = BufReader::new(file);
        let mut good_offset = 0u64;
        let mut count = 0u64;

        loop {
            match record::read_record(&mut reader)? {
                RecordRead::Entry(_) => {
                    good_offset = reader.stream_position()?;
                    count += 1;
                }
                RecordRead::Eof => break,
                RecordRead::Truncated => {
                    // Ran out of bytes mid-record: a torn tail. Everything up to
                    // `good_offset` is intact; drop the rest.
                    warn!(
                        segment = %path.display(),
                        offset = good_offset,
                        "discarding torn record at tail during recovery",
                    );
                    break;
                }
                RecordRead::Corrupt => {
                    let pos = reader.stream_position()?;
                    if pos < file_len && rest_parses_cleanly(&mut reader)? {
                        // There is more data after the bad record, and it decodes as a
                        // clean run of valid records - this isn't a torn tail, it's
                        // real corruption sitting behind otherwise-good data.
                        return Err(Error::Corruption(format!(
                            "crc mismatch in {} at offset {good_offset}",
                            path.display()
                        )));
                    }
                    // A CRC failure with nothing salvageable after it (either nothing
                    // follows, or what follows doesn't parse either) is most likely a
                    // torn write that happened to fill the length field.
                    warn!(
                        segment = %path.display(),
                        offset = good_offset,
                        "discarding corrupt trailing record during recovery",
                    );
                    break;
                }
            }
        }

        // Physically truncate to the last good boundary and reopen for appending.
        let file = OpenOptions::new().read(true).write(true).open(&path)?;
        file.set_len(good_offset)?;
        file.sync_all()?;
        let mut writer = BufWriter::new(file);
        writer.seek(SeekFrom::End(0))?;

        Ok(Segment {
            base_index,
            path,
            writer,
            bytes: good_offset,
            count,
        })
    }

    // Append one record. Buffered only; call sync() to make it durable.
    pub fn append(&mut self, payload: &[u8]) -> Result<()> {
        let written = record::write_record(&mut self.writer, payload)?;
        self.bytes += written as u64;
        self.count += 1;
        Ok(())
    }

    /// Flush buffered data and `fsync` the file, making every prior append durable.
    pub fn sync(&mut self) -> Result<()> {
        self.writer.flush()?;
        self.writer.get_ref().sync_all()?;
        Ok(())
    }

    /// Iterate the records in this segment from the beginning. Reads through a fresh
    /// handle, so it is independent of the append position.
    pub fn iter(&self) -> Result<SegmentIter> {
        let file = OpenOptions::new().read(true).open(&self.path)?;
        Ok(SegmentIter { reader: BufReader::new(file), done: false })
    }

    /// Logical index of the first record in this segment.
    pub fn base_index(&self) -> u64 {
        self.base_index
    }

    /// Number of records currently stored.
    pub fn count(&self) -> u64 {
        self.count
    }

    /// Bytes currently on disk.
    pub fn len_bytes(&self) -> u64 {
        self.bytes
    }

    /// Path to the backing file.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Delete the segment file. Consumes the segment.
    pub fn remove(self) -> Result<()> {
        std::fs::remove_file(&self.path)?;
        Ok(())
    }
}

// Check whether everything from the reader's current position parses as a clean run
// of valid records reaching a natural EOF. Used to tell genuine interior corruption
// (good data follows the bad record) apart from a torn tail (the bad record was
// itself part of the same torn write, and what follows is more unparseable garbage).
fn rest_parses_cleanly(reader: &mut BufReader<File>) -> Result<bool> {
    loop {
        match record::read_record(reader)? {
            RecordRead::Entry(_) => continue,
            RecordRead::Eof => return Ok(true),
            RecordRead::Truncated | RecordRead::Corrupt => return Ok(false),
        }
    }
}

// Forward iterator over a segment's records. After recover() a segment holds only
// whole records, so a Truncated/Corrupt here is a bug and surfaces as an error.
#[derive(Debug)]
pub struct SegmentIter {
    reader: BufReader<File>,
    done: bool,
}

impl Iterator for SegmentIter {
    type Item = Result<Vec<u8>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        match record::read_record(&mut self.reader) {
            Ok(RecordRead::Entry(p)) => Some(Ok(p)),
            Ok(RecordRead::Eof) => {
                self.done = true;
                None
            }
            Ok(RecordRead::Truncated) | Ok(RecordRead::Corrupt) => {
                self.done = true;
                Some(Err(Error::wal("unexpected partial/corrupt record while iterating")))
            }
            Err(e) => {
                self.done = true;
                Some(Err(e.into()))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn append_sync_and_iterate() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("000001.log");

        let mut seg = Segment::create(&path, 1).unwrap();
        seg.append(b"one").unwrap();
        seg.append(b"two").unwrap();
        seg.append(b"three").unwrap();
        seg.sync().unwrap();
        assert_eq!(seg.count(), 3);

        let got: Vec<_> = seg.iter().unwrap().collect::<Result<_>>().unwrap();
        assert_eq!(got, vec![b"one".to_vec(), b"two".to_vec(), b"three".to_vec()]);
    }

    #[test]
    fn recover_truncates_torn_tail() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("000001.log");

        {
            let mut seg = Segment::create(&path, 1).unwrap();
            seg.append(b"durable-one").unwrap();
            seg.append(b"durable-two").unwrap();
            seg.sync().unwrap();
        }

        // Simulate a crash mid-append by appending a few raw garbage bytes.
        {
            use std::io::Write;
            let mut f = OpenOptions::new().append(true).open(&path).unwrap();
            f.write_all(&[0xAB, 0xCD, 0xEF]).unwrap();
            f.sync_all().unwrap();
        }

        let seg = Segment::recover(&path, 1).unwrap();
        assert_eq!(seg.count(), 2, "torn tail should be dropped");
        let got: Vec<_> = seg.iter().unwrap().collect::<Result<_>>().unwrap();
        assert_eq!(got, vec![b"durable-one".to_vec(), b"durable-two".to_vec()]);
    }

    #[test]
    fn recover_then_append_more() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("000001.log");
        {
            let mut seg = Segment::create(&path, 1).unwrap();
            seg.append(b"first").unwrap();
            seg.sync().unwrap();
        }
        let mut seg = Segment::recover(&path, 1).unwrap();
        seg.append(b"second").unwrap();
        seg.sync().unwrap();
        let got: Vec<_> = seg.iter().unwrap().collect::<Result<_>>().unwrap();
        assert_eq!(got, vec![b"first".to_vec(), b"second".to_vec()]);
    }
}
