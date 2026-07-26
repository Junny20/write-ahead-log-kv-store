// Record framing for the log. Each record is [crc32][length][payload], all
// little-endian with a 4-byte header. The CRC covers length + payload, so a corrupt
// length is caught before we allocate on it.
//
// Reading returns Eof (clean boundary), Truncated (partial record at the tail, i.e. a
// torn write - safe to drop on recovery), or Corrupt (CRC mismatch on a complete
// record - real corruption).

use std::io::{self, Read, Write};

// Size of the fixed record header (crc32 + length).
pub const HEADER_LEN: usize = 8;

// Upper bound on a payload. Anything larger is treated as a corrupt length field,
// guarding against huge bogus allocations.
pub const MAX_RECORD_LEN: u32 = 64 * 1024 * 1024;

// The outcome of reading one record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecordRead {
    Entry(Vec<u8>),   // a complete, CRC-verified payload
    Eof,              // clean end of file on a record boundary
    Truncated,        // partial record at the tail (torn write); drop on recovery
    Corrupt,          // complete record whose CRC didn't match
}

// Append one framed record and return bytes written. Does not flush/fsync.
pub fn write_record<W: Write>(w: &mut W, payload: &[u8]) -> io::Result<usize> {
    debug_assert!(
        payload.len() as u64 <= MAX_RECORD_LEN as u64,
        "payload exceeds MAX_RECORD_LEN",
    );
    let len = payload.len() as u32;
    let len_bytes = len.to_le_bytes();

    // CRC over length ++ payload.
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&len_bytes);
    hasher.update(payload);
    let crc = hasher.finalize();

    w.write_all(&crc.to_le_bytes())?;
    w.write_all(&len_bytes)?;
    w.write_all(payload)?;
    Ok(HEADER_LEN + payload.len())
}

// Read one framed record. A torn tail returns Truncated rather than erroring; real I/O
// failures still propagate.
pub fn read_record<R: Read>(r: &mut R) -> io::Result<RecordRead> {
    let mut header = [0u8; HEADER_LEN];
    match fill(r, &mut header)? {
        0 => return Ok(RecordRead::Eof), // clean boundary
        n if n < HEADER_LEN => return Ok(RecordRead::Truncated),
        _ => {}
    }

    let crc = u32::from_le_bytes(header[0..4].try_into().unwrap());
    let len = u32::from_le_bytes(header[4..8].try_into().unwrap());

    // A length past our ceiling almost certainly means the length field itself is
    // corrupt; refuse to allocate on it.
    if len > MAX_RECORD_LEN {
        return Ok(RecordRead::Corrupt);
    }

    let mut payload = vec![0u8; len as usize];
    if fill(r, &mut payload)? < payload.len() {
        return Ok(RecordRead::Truncated);
    }

    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&len.to_le_bytes());
    hasher.update(&payload);
    if hasher.finalize() != crc {
        return Ok(RecordRead::Corrupt);
    }

    Ok(RecordRead::Entry(payload))
}

// Read up to buf.len() bytes; return how many. A short read at EOF is reported, not
// errored - that's how we detect torn tails.
fn fill<R: Read>(r: &mut R, buf: &mut [u8]) -> io::Result<usize> {
    let mut read = 0;
    while read < buf.len() {
        match r.read(&mut buf[read..]) {
            Ok(0) => break,
            Ok(n) => read += n,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        }
    }
    Ok(read)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn roundtrip_several_records() {
        let mut buf = Vec::new();
        for payload in [b"".as_ref(), b"a", b"hello world", &[0u8; 1000]] {
            write_record(&mut buf, payload).unwrap();
        }

        let mut cur = Cursor::new(buf);
        for expected in [b"".as_ref(), b"a", b"hello world", &[0u8; 1000]] {
            match read_record(&mut cur).unwrap() {
                RecordRead::Entry(p) => assert_eq!(p, expected),
                other => panic!("expected entry, got {other:?}"),
            }
        }
        assert_eq!(read_record(&mut cur).unwrap(), RecordRead::Eof);
    }

    #[test]
    fn torn_header_is_truncated() {
        let mut buf = Vec::new();
        write_record(&mut buf, b"payload").unwrap();
        buf.truncate(3); // fewer than HEADER_LEN bytes survive
        let mut cur = Cursor::new(buf);
        assert_eq!(read_record(&mut cur).unwrap(), RecordRead::Truncated);
    }

    #[test]
    fn torn_payload_is_truncated() {
        let mut buf = Vec::new();
        write_record(&mut buf, b"a longer payload").unwrap();
        buf.truncate(HEADER_LEN + 4); // header intact, payload cut short
        let mut cur = Cursor::new(buf);
        assert_eq!(read_record(&mut cur).unwrap(), RecordRead::Truncated);
    }

    #[test]
    fn flipped_bit_is_corrupt() {
        let mut buf = Vec::new();
        write_record(&mut buf, b"important").unwrap();
        *buf.last_mut().unwrap() ^= 0xFF; // corrupt the payload but keep its length
        let mut cur = Cursor::new(buf);
        assert_eq!(read_record(&mut cur).unwrap(), RecordRead::Corrupt);
    }
}
