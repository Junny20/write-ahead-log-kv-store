// The write-ahead log, bottom-up: record (framing + CRC), segment (one append-only
// file with fsync and recovery), log (segments stitched into one sequence). The entry
// point is Log; it stores opaque bytes.

pub mod log;
pub mod record;
pub mod segment;

pub use log::Log;
pub use record::{RecordRead, MAX_RECORD_LEN};
pub use segment::Segment;
