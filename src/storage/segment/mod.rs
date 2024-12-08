pub use self::index::LogIndex;
pub use self::log::LogFile;

pub mod index;
pub mod log;

// TODO: synchronization (of the log file and index)
/// Structure representing a log segment.
///
/// A segment is a collection of associated files corresponding to consecutive parts of a
/// `(topic, partition)` file-system directory (identified by the base/first offset). Notably, each
/// segment contains:
///  - A _log file_, which stores raw record batches
///  - An _index_, which serves for efficient record lookup within the associated log file (note
///    that this sidesteps the need to fetch possibly large chunks of records into memory or to
///    seek the log file). The index can be very small compared to the log file.
///
/// Log segments are immutable modulo the last (active) one, which is append-only, and are only
/// discarded due to set retention policy (typically total size). Note that rolling segments it not
/// implemented here.
#[derive(Debug)]
pub struct Segment {
    base_offset: i64,
    log: LogFile,
    index: LogIndex,
}

impl Segment {
    #[inline]
    pub fn new(log: LogFile, index: LogIndex) -> Self {
        Self {
            base_offset: log.base_offset(),
            log,
            index,
        }
    }

    #[inline]
    pub fn base_offset(&self) -> i64 {
        self.base_offset
    }

    #[inline]
    pub fn log(&self) -> &LogFile {
        &self.log
    }

    #[inline]
    pub fn index(&self) -> &LogIndex {
        &self.index
    }
}
