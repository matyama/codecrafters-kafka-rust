use anyhow::{ensure, Context as _, Result};

#[derive(Debug, PartialEq)]
pub struct OffsetPosition {
    pub offset: i64,
    pub position: i32,
}

// XXX: seems to be serialized simply as two consecutive i64s
#[derive(Debug, PartialEq)]
struct IndexEntry {
    /// offset relative to the base offset
    relative_offset: i32,
    /// byte offset within the log file
    physical_position: i32,
}

// TODO: Kafka uses a .index file instead and memory-mapps it
//  - it does a binary search on the (mmaped) index file (note that all entries have fixed size)
//  - https://stackoverflow.com/a/30233048
//  - https://docs.rs/memmap2/latest/memmap2
/// Append-only index of record offset-to-position for a corresponding log file
///
/// [Reference implementation](https://github.com/apache/kafka/blob/50b6953661a46d7d57a8aca5c875e91a19166253/storage/src/main/java/org/apache/kafka/storage/internals/log/OffsetIndex.java)
#[derive(Debug)]
pub struct LogIndex {
    /// The absolute value of the first inserted entry
    base_offset: i64,
    /// The absolute value of the last inserted entry
    last_offset: i64,
    // path: PathBuf,
    /// Index entries with monotonically increasing relative offsets
    entries: Vec<IndexEntry>,
}

impl std::fmt::Display for LogIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("LogIndex([")?;

        for &IndexEntry {
            relative_offset,
            physical_position,
        } in self.entries.iter()
        {
            let offset = self.offset(relative_offset);
            write!(f, "({offset}, {physical_position})")?;
            if offset < self.last_offset {
                f.write_str(", ")?;
            }
        }

        f.write_str("])")
    }
}

impl LogIndex {
    // TODO: max_entries: usize
    #[inline]
    pub fn new(base_offset: i64) -> Self {
        debug_assert!(
            base_offset >= 0,
            "base offset should be non-negative, got: {base_offset}"
        );
        Self {
            base_offset,
            last_offset: -1,
            entries: Vec::new(),
        }
    }

    #[inline]
    fn offset(&self, relative_offset: i32) -> i64 {
        self.base_offset + relative_offset as i64
    }

    #[inline]
    fn relative_offset(&self, offset: i64) -> Option<i32> {
        offset
            .checked_sub(self.base_offset)
            .map(|offset| offset as i32)
    }

    /// Search the index for the physical position of the start of a record batch that contains a
    /// record with the given (logical/message) `offset`.
    pub fn lookup(&self, offset: i64) -> Option<OffsetPosition> {
        // XXX: Kafka does not seem to do this
        //if offset > self.last_offset {
        //    return Err(None);
        //}

        match self
            .entries
            .binary_search_by_key(&offset, |entry| self.offset(entry.relative_offset))
            .map_err(|pos| pos.checked_sub(1))
        {
            Ok(i) => {
                // SAFETY: index returned by a binary search hit, so must be within bounds
                let &IndexEntry {
                    relative_offset,
                    physical_position,
                } = unsafe { self.entries.get_unchecked(i) };

                Some(OffsetPosition {
                    offset: self.offset(relative_offset),
                    position: physical_position,
                })
            }
            Err(Some(i)) => self.entries.get(i).map(
                |&IndexEntry {
                     relative_offset,
                     physical_position,
                 }| OffsetPosition {
                    offset: self.offset(relative_offset),
                    position: physical_position,
                },
            ),
            Err(None) => None,
        }
    }

    pub fn append(&mut self, offset: i64, position: i32) -> Result<()> {
        ensure!(
            self.entries.is_empty() || offset > self.last_offset,
            "append({offset}, {position}) into a non-empty index with the last offset {}",
            self.last_offset
        );

        let relative_offset = self
            .relative_offset(offset)
            .with_context(|| format!("offset ({offset}) <= base offset ({})", self.base_offset))?;

        let entry = IndexEntry {
            relative_offset,
            physical_position: position,
        };

        self.entries.push(entry);
        self.last_offset = offset;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn log_index() {
        let mut index = LogIndex::new(10);

        assert!(
            index.lookup(index.base_offset).is_none(),
            "lookup on an empty index should fail"
        );

        index.append(10, 0).expect("new index entry");
        index.append(20, 10).expect("new index entry");
        index
            .append(15, 0)
            .expect_err("offset 15 is less than the base offset (10)");
        index.append(30, 15).expect("new index entry");
        index
            .append(30, 15)
            .expect_err("offset 30 is less than or equal to the last offset (30)");
        index
            .append(25, 10)
            .expect_err("offset 25 is less than or equal to the last offset (30)");
        index.append(40, 30).expect("new index entry");
        index.append(50, 45).expect("new index entry");

        let first = index
            .lookup(index.base_offset)
            .expect("non-empty index should contain an entry for the base offset");

        assert_eq!(
            OffsetPosition {
                offset: 10,
                position: 0,
            },
            first
        );

        let last = index
            .lookup(50)
            .expect("non-empty index should contain a last entry");

        assert_eq!(
            OffsetPosition {
                offset: 50,
                position: 45,
            },
            last
        );

        let inner = index
            .lookup(35)
            .expect("offset 35 is within an indexed batch");

        assert_eq!(
            OffsetPosition {
                offset: 30,
                position: 15,
            },
            inner
        );

        assert!(
            index.lookup(5).is_none(),
            "offset 0 is not in range of any indexed batch"
        );

        let out_of_range = index
            .lookup(60)
            .expect("out-of-range offset 60 should be bound to the last offset (50)");

        assert_eq!(
            OffsetPosition {
                offset: 50,
                position: 45,
            },
            out_of_range,
        );

        //assert!(
        //    index.lookup(60).is_none(),
        //    "offset 60 is not in range of any indexed batch",
        //);
    }
}
