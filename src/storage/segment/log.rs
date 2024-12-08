use std::io::SeekFrom;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context as _, Result};
use bytes::Bytes;
use tokio::fs::File;
use tokio::io::{self, AsyncReadExt as _, AsyncSeekExt as _, AsyncWriteExt};

use crate::kafka::types::{Compact, Sequence, Uuid};
use crate::kafka::{AsyncSerialize, Serialize};
use crate::storage::Storage;

#[derive(Debug)]
pub struct LogFile {
    /// min offset of the log file (also contained in the file name)
    base_offset: i64,
    /// file location
    path: PathBuf,
    /// open log file
    file: File,
}

impl LogFile {
    #[inline]
    pub fn new(base_offset: i64, path: PathBuf, file: File) -> Self {
        Self {
            base_offset,
            path,
            file,
        }
    }

    #[inline]
    pub fn base_offset(&self) -> i64 {
        self.base_offset
    }

    #[inline]
    pub fn path(&self) -> &Path {
        &self.path
    }

    // XXX: store and manage file size in self, so that we don't have to request file metadata
    /// Get the total length in bytes of this log file
    pub async fn len(&self) -> Result<u64> {
        // XXX: fsync here / or rather after writing new records
        //self.file.sync_all().await.with_context(|| format!("sync log file: {:?}", self.path))?;
        self.file
            .metadata()
            .await
            .map(|metadata| metadata.len())
            .with_context(|| format!("{:?} metadata", self.path))
    }

    /// Re-open this log file in read-only mode
    async fn read_open(&self) -> Result<File> {
        let path = self.path();
        File::options()
            .read(true)
            .open(path)
            .await
            .with_context(|| format!("failed to open log file {path:?}"))
    }
}

impl AsRef<File> for LogFile {
    #[inline]
    fn as_ref(&self) -> &File {
        &self.file
    }
}

impl AsMut<File> for LogFile {
    #[inline]
    fn as_mut(&mut self) -> &mut File {
        &mut self.file
    }
}

#[derive(Clone)]
pub struct LogRef {
    topic: Uuid,
    partition: i32,
    /// Index of the corresponding [`LogFile`] in the [`PartitionIndex`]
    index: usize,
    /// Byte offset within the log [`File`]
    position: u64,
    /// The number of bytes to copy from the log [`File`], starting at `position`
    length: u64,
    storage: Arc<Storage>,
}

impl LogRef {
    #[inline]
    pub fn new(
        topic: Uuid,
        partition: i32,
        index: usize,
        position: u64,
        length: u64,
        storage: Arc<Storage>,
    ) -> Option<Self> {
        if length > 0 {
            Some(Self {
                topic,
                partition,
                index,
                position,
                length,
                storage,
            })
        } else {
            None
        }
    }
}

impl std::fmt::Debug for LogRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LogRef")
            .field("topic", &self.topic)
            .field("partition", &self.partition)
            .field("index", &self.index)
            .field("position", &self.position)
            .field("length", &self.length)
            .finish_non_exhaustive()
    }
}

impl std::fmt::Display for LogRef {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "LogRef({}, {}, {}, {}, {})",
            self.topic.as_hex(),
            self.partition,
            self.index,
            self.position,
            self.length,
        )
    }
}

impl Sequence for LogRef {
    #[inline]
    fn length(&self) -> usize {
        (self.length - self.position) as usize
    }
}

impl Serialize for LogRef {
    #[inline]
    fn encode_size(&self, _version: i16) -> usize {
        self.length()
    }
}

impl Serialize for Option<LogRef> {
    const SIZE: usize = Bytes::SIZE;

    #[inline]
    fn encode_size(&self, version: i16) -> usize {
        self.as_ref().map_or(Self::SIZE, |b| b.encode_size(version))
    }
}

impl AsyncSerialize for LogRef {
    async fn write_into<W>(self, writer: &mut W, _version: i16) -> Result<()>
    where
        W: AsyncWriteExt + Send + Unpin + ?Sized,
    {
        // XXX: it's a bit unfortunate that to keep LogRef 'static we have to re-access the File
        let storage = self.storage.inner.read().await;

        let ix = storage
            .log_index
            .get(&self.topic)
            .with_context(|| format!("{self}: topic not found in storage"))?;

        let ix = ix
            .partitions
            .get(&self.partition)
            .with_context(|| format!("{self}: partition not found in storage"))?;

        let segment = ix
            .get_segment(self.index)
            .with_context(|| format!("{self}: invalid log file index"))?;

        // TODO: An interesting optimization would be to use https://linux.die.net/man/2/sendfile
        //  - sendfile(2) can send data (fd -> socket) without copying them to the userspace
        //  - https://docs.rs/sendfile/latest/sendfile
        //  - NOTE: codecrafters runners unfortunately don't allow modifying Cargo.lock

        // open new log file descriptor instead of contesting for an exclusive lock
        let mut log = segment
            .log()
            .read_open()
            .await
            .context("serializing log file")?;

        log.seek(SeekFrom::Start(self.position))
            .await
            .with_context(|| format!("{self}: invalid physical position"))?;

        // NOTE: the encoded length of the response has already been written, so we must respect it
        let mut reader = log.take(self.length() as u64);

        io::copy(&mut reader, writer)
            .await
            .with_context(|| format!("{self}: copying log file contents"))
            .map(|_| ())
    }
}

impl AsyncSerialize for Option<LogRef> {
    async fn write_into<W>(self, writer: &mut W, version: i16) -> Result<()>
    where
        W: AsyncWriteExt + Send + Unpin + ?Sized,
    {
        if let Some(log) = self {
            log.write_into(writer, version).await
        } else {
            writer.write_i32(-1).await.context("log bytes length")
        }
    }
}

impl AsyncSerialize for Compact<LogRef> {
    async fn write_into<W>(self, writer: &mut W, version: i16) -> Result<()>
    where
        W: AsyncWriteExt + Send + Unpin + ?Sized,
    {
        self.enc_len()
            .write_into(writer, version)
            .await
            .context("compact log bytes length")?;

        self.0
            .write_into(writer, version)
            .await
            .context("compact log bytes contents")
    }
}
