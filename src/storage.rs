use std::collections::{BTreeMap, HashMap};
use std::io::SeekFrom;
use std::ops::ControlFlow;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{bail, Context as _, Result};
use bytes::Bytes;
use tokio::fs::{self, File};
use tokio::io::{self, AsyncReadExt as _, AsyncSeekExt as _, AsyncWriteExt};
use tokio::sync::{Mutex, MutexGuard, RwLock};
use tokio::task::JoinSet;

use crate::kafka::record::{RecordBatch, RecordBatchHeader, Topic, TopicRecord};
use crate::kafka::types::{Compact, Sequence, StrBytes, Uuid};
use crate::kafka::{AsyncDeserialize, AsyncSerialize, Deserialize, Serialize};
use crate::logs::LogDir;

#[derive(Debug)]
pub enum Store<M, F> {
    Memory(M),
    FileSystem(F),
}

impl<M, F> Serialize for Store<M, F>
where
    M: Serialize,
    F: Serialize,
{
    fn encode_size(&self, version: i16) -> usize {
        match self {
            Self::Memory(data) => data.encode_size(version),
            Self::FileSystem(entry) => entry.encode_size(version),
        }
    }
}

impl<M, F> Serialize for Compact<Store<M, F>>
where
    M: Serialize + Sequence,
    F: Serialize + Sequence,
{
    fn encode_size(&self, version: i16) -> usize {
        match self.0 {
            Store::Memory(ref data) => data.encode_size(version),
            Store::FileSystem(ref entry) => entry.encode_size(version),
        }
    }
}

impl<M, F> Serialize for Option<Store<M, F>>
where
    M: Serialize + Sequence,
    F: Serialize + Sequence,
{
    // Sequence length
    const SIZE: usize = Bytes::SIZE;

    #[inline]
    fn encode_size(&self, version: i16) -> usize {
        self.as_ref().map_or(Self::SIZE, |b| b.encode_size(version))
    }
}

impl<M, F> AsyncSerialize for Store<M, F>
where
    M: AsyncSerialize,
    F: AsyncSerialize,
{
    async fn write_into<W>(self, writer: &mut W, version: i16) -> Result<()>
    where
        W: AsyncWriteExt + Send + Unpin + ?Sized,
    {
        match self {
            Self::Memory(data) => data
                .write_into(writer, version)
                .await
                .context("memory data"),

            Self::FileSystem(entry) => entry
                .write_into(writer, version)
                .await
                .context("file system entry"),
        }
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

        let log_file = ix
            .get_log(self.index)
            .with_context(|| format!("{self}: invalid log file index"))?;

        let mut log_file = log_file.file.lock().await;
        let log = &mut *log_file;

        // TODO: An interesting optimization would be to use https://linux.die.net/man/2/sendfile
        //  - sendfile(2) can send data (fd -> socket) without copying them to the userspace
        //  - https://docs.rs/sendfile/latest/sendfile
        //  - NOTE: codecrafters runners unfortunately don't allow modifying Cargo.lock

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

#[derive(Debug)]
struct LogFile {
    /// min offset of the log file (also contained in the file name)
    base_offset: i64,
    #[allow(dead_code)]
    /// file location
    path: PathBuf,
    /// open log file
    file: Mutex<File>,
}

impl LogFile {
    #[inline]
    fn new(base_offset: i64, path: PathBuf, file: File) -> Self {
        Self {
            base_offset,
            path,
            file: Mutex::new(file),
        }
    }

    #[inline]
    fn base_offset(&self) -> i64 {
        self.base_offset
    }

    #[inline]
    async fn lock(&self) -> MutexGuard<'_, File> {
        self.file.lock().await
    }
}

//#[derive(Debug)]
//struct IndexEntry {
//    /// offset relative to the base offset
//    relative_offset: i64,
//    /// byte offset within the log file
//    physical_position: i32,
//}
//
// TODO: Kafka uses a .index file instead
//  - most likely does not manage this in memory
//  - it still may do a binary search seeks on the .index file, given that all entries have
//    uniform size
//  - https://stackoverflow.com/a/30233048
//  - https://github.com/apache/kafka/blob/trunk/storage/src/main/java/org/apache/kafka/storage/internals/log/OffsetIndex.java
//  - apparently Kafka mmaps the index file to do the search
//  - https://docs.rs/memmap2/latest/memmap2
///// Append-only index of record offset-to-position for a corresponding log file
//#[derive(Debug)]
//struct LogIndex {
//    base_offset: i64,
//    // path: PathBuf,
//    /// Index entries with monotonically increasing relative offsets
//    entries: Vec<IndexEntry>,
//}
//
//impl LogIndex {
//    fn lookup(&self, offset: i64) -> Result<&IndexEntry, Option<&IndexEntry>> {
//        match self.entries.binary_search_by_key(&offset, |entry| {
//            self.base_offset + entry.relative_offset as i64
//        }) {
//            // SAFETY: index returned by a binary search hit, so must be within bounds
//            Ok(i) => Ok(unsafe { self.entries.get_unchecked(i) }),
//            Err(i) => Err(self.entries.get(i)),
//        }
//    }
//
//    fn append(&mut self, offset: i64, position: i32) -> Result<()> {
//        use anyhow::ensure;
//
//        ensure!(offset > self.base_offset, "offset <= base offset");
//        let relative_offset = offset - self.base_offset;
//
//        if let Some(last) = self.entries.last() {
//            ensure!(
//                relative_offset > last.relative_offset,
//                "offset <= last offset"
//            );
//        }
//
//        self.entries.push(IndexEntry {
//            relative_offset,
//            physical_position: position,
//        });
//
//        Ok(())
//    }
//}

#[derive(Debug)]
struct PartitionIndex {
    //topic: Topic,
    //partition: i32,
    /// path to the partition directory
    dir: PathBuf,
    /// vector of (base offset, log file) pairs, sorted by the offset (asc)
    logs: Vec<LogFile>,
}

impl PartitionIndex {
    #[inline]
    fn new(dir: impl AsRef<Path>) -> Self {
        Self {
            //topic: topic.clone(),
            //partition,
            dir: dir.as_ref().to_path_buf(),
            logs: Vec::new(),
        }
    }

    #[inline]
    fn get_log(&self, index: usize) -> Option<&LogFile> {
        self.logs.get(index)
    }

    fn lookup_file(&self, offset: i64) -> Option<(usize, &LogFile)> {
        let (Ok(ix) | Err(ix)) = self
            .logs
            .binary_search_by_key(&offset, LogFile::base_offset);
        self.get_log(ix).map(|file| (ix, file))
    }
}

#[derive(Debug, Default)]
struct TopicIndex {
    partitions: BTreeMap<i32, PartitionIndex>,
}

#[derive(Debug)]
pub struct StorageInner {
    #[allow(dead_code)]
    log_dirs: Vec<LogDir>,
    /// (topic id, partition) -> ({base offset -> log file}, ..metadata)
    log_index: HashMap<Uuid, TopicIndex>,
    /// Topic IDs by names
    topics: HashMap<StrBytes, Topic>,
    // TODO: similar mapping to `log_index` for the __cluster_metadata logs (`metadata_index`)
}

impl StorageInner {
    async fn new(log_dirs: Vec<LogDir>) -> Result<Self> {
        let topics = lookup_topics(&log_dirs)
            .await
            .context("find topics in cluster metadata")?;

        let topics = topics
            .into_iter()
            .map(|topic| (topic.name.clone(), topic))
            .collect::<HashMap<_, _>>();

        let log_index = index_logs(&log_dirs, &topics)
            .await
            .context("indexing partition logs")?;

        Ok(Self {
            log_dirs,
            log_index,
            topics,
        })
    }
}

#[derive(Debug)]
pub struct Storage {
    // TODO: more fine-grained access control (most operations are topic/partition-based)
    inner: RwLock<StorageInner>,
}

impl Storage {
    pub async fn new(log_dirs: Vec<LogDir>) -> Result<Self> {
        let inner = StorageInner::new(log_dirs).await.map(RwLock::new)?;
        Ok(Self { inner })
    }

    /// Given a `topic` name, returns corresponding topic ID and a list of partitions in it.
    ///
    /// ### Additional parameters
    ///  - `min_partition` is the first partition to return
    ///  - `partition_limit` is the maximum number of partitions to return
    ///
    /// ### Output
    /// In addition to the topic ID and partitions, this returns an optional cursor information if
    /// the partition limit is exceeded.
    ///
    /// Exceeding the quota is indicated by a [`ControlFlow::Break`] with
    ///  - `Some((topic_name, next_partition))` if there are some partitions left in this topic
    ///  - `None` if the limit has been reached at the very last partition in this topic
    pub async fn describe_topic(
        &self,
        topic: &StrBytes,
        min_partition: i32,
        partition_limit: usize,
    ) -> Option<(Uuid, Vec<i32>, ControlFlow<Option<(StrBytes, i32)>>)> {
        let storage = self.inner.read().await;

        let topic = storage.topics.get(topic)?;

        let ix = storage.log_index.get(&topic.topic_id)?;

        let max_partition = min_partition + partition_limit as i32;

        // NOTE: range UB is intentionally inclusive to fetch next partition if limit is exceeded
        let mut partitions = ix
            .partitions
            .range(min_partition..=max_partition)
            .map(|(&partition, _)| partition)
            .collect::<Vec<_>>();

        // check if we exceeded the partition limit and determine where to continue in the future
        let next = if partitions.len() > partition_limit {
            ControlFlow::Break(partitions.pop().map(|p| (topic.name.clone(), p)))
        } else {
            ControlFlow::Continue(())
        };

        Some((topic.topic_id(), partitions, next))
    }

    // TODO: Kafka does not fetch records into memory
    //  - https://www.confluent.io/blog/kafka-producer-and-consumer-internals-4-consumer-fetch-requests
    /// Fetch a record batch containing given `offset` for the `topic` and `partition`.
    ///
    /// Note that this function just determines the physical position of the fetched chunk of
    /// records within the log file and defers the I/O to the message serialization phase (see the
    /// [`impl AsyncSerialize for LogRef`](<LogRef as AsyncSerialize>)).
    ///
    /// Once the file offset is calculated, the response writer can perform an optimized I/O
    /// operation without actually loading all the records to the memory.
    ///
    /// The returned [`LogRef`] logically represents a batch of raw records (file contents). This
    /// means that:
    ///  1. There can (and most likely will) be multiple records returned, all inside the record
    ///     batch which includes given `offset`.
    ///  2. Record batches can in general contain offsets _smaller_ than the `offset` and it's up
    ///     to the consumer to filter these out.
    ///
    /// Finally, note that the `LogRef::length` is also determined here. We need to calculate the
    /// length ahead of time, because the wire protocol specifies the encoded (message) length
    /// before the encoded message itself. So we must commit to some record chunk at some point, we
    /// do it here when determining the physical offset (`LogRef::position`).
    pub async fn fetch_records(
        self: Arc<Self>,
        topic: Uuid,
        partition: i32,
        offset: i64,
    ) -> Result<FetchResult> {
        let storage = self.inner.read().await;

        let Some(topic_ix) = storage.log_index.get(&topic) else {
            return Ok(FetchResult::UnknownTopic);
        };

        let Some(ix) = topic_ix.partitions.get(&partition) else {
            return Ok(FetchResult::UnknownTopic);
        };

        let Some((log_ix, log_file)) = ix.lookup_file(offset) else {
            return Ok(FetchResult::OffsetOutOfRange);
        };

        // TODO: This lock is unfortunate, because two clients can't really fetch concurrently
        //  - An exclusive lock is only required due to seek/rewind
        //  - With a memory-mapped index, this would not be necessary!
        let mut log_file = log_file.lock().await;
        let log = &mut *log_file;

        // start at the beginning of the log file
        let mut batch_start = log.rewind().await.context("seek to start of the log")?;

        // TODO: Kafka uses a separate index file alongside each log (i.e., per segment) for this
        //  - The index contains the file offset of each record (batch)
        //    (see https://stackoverflow.com/a/30233048)
        //  - So this file offset lookup can be made just based on the index without ever touching
        //    the data (log file)
        //  - So this really should be part of an increment indexing (segments are immutable,
        //    modulo the active one, which is append-only, and discarding due to retention)
        let records = loop {
            let Some(header) = try_read_header(log, batch_start)
                .await
                .context("failed to read batch header")?
            else {
                break Store::Memory(None);
            };

            if header.contains(offset) {
                // hit: read the rest of the log file, starting with this batch

                // XXX: fsync before? or seek end?
                //log.sync_all().await.context("sync log file")?;
                let metadata = log.metadata().await.context("retrieve log file metadata")?;
                let log_len = metadata.len();

                //let log_len = log
                //    .seek(SeekFrom::End(0))
                //    .await
                //    .context("seek the log file end")?;

                let length = log_len - batch_start;

                let log_ref = if length > 0 {
                    Some(LogRef {
                        topic,
                        partition,
                        index: log_ix,
                        position: batch_start,
                        length,
                        storage: Arc::clone(&self),
                    })
                } else {
                    None
                };

                break Store::FileSystem(log_ref);
            }

            // otherwise seek/skip over the contents of the current batch
            batch_start = log
                .seek(SeekFrom::Current(header.records_len() as i64))
                .await
                .context("seek next batch")?;
        };

        Ok(FetchResult::Records(records))
    }
}

async fn try_read_header(log: &mut File, batch_start: u64) -> Result<Option<RecordBatchHeader>> {
    use std::io::{Error, ErrorKind};
    match RecordBatchHeader::read(log).await {
        Ok((header, _)) => Ok(Some(header)),
        Err(e) if batch_start == 0 => match e.downcast::<Error>() {
            Ok(e) if matches!(e.kind(), ErrorKind::UnexpectedEof) => {
                // check whether the log file is actually empty
                let meta = log.metadata().await.context("log file metadata")?;
                if meta.len() > 0 {
                    Err(e).context("unexpected EOF on non-empty log file")
                } else {
                    Ok(None)
                }
            }
            Ok(e) => Err(e).context("I/O error other than unexpected EOF"),
            Err(e) => Err(e).context("not an I/O error"),
        },
        Err(e) => Err(e).context("non-empty log file: not a first batch in the log"),
    }
}

pub enum FetchResult {
    Records(Store<Option<Bytes>, Option<LogRef>>),
    UnknownTopic,
    OffsetOutOfRange,
}

async fn index_logs(
    log_dirs: &[LogDir],
    topics: &HashMap<StrBytes, Topic>,
) -> Result<HashMap<Uuid, TopicIndex>> {
    let mut index = HashMap::new();

    let partition_dirs = log_dirs.iter().flat_map(|dir| {
        dir.partitions
            .iter()
            .filter(|dir| !dir.is_cluster_metadata())
    });

    for dir in partition_dirs {
        let (topic, partition) = dir.topic_partition()?;
        let topic = StrBytes::from(topic);

        let Some(topic) = topics.get(&topic) else {
            bail!("topic '{topic:?}' not found in cluster metadata");
        };

        // NOTE: specifically taking the topic id from the cluster metadata here
        let ix: &mut TopicIndex = index.entry(topic.topic_id()).or_default();

        let ix = ix
            .partitions
            .entry(partition)
            .or_insert_with(|| PartitionIndex::new(&dir.path));

        index_partition(ix)
            .await
            .with_context(|| format!("failed to update index for parition {:?}", ix.dir))?;
    }

    Ok(index)
}

async fn index_partition(index: &mut PartitionIndex) -> Result<()> {
    let mut dir = fs::read_dir(&index.dir)
        .await
        .context("failed to read partition dir")?;

    let mut needs_sorting = false;
    let mut prev_offset = -1;

    while let Some(entry) = dir.next_entry().await.context("next dir entry")? {
        let path = entry.path();
        if let Some("log") = path.extension().and_then(|ext| ext.to_str()) {
            debug_assert!(path.is_file(), "'.log' should indicate a file");

            let Some(filename) = path.file_name().and_then(|name| name.to_str()) else {
                bail!("invalid log file {path:?}");
            };

            let Some(base_offset) = filename.strip_suffix(".log") else {
                unreachable!("path has already been checked for the '.log' extension");
            };

            let base_offset = base_offset.parse().with_context(|| {
                format!("'{base_offset}' does not encode a valid i64 base offset")
            })?;

            let log_file = fs::File::options()
                .read(true)
                .write(true)
                .open(&path)
                .await
                .context("failed to open log file")?;

            index.logs.push(LogFile::new(base_offset, path, log_file));

            needs_sorting &= prev_offset < base_offset;
            prev_offset = base_offset;
        }
    }

    if needs_sorting {
        index.logs.sort_unstable_by_key(LogFile::base_offset);
    }

    Ok(())
}

async fn lookup_topics(log_dirs: &[LogDir]) -> Result<Vec<Topic>> {
    let cluster_metadata_dirs = log_dirs.iter().flat_map(|log_dir| {
        log_dir
            .partitions
            .iter()
            .filter(|dir| dir.is_cluster_metadata())
            .map(|dir| dir.path.clone())
    });

    let mut tasks = JoinSet::new();
    for cluster_metadata_dir in cluster_metadata_dirs {
        tasks.spawn(discover_topics(cluster_metadata_dir));
    }

    let mut topics = Vec::new();

    while let Some(result) = tasks.join_next().await {
        match result {
            Ok(Ok(ts)) => topics.extend(ts),
            Ok(Err(e)) => bail!(e),
            Err(e) if e.is_cancelled() => tokio::task::yield_now().await,
            Err(e) if e.is_panic() => std::panic::resume_unwind(e.into_panic()),
            Err(e) => unreachable!("task neither panicked nor has been canceled: {e:?}"),
        }
    }

    Ok(topics)
}

async fn discover_topics(cluster_metadata_dir: impl AsRef<Path>) -> Result<Vec<Topic>> {
    let log_file = load_last_log_file(cluster_metadata_dir)
        .await
        .context("load last cluster metadata log")?;

    let Some(log_file) = log_file else {
        return Ok(Vec::new());
    };

    let (segment, _) = <Vec<RecordBatch>>::deserialize(&mut log_file.as_slice())?;
    //println!("loaded segment: {segment:?}");

    let topics = segment
        .into_iter()
        .flat_map(|batch| batch.records)
        .filter_map(
            |record| match TopicRecord::deserialize(&mut record.value?) {
                Ok((topic, _)) => Some(topic.into()),
                _ => None,
            },
        )
        .collect();

    Ok(topics)
}

async fn load_last_log_file(dir: impl AsRef<Path>) -> Result<Option<Vec<u8>>> {
    let dir = dir.as_ref();
    debug_assert!(dir.is_dir(), "input must be a directory, got {dir:?}");

    let mut entries = tokio::fs::read_dir(dir)
        .await
        .with_context(|| format!("read {dir:?}"))?;

    let mut log_file = None;

    while let Some(entry) = entries
        .next_entry()
        .await
        .with_context(|| format!("fetch next entry in {dir:?}"))?
    {
        let path = entry.path();
        match path.extension() {
            Some(ext) if ext == "log" => match &log_file {
                Some(last) if last < &path => log_file.insert(path),
                Some(_) => continue,
                _ => log_file.insert(path),
            },
            _ => continue,
        };
    }

    let Some(log_file) = log_file else {
        return Ok(None);
    };

    load_log_file(log_file).await.map(Some)
}

// TODO: don't load this into memory (just open file, then parse while reading)
async fn load_log_file(file_path: impl AsRef<Path>) -> Result<Vec<u8>> {
    let file_path = file_path.as_ref();
    fs::read(file_path)
        .await
        .with_context(|| format!("failed to read {file_path:?}"))
}
