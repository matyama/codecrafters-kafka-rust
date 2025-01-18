use std::collections::{BTreeMap, HashMap};
use std::io::SeekFrom;
use std::ops::ControlFlow;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{bail, Context as _, Result};
use bytes::Bytes;
use tokio::fs;
use tokio::io::{AsyncSeekExt as _, AsyncWriteExt};
use tokio::sync::RwLock;
use tokio::task::JoinSet;

use self::segment::index::LogIndex;
use self::segment::log::LogFile;
use self::segment::Segment;
use crate::kafka::record::{BatchHeader, RecordBatch, RecordBatchHeader, Topic, TopicRecord};
use crate::kafka::types::{Compact, Sequence, StrBytes, Uuid};
use crate::kafka::{AsyncDeserialize, AsyncSerialize, Deserialize, Serialize};
use crate::logs::LogDir;

pub(crate) use self::segment::log::LogRef;

pub(crate) mod segment;

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

#[derive(Debug)]
struct PartitionIndex {
    //topic: Topic,
    //partition: i32,
    /// path to the partition directory
    dir: PathBuf,
    /// vector of log [`Segment`]s, sorted by their base offsets (asc)
    segments: Vec<Segment>,
}

impl PartitionIndex {
    #[inline]
    fn new(dir: impl AsRef<Path>) -> Self {
        Self {
            //topic: topic.clone(),
            //partition,
            dir: dir.as_ref().to_path_buf(),
            segments: Vec::new(),
        }
    }

    #[inline]
    fn get_segment(&self, index: usize) -> Option<&Segment> {
        self.segments.get(index)
    }

    fn lookup_segment(&self, offset: i64) -> Option<(usize, &Segment)> {
        let (Ok(ix) | Err(ix)) = self
            .segments
            .binary_search_by_key(&offset, Segment::base_offset);
        self.get_segment(ix).map(|file| (ix, file))
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

        // NOTE: Topic IDs (UUID) is treated as immutable
        #[allow(clippy::mutable_key_type)]
        let topics = topics
            .into_iter()
            .map(|topic| (topic.name.clone(), topic))
            .collect::<HashMap<_, _>>();

        // NOTE: Bytes are only mutable due to ref-counting, but otherwise stay immutable
        #[allow(clippy::mutable_key_type)]
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

    /// Fetch a record batch containing given `offset` for the `topic` and `partition`.
    ///
    /// Note that this function just determines the physical position of the fetched chunk of
    /// records within the log file and defers the I/O to the message serialization phase (see the
    /// [`impl AsyncSerialize for LogRef`](<LogRef as AsyncSerialize>)).
    ///
    /// Once the file offset is calculated, the response writer can perform an optimized I/O
    /// operation without actually loading all the records to the memory.
    ///
    /// The returned `LogRef` logically represents a batch of raw records (file contents). This
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

        let Some((segment_ix, segment)) = ix.lookup_segment(offset) else {
            return Ok(FetchResult::OffsetOutOfRange);
        };

        let index = segment.index();

        // Search a physical record batch position using the index (i.e., without touching the log)
        let records = match index.lookup(offset) {
            Some(entry) => {
                // TODO: store this info within the LogIndex or LogFile
                //  - then this match could be turned into a nicer map_or_else
                let log_len = segment.log().len().await.context("fetch records")?;

                let log_ref = LogRef::new(
                    topic,
                    partition,
                    segment_ix,
                    entry.position as u64,
                    log_len - entry.offset as u64,
                    Arc::clone(&self),
                );

                Store::FileSystem(log_ref)
            }
            None => Store::Memory(None),
        };

        Ok(FetchResult::Records(records))
    }
}

#[derive(Debug)]
pub enum FetchResult {
    Records(Store<Option<Bytes>, Option<LogRef>>),
    UnknownTopic,
    OffsetOutOfRange,
}

#[allow(clippy::mutable_key_type)]
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
    debug_assert!(
        index.segments.is_empty(),
        "indexing partition with non-empty segments"
    );

    let mut dir = fs::read_dir(&index.dir)
        .await
        .context("failed to read partition dir")?;

    let mut tasks = JoinSet::new();

    while let Some(entry) = dir.next_entry().await.context("next dir entry")? {
        let path = entry.path();
        if let Some("log") = path.extension().and_then(|ext| ext.to_str()) {
            debug_assert!(path.is_file(), "'.log' should indicate a file");
            tasks.spawn(index_segment(path));
        }
    }

    index.segments.reserve(tasks.len());

    while let Some(result) = tasks.join_next().await {
        match result {
            Ok(Ok(segment)) => index.segments.push(segment),
            Ok(Err(e)) => bail!(e),
            Err(e) if e.is_cancelled() => tokio::task::yield_now().await,
            Err(e) if e.is_panic() => std::panic::resume_unwind(e.into_panic()),
            Err(e) => unreachable!("task neither panicked nor has been canceled: {e:?}"),
        }
    }

    index.segments.sort_unstable_by_key(Segment::base_offset);

    Ok(())
}

async fn index_segment(path: PathBuf) -> Result<Segment> {
    let Some(filename) = path.file_name().and_then(|name| name.to_str()) else {
        bail!("invalid log file {path:?}");
    };

    let Some(base_offset) = filename.strip_suffix(".log") else {
        unreachable!("path has already been checked for the '.log' extension");
    };

    let base_offset = base_offset
        .parse()
        .with_context(|| format!("'{base_offset}' does not encode a valid i64 base offset"))?;

    let log_file = fs::File::options()
        .read(true)
        .append(true)
        .open(&path)
        .await
        .context("failed to open log file")?;

    let mut log = LogFile::new(base_offset, path, log_file);

    let index = index_log(&mut log)
        .await
        .with_context(|| format!("failed to index log file {:?}", log.path()))?;

    //println!("{:?}: {index}", log.path());
    Ok(Segment::new(log, index))
}

async fn index_log(log: &mut LogFile) -> Result<LogIndex> {
    //let path = log.path().to_path_buf();
    //println!("indexing log file {path:?}");
    let mut index = LogIndex::new(log.base_offset());

    let file = log.as_mut();
    let metadata = file.metadata().await.context("log file metadata")?;

    let file_end = metadata.len();
    let read_end = match file_end.checked_sub(BatchHeader::SIZE as u64) {
        Some(0) | None => return Ok(index),
        Some(n) => n,
    };

    let mut batch_start = file.rewind().await.context("seek to start of the log")?;

    while batch_start < read_end {
        //println!("{path:?}: start={batch_start} read_end={read_end} file_end={file_end}");

        let (header, _header_len) = RecordBatchHeader::read(file)
            .await
            .context("read batch header")?;

        // TODO: dig deeper into this, it's odd to have "corrupted" data in the test set
        // NOTE: sanity check so we don't include some partially written data
        if header.batch_length as u64 > file_end - batch_start {
            eprintln!(
                "{:?}: indexing interrupted after reading {header:?}: remaining ({})",
                log.path(),
                file_end - batch_start
            );
            break;
        }

        //println!(
        //    "{path:?}: start={batch_start} len={} header={header:?}",
        //    header_len + header.records_len()
        //);

        // append new index entry for this batch of records
        index
            .append(header.base_offset, batch_start as i32)
            .with_context(|| format!("failed to index batch: {header:?}"))?;

        // seek/skip over the contents of the current batch
        batch_start = file
            .seek(SeekFrom::Current(header.records_len() as i64))
            .await
            .context("seek next batch")?;

        // cooperatively yield back to the scheduler
        tokio::task::yield_now().await;
    }

    Ok(index)
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
