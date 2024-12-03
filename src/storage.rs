use std::collections::{BTreeMap, HashMap};
use std::io::SeekFrom;
use std::ops::ControlFlow;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context as _, Result};
use bytes::Bytes;
use tokio::fs::{self, File};
use tokio::io::{AsyncReadExt as _, AsyncSeekExt as _};
use tokio::sync::{Mutex, MutexGuard, RwLock};
use tokio::task::JoinSet;

use crate::kafka::record::{RecordBatch, RecordBatchHeader, Topic, TopicRecord};
use crate::kafka::types::{StrBytes, Uuid};
use crate::kafka::{AsyncDeserialize, Deserialize};
use crate::logs::LogDir;

#[derive(Debug)]
struct LogFile {
    /// min offset of the log file (also contained in the file name)
    offset: i64,
    /// open log file
    file: Mutex<File>,
}

impl LogFile {
    #[inline]
    fn new(offset: i64, file: File) -> Self {
        Self {
            offset,
            file: Mutex::new(file),
        }
    }

    #[inline]
    fn offset(&self) -> i64 {
        self.offset
    }

    #[inline]
    async fn lock(&self) -> MutexGuard<'_, File> {
        self.file.lock().await
    }
}

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
    fn lookup_file(&self, offset: i64) -> Option<&LogFile> {
        let (Ok(ix) | Err(ix)) = self.logs.binary_search_by_key(&offset, LogFile::offset);
        self.logs.get(ix)
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
    //  - use sendfile(2) to zero-copy data (fd -> socket) https://linux.die.net/man/2/sendfile
    //  - https://github.com/rust-lang/rust/issues/60689#issuecomment-491255226
    //  - or at least something like seek + `tokio::io::{copy, copy_buf}`
    //  - this will require changing the fetch response / response writer
    /// Fetch a record batch containing given `offset` for the `topic` and `partition`.
    ///
    /// Note that this returns batch of raw records (file contents). This means that
    ///  1. There can (and most likely will) be multiple records returned, all inside the record
    ///     batch which includes given `offset`.
    ///  2. Record batches can in general contain offsets _smaller_ than the `offset` and it's up
    ///     to the consumer to filter these out.
    pub async fn fetch_records(
        &self,
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

        let Some(log_file) = ix.lookup_file(offset) else {
            return Ok(FetchResult::OffsetOutOfRange);
        };

        // XXX: this locking is rather unfortunate
        //  - maybe store just the path and open the file ad-hoc
        let mut log_file = log_file.lock().await;
        let log = &mut *log_file;

        // start at the beginning of the log file
        let mut batch_start = log.rewind().await.context("seek to start of the log")?;

        // TODO: Kafka uses a separate index file alongside each log (i.e., per segment) for this
        //  - The index contains the file offset of each record (batch)
        //  - So this file offset lookup can be made just based on the index without ever touching
        //    the data (log file)
        //  - Once the file offset is calculated, the response writer can perform single `sendfile`
        //    syscall to write the records to the socket without actually copying them to userspace
        //  - So this really should be part of an increment indexing (segments are immutable,
        //    modulo the active one, which is append-only, and discarding due to retention)
        let batch = loop {
            let Some(header) = try_read_header(log, batch_start)
                .await
                .context("failed to read batch header")?
            else {
                break Bytes::new();
            };

            if header.contains(offset) {
                // hit: read the rest of the log file, starting with this batch

                log.seek(SeekFrom::Start(batch_start))
                    .await
                    .context("seek to batch start")?;

                let batch_len = header.batch_length as usize;
                let mut buf = Vec::with_capacity(batch_len);

                //println!("header CRC={} ({:02x?})", header.crc, header.crc);
                //println!("{header:?}");

                //// serialize the header into the buffer (so we don't have to issue a seek)
                //let Ok(_) = header.encode(&mut buf) else {
                //    unreachable!("buffer has sufficient capacity for the whole batch");
                //};

                //println!("batch buf (header): {buf:x?}");

                let _ = log.read_to_end(&mut buf).await.with_context(|| {
                    format!(
                        "failed to fetch records for ({}, {partition}, {offset})",
                        topic.as_hex()
                    )
                });

                //println!("batch buf: {buf:x?}");

                buf.shrink_to_fit();
                break Bytes::from(buf);
            }

            // otherwise seek/skip over the contents of the current batch
            batch_start = log
                .seek(SeekFrom::Current(header.records_len() as i64))
                .await
                .context("seek next batch")?;
        };

        Ok(FetchResult::Records(if batch.is_empty() {
            None
        } else {
            Some(batch)
        }))
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
    Records(Option<Bytes>),
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
                .open(path)
                .await
                .context("failed to open log file")?;

            index.logs.push(LogFile::new(base_offset, log_file));

            needs_sorting &= prev_offset < base_offset;
            prev_offset = base_offset;
        }
    }

    if needs_sorting {
        index.logs.sort_unstable_by_key(LogFile::offset);
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
