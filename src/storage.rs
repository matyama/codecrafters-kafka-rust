use std::collections::{BTreeMap, HashMap};
use std::ops::Deref;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context as _, Result};
use bytes::Bytes;
use tokio::fs::{self, File};
use tokio::io::AsyncReadExt as _;
use tokio::sync::{Mutex, RwLock};
use tokio::task::JoinSet;

use crate::kafka::record::{self, Topic};
use crate::kafka::types::Uuid;
use crate::kafka::Deserialize;
use crate::logs::LogDir;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct TopicPartition {
    topic: Uuid,
    partition: i32,
}

#[derive(Debug)]
#[repr(transparent)]
struct LogFile(Mutex<File>);

impl LogFile {
    #[inline]
    fn new(file: File) -> Self {
        Self(Mutex::new(file))
    }
}

impl Deref for LogFile {
    type Target = Mutex<File>;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Debug)]
struct PartitionIndex {
    //topic: Topic,
    //partition: i32,
    /// path to the partition directory
    dir: PathBuf,
    /// base offset -> log file
    logs: BTreeMap<i64, LogFile>,
}

impl PartitionIndex {
    #[inline]
    fn new(dir: impl AsRef<Path>) -> Self {
        Self {
            //topic: topic.clone(),
            //partition,
            dir: dir.as_ref().to_path_buf(),
            logs: BTreeMap::new(),
        }
    }
}

#[derive(Debug)]
pub struct StorageInner {
    #[allow(dead_code)]
    log_dirs: Vec<LogDir>,
    /// (topic id, partition) -> ({base offset -> log file}, ..metadata)
    log_index: HashMap<TopicPartition, PartitionIndex>,
    // TODO: similar mapping to `log_index` for the __cluster_metadata logs (`metadata_index`)
}

impl StorageInner {
    async fn new(log_dirs: Vec<LogDir>) -> Result<Self> {
        let topics = lookup_topics(&log_dirs)
            .await
            .context("find topics in cluster metadata")?;

        let log_index = index_logs(&log_dirs, &topics)
            .await
            .context("indexing partition logs")?;

        Ok(Self {
            log_dirs,
            log_index,
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

    #[inline]
    pub async fn contains(&self, topic: Uuid, partition: i32) -> bool {
        let storage = self.inner.read().await;
        storage
            .log_index
            .contains_key(&TopicPartition { topic, partition })
    }

    // XXX: add version?
    pub async fn fetch_records(
        &self,
        topic: Uuid,
        partition: i32,
        offset: i64,
    ) -> Result<FetchResult> {
        let storage = self.inner.read().await;

        let tp = TopicPartition { topic, partition };

        let Some(ix) = storage.log_index.get(&tp) else {
            return Ok(FetchResult::UnknownTopic);
        };

        let Some(log_file) = ix.logs.get(&offset) else {
            return Ok(FetchResult::OffsetOutOfRange);
        };

        // XXX: this locking is rather unfortunate
        //  - maybe store just the path and open the file ad-hoc
        let mut log_file = log_file.lock().await;

        // TODO: read batch header first to know and reserve buffer capacity
        let mut buf = Vec::new();

        let _ = log_file.read_to_end(&mut buf).await.with_context(|| {
            format!(
                "failed to fetch records for ({}, {partition}, {offset})",
                tp.topic.as_hex()
            )
        });

        buf.shrink_to_fit();

        Ok(FetchResult::Records(if buf.is_empty() {
            None
        } else {
            Some(Bytes::from(buf))
        }))
    }
}

pub enum FetchResult {
    Records(Option<Bytes>),
    UnknownTopic,
    OffsetOutOfRange,
}

async fn index_logs(
    log_dirs: &[LogDir],
    topics: &[Topic],
) -> Result<HashMap<TopicPartition, PartitionIndex>> {
    // XXX: return this from lookup_topics
    let topics = topics
        .iter()
        .map(|topic| (topic.name(), topic))
        .collect::<HashMap<_, _>>();

    let mut index = HashMap::new();

    let partition_dirs = log_dirs.iter().flat_map(|dir| {
        dir.partitions
            .iter()
            .filter(|dir| !dir.is_cluster_metadata())
    });

    for dir in partition_dirs {
        let (topic, partition) = dir.topic_partition()?;

        let Some(topic) = topics.get(topic) else {
            bail!("topic '{topic:?}' not found in cluster metadata");
        };

        // NOTE: specifically taking the topic id from the cluster metadata here
        let tp = TopicPartition {
            topic: topic.topic_id(),
            partition,
        };

        let ix = index
            .entry(tp)
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
                .map(LogFile::new)
                .context("failed to open log file")?;

            index.logs.insert(base_offset, log_file);
        }
    }

    Ok(())
}

async fn lookup_topics(log_dirs: &[LogDir]) -> Result<Vec<record::Topic>> {
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
            Err(e) => bail!(e),
        }
    }

    Ok(topics)
}

async fn discover_topics(cluster_metadata_dir: impl AsRef<Path>) -> Result<Vec<record::Topic>> {
    let log_file = load_last_log_file(cluster_metadata_dir)
        .await
        .context("load last cluster metadata log")?;

    let Some(log_file) = log_file else {
        return Ok(Vec::new());
    };

    let segment = record::decode(&mut log_file.as_slice())?;
    //println!("loaded segment: {segment:?}");

    let topics = segment
        .into_iter()
        .flat_map(|batch| batch.records)
        .filter_map(
            |record| match record::Topic::deserialize(&mut record.value?) {
                Ok((topic, _)) => Some(topic),
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
