use std::path::{Path, PathBuf};

use anyhow::{Context as _, Result};
use tokio::fs;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::task::JoinSet;

use partition::PartitionDir;

pub mod partition;

/// Note that the resulting log directory metadata might be returned in arbitrary input path order.
pub async fn load_metadata(log_dirs: impl IntoIterator<Item = PathBuf>) -> Result<Vec<LogDir>> {
    let mut tasks = JoinSet::new();

    for log_dir in log_dirs.into_iter() {
        tasks.spawn(async move { LogDir::load(log_dir).await });
    }

    let mut dirs = Vec::with_capacity(tasks.len());

    while let Some(result) = tasks.join_next().await {
        match result {
            Ok(Ok(log_dir)) => dirs.push(log_dir),
            Err(error) if error.is_panic() => std::panic::resume_unwind(error.into_panic()),
            Ok(Err(error)) => return Err(error).context("failed to load metadata"),
            Err(error) => return Err(error).context("failed to load metadata"),
        }
    }

    Ok(dirs)
}

// TODO: implement load_logs()
//  - https://jaceklaskowski.gitbooks.io/apache-kafka/content/kafka-log-LogManager.html
//  - check for .kafka_cleanshutdown next to meta.properties (i.e., in the LogDir)
//
// `loadLogs` then checks whether .kafka_cleanshutdown file exists in the log directory. If so,
// `loadLogs` prints out the following DEBUG message to the logs:
//
// ```
// Found clean shutdown file. Skipping recovery for all logs in data directory: [dir]
// ```

#[allow(dead_code)]
#[derive(Debug)]
pub struct MetaProperties {
    cluster_id: String,
    directory_id: Option<String>,
    node_id: i32,
    version: i16,
}

impl MetaProperties {
    pub async fn load(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        println!("Loading {path:?}");

        // XXX: debug print
        //crate::utils::hexdump(&path).await?;

        let file = fs::File::open(&path)
            .await
            .with_context(|| format!("cannot open metadata file {path:?}"))?;

        let mut reader = BufReader::new(file);
        let mut line = String::new();
        let mut meta = MetaPropertiesBuilder::default();

        loop {
            line.clear();

            let n = reader.read_line(&mut line).await?;

            if n == 0 {
                break meta
                    .build()
                    .with_context(|| format!("invalid or incomplete {path:?}"));
            }

            match line.trim() {
                // skip comments
                line if line.starts_with('#') => continue,

                line if line.starts_with("cluster.id") => {
                    meta.cluster_id = line.split_once('=').map(|(_, id)| id.to_string());
                }

                line if line.starts_with("directory.id") => {
                    meta.directory_id = line.split_once('=').map(|(_, id)| id.to_string());
                }

                line if line.starts_with("node.id") => {
                    meta.node_id = line.split_once('=').and_then(|(_, id)| id.parse().ok());
                }

                line if line.starts_with("version") => {
                    meta.version = line.split_once('=').and_then(|(_, v)| v.parse().ok());
                }

                _ => continue,
            }
        }
    }
}

#[derive(Default)]
struct MetaPropertiesBuilder {
    cluster_id: Option<String>,
    directory_id: Option<String>,
    node_id: Option<i32>,
    version: Option<i16>,
}

impl MetaPropertiesBuilder {
    // TODO: use default values
    fn build(self) -> Result<MetaProperties> {
        Ok(MetaProperties {
            cluster_id: self.cluster_id.context("missing cluster.id")?,
            directory_id: self.directory_id,
            node_id: self.node_id.context("missing node.id")?,
            version: self.version.context("missing version")?,
        })
    }
}

#[derive(Debug)]
pub struct LogDir {
    pub path: PathBuf,
    pub meta: MetaProperties,
    pub partitions: Vec<PartitionDir>,
    pub kafka_cleanshutdown: Option<PathBuf>,
}

impl LogDir {
    /// Reads [`LogDir`] metadata from given directory path.
    pub async fn load(path: PathBuf) -> Result<Self> {
        let mut reader = tokio::fs::read_dir(&path)
            .await
            .with_context(|| format!("reading directory {path:?}"))?;

        let mut meta = None;
        let mut partitions = Vec::new();
        let mut kafka_cleanshutdown = None;

        while let Some(entry) = reader.next_entry().await? {
            let path = entry.path();

            let ty = entry
                .file_type()
                .await
                .with_context(|| format!("cannot access file type: {path:?}"))?;

            if ty.is_file() {
                match entry.file_name().as_encoded_bytes() {
                    b"meta.properties" => {
                        let path = entry.path();

                        let meta_properties = MetaProperties::load(&path)
                            .await
                            .with_context(|| format!("failed to read {path:?}"))?;

                        meta.replace(meta_properties);
                    }

                    b".kafka_cleanshutdown" => {
                        let _ = kafka_cleanshutdown.replace(entry.path());
                    }

                    _ => continue,
                }
            }

            if ty.is_dir() {
                let partition_dir = PartitionDir::load(path)
                    .await
                    .context("partition directory metadata")?;

                partitions.push(partition_dir);
            }
        }

        // TODO: move to load_logs()
        if kafka_cleanshutdown.is_some() {
            println!(
                "Found clean shutdown file. \
                Skipping recovery for all logs in data directory: {path:?}"
            );
        }

        // XXX: defaults if missing?
        Ok(Self {
            path,
            meta: meta.context("missing meta.properties")?,
            partitions,
            kafka_cleanshutdown,
        })
    }
}
