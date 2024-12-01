use std::path::{Path, PathBuf};

use anyhow::{bail, Context as _, Result};
use tokio::fs;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::time::{sleep, Duration};

#[derive(Clone, Debug)]
pub struct PartitionDir {
    pub path: PathBuf,
    pub meta: Option<PartitionMetadata>,
}

impl PartitionDir {
    pub async fn load(path: PathBuf) -> Result<Self> {
        let meta_path = path.join("partition.metadata");

        let meta = PartitionMetadata::load(&meta_path)
            .await
            .with_context(|| format!("failed to read {meta_path:?}"))?;

        // XXX: dump log files
        /*
        let mut reader = fs::read_dir(&path)
            .await
            .with_context(|| format!("reading directory {path:?}"))?;

        while let Some(entry) = reader.next_entry().await? {
            if entry.file_name().as_encoded_bytes().ends_with(b".log") {
                crate::utils::hexdump(entry.path()).await?;
            }
        }
        */

        Ok(Self { path, meta })
    }

    pub(crate) fn is_cluster_metadata(&self) -> bool {
        self.path
            .components()
            .last()
            .and_then(|dir| dir.as_os_str().to_str())
            .map_or(false, |dir| dir.starts_with("__cluster_metadata"))
    }

    pub(crate) fn topic_partition(&self) -> Result<(&str, i32)> {
        let Some(dir_name) = self.path.file_name() else {
            bail!("invalid partition dir {:?}", self.path);
        };

        let dir_name = dir_name.as_encoded_bytes();
        let Some((dash_ix, _)) = dir_name.iter().enumerate().rev().find(|(_, &b)| b == b'-') else {
            bail!(
                "partition dir must contain '-' separator, got: {:?}",
                self.path
            );
        };

        let (topic, partition) = dir_name.split_at(dash_ix);

        let topic = std::str::from_utf8(topic)
            .with_context(|| format!("invalid topic part of {:?}", self.path))?;

        let partition = std::str::from_utf8(&partition[1..])
            .with_context(|| format!("invalid partition part of {:?}", self.path))
            .and_then(|p| p.parse().context("partition must be an i32"))?;

        Ok((topic, partition))
    }
}

#[derive(Clone, Debug)]
pub struct PartitionMetadata {
    pub version: i16,
    // TODO: Uuid / Bytes / [u8; 16]
    pub topic_id: String,
}

impl PartitionMetadata {
    const MAX_RETRIES: usize = 3;
    const RETRY_DELAY: Duration = Duration::from_millis(100);

    pub async fn load(path: impl AsRef<Path>) -> Result<Option<Self>> {
        let path = path.as_ref();
        println!("Loading {path:?}");

        // XXX: debug print
        //crate::utils::hexdump(&path).await?;

        if !path.exists() {
            return Ok(None);
        }

        // NOTE: retry to wait for the CodeCrafters test runner to write all the files
        let mut i = Self::MAX_RETRIES;
        loop {
            i -= 1;
            match Self::try_load(path).await {
                Ok(meta) => break Ok(Some(meta)),
                Err(_) if i > 0 => sleep(Self::RETRY_DELAY.mul_f64(i as f64)).await,
                Err(e) => break Err(e),
            }
        }
    }

    async fn try_load(path: &Path) -> Result<Self> {
        let file = fs::OpenOptions::new()
            .read(true)
            .open(path)
            .await
            .with_context(|| format!("cannot open metadata file {path:?}"))?;

        let mut reader = BufReader::new(file);
        let mut line = String::new();
        let mut builder = PartitionMetadataBuilder::default();

        loop {
            line.clear();

            let n = reader.read_line(&mut line).await?;

            if n == 0 {
                break builder
                    .build()
                    .with_context(|| format!("invalid or incomplete {path:?}"));
            }

            match line.trim() {
                line if line.starts_with("version") => {
                    builder.version = line
                        .split_once(':')
                        .and_then(|(_, v)| v.trim().parse().ok());
                }

                line if line.starts_with("topic_id") => {
                    builder.topic_id = line.split_once(':').map(|(_, id)| id.trim().to_string());
                }

                _ => continue,
            }
        }
    }
}

#[derive(Debug, Default)]
struct PartitionMetadataBuilder {
    version: Option<i16>,
    topic_id: Option<String>,
}

impl PartitionMetadataBuilder {
    fn build(self) -> Result<PartitionMetadata> {
        Ok(PartitionMetadata {
            version: self.version.context("version")?,
            topic_id: self.topic_id.context("topic_id")?,
        })
    }
}
