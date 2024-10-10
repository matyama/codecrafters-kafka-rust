use std::path::{Path, PathBuf};

use anyhow::{Context as _, Result};
use tokio::fs;
use tokio::io::{AsyncBufReadExt, BufReader};

#[derive(Debug)]
pub struct PartitionDir {
    pub path: PathBuf,
    pub meta: PartitionMetadata,
}

impl PartitionDir {
    pub async fn load(path: PathBuf) -> Result<Self> {
        let meta_path = path.join("partition.metadata");

        let meta = PartitionMetadata::load(&meta_path)
            .await
            .with_context(|| format!("failed to read {meta_path:?}"))?;

        Ok(Self { path, meta })
    }
}

#[derive(Debug)]
pub struct PartitionMetadata {
    pub version: i16,
    // TODO: Uuid / Bytes / [u8; 16]
    pub topic_id: String,
}

impl PartitionMetadata {
    pub async fn load(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();

        let file = fs::File::open(&path)
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
