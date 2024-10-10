use std::path::{Path, PathBuf};

use anyhow::{Context as _, Result};
use tokio::fs;
use tokio::io::{AsyncBufReadExt, BufReader};

// TODO: other properties
#[derive(Debug)]
pub struct ServerProperties {
    /// The `node.id` property
    pub node_id: i32,
    /// The `log.dirs` property
    pub log_dirs: Vec<PathBuf>,
}

impl ServerProperties {
    pub async fn load(path: impl AsRef<Path>) -> Result<Self> {
        let file = fs::File::open(path).await?;

        let mut reader = BufReader::new(file);
        let mut line = String::new();
        let mut props = ServerPropertiesBuilder::default();

        loop {
            line.clear();

            let n = reader
                .read_line(&mut line)
                .await
                .context("reading server.properties line")?;

            if n == 0 {
                break props.build();
            }

            match line.trim() {
                // skip comments
                line if line.starts_with('#') => continue,

                line if line.starts_with("node.id") => {
                    props.node_id = line.split_once('=').and_then(|(_, id)| id.parse().ok());
                }

                line if line.starts_with("log.dirs") => {
                    props.log_dirs = line
                        .split_once('=')
                        .map(|(_, log_dirs)| log_dirs.split(',').map(PathBuf::from).collect());
                }

                // TODO: parse other properties here
                // ZK: https://github.com/apache/kafka/blob/trunk/config/server.properties
                // KRaft: https://github.com/apache/kafka/blob/trunk/config/kraft/server.properties
                //  - listeners
                //  - advertised.listeners
                _ => continue,
            }
        }
    }
}

#[derive(Default)]
struct ServerPropertiesBuilder {
    node_id: Option<i32>,
    log_dirs: Option<Vec<PathBuf>>,
}

impl ServerPropertiesBuilder {
    // TODO: use default values
    fn build(self) -> Result<ServerProperties> {
        Ok(ServerProperties {
            node_id: self.node_id.context("missing node.id")?,
            log_dirs: self.log_dirs.context("missing log.dirs")?,
        })
    }
}
