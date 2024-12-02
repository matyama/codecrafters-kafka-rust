use std::ops::ControlFlow;
use std::path::Path;
use std::sync::Arc;

use anyhow::{bail, Context as _, Result};
use tokio::net::TcpStream;

use handler::Handler;
use kafka::error::KafkaError;
use kafka::request::{RequestBody, RequestMessage};
use kafka::response::{ResponseHeader, ResponseMessage};
use kafka::{ApiKey, HeaderVersion, MessageReader, MessageWriter, Serialize as _};
use properties::ServerProperties;
use storage::Storage;

pub mod handler;
pub mod kafka;
pub mod logs;
pub mod properties;
pub mod storage;

#[derive(Debug)]
pub struct Server {
    storage: Arc<Storage>,
}

impl Server {
    pub async fn new(properties: impl AsRef<Path>) -> Result<Self> {
        let properties = properties.as_ref();
        let properties = ServerProperties::load(properties)
            .await
            .with_context(|| format!("parse {properties:?}"))?;

        println!("Loaded {properties:?}");

        let log_dirs = properties.log_dirs.iter().cloned();
        let log_dirs = logs::load_metadata(log_dirs)
            .await
            .context("reading log directory metadata")?;

        //println!("Found log dir metadata {log_dirs:?}");

        let storage = storage::Storage::new(log_dirs)
            .await
            .context("initializing storage")?;

        println!("Storage initialized");
        //println!("Initialized storage: {storage:?}");

        Ok(Self {
            storage: Arc::new(storage),
        })
    }

    pub async fn handle_connection(&self, mut conn: TcpStream) -> Result<()> {
        let (reader, writer) = conn.split();

        let mut reader = MessageReader::new(reader);
        let mut writer = MessageWriter::new(writer);

        loop {
            let msg = reader.read_request().await.context("read request");

            let (msg, version, control) = match msg {
                // connection disconnected
                Ok(None) => break Ok(()),

                // request handling
                Ok(Some(msg)) => {
                    let version = msg.header.request_api_version.into_inner();
                    let msg = self.handle_message(msg).await.context("handle message")?;
                    (msg, version, ControlFlow::Continue(()))
                }

                // error handling
                Err(err) => match err.downcast::<KafkaError>() {
                    Ok(err) => {
                        let version = err.api_version;
                        let msg = self.handle_error(err).await.context("handle error")?;
                        (msg, version, ControlFlow::Break(()))
                    }
                    Err(err) => bail!(err),
                },
            };

            println!("Response: {msg:?}");
            writer
                .write_response(msg, version)
                .await
                .context("write response")?;

            if control.is_break() {
                break Ok(());
            }

            // cooperatively yield from the connection handler
            tokio::task::yield_now().await;
        }
    }

    async fn handle_message(&self, msg: RequestMessage) -> Result<ResponseMessage> {
        println!("Handling: {msg:?}");

        let api_key = msg.header.request_api_key;
        let api_version = msg.header.request_api_version.into_inner();

        let (body_size, body) = match msg.body {
            RequestBody::ApiVersions(body) => {
                handler::ApiVersionsHandler
                    .handle_message(&msg.header, body)
                    .await
            }

            RequestBody::Fetch(body) => {
                handler::FetchHandler::new(Arc::clone(&self.storage))
                    .handle_message(&msg.header, body)
                    .await
            }

            RequestBody::DescribeTopicPartitions(body) => {
                handler::DescribeTopicPartitionsHandler::new(Arc::clone(&self.storage))
                    .handle_message(&msg.header, body)
                    .await
            }
        }
        .with_context(|| format!("handle {api_key:?} v{api_version} request"))?;

        // NOTE: response header version may depend on the API key/version
        let header_version = body.header_version(api_version);

        let header = ResponseHeader {
            correlation_id: msg.header.correlation_id,
            // TODO: TAG_BUFFER
            tagged_fields: Default::default(),
        };

        Ok(ResponseMessage {
            size: (header.encode_size(header_version) + body_size) as i32,
            header,
            body,
        })
    }

    async fn handle_error(&self, err: KafkaError) -> Result<ResponseMessage> {
        println!("Handling: {err:?}");

        let api_version = err.api_version;
        let correlation_id = err.correlation_id;

        let Ok(api_key) = ApiKey::try_from(err.api_key) else {
            bail!(
                "Unsupported API key ({} v{}), ERR={:?}, ID={}",
                err.api_key,
                err.api_version,
                err.error_code,
                err.correlation_id
            );
        };

        // NOTE: `handler: impl Handler` is not (yet) allowed, so we must live with a bit of code dup.
        let (body_size, body) = match api_key {
            ApiKey::ApiVersions => handler::ApiVersionsHandler.handle_error(err).await,

            ApiKey::Fetch => {
                handler::FetchHandler::new(Arc::clone(&self.storage))
                    .handle_error(err)
                    .await
            }

            ApiKey::DescribeTopicPartitions => {
                handler::DescribeTopicPartitionsHandler::new(Arc::clone(&self.storage))
                    .handle_error(err)
                    .await
            }

            key => unimplemented!("error handling for {key:?} v{api_version}"),
        }
        .with_context(|| format!("handle {api_key:?} v{api_version} error"))?;

        // NOTE: response header version may depend on the API key/version
        let header_version = body.header_version(api_version);

        let header = ResponseHeader {
            correlation_id,
            // TODO: TAG_BUFFER
            tagged_fields: Default::default(),
        };

        Ok(ResponseMessage {
            size: (header.encode_size(header_version) + body_size) as i32,
            header,
            body,
        })
    }
}

pub(crate) mod utils {
    use std::fmt::Write as _;
    use std::path::Path;

    use anyhow::{Context as _, Result};

    #[allow(dead_code)]
    pub async fn hexdump(path: impl AsRef<Path>) -> Result<()> {
        let path = path.as_ref();

        let data = tokio::fs::read(&path)
            .await
            .with_context(|| format!("read {path:?}"))?;

        let mut hexdump = String::new();

        write!(&mut hexdump, "{data:02x?}").with_context(|| format!("hexdump {path:?}"))?;

        hexdump.retain(char::is_alphanumeric);

        println!("{path:?}: {hexdump}");

        Ok(())
    }
}
