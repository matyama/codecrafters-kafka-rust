use std::ops::ControlFlow;

use anyhow::{bail, Context as _, Result};
use tokio::net::TcpStream;

use kafka::error::{ErrorCode, KafkaError};
use kafka::request::{self, RequestBody, RequestMessage};
use kafka::response::{self, ApiVersion, ResponseBody, ResponseHeader, ResponseMessage};
use kafka::{ApiKey, HeaderVersion, MessageReader, MessageWriter, WireSize as _};

pub(crate) mod kafka;

pub async fn handle_connection(mut conn: TcpStream) -> Result<()> {
    let (reader, writer) = conn.split();

    let mut reader = MessageReader::new(reader);
    let mut writer = MessageWriter::new(writer);

    loop {
        let msg = reader.read_request().await.context("read request");

        let (msg, version, control) = match msg {
            // request handling
            Ok(msg) => {
                let version = msg.header.request_api_version.into_inner();
                let msg = handle_message(msg).await.context("handle message")?;
                (msg, version, ControlFlow::Continue(()))
            }

            // error handling
            Err(err) => match err.downcast::<KafkaError>() {
                Ok(err) => {
                    let version = err.api_version;
                    let msg = handle_error(err).await.context("handle error")?;
                    (msg, version, ControlFlow::Break(()))
                }
                Err(err) => bail!(err),
            },
        };

        println!("response: {msg:?}");
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

async fn handle_message(msg: RequestMessage) -> Result<ResponseMessage> {
    println!("handling: {msg:?}");

    let version = msg.header.request_api_version.into_inner();

    let (body_size, body) = match msg.body {
        RequestBody::ApiVersions(_) => {
            let body = response::ApiVersions {
                api_keys: ApiKey::iter().filter_map(ApiVersion::new).collect(),
                ..Default::default()
            };

            (body.size(version), ResponseBody::ApiVersions(body))
        }

        RequestBody::Fetch(fetch) => {
            use request::fetch::*;
            use response::fetch::*;

            // TODO: actual impl
            let responses = fetch
                .topics
                .into_iter()
                .map(
                    |FetchTopic {
                         topic, topic_id, ..
                     }| {
                        let p = PartitionData::new(0, ErrorCode::UNKNOWN_TOPIC_ID, 0);

                        FetchableTopicResponse {
                            topic,
                            topic_id,
                            partitions: vec![p],
                            tagged_fields: Default::default(),
                        }
                    },
                )
                .collect();

            let body = response::Fetch {
                throttle_time_ms: 0,
                error_code: ErrorCode::NONE,
                session_id: 0,
                responses,
                ..Default::default()
            };

            (body.size(version), ResponseBody::Fetch(body))
        }
    };

    // NOTE: response header version may depend on the API key/version
    let header_version = body.header_version(version);

    let header = ResponseHeader {
        correlation_id: msg.header.correlation_id,
        // TODO: TAG_BUFFER
        tagged_fields: Default::default(),
    };

    Ok(ResponseMessage {
        size: (header.size(header_version) + body_size) as i32,
        header,
        body,
    })
}

async fn handle_error(err: KafkaError) -> Result<ResponseMessage> {
    println!("handling: {err:?}");

    let Ok(api_key) = ApiKey::try_from(err.api_key) else {
        bail!(
            "Unsupported API key ({} v{}), ERR={:?}, ID={}",
            err.api_key,
            err.api_version,
            err.error_code,
            err.correlation_id
        );
    };

    let (body_size, body) = match api_key {
        ApiKey::ApiVersions => {
            // Starting from Apache Kafka 2.4 (KIP-511), ApiKeys field is populated with the
            // supported versions of the ApiVersionsRequest when an UNSUPPORTED_VERSION error is
            // returned.
            let body = match err.error_code {
                error_code @ ErrorCode::UNSUPPORTED_VERSION => response::ApiVersions {
                    error_code,
                    api_keys: [api_key].into_iter().filter_map(ApiVersion::new).collect(),
                    ..Default::default()
                },
                error_code => response::ApiVersions {
                    error_code,
                    ..Default::default()
                },
            };

            (body.size(err.api_version), ResponseBody::ApiVersions(body))
        }

        ApiKey::Fetch => {
            // TODO: throttle_time_ms, session_id for Fetch errors
            let body = response::Fetch::error(0, err.error_code, 0);
            (body.size(err.api_version), ResponseBody::Fetch(body))
        }

        key => unimplemented!("error handling for {key:?}"),
    };

    // NOTE: response header version may depend on the API key/version
    let header_version = body.header_version(err.api_version);

    let header = ResponseHeader {
        correlation_id: err.correlation_id,
        // TODO: TAG_BUFFER
        tagged_fields: Default::default(),
    };

    Ok(ResponseMessage {
        size: (header.size(header_version) + body_size) as i32,
        header,
        body,
    })
}
