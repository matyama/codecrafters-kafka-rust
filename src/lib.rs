use anyhow::{bail, Context as _, Result};
use tokio::net::TcpStream;

use kafka::error::KafkaError;
use kafka::{ApiKey, MessageReader, MessageWriter, RequestMessage, ResponseBody, ResponseMessage};

pub(crate) mod kafka;

pub async fn handle_connection(mut conn: TcpStream) -> Result<()> {
    let (reader, writer) = conn.split();

    let mut reader = MessageReader::new(reader);
    let mut writer = MessageWriter::new(writer);

    // TODO: loop to keep connection alive

    let msg = reader.read_request().await.context("read request");

    let msg = match msg {
        // request handling
        Ok(msg) => handle_message(msg).await.context("handle message")?,

        // error handling
        Err(err) => match err.downcast::<KafkaError>() {
            Ok(err) => handle_error(err).await.context("handle error")?,
            Err(err) => bail!(err),
        },
    };

    println!("response: {msg:?}");
    writer.write_response(msg).await.context("write response")
}

async fn handle_message(msg: RequestMessage) -> Result<ResponseMessage> {
    println!("handling: {msg:?}");

    let body = match msg.header.request_api_key {
        ApiKey::ApiVersions => ResponseBody::ApiVersions {
            error_code: Default::default(),
        },
        key => unimplemented!("message handling for {key:?}"),
    };

    // XXX: might need to serialize first to get the full response size
    // (unless it can be determined statically with the knowledge of payload length)
    Ok(ResponseMessage::new(8, msg.header.correlation_id, body))
}

async fn handle_error(err: KafkaError) -> Result<ResponseMessage> {
    println!("handling: {err:?}");

    let Ok(api_key) = ApiKey::try_from(err.api_key) else {
        bail!(
            "Unsupported API key ({}), ERR={:?}, ID={}",
            err.api_key,
            err.error_code,
            err.correlation_id
        );
    };

    let body = match api_key {
        ApiKey::ApiVersions => ResponseBody::ApiVersions {
            error_code: err.error_code,
        },
        key => unimplemented!("error handling for {key:?}"),
    };

    // TODO: compute actual size
    let size = 8;

    Ok(ResponseMessage::new(size, err.correlation_id, body))
}
