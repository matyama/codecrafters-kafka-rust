use anyhow::{Context as _, Result};
use tokio::net::TcpStream;

use kafka::{MessageReader, MessageWriter, Response};

pub(crate) mod kafka;

pub async fn handle_connection(mut conn: TcpStream) -> Result<()> {
    let (reader, writer) = conn.split();

    let mut reader = MessageReader::new(reader);
    let mut writer = MessageWriter::new(writer);

    let req = reader.read_req().await.context("read request message")?;
    println!("received: {req:?}");

    let resp = Response::ApiVersions {
        size: 8,
        correlation_id: 7,
    };
    println!("sending: {resp:?}");

    writer
        .write_resp(resp)
        .await
        .context("write response message: ApiVersions")?;

    writer.flush().await.context("flush data")
}
