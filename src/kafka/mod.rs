use anyhow::{ensure, Context as _, Result};
use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter};

pub(crate) use request::{ApiKey, RequestBody, RequestHeader, RequestMessage};
pub(crate) use response::{ResponseBody, ResponseMessage};

pub(crate) mod error;

mod request;
mod response;

pub struct MessageReader<R> {
    inner: BufReader<R>,
}

impl<R> MessageReader<R>
where
    R: AsyncReadExt + Send + Unpin,
{
    #[inline]
    pub fn new(reader: R) -> Self {
        Self {
            inner: BufReader::new(reader),
        }
    }

    pub async fn read_request(&mut self) -> Result<RequestMessage> {
        let size = self.inner.read_i32().await.context("message size")?;
        ensure!(size > 0, "received a zero-sized message");

        // TODO: buffer pooling
        let mut buf = BytesMut::with_capacity(size as usize);
        buf.resize(size as usize, 0);

        self.inner
            .read_exact(&mut buf[..size as usize])
            .await
            .context("message content")?;

        // TODO: other requests
        let (header, header_bytes) = RequestHeader::parse(&buf).context("header")?;

        let body = RequestBody::ApiVersions {
            // skip over header bytes
            data: buf.split_off(header_bytes).freeze(),
        };

        Ok(RequestMessage { size, header, body })
    }
}

pub struct MessageWriter<W> {
    inner: BufWriter<W>,
}

impl<W> MessageWriter<W>
where
    W: AsyncWriteExt + Send + Unpin,
{
    #[inline]
    pub fn new(writer: W) -> Self {
        Self {
            inner: BufWriter::new(writer),
        }
    }

    pub async fn write_response(
        &mut self,
        ResponseMessage { size, header, body }: ResponseMessage,
    ) -> Result<()> {
        self.inner.write_i32(size).await.context("message size")?;

        self.inner
            .write_i32(header.correlation_id)
            .await
            .context("header: correlation id")?;

        match body {
            // TODO: serialize [api_keys]
            body @ ResponseBody::ApiVersions { error_code } => self
                .inner
                .write_i16(error_code as i16)
                .await
                .with_context(|| format!("{body} error code"))?,
        }

        self.inner.flush().await.context("flush")
    }
}
