use anyhow::{bail, ensure, Context as _, Result};
use bytes::{Buf, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter};

pub(crate) use api::ApiKey;
pub(crate) use request::{RequestBody, RequestHeader, RequestMessage};
pub(crate) use response::ResponseMessage;

use error::{ErrorCode, KafkaError};

pub(crate) mod api;
pub(crate) mod error;
pub(crate) mod request;
pub(crate) mod response;
pub(crate) mod types;

// XXX: merge with Serialize
pub trait WireSize {
    const SIZE: usize = 0;

    fn size(&self, version: i16) -> usize;
}

pub trait HeaderVersion {
    // NOTE: header version is generally different, but derivable from the API version
    fn header_version(&self, api_version: i16) -> i16;
}

//pub trait Flexible {
//    fn num_tagged_fields(&self, version: i16) -> usize;
//}

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

        // TODO: let caller provide their own buffer (owned) and let the message point into it
        //  - realistically, the buffer would be provided for a message set
        //  - i.e., given an owned buf, return a RequestMessageSet = (buf, [ReqMsg<'self::buf>])
        let mut buf = BytesMut::with_capacity(size as usize);
        buf.resize(size as usize, 0);

        self.inner
            .read_exact(&mut buf)
            .await
            .context("message content")?;

        let (header, _) = RequestHeader::read_from(&mut buf).context("message header")?;

        let api_key = header.request_api_key;
        let api_version = header.request_api_version.into_inner();

        // TODO: other requests
        let body = match api_key {
            ApiKey::ApiVersions => {
                let (body, _) = request::ApiVersions::read_from(&mut buf, api_version)
                    .with_context(|| format!("{api_key:?} v{api_version} message body"))?;

                RequestBody::ApiVersions(body)
            }

            ApiKey::Fetch => {
                let (body, _) = request::Fetch::read_from(&mut buf, api_version)
                    .with_context(|| format!("{api_key:?} v{api_version} message body"))?;

                RequestBody::Fetch(body)
            }

            // NOTE: placeholder for unimplemented API requests
            api_key => {
                bail!(KafkaError {
                    error_code: ErrorCode::INVALID_REQUEST,
                    api_key: api_key as i16,
                    api_version,
                    correlation_id: header.correlation_id,
                });
            }
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

    pub async fn write_response(&mut self, msg: ResponseMessage, version: i16) -> Result<()> {
        msg.write_into(&mut self.inner, version)
            .await
            .context("write response")?;

        self.inner.flush().await.context("flush response")
    }
}

pub trait Serialize: Sized {
    // TODO: parametrize by version
    // TODO: resolve async_fn_in_trait lint and remove the allow
    #[allow(async_fn_in_trait)]
    async fn write_into<W>(self, writer: &mut W, version: i16) -> Result<()>
    where
        W: AsyncWriteExt + Send + Unpin;
}

pub trait Deserialize: Sized {
    fn read_from<B: Buf>(buf: &mut B, version: i16) -> Result<(Self, usize)>;
}
