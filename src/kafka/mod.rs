use std::io;

use bytes::{Buf, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter};

pub(crate) use request::Request;
pub(crate) use response::Response;

mod request;
mod response;

pub struct MessageReader<R> {
    inner: BufReader<R>,
    //buf: Vec<u8>,
}

impl<R> MessageReader<R>
where
    R: AsyncReadExt + Send + Unpin,
{
    #[inline]
    pub fn new(reader: R) -> Self {
        Self {
            inner: BufReader::new(reader),
            //buf: Vec::with_capacity(1024),
        }
    }

    pub async fn read_req(&mut self) -> io::Result<Request> {
        let size = self.inner.read_i32().await?;

        if size <= 0 {
            return Err(io::Error::other(
                "protocol violation: received zero-size message",
            ));
        }

        // TODO: buffer pooling
        let mut buf = BytesMut::with_capacity(size as usize);
        buf.resize(size as usize, 0);

        self.inner.read_exact(&mut buf[..size as usize]).await?;

        let (header, header_bytes) = Self::read_header(&buf)?;

        // TODO: other requests
        Ok(Request::ApiVersions {
            size,
            header,
            // skip over header bytes
            data: buf.split_off(header_bytes).freeze(),
        })
    }

    // TODO: no real need to do this async (use Buf::get_*)
    fn read_header(mut buf: &[u8]) -> io::Result<(request::Header, usize)> {
        if buf.len() < 8 {
            return Err(invalid_data(format!(
                "request header has at least 8B, got {}B",
                buf.len()
            )));
        }

        let request_api_key = buf.get_i16().try_into().map_err(invalid_data)?;
        let request_api_version = buf.get_i16();
        let correlation_id = buf.get_i32();

        // TODO: read more headers (based on api key and version)

        let header = request::Header {
            request_api_key,
            request_api_version,
            correlation_id,
        };

        Ok((header, 8))
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

    pub async fn write_resp(&mut self, resp: Response) -> io::Result<()> {
        match resp {
            Response::ApiVersions {
                size,
                correlation_id,
            } => {
                self.inner.write_i32(size).await?;
                self.inner.write_i32(correlation_id).await?;
            }
        }

        Ok(())
    }

    pub async fn flush(&mut self) -> io::Result<()> {
        self.inner.flush().await
    }
}

#[inline]
fn invalid_data(error: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error)
}
