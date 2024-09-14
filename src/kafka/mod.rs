use std::io;

use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter};

pub(crate) use request::Request;
pub(crate) use response::Response;

pub(crate) mod request;
pub(crate) mod response;

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

        let mut buf = BytesMut::with_capacity(size as usize);
        buf.resize(size as usize, 0);

        self.inner.read_exact(&mut buf[..size as usize]).await?;

        // TODO: other requests
        Ok(Request::ApiVersions {
            size,
            data: buf.freeze(),
        })
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
