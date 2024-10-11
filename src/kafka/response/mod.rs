use anyhow::{Context as _, Result};
use tokio::io::AsyncWriteExt;

use crate::kafka::types::TagBuffer;
use crate::kafka::{HeaderVersion, Serialize, WireSize};

pub(crate) use api_versions::ApiVersions;
pub(crate) use fetch::Fetch;

pub mod api_versions;
pub mod fetch;

#[derive(Debug)]
pub struct ResponseHeader {
    /// An integer that uniquely identifies the request (API v0+)
    pub correlation_id: i32,
    /// Other tagged fields (API v1+)
    pub tagged_fields: TagBuffer,
}

impl WireSize for ResponseHeader {
    const SIZE: usize = 4;

    #[inline]
    fn size(&self, version: i16) -> usize {
        match version {
            0 => Self::SIZE,
            _ => Self::SIZE + self.tagged_fields.size(version),
        }
    }
}

impl Serialize for ResponseHeader {
    async fn write_into<W>(self, writer: &mut W, version: i16) -> Result<()>
    where
        W: AsyncWriteExt + Send + Unpin,
    {
        writer
            .write_i32(self.correlation_id)
            .await
            .context("correlation id")?;

        if version >= 1 {
            self.tagged_fields
                .write_into(writer, version)
                .await
                .context("tagged fields")?;
        }

        Ok(())
    }
}

#[derive(Debug)]
pub struct ResponseMessage {
    pub size: i32,
    pub header: ResponseHeader,
    pub body: ResponseBody,
}

impl Serialize for ResponseMessage {
    async fn write_into<W>(self, writer: &mut W, version: i16) -> Result<()>
    where
        W: AsyncWriteExt + Send + Unpin,
    {
        writer.write_i32(self.size).await.context("message size")?;

        // FIXME: this is awkward
        let header_version = self.body.header_version(version);

        self.header
            .write_into(writer, header_version)
            .await
            .context("message header")?;

        self.body
            .write_into(writer, version)
            .await
            .context("message body")?;

        Ok(())
    }
}

#[derive(Debug)]
pub enum ResponseBody {
    ApiVersions(ApiVersions),
    #[allow(dead_code)]
    Fetch(Fetch),
}

impl HeaderVersion for ResponseBody {
    fn header_version(&self, api_version: i16) -> i16 {
        match self {
            // ApiVersions responses always have headers without tagged fields (i.e., v0)
            // Tagged fields are only supported in the body but not in the header.
            Self::ApiVersions(_) => 0,
            Self::Fetch(_) if api_version >= 12 => 1,
            _ => 0,
        }
    }
}

impl Serialize for ResponseBody {
    async fn write_into<W>(self, writer: &mut W, version: i16) -> Result<()>
    where
        W: AsyncWriteExt + Send + Unpin,
    {
        match self {
            Self::ApiVersions(body) => body.write_into(writer, version).await,
            Self::Fetch(body) => body.write_into(writer, version).await,
        }
    }
}
