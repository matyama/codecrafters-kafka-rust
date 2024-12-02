use anyhow::{Context as _, Result};
use bytes::Buf;
use tokio::io::AsyncWriteExt;

use crate::kafka::types::{CompactStr, StrBytes, TagBuffer};
use crate::kafka::{AsyncSerialize, Deserialize, Serialize};

#[derive(Debug, PartialEq)]
pub struct Cursor {
    /// The name for the first topic to process. (API v0+)
    pub topic_name: StrBytes,
    /// The partition index to start with. (API v0+)
    pub partition_index: i32,
    /// Other tagged fields. (API v0+)
    pub tagged_fields: TagBuffer,
}

impl Deserialize for Cursor {
    fn decode<B: Buf>(buf: &mut B, version: i16) -> Result<(Self, usize)> {
        let (topic_name, mut size) = CompactStr::decode(buf, version)?;

        let (partition_index, n) = i32::decode(buf, version)?;
        size += n;

        let (tagged_fields, n) = TagBuffer::decode(buf, version)?;
        size += n;

        let cursor = Self {
            topic_name: topic_name.into(),
            partition_index,
            tagged_fields,
        };

        Ok((cursor, size))
    }
}

impl Deserialize for Option<Cursor> {
    fn decode<B: Buf>(buf: &mut B, version: i16) -> Result<(Self, usize)> {
        let (present, n) = i8::decode(buf, version)?;
        if present == 1 {
            Cursor::decode(buf, version).map(|(cursor, size)| (Some(cursor), size + n))
        } else {
            Ok((None, n))
        }
    }
}

impl AsyncSerialize for Cursor {
    async fn write_into<W>(self, writer: &mut W, version: i16) -> Result<()>
    where
        W: AsyncWriteExt + Send + Unpin,
    {
        CompactStr::from(self.topic_name)
            .write_into(writer, version)
            .await
            .context("topic_name")?;

        self.partition_index
            .write_into(writer, version)
            .await
            .context("partition")?;

        self.tagged_fields
            .write_into(writer, version)
            .await
            .context("tagged_fields")?;

        Ok(())
    }
}

impl AsyncSerialize for Option<Cursor> {
    async fn write_into<W>(self, writer: &mut W, version: i16) -> Result<()>
    where
        W: AsyncWriteExt + Send + Unpin,
    {
        match self {
            Some(cursor) => {
                writer.write_i8(1).await.context("cursor option")?;
                cursor.write_into(writer, version).await
            }
            None => writer.write_i8(-1).await.context("cursor option"),
        }
    }
}

impl Serialize for Cursor {
    const SIZE: usize = 4;

    fn encode_size(&self, version: i16) -> usize {
        let mut size = Self::SIZE;
        size += CompactStr::from(&self.topic_name).encode_size(version);
        size += self.tagged_fields.encode_size(version);
        size
    }
}

impl Serialize for Option<Cursor> {
    #[inline]
    fn encode_size(&self, version: i16) -> usize {
        self.as_ref().map_or(1, |c| c.encode_size(version))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn null_cursor() {
        let mut buf = b"\xff".as_slice();
        let (cursor, n) = <Option<Cursor>>::deserialize(&mut buf).expect("valid null cursor");
        assert_eq!(n, 1);
        assert_eq!(cursor, None);
    }
}
