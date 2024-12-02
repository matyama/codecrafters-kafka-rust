use anyhow::{Context as _, Result};
use bytes::Buf;
use tokio::io::AsyncWriteExt;

use crate::kafka::types::{CompactStr, TagBuffer};
use crate::kafka::{Deserialize, Serialize, WireSize};

#[derive(Debug, PartialEq)]
pub struct Cursor {
    /// The name for the first topic to process. (API v0+)
    pub topic_name: CompactStr,
    /// The partition index to start with. (API v0+)
    pub partition_index: i32,
    /// Other tagged fields. (API v0+)
    pub tagged_fields: TagBuffer,
}

impl Deserialize for Cursor {
    fn read_from<B: Buf>(buf: &mut B, version: i16) -> Result<(Self, usize)> {
        let (topic_name, mut size) = CompactStr::read_from(buf, version)?;

        let (partition_index, n) = i32::read_from(buf, version)?;
        size += n;

        let (tagged_fields, n) = TagBuffer::read_from(buf, version)?;
        size += n;

        let cursor = Self {
            topic_name,
            partition_index,
            tagged_fields,
        };

        Ok((cursor, size))
    }
}

impl Deserialize for Option<Cursor> {
    fn read_from<B: Buf>(buf: &mut B, version: i16) -> Result<(Self, usize)> {
        let (present, n) = i8::read_from(buf, version)?;
        if present == 1 {
            Cursor::read_from(buf, version).map(|(cursor, size)| (Some(cursor), size + n))
        } else {
            Ok((None, n))
        }
    }
}

impl Serialize for Cursor {
    async fn write_into<W>(self, writer: &mut W, version: i16) -> Result<()>
    where
        W: AsyncWriteExt + Send + Unpin,
    {
        self.topic_name
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

impl Serialize for Option<Cursor> {
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

impl WireSize for Cursor {
    const SIZE: usize = 4;

    fn size(&self, version: i16) -> usize {
        let mut size = Self::SIZE;
        size += self.topic_name.size(version);
        size += self.tagged_fields.size(version);
        size
    }
}

impl WireSize for Option<Cursor> {
    #[inline]
    fn size(&self, version: i16) -> usize {
        self.as_ref().map_or(1, |c| c.size(version))
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
