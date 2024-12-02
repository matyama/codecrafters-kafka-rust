use anyhow::{ensure, Context as _, Result};
use bytes::Buf;

use crate::kafka::types::{CompactStr, StrBytes, TagBuffer, Uuid};
use crate::kafka::Deserialize;

#[derive(Clone, Debug, PartialEq)]
pub struct Topic {
    pub name: StrBytes,
    pub topic_id: Uuid,
}

impl Topic {
    #[inline]
    pub(crate) fn topic_id(&self) -> Uuid {
        self.topic_id.clone()
    }
}

impl From<TopicRecord> for Topic {
    #[inline]
    fn from(record: TopicRecord) -> Self {
        Self {
            name: record.topic_name,
            topic_id: record.topic_id,
        }
    }
}

impl From<&TopicRecord> for Topic {
    #[inline]
    fn from(record: &TopicRecord) -> Self {
        Self {
            name: record.topic_name.clone(),
            topic_id: record.topic_id.clone(),
        }
    }
}

/// # TopicRecord
///
/// [Record schema][schema]
///
/// [schema]: https://github.com/apache/kafka/blob/5b3027dfcbcb62d169d4b4421260226e620459af/metadata/src/main/resources/common/metadata/TopicRecord.json
#[derive(Debug, PartialEq)]
pub struct TopicRecord {
    pub frame_version: i8,
    pub record_type: i8,
    pub version: i8,
    /// The topic name. (API v0+)
    pub topic_name: StrBytes,
    /// The unique ID of this topic. (API v0+)
    pub topic_id: Uuid,
    /// Other tagged fields
    pub tagged_fields: TagBuffer,
}

impl Deserialize for TopicRecord {
    fn decode<B: Buf>(buf: &mut B, version: i16) -> Result<(Self, usize)> {
        let (frame_version, mut size) = i8::decode(buf, version).context("frame_version")?;

        let (record_type, n) = i8::decode(buf, version).context("type")?;
        size += n;

        ensure!(record_type == 2, "invalid record type");

        let (ver, n) = i8::decode(buf, version).context("version")?;
        size += n;

        let (topic_name, n) = CompactStr::decode(buf, version).context("topic_name")?;
        size += n;

        let (topic_id, n) = Uuid::decode(buf, version).context("topic_id")?;
        size += n;

        let (tagged_fields, n) = TagBuffer::decode(buf, version).context("tagged_fields")?;
        size += n;

        let record = Self {
            frame_version,
            record_type,
            version: ver,
            topic_name: topic_name.into(),
            topic_id,
            tagged_fields,
        };

        Ok((record, size))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deserialize() {
        let mut buf = Vec::with_capacity(24);
        buf.extend_from_slice(b"\x01"); // frame version
        buf.extend_from_slice(b"\x02"); // type
        buf.extend_from_slice(b"\x00"); // version
        buf.extend_from_slice(b"\x04"); // topic name length
        buf.extend_from_slice(b"\x73\x61\x7a"); // topic name & contents
        buf.extend_from_slice(b"\x00\x00\x00\x00\x00\x00\x40\x00\x80\x00\x00\x00\x00\x00\x00\x91");
        buf.extend_from_slice(b"\x00"); // tagged_fields

        let topic_id =
            Uuid::from_static(b"\x00\x00\x00\x00\x00\x00\x40\x00\x80\x00\x00\x00\x00\x00\x00\x91");

        let expected = TopicRecord {
            frame_version: 1,
            record_type: 2,
            version: 0,
            topic_name: "saz".into(),
            topic_id,
            tagged_fields: TagBuffer::default(),
        };

        let mut buf = std::io::Cursor::new(buf);

        let (actual, size) = TopicRecord::deserialize(&mut buf).expect("valid topic record");

        assert_eq!(size, 24);
        assert_eq!(expected, actual);
    }
}
