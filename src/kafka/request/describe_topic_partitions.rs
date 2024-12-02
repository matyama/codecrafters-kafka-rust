use anyhow::{ensure, Context as _, Result};
use bytes::Buf;

use crate::kafka::common::Cursor;
use crate::kafka::types::{CompactArray, CompactStr, StrBytes, TagBuffer};
use crate::kafka::Deserialize;

/// # DescribeTopicPartitions Request
///
/// [Request schema][schema]
///
/// [schema]: https://github.com/apache/kafka/blob/trunk/clients/src/main/resources/common/message/DescribeTopicPartitionsRequest.json
#[derive(Debug, PartialEq)]
pub struct DescribeTopicPartitions {
    /// The topics to fetch details for. (API v0+)
    pub topics: Vec<TopicRequest>,
    /// The maximum number of partitions included in the response. (API v0+)
    pub response_partition_limit: i32,
    /// The first topic and partition index to fetch details for. (API v0+, nullable v0+)
    pub cursor: Option<Cursor>,
    /// Other tagged fields. (API v0+)
    pub tagged_fields: TagBuffer,
}

impl Deserialize for DescribeTopicPartitions {
    fn decode<B: Buf>(buf: &mut B, version: i16) -> Result<(Self, usize)> {
        let mut body_bytes = 0;

        let (CompactArray(topics), n) = CompactArray::decode(buf, version).context("topics")?;
        body_bytes += n;

        let (response_partition_limit, n) =
            i32::decode(buf, version).context("response_partition_limit")?;
        body_bytes += n;

        let (cursor, n) = Deserialize::decode(buf, version).context("cursor")?;
        body_bytes += n;

        let (tagged_fields, n) = TagBuffer::decode(buf, version).context("tagged_fields")?;
        body_bytes += n;

        ensure!(
            !buf.has_remaining(),
            "DescribeTopicPartitions: buffer contains leftover bytes"
        );

        let body = Self {
            topics,
            response_partition_limit,
            cursor,
            tagged_fields,
        };

        Ok((body, body_bytes))
    }
}

#[derive(Debug, PartialEq)]
pub struct TopicRequest {
    /// The topic name. (API v0+)
    pub name: StrBytes,
    /// Other tagged fields. (API v0+)
    pub tagged_fields: TagBuffer,
}

impl Deserialize for TopicRequest {
    fn decode<B: Buf>(buf: &mut B, version: i16) -> Result<(Self, usize)> {
        let (name, mut size) = CompactStr::decode(buf, version).context("name")?;

        let (tagged_fields, n) = TagBuffer::decode(buf, version).context("tagged_fields")?;
        size += n;

        let req = Self {
            name: name.into(),
            tagged_fields,
        };

        Ok((req, size))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deserialize_topic() {
        let expected = TopicRequest {
            name: "foo".into(),
            tagged_fields: TagBuffer::default(),
        };
        let mut buf = b"\x04\x66\x6f\x6f\x00".as_slice();
        let (actual, n) = TopicRequest::deserialize(&mut buf).expect("valid topic request");
        assert_eq!(5, n);
        assert_eq!(expected, actual);
    }

    #[test]
    fn deserialize_request_body() {
        let expected = DescribeTopicPartitions {
            topics: vec![TopicRequest {
                name: "foo".into(),
                tagged_fields: TagBuffer::default(),
            }],
            response_partition_limit: 100,
            cursor: None,
            tagged_fields: TagBuffer::default(),
        };

        let mut buf = b"\x02\x04\x66\x6f\x6f\x00\x00\x00\x00\x64\xff\x00".as_slice();
        let expected_size = buf.len();

        let (actual, actual_size) =
            DescribeTopicPartitions::deserialize(&mut buf).expect("valid request");

        assert_eq!(expected_size, actual_size);
        assert_eq!(expected, actual);
    }
}
