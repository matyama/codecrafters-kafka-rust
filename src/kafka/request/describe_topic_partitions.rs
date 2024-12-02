use anyhow::{ensure, Result};
use bytes::Buf;

use crate::kafka::common::Cursor;
use crate::kafka::types::{CompactArray, CompactStr, StrBytes, TagBuffer};
use crate::kafka::Deserialize;

/// # DescribeTopicPartitions Request
///
/// [Request schema][schema]
///
/// [schema]: https://github.com/apache/kafka/blob/trunk/clients/src/main/resources/common/message/DescribeTopicPartitionsRequest.json
#[allow(dead_code)]
#[derive(Debug)]
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
    fn read_from<B: Buf>(buf: &mut B, version: i16) -> Result<(Self, usize)> {
        let mut body_bytes = 0;

        let (CompactArray(topics), n) = CompactArray::read_from(buf, version)?;
        body_bytes += n;

        let (response_partition_limit, n) = i32::read_from(buf, version)?;
        body_bytes += n;

        let (cursor, n) = Deserialize::read_from(buf, version)?;
        body_bytes += n;

        let (tagged_fields, n) = TagBuffer::read_from(buf, version)?;
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

#[derive(Debug)]
pub struct TopicRequest {
    /// The topic name. (API v0+)
    pub name: StrBytes,
    /// Other tagged fields. (API v0+)
    pub tagged_fields: TagBuffer,
}

impl Deserialize for TopicRequest {
    fn read_from<B: Buf>(buf: &mut B, version: i16) -> Result<(Self, usize)> {
        let (name, mut size) = CompactStr::read_from(buf, version)?;

        let (tagged_fields, n) = TagBuffer::read_from(buf, version)?;
        size += n;

        let req = Self {
            name: name.into(),
            tagged_fields,
        };

        Ok((req, size))
    }
}
