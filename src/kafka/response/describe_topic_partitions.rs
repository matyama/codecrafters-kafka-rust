use anyhow::{Context as _, Result};
use tokio::io::AsyncWriteExt;

use crate::kafka::common::Cursor;
use crate::kafka::error::ErrorCode;
use crate::kafka::types::{CompactArray, CompactStr, StrBytes, TagBuffer, Uuid};
use crate::kafka::{Serialize, WireSize};

/// # ApiVersions Request
///
/// [Response schema][schema]
///
/// [schema]: https://github.com/apache/kafka/blob/trunk/clients/src/main/resources/common/message/DescribeTopicPartitionsResponse.json
#[allow(dead_code)]
#[derive(Debug, Default)]
pub struct DescribeTopicPartitions {
    /// The duration in milliseconds for which the request was throttled due to a quota violation,
    /// or zero if the request did not violate any quota.
    ///
    /// API v0+
    pub throttle_time_ms: i32,

    /// Each topic in the response. (API v0+)
    pub topics: Vec<DescribeTopicPartitionsResponseTopic>,

    /// The next topic and partition index to fetch details for. (API v0+, nullable v0+)
    pub cursor: Option<Cursor>,

    /// Other tagged fields. (API v0+)
    pub tagged_fields: TagBuffer,
}

impl WireSize for DescribeTopicPartitions {
    const SIZE: usize = 4;

    fn size(&self, version: i16) -> usize {
        let mut size = Self::SIZE;
        size += CompactArray(self.topics.as_slice()).size(version);
        size += self.cursor.size(version);
        size += self.tagged_fields.size(version);
        size
    }
}

impl Serialize for DescribeTopicPartitions {
    async fn write_into<W>(self, writer: &mut W, version: i16) -> Result<()>
    where
        W: AsyncWriteExt + Send + Unpin,
    {
        self.throttle_time_ms
            .write_into(writer, version)
            .await
            .context("throttle_time_ms")?;

        CompactArray(self.topics)
            .write_into(writer, version)
            .await
            .context("topics")?;

        self.cursor
            .write_into(writer, version)
            .await
            .context("cursor")?;

        self.tagged_fields
            .write_into(writer, version)
            .await
            .context("tagged_fields")?;

        Ok(())
    }
}

#[derive(Debug)]
pub struct DescribeTopicPartitionsResponseTopic {
    /// The topic error, or 0 if there was no error. (API v0+)
    pub error_code: ErrorCode,

    /// The topic name. (API v0+, nullable v0+)
    pub name: Option<CompactStr>,

    /// The topic id. (API v0+)
    pub topic_id: Uuid,

    /// True if the topic is internal. (API v0+, default: false)
    pub is_internal: bool,

    /// Each partition in the topic. (API v0+)
    pub partitions: Vec<DescribeTopicPartitionsResponsePartition>,

    /// 32-bit bitfield to represent authorized operations for this topic.
    ///
    /// (API v0+, default: -2147483648)
    pub topic_authorized_operations: i32,

    /// Other tagged fields. (API v0+)
    pub tagged_fields: TagBuffer,
}

impl DescribeTopicPartitionsResponseTopic {
    #[inline]
    pub fn new(topic_id: Uuid) -> Self {
        Self {
            error_code: ErrorCode::NONE,
            name: None,
            topic_id,
            is_internal: false,
            partitions: Vec::new(),
            topic_authorized_operations: -2147483648,
            tagged_fields: TagBuffer::default(),
        }
    }

    #[inline]
    pub fn with_err(mut self, error_code: ErrorCode) -> Self {
        self.error_code = error_code;
        self
    }

    #[inline]
    pub fn with_name(mut self, name: StrBytes) -> Self {
        self.name = Some(name.into());
        self
    }
}

impl WireSize for DescribeTopicPartitionsResponseTopic {
    const SIZE: usize = ErrorCode::SIZE + Uuid::SIZE + 1 + 4;

    fn size(&self, version: i16) -> usize {
        let mut size = Self::SIZE;
        size += self.name.size(version);
        size += CompactArray(self.partitions.as_slice()).size(version);
        size += self.tagged_fields.size(version);
        size
    }
}

impl Serialize for DescribeTopicPartitionsResponseTopic {
    async fn write_into<W>(self, writer: &mut W, version: i16) -> Result<()>
    where
        W: AsyncWriteExt + Send + Unpin,
    {
        writer
            .write_i16(self.error_code as i16)
            .await
            .context("error_code")?;

        self.name
            .write_into(writer, version)
            .await
            .context("name")?;

        self.topic_id
            .write_into(writer, version)
            .await
            .context("topic_id")?;

        self.is_internal
            .write_into(writer, version)
            .await
            .context("is_internal")?;

        CompactArray(self.partitions)
            .write_into(writer, version)
            .await
            .context("partitions")?;

        writer
            .write_i32(self.topic_authorized_operations)
            .await
            .context("topic_authorized_operations")?;

        self.tagged_fields
            .write_into(writer, version)
            .await
            .context("tagged_fields")?;

        Ok(())
    }
}

#[derive(Debug)]
pub struct DescribeTopicPartitionsResponsePartition {
    /// The partition error, or 0 if there was no error. (API v0+)
    pub error_code: ErrorCode,

    /// The partition index. (API v0+)
    pub partition_index: i32,

    /// The ID of the leader broker. (API v0+)
    pub leader_id: i32,

    /// The leader epoch of this partition. (API v0+, default: -1)
    pub leader_epoch: i32,

    /// The set of all nodes that host this partition. (API v0+)
    pub replica_nodes: Vec<i32>,

    /// The set of nodes that are in sync with the leader for this partition. (API v0+)
    pub isr_nodes: Vec<i32>,

    /// The new eligible leader replicas otherwise. (API v0+, nullable v0+, default: None)
    pub eligible_leader_replicas: Option<Vec<i32>>,

    /// The last known ELR. (API v0+, nullable v0+, default: None)
    pub last_known_elr: Option<Vec<i32>>,

    /// The set of offline replicas of this partition. (API v0+)
    pub offline_replicas: Vec<i32>,

    /// Other tagged fields. (API v0+)
    pub tagged_fields: TagBuffer,
}

impl DescribeTopicPartitionsResponsePartition {
    #[inline]
    pub(crate) fn new(partition_index: i32) -> Self {
        Self {
            error_code: ErrorCode::NONE,
            partition_index,
            // TODO: default value
            leader_id: 0,
            leader_epoch: -1,
            replica_nodes: Vec::new(),
            isr_nodes: Vec::new(),
            eligible_leader_replicas: None,
            last_known_elr: None,
            offline_replicas: Vec::new(),
            tagged_fields: TagBuffer::default(),
        }
    }
}

impl WireSize for DescribeTopicPartitionsResponsePartition {
    const SIZE: usize = ErrorCode::SIZE + 4 + 4 + 4;

    fn size(&self, version: i16) -> usize {
        let mut size = Self::SIZE;
        size += CompactArray(self.replica_nodes.as_slice()).size(version);
        size += CompactArray(self.isr_nodes.as_slice()).size(version);
        size += CompactArray(&self.eligible_leader_replicas).size(version);
        size += CompactArray(&self.last_known_elr).size(version);
        size += CompactArray(self.offline_replicas.as_slice()).size(version);
        size += self.tagged_fields.size(version);
        size
    }
}

impl Serialize for DescribeTopicPartitionsResponsePartition {
    async fn write_into<W>(self, writer: &mut W, version: i16) -> Result<()>
    where
        W: tokio::io::AsyncWriteExt + Send + Unpin,
    {
        writer
            .write_i16(self.error_code as i16)
            .await
            .context("error_code")?;

        writer
            .write_i32(self.partition_index)
            .await
            .context("partition_index")?;

        writer
            .write_i32(self.leader_id)
            .await
            .context("leader_id")?;

        writer
            .write_i32(self.leader_epoch)
            .await
            .context("leader_epoch")?;

        CompactArray(self.replica_nodes)
            .write_into(writer, version)
            .await
            .context("replica_nodes")?;

        CompactArray(self.isr_nodes)
            .write_into(writer, version)
            .await
            .context("isr_nodes")?;

        CompactArray(self.eligible_leader_replicas)
            .write_into(writer, version)
            .await
            .context("eligible_leader_replicas")?;

        CompactArray(self.last_known_elr)
            .write_into(writer, version)
            .await
            .context("last_known_elr")?;

        CompactArray(self.offline_replicas)
            .write_into(writer, version)
            .await
            .context("offline_replicas")?;

        self.tagged_fields
            .write_into(writer, version)
            .await
            .context("tagged_fields")?;

        Ok(())
    }
}

#[derive(Clone, Copy, Debug)]
#[non_exhaustive]
#[repr(i8)]
pub enum AclOperation {
    Unknown = 0,
    Any = 1,
    All = 2,
    Read = 3,
    Write = 4,
    Create = 5,
    Delete = 6,
    Alter = 7,
    Describe = 8,
    ClusterAction = 9,
    DescribeConfigs = 10,
    AlterConfigs = 11,
    IdempotentWrite = 12,
    CreateTokens = 13,
    DescribeTokens = 14,
}

impl std::ops::BitOr<AclOperation> for i32 {
    type Output = Self;

    #[inline]
    fn bitor(self, rhs: AclOperation) -> Self::Output {
        self | (1 << rhs as i8 as i32)
    }
}

impl std::ops::BitOr<AclOperation> for AclOperation {
    type Output = i32;

    #[inline]
    fn bitor(self, rhs: AclOperation) -> Self::Output {
        0 | self | rhs
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn acl_operations() {
        use AclOperation::*;

        let expected = 3576;

        let actual =
            Read | Write | Create | Delete | Alter | Describe | DescribeConfigs | AlterConfigs;

        assert_eq!(
            expected, actual,
            "expected={expected:#010x} ({expected:#018b}) actual={actual:#010x} ({actual:#018b})"
        );
    }

    #[tokio::test]
    async fn serialize_error_response() {
        use AclOperation::*;

        let topic_authorized_operations =
            Read | Write | Create | Delete | Alter | Describe | DescribeConfigs | AlterConfigs;

        let response = DescribeTopicPartitions {
            throttle_time_ms: 0,
            topics: vec![DescribeTopicPartitionsResponseTopic {
                error_code: ErrorCode::UNKNOWN_TOPIC_OR_PARTITION,
                name: Some(StrBytes::from("foo").into()),
                topic_id: Uuid::zero(),
                is_internal: false,
                partitions: vec![],
                topic_authorized_operations,
                tagged_fields: TagBuffer::default(),
            }],
            cursor: None,
            tagged_fields: TagBuffer::default(),
        };

        let mut expected = Vec::new();
        expected.extend_from_slice(b"\x00\x00\x00\x00"); // throttle_time_ms
        expected.extend_from_slice(b"\x02"); // topics array length
        expected.extend_from_slice(b"\x00\x03"); // topic #1 error code
        expected.extend_from_slice(b"\x04"); // topic #1 name length
        expected.extend_from_slice(b"\x66\x6f\x6f"); // topic #1 name contents
                                                     // topic #1 UUID
        expected
            .extend_from_slice(b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00");
        expected.extend_from_slice(b"\x00"); // topic #1 is_internal
        expected.extend_from_slice(b"\x01"); // topic #1 partitions array
        expected.extend_from_slice(b"\x00\x00\x0d\xf8"); // topic #1 topic_authorized_operations
        expected.extend_from_slice(b"\x00"); // topic #1 tagged_fields
        expected.extend_from_slice(b"\xff"); // next cursor
        expected.extend_from_slice(b"\x00"); // tagged_fields

        let mut writer = Cursor::new(Vec::new());

        response
            .write_into(&mut writer, 1)
            .await
            .expect("response serialized");

        let actual = writer.into_inner();

        assert_eq!(expected, actual);
    }
}
