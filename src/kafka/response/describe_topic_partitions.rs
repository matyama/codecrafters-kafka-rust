use anyhow::{Context as _, Result};
use tokio::io::AsyncWriteExt;

use crate::kafka::common::Cursor;
use crate::kafka::error::ErrorCode;
use crate::kafka::types::{CompactArray, CompactStr, TagBuffer, Uuid};
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
            error_code: ErrorCode::default(),
            name: None,
            topic_id,
            is_internal: false,
            partitions: Vec::new(),
            topic_authorized_operations: -2147483648,
            tagged_fields: TagBuffer::default(),
        }
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

    /// The leader epoch of this partition. (API v0+)
    pub leader_epoch: i32,

    /// The set of all nodes that host this partition. (API v0+)
    pub replica_nodes: Vec<i32>,

    /// The set of nodes that are in sync with the leader for this partition. (API v0+)
    pub isr_nodes: Vec<i32>,

    /// The new eligible leader replicas otherwise. (API v0+, nullable v0+)
    pub eligible_leader_replicas: Option<Vec<i32>>,

    /// The last known ELR. (API v0+, nullable v0+)
    pub last_known_elr: Option<Vec<i32>>,

    /// The set of offline replicas of this partition. (API v0+)
    pub offline_replicas: Vec<i32>,

    /// Other tagged fields. (API v0+)
    pub tagged_fields: TagBuffer,
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
