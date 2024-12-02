use anyhow::{bail, Context as _, Result};
use bytes::Bytes;
use tokio::io::AsyncWriteExt;

use crate::kafka::error::ErrorCode;
use crate::kafka::types::{
    Array, CompactArray, CompactBytes, CompactStr, Str, StrBytes, TagBuffer, Uuid,
};
use crate::kafka::{AsyncSerialize, Serialize};

/// # Fetch Response
///
/// [Response schema][schema]
///
/// Version 1 adds throttle time.
///
/// Version 2 and 3 are the same as version 1.
///
/// Version 4 adds features for transactional consumption.
///
/// Version 5 adds LogStartOffset to indicate the earliest available offset of
/// partition data that can be consumed.
///
/// Starting in version 6, we may return KAFKA_STORAGE_ERROR as an error code.
///
/// Version 7 adds incremental fetch request support.
///
/// Starting in version 8, on quota violation, brokers send out responses before throttling.
///
/// Version 9 is the same as version 8.
///
/// Version 10 indicates that the response data can use the ZStd compression
/// algorithm, as described in KIP-110.
/// Version 12 adds support for flexible versions, epoch detection through the `TruncationOffset`
/// field, and leader discovery through the `CurrentLeader` field
///
/// Version 13 replaces the topic name field with topic ID (KIP-516).
///
/// Version 14 is the same as version 13 but it also receives a new error called
/// OffsetMovedToTieredStorageException (KIP-405)
///
/// Version 15 is the same as version 14 (KIP-903).
///
/// Version 16 adds the 'NodeEndpoints' field (KIP-951).
///
/// Version 17 no changes to the response (KIP-853).
///
/// [schema]: https://github.com/apache/kafka/blob/trunk/clients/src/main/resources/common/message/FetchResponse.json
#[derive(Debug)]
pub struct Fetch {
    /// The duration in milliseconds for which the request was throttled due to a quota violation,
    /// or zero if the request did not violate any quota. (API v1+)
    pub throttle_time_ms: i32,

    /// The top-level error code. (API v7+)
    pub error_code: ErrorCode,

    /// The fetch session ID, or 0 if this is not part of a fetch session. (API v7+)
    pub session_id: i32,

    /// The response topics. (API v0+)
    ///
    /// ### Encoding
    ///  - v0-11: [`Array`]
    ///  - v12+: [`CompactArray`]
    pub responses: Vec<FetchableTopicResponse>,

    // TODO: node_endpoints - tagged
    ///// Endpoints for all current-leaders enumerated in PartitionData, with errors
    ///// NOT_LEADER_OR_FOLLOWER & FENCED_LEADER_EPOCH. (API v16+)
    //// or Map<BrokerId, NodeEndpoint> where NodeEndpoint has no node_id: BrokerId
    //pub node_endpoints: Vec<NodeEndpoint>,
    //
    /// The tagged fields. (API v12+)
    pub tagged_fields: TagBuffer,
}

impl Fetch {
    #[inline]
    pub fn error(throttle_time_ms: i32, error_code: ErrorCode, session_id: i32) -> Self {
        Self {
            throttle_time_ms,
            error_code,
            session_id,
            ..Default::default()
        }
    }
}

impl Default for Fetch {
    #[inline]
    fn default() -> Self {
        Self {
            throttle_time_ms: 0,
            error_code: ErrorCode::default(),
            session_id: 0,
            responses: vec![],
            tagged_fields: TagBuffer::default(),
        }
    }
}

impl Serialize for Fetch {
    #[inline]
    fn encode_size(&self, version: i16) -> usize {
        let mut size = 0;

        if version >= 1 {
            // throttle_time_ms
            size += 4;
        }

        if version >= 7 {
            size += self.error_code.encode_size(version);
            // session_id
            size += 4;
        }

        // responses
        size += if version >= 12 {
            CompactArray(self.responses.as_slice()).encode_size(version)
        } else {
            Array(self.responses.as_slice()).encode_size(version)
        };

        if version >= 12 {
            if version >= 16 {
                // TODO: node_endpoints
            }

            size += self.tagged_fields.encode_size(version)
        }

        size
    }
}

impl AsyncSerialize for Fetch {
    async fn write_into<W>(self, writer: &mut W, version: i16) -> Result<()>
    where
        W: AsyncWriteExt + Send + Unpin,
    {
        if version >= 1 {
            writer
                .write_i32(self.throttle_time_ms)
                .await
                .context("throttle time ms")?;
        }

        if version >= 7 {
            writer
                .write_i16(self.error_code as i16)
                .await
                .context("error code")?;

            writer
                .write_i32(self.session_id)
                .await
                .context("session id")?;
        }

        if version >= 12 {
            CompactArray(self.responses)
                .write_into(writer, version)
                .await
                .context("responses")?;
        } else {
            Array(self.responses)
                .write_into(writer, version)
                .await
                .context("responses")?;
        };

        if version >= 12 {
            if version >= 16 {
                // TODO: serialize self.node_endpoints
            }

            self.tagged_fields
                .write_into(writer, version)
                .await
                .context("tagged fields")?;
        }

        Ok(())
    }
}

#[derive(Debug)]
pub struct FetchableTopicResponse {
    /// The topic name. (API v0-12)
    ///
    /// ### Encoding
    ///  - v0-11: [`Str`]
    ///  - v12+: [`CompactStr`]
    pub topic: StrBytes,

    /// The unique topic ID (API v13+)
    pub topic_id: Uuid,

    /// The topic partitions. (API v0+)
    ///
    /// ### Encoding
    ///  - v0-11: [`Array`]
    ///  - v12+: [`CompactArray`]
    pub partitions: Vec<PartitionData>,

    /// The tagged fields. (API v12+)
    pub tagged_fields: TagBuffer,
}

impl Serialize for FetchableTopicResponse {
    fn encode_size(&self, version: i16) -> usize {
        let mut size = 0;

        size += match version {
            0..=11 => Str::from(&self.topic).encode_size(version),
            12 => CompactStr::from(&self.topic).encode_size(version),
            13.. => self.topic_id.encode_size(version),
            _ => 0,
        };

        size += if version >= 12 {
            CompactArray(self.partitions.as_slice()).encode_size(version)
        } else {
            Array(self.partitions.as_slice()).encode_size(version)
        };

        if version >= 12 {
            size += self.tagged_fields.encode_size(version);
        }

        size
    }
}

impl AsyncSerialize for FetchableTopicResponse {
    async fn write_into<W>(self, writer: &mut W, version: i16) -> Result<()>
    where
        W: AsyncWriteExt + Send + Unpin,
    {
        match version {
            0..=11 => Str::from(self.topic)
                .write_into(writer, version)
                .await
                .context("topic")?,
            12 => CompactStr::from(self.topic)
                .write_into(writer, version)
                .await
                .context("topic")?,
            13.. => self
                .topic_id
                .write_into(writer, version)
                .await
                .context("topic id")?,
            // XXX: UNSUPPORTED_VERSION
            v => bail!("invalid API version: v{v}"),
        }

        if version >= 12 {
            CompactArray(self.partitions)
                .write_into(writer, version)
                .await
                .context("partitions")?;
        } else {
            Array(self.partitions)
                .write_into(writer, version)
                .await
                .context("partitions")?;
        }

        if version >= 12 {
            self.tagged_fields
                .write_into(writer, version)
                .await
                .context("tagged fields")?;
        }

        Ok(())
    }
}

#[derive(Debug)]
pub struct PartitionData {
    /// The partition index. (API v0+)
    pub partition_index: i32,

    /// The error code, or 0 if there was no fetch error. (API v0+)
    pub error_code: ErrorCode,

    /// The current high water mark. (API v0+)
    pub high_watermark: i64,

    /// The last stable offset (or LSO) of the partition. (API v4+)
    ///
    /// This is the last offset such that the state of all transactional records prior to this
    /// offset have been decided (ABORTED or COMMITTED).
    pub last_stable_offset: i64,

    /// The current log start offset. (API v5+)
    pub last_start_offset: i64,

    ///// In case divergence is detected based on the `LastFetchedEpoch` and `FetchOffset` in the
    ///// request, this field indicates the largest epoch and its end offset such that subsequent
    ///// records are known to diverge.
    /////
    ///// API v12+ (tagged)
    //pub diverging_epoch: EpochEndOffset,

    ///// API v12+ (tagged)
    //pub current_leader: LeaderIdAndEpoch,

    ///// In the case of fetching an offset less than the LogStartOffset, this is the end offset and
    ///// epoch that should be used in the FetchSnapshot request.
    /////
    ///// API v12+ (tagged)
    //pub snapshot_id: SnapshotId,
    /// The aborted transactions. (API v4+, nullable v4+)
    ///
    /// ### Encoding
    ///  - v4-11: [`Array`]
    ///  - v12+: [`CompactArray`]
    pub aborted_transactions: Option<Vec<AbortedTransaction>>,

    // XXX: BrokerId
    /// The preferred read replica for the consumer to use on its next fetch request. (API v11+)
    pub preferred_read_replica: i32,

    /// The record data. (API v0+, nullable v0+)
    ///
    /// ### Encoding
    ///  - v0-11: [`Bytes`]
    ///  - v12+: [`CompactBytes`]
    pub records: Option<Bytes>,

    /// The tagged fields. (API v12+)
    pub tagged_fields: TagBuffer,
}

impl PartitionData {
    pub fn new(partition_index: i32, error_code: ErrorCode, high_watermark: i64) -> Self {
        Self {
            partition_index,
            error_code,
            high_watermark,
            last_stable_offset: -1,
            last_start_offset: -1,
            //diverging_epoch: Default::default(),
            //current_leader: Default::default(),
            //snapshot_id: Default::default(),
            aborted_transactions: Default::default(),
            preferred_read_replica: -1,
            records: None,
            tagged_fields: TagBuffer::default(),
        }
    }

    #[inline]
    pub fn with_records(mut self, records: Option<Bytes>) -> Self {
        self.records = records;
        self
    }
}

impl Serialize for PartitionData {
    fn encode_size(&self, version: i16) -> usize {
        // partition_index + error_code + high_watermark
        let mut size = 4 + self.error_code.encode_size(version) + 8;

        if version >= 4 {
            // last_stable_offset
            size += 8;
        }

        if version >= 5 {
            // last_start_offset
            size += 8
        }

        size += match version {
            4..=11 => Array(&self.aborted_transactions).encode_size(version),
            12.. => CompactArray(&self.aborted_transactions).encode_size(version),
            _ => 0,
        };

        if version >= 11 {
            // preferred_read_replica
            size += 4;
        }

        size += if version >= 12 {
            // NOTE: does not clone the raw bytes, just increments the ref count
            self.records.clone().map(CompactBytes).encode_size(version)
        } else {
            self.records.encode_size(version)
        };

        if version >= 12 {
            // TODO tagged fields specific to this API key (response)
            size += self.tagged_fields.encode_size(version);
        }

        size
    }
}

impl AsyncSerialize for PartitionData {
    async fn write_into<W>(self, writer: &mut W, version: i16) -> Result<()>
    where
        W: AsyncWriteExt + Send + Unpin,
    {
        writer
            .write_i32(self.partition_index)
            .await
            .context("partition index")?;

        writer
            .write_i16(self.error_code as i16)
            .await
            .context("error code")?;

        writer
            .write_i64(self.high_watermark)
            .await
            .context("high watermark")?;

        if version >= 4 {
            writer
                .write_i64(self.last_stable_offset)
                .await
                .context("last stable offset")?;
        }

        if version >= 5 {
            writer
                .write_i64(self.last_start_offset)
                .await
                .context("last start offset")?;
        }

        match version {
            4..=11 => Array(self.aborted_transactions)
                .write_into(writer, version)
                .await
                .context("aborted transactions")?,
            12.. => CompactArray(self.aborted_transactions)
                .write_into(writer, version)
                .await
                .context("aborted transactions")?,
            _ => {}
        };

        if version >= 11 {
            writer
                .write_i32(self.preferred_read_replica)
                .await
                .context("preferred read replica")?;
        }

        if version >= 12 {
            self.records
                .map(CompactBytes)
                .write_into(writer, version)
                .await
                .context("records")?;
        } else {
            self.records
                .write_into(writer, version)
                .await
                .context("records")?;
        };

        if version >= 12 {
            // TODO: other parsed tagged fields
            self.tagged_fields
                .write_into(writer, version)
                .await
                .context("tagged fields")?;
        }

        Ok(())
    }
}

/// API v12+
#[derive(Debug)]
pub struct EpochEndOffset {
    pub epoch: i32,
    pub end_offset: i64,
    pub tagged_fields: TagBuffer,
}

impl Default for EpochEndOffset {
    #[inline]
    fn default() -> Self {
        Self {
            epoch: -1,
            end_offset: -1,
            tagged_fields: Default::default(),
        }
    }
}

impl Serialize for EpochEndOffset {
    /// Static size (note: only valid for v12+)
    ///
    /// `SIZE = size(epoch) + size(end_offset)`
    const SIZE: usize = 4 + 8;

    #[inline]
    fn encode_size(&self, version: i16) -> usize {
        if version >= 12 {
            Self::SIZE + self.tagged_fields.encode_size(version)
        } else {
            0
        }
    }
}

impl AsyncSerialize for EpochEndOffset {
    async fn write_into<W>(self, writer: &mut W, version: i16) -> Result<()>
    where
        W: AsyncWriteExt + Send + Unpin,
    {
        if version >= 12 {
            writer.write_i32(self.epoch).await.context("epoch")?;
        }

        if version >= 12 {
            writer
                .write_i64(self.end_offset)
                .await
                .context("end offset")?;
        }

        if version >= 12 {
            self.tagged_fields
                .write_into(writer, version)
                .await
                .context("tagged fields")?;
        }

        Ok(())
    }
}

/// API v12+
#[derive(Debug)]
pub struct LeaderIdAndEpoch {
    // TODO: BrokerId
    /// The ID of the current leader or -1 if the leader is unknown.
    pub leader_id: i32,
    /// The latest known leader epoch.
    pub leader_epoch: i32,
    /// The tagged fields. (API v12+)
    pub tagged_fields: TagBuffer,
}

impl Default for LeaderIdAndEpoch {
    #[inline]
    fn default() -> Self {
        Self {
            leader_id: -1,
            leader_epoch: -1,
            tagged_fields: Default::default(),
        }
    }
}

impl Serialize for LeaderIdAndEpoch {
    /// Static size (note: only valid for v12+)
    ///
    /// `SIZE = size(leader_id) + size(leader_epoch)`
    const SIZE: usize = 4 + 4;

    fn encode_size(&self, version: i16) -> usize {
        if version >= 12 {
            Self::SIZE + self.tagged_fields.encode_size(version)
        } else {
            0
        }
    }
}

impl AsyncSerialize for LeaderIdAndEpoch {
    async fn write_into<W>(self, writer: &mut W, version: i16) -> Result<()>
    where
        W: AsyncWriteExt + Send + Unpin,
    {
        if version >= 12 {
            writer
                .write_i32(self.leader_id)
                .await
                .context("leader id")?;
        }

        if version >= 12 {
            writer
                .write_i32(self.leader_epoch)
                .await
                .context("leader epoch")?;
        }

        if version >= 12 {
            self.tagged_fields
                .write_into(writer, version)
                .await
                .context("tagged fields")?;
        }

        Ok(())
    }
}

/// API v12+
#[derive(Debug)]
pub struct SnapshotId {
    /// API v0+
    pub end_offset: i64,
    /// API v0+
    pub epoch: i32,
    /// The tagged fields. (API v12+)
    pub tagged_fields: TagBuffer,
}

impl Default for SnapshotId {
    #[inline]
    fn default() -> Self {
        Self {
            end_offset: -1,
            epoch: -1,
            tagged_fields: Default::default(),
        }
    }
}

impl Serialize for SnapshotId {
    /// Static size
    ///
    /// `SIZE = size(end_offset) + size(epoch)`
    const SIZE: usize = 8 + 4;

    fn encode_size(&self, version: i16) -> usize {
        let mut size = Self::SIZE;

        if version >= 12 {
            size += self.tagged_fields.encode_size(version);
        }

        size
    }
}

impl AsyncSerialize for SnapshotId {
    async fn write_into<W>(self, writer: &mut W, version: i16) -> Result<()>
    where
        W: AsyncWriteExt + Send + Unpin,
    {
        writer
            .write_i64(self.end_offset)
            .await
            .context("end offset")?;

        writer.write_i32(self.epoch).await.context("epoch")?;

        if version >= 12 {
            self.tagged_fields
                .write_into(writer, version)
                .await
                .context("tagged fields")?;
        }

        Ok(())
    }
}

#[derive(Debug)]
pub struct AbortedTransaction {
    /// The producer id associated with the aborted transaction. (API v4+)
    pub producer_id: i64,
    /// The first offset in the aborted transaction. (API v4+)
    pub first_offset: i64,
    /// The tagged fields. (API v12+)
    pub tagged_fields: TagBuffer,
}

impl Serialize for AbortedTransaction {
    fn encode_size(&self, version: i16) -> usize {
        let mut size = Self::SIZE;

        if version >= 4 {
            // producer_id + first_offset
            size += 8 + 8;
        }

        if version >= 12 {
            size += self.tagged_fields.encode_size(version);
        }

        size
    }
}

impl AsyncSerialize for AbortedTransaction {
    async fn write_into<W>(self, writer: &mut W, version: i16) -> Result<()>
    where
        W: AsyncWriteExt + Send + Unpin,
    {
        if version >= 4 {
            writer
                .write_i64(self.producer_id)
                .await
                .context("producer id")?;

            writer
                .write_i64(self.first_offset)
                .await
                .context("first offset")?;
        }

        if version >= 12 {
            self.tagged_fields
                .write_into(writer, version)
                .await
                .context("tagged fields")?;
        }

        Ok(())
    }
}

/// API v16+
#[allow(dead_code)]
#[derive(Debug)]
pub struct NodeEndpoint {
    // XXX: BrokerId
    /// The ID of the associated node.
    pub node_id: i64,
    /// The node's hostname.
    pub host: Str,
    /// The node's port.
    pub port: i32,
    /// The rack of the node, or null if it has not been assigned to a rack. (nullable v16+)
    pub rack: Option<Str>,
    /// Other tagged fields. (API v12+)
    pub tagged_fields: TagBuffer,
}
