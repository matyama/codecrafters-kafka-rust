use anyhow::{Context as _, Result};
use bytes::Bytes;
use tokio::io::AsyncWriteExt;

use crate::kafka::api::ApiKey;
use crate::kafka::error::ErrorCode;
use crate::kafka::types::{
    Array, CompactArray, CompactBytes, CompactStr, Str, StrBytes, TagBuffer, Uuid,
};
use crate::kafka::{HeaderVersion, Serialize, WireSize};

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

/// # ApiVersions Response
///
/// [Response schema][schema]
///
/// Version 1 adds throttle time to the response.
///
/// Starting in version 2, on quota violation, brokers send out responses before throttling.
///
/// Version 3 is the first flexible version. Tagged fields are only supported in the body but
/// not in the header. The length of the header must not change in order to guarantee the
/// backward compatibility.
///
/// Starting from Apache Kafka 2.4 (KIP-511), ApiKeys field is populated with the supported
/// versions of the ApiVersionsRequest when an UNSUPPORTED_VERSION error is returned.
///
/// Version 4 fixes KAFKA-17011, which blocked SupportedFeatures.MinVersion from being 0.
///
/// [schema]: https://github.com/apache/kafka/blob/trunk/clients/src/main/resources/common/message/ApiVersionsResponse.json
#[derive(Debug)]
pub struct ApiVersions {
    /// The top-level error code. (API v0+)
    pub error_code: ErrorCode,

    /// The APIs supported by the broker. (API v0+)
    ///
    /// Represented as a COMPACT_ARRAY for API v3+, otherwise as an ARRAY.
    pub api_keys: Vec<ApiVersion>,

    /// The duration in milliseconds for which the request was throttled due to a quota violation,
    /// or zero if the request did not violate any quota. (API v1+)
    pub throttle_time_ms: i32,

    // XXX: This could be something like IndexMap<Str, SupportedFeatureKey> indexed by name
    ///// Features supported by the broker. (API v3+)
    /////
    ///// Note: in v0-v3, features with MinSupportedVersion = 0 are omitted.
    //pub supported_features: Vec<SupportedFeatureKey>,
    //
    ///// The monotonically increasing epoch for the finalized features information. (API v3+)
    /////
    ///// Valid values are >= 0. A value of -1 is special and represents unknown epoch.
    //pub finalized_features_epoch: i64,

    // XXX: This could be something like IndexMap<Str, FinalizedFeatureKey> indexed by name
    ///// List of cluster-wide finalized features. (API v3+)
    /////
    ///// The information is valid only if FinalizedFeaturesEpoch >= 0.
    //pub finalized_features: Vec<FinalizedFeatureKey>,
    //
    ///// Set by a KRaft controller if the required configurations for ZK migration are present.
    ///// (API v3+)
    //pub zk_migration_ready: bool,
    /// The tagged fields (API v3+)
    pub tagged_fields: TagBuffer,
}

impl Default for ApiVersions {
    #[inline]
    fn default() -> Self {
        Self {
            error_code: Default::default(),
            api_keys: Default::default(),
            throttle_time_ms: 0,
            //finalized_features_epoch: -1,
            //zk_migration_ready: false,
            tagged_fields: Default::default(),
        }
    }
}

impl WireSize for ApiVersions {
    const SIZE: usize = ErrorCode::SIZE;

    #[inline]
    fn size(&self, version: i16) -> usize {
        match version {
            0 => Self::SIZE + Array(self.api_keys.as_slice()).size(version),
            1 | 2 => Self::SIZE + Array(self.api_keys.as_slice()).size(version) + 4,
            _ => {
                let api_keys = CompactArray(self.api_keys.as_slice());
                Self::SIZE + api_keys.size(version) + 4 + self.tagged_fields.size(version)
            }
        }
    }
}

//impl Flexible for ApiVersions {
//    fn num_tagged_fields(&self, version: i16) -> usize {
//        //let mut n = self.tagged_fields.len();
//        let mut n = 0;
//
//        if version >= 3 {
//            //if !self.supported_features.is_empty() {
//            //    n += 1;
//            //}
//
//            if self.finalized_features_epoch != -1 {
//                n += 1;
//            }
//
//            //if !self.finalized_features.is_empty() {
//            //    n += 1;
//            //}
//
//            if self.zk_migration_ready {
//                n += 1;
//            }
//        }
//
//        n
//    }
//}

impl Serialize for ApiVersions {
    async fn write_into<W>(self, writer: &mut W, version: i16) -> Result<()>
    where
        W: AsyncWriteExt + Send + Unpin,
    {
        writer
            .write_i16(self.error_code as i16)
            .await
            .context("error code")?;

        if version < 3 {
            Array(self.api_keys)
                .write_into(writer, version)
                .await
                .context("API keys")?;
        } else {
            CompactArray(self.api_keys)
                .write_into(writer, version)
                .await
                .context("API keys")?;
        }

        if version >= 1 {
            writer
                .write_i32(self.throttle_time_ms)
                .await
                .context("throttle time ms")?;
        }

        if version >= 3 {
            // TODO
            //let num_tagged_fields = self.num_tagged_fields(version);
            // types::UnsignedVarInt.encode(buf, num_tagged_fields as u32)?;

            self.tagged_fields
                .write_into(writer, version)
                .await
                .context("tagged fields")?;
        }

        Ok(())
    }
}

#[derive(Debug)]
pub struct ApiVersion {
    /// The API index. (API v0+)
    pub api_key: ApiKey,
    /// The minimum supported version, inclusive. (API v0+)
    pub min_version: i16,
    /// The maximum supported version, inclusive. (API v0+)
    pub max_version: i16,
    /// The tagged fields. (API v3+)
    pub tagged_fields: TagBuffer,
}

impl ApiVersion {
    pub fn new(api_key: ApiKey) -> Option<Self> {
        let versions = api_key.api_versions()?;
        Some(Self {
            api_key,
            min_version: versions.start().into_inner(),
            max_version: versions.end().into_inner(),
            tagged_fields: TagBuffer::default(),
        })
    }
}

impl WireSize for ApiVersion {
    const SIZE: usize = ApiKey::SIZE + 2 + 2;

    #[inline]
    fn size(&self, version: i16) -> usize {
        Self::SIZE + self.tagged_fields.size(version)
    }
}

impl Serialize for ApiVersion {
    async fn write_into<W>(self, writer: &mut W, version: i16) -> Result<()>
    where
        W: AsyncWriteExt + Send + Unpin,
    {
        writer.write_i16(self.api_key as i16).await?;
        writer.write_i16(self.min_version).await?;
        writer.write_i16(self.max_version).await?;
        self.tagged_fields.write_into(writer, version).await?;
        Ok(())
    }
}

//#[derive(Debug)]
//pub struct SupportedFeatureKey {
//    /// The name of the feature. (API v3+)
//    pub name: Str,
//    /// The minimum supported version for the feature. (API v0+)
//    pub min_version: i16,
//    /// The maximum supported version for the feature. (API v0+)
//    pub max_version: i16,
//}
//
//impl WireSize for SupportedFeatureKey {
//    const SIZE: usize = 2 + 2;
//
//    #[inline]
//    fn size(&self, version: i16) -> usize {
//        if version < 3 {
//            return 0;
//        }
//        Self::SIZE + self.name.size(version)
//    }
//}
//
//impl Serialize for SupportedFeatureKey {
//    async fn write_into<W>(self, writer: &mut W, version: i16) -> Result<()>
//    where
//        W: AsyncWriteExt + Send + Unpin,
//    {
//        if version >= 3 {
//            self.name
//                .write_into(writer, version)
//                .await
//                .context("supported feature name")?;
//
//            writer
//                .write_i16(self.min_version)
//                .await
//                .context("supported feature min version")?;
//
//            writer
//                .write_i16(self.max_version)
//                .await
//                .context("supported feature max version")?;
//        }
//
//        Ok(())
//    }
//}
//
//#[derive(Debug)]
//pub struct FinalizedFeatureKey {
//    /// The name of the feature. (API v3+)
//    pub name: Str,
//    /// The cluster-wide finalized min version level for the feature. (API v0+)
//    pub max_version: i16,
//    /// The cluster-wide finalized max version level for the feature. (API v0+)
//    pub min_version: i16,
//}
//
//impl WireSize for FinalizedFeatureKey {
//    const SIZE: usize = 2 + 2;
//
//    #[inline]
//    fn size(&self, version: i16) -> usize {
//        if version < 3 {
//            return 0;
//        }
//        Self::SIZE + self.name.size(version)
//    }
//}
//
//impl Serialize for FinalizedFeatureKey {
//    async fn write_into<W>(self, writer: &mut W, version: i16) -> Result<()>
//    where
//        W: AsyncWriteExt + Send + Unpin,
//    {
//        if version >= 3 {
//            self.name
//                .write_into(writer, version)
//                .await
//                .context("finalized feature name")?;
//
//            writer
//                .write_i16(self.max_version)
//                .await
//                .context("finalized feature max version")?;
//
//            writer
//                .write_i16(self.min_version)
//                .await
//                .context("finalized feature min version")?;
//        }
//
//        Ok(())
//    }
//}

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

impl WireSize for Fetch {
    #[inline]
    fn size(&self, version: i16) -> usize {
        let mut size = 0;

        if version >= 1 {
            // throttle_time_ms
            size += 4;
        }

        if version >= 7 {
            size += self.error_code.size(version);
            // session_id
            size += 4;
        }

        // responses
        size += if version >= 12 {
            CompactArray(self.responses.as_slice()).size(version)
        } else {
            Array(self.responses.as_slice()).size(version)
        };

        if version >= 12 {
            if version >= 16 {
                // TODO: node_endpoints
            }

            size += self.tagged_fields.size(version)
        }

        size
    }
}

impl Serialize for Fetch {
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

impl WireSize for FetchableTopicResponse {
    fn size(&self, version: i16) -> usize {
        let mut size = 0;

        size += match version {
            0..=11 => Str::from(&self.topic).size(version),
            12 => CompactStr::from(&self.topic).size(version),
            _ => 0,
        };

        if version >= 13 {
            size += self.topic_id.size(version);
        }

        size += if version >= 12 {
            CompactArray(self.partitions.as_slice()).size(version)
        } else {
            Array(self.partitions.as_slice()).size(version)
        };

        if version >= 12 {
            size += self.tagged_fields.size(version);
        }

        size
    }
}

impl Serialize for FetchableTopicResponse {
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
            _ => {}
        }

        if version >= 13 {
            self.topic_id
                .write_into(writer, version)
                .await
                .context("topic id")?;
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
    #[allow(dead_code)]
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
}

impl WireSize for PartitionData {
    fn size(&self, version: i16) -> usize {
        // partition_index + error_code + high_watermark
        let mut size = 4 + self.error_code.size(version) + 8;

        if version >= 4 {
            // last_stable_offset
            size += 8;
        }

        if version >= 5 {
            // last_start_offset
            size += 8
        }

        size += match version {
            4..=11 => Array(&self.aborted_transactions).size(version),
            12.. => CompactArray(&self.aborted_transactions).size(version),
            _ => 0,
        };

        if version >= 11 {
            // preferred_read_replica
            size += 4;
        }

        size += if version >= 12 {
            self.records.size(version)
        } else {
            // NOTE: does not clone the raw bytes, just increments the ref count
            self.records.clone().map(CompactBytes).size(version)
        };

        if version >= 12 {
            // TODO tagged fields specific to this API key (response)
            size += self.tagged_fields.size(version);
        }

        size
    }
}

impl Serialize for PartitionData {
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
                .write_into(writer, version)
                .await
                .context("records")?;
        } else {
            self.records
                .map(CompactBytes)
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

impl WireSize for EpochEndOffset {
    /// Static size (note: only valid for v12+)
    ///
    /// `SIZE = size(epoch) + size(end_offset)`
    const SIZE: usize = 4 + 8;

    #[inline]
    fn size(&self, version: i16) -> usize {
        if version >= 12 {
            Self::SIZE + self.tagged_fields.size(version)
        } else {
            0
        }
    }
}

impl Serialize for EpochEndOffset {
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

impl WireSize for LeaderIdAndEpoch {
    /// Static size (note: only valid for v12+)
    ///
    /// `SIZE = size(leader_id) + size(leader_epoch)`
    const SIZE: usize = 4 + 4;

    fn size(&self, version: i16) -> usize {
        if version >= 12 {
            Self::SIZE + self.tagged_fields.size(version)
        } else {
            0
        }
    }
}

impl Serialize for LeaderIdAndEpoch {
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

impl WireSize for SnapshotId {
    /// Static size
    ///
    /// `SIZE = size(end_offset) + size(epoch)`
    const SIZE: usize = 8 + 4;

    fn size(&self, version: i16) -> usize {
        let mut size = Self::SIZE;

        if version >= 12 {
            size += self.tagged_fields.size(version);
        }

        size
    }
}

impl Serialize for SnapshotId {
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

impl WireSize for AbortedTransaction {
    fn size(&self, version: i16) -> usize {
        let mut size = Self::SIZE;

        if version >= 4 {
            // producer_id + first_offset
            size += 8 + 8;
        }

        if version >= 12 {
            size += self.tagged_fields.size(version);
        }

        size
    }
}

impl Serialize for AbortedTransaction {
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
