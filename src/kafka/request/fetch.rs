use anyhow::{bail, ensure, Context as _, Result};
use bytes::Buf;

use crate::kafka::types::{Array, CompactArray, CompactStr, Str, StrBytes, TagBuffer, Uuid};
use crate::kafka::Deserialize;

/// # Fetch Request
///
/// [Request schema][schema]
///
/// Version 1 is the same as version 0.
///
/// Starting in Version 2, the requester must be able to handle Kafka Log
/// Message format version 1.
///
/// Version 3 adds MaxBytes.  Starting in version 3, the partition ordering in
/// the request is now relevant.  Partitions will be processed in the order
/// they appear in the request.
///
/// Version 4 adds IsolationLevel.  Starting in version 4, the reqestor must be
/// able to handle Kafka log message format version 2.
///
/// Version 5 adds LogStartOffset to indicate the earliest available offset of
/// partition data that can be consumed.
///
/// Version 6 is the same as version 5.
///
/// Version 7 adds incremental fetch request support.
///
/// Version 8 is the same as version 7.
///
/// Version 9 adds CurrentLeaderEpoch, as described in KIP-320.
///
/// Version 10 indicates that we can use the ZStd compression algorithm, as
/// described in KIP-110.
/// Version 12 adds flexible versions support as well as epoch validation through
/// the `LastFetchedEpoch` field
///
/// Version 13 replaces topic names with topic IDs (KIP-516). May return UNKNOWN_TOPIC_ID error
/// code.
///
/// Version 14 is the same as version 13 but it also receives a new error called
/// OffsetMovedToTieredStorageException(KIP-405)
///
/// Version 15 adds the ReplicaState which includes new field ReplicaEpoch and the ReplicaId. Also,
/// deprecate the old ReplicaId field and set its default value to -1. (KIP-903)
///
/// Version 16 is the same as version 15 (KIP-951).
///
/// Version 17 adds directory id support from KIP-853
///
/// [schema]: https://github.com/apache/kafka/blob/trunk/clients/src/main/resources/common/message/FetchRequest.json
#[allow(dead_code)]
#[derive(Debug)]
pub struct Fetch {
    /// The clusterId if known. This is used to validate metadata fetches prior to broker
    /// registration.
    ///
    /// API v12+ (tagged, nullable v12+, default: `None`)
    pub cluster_id: Option<StrBytes>,

    /// The broker ID of the follower, of -1 if this request is from a consumer.
    ///
    /// API v0-14 (default: -1)
    pub replica_id: i32,

    /// API v15+ (tagged v15+)
    pub replica_state: ReplicaState,

    /// The maximum time in milliseconds to wait for the response. (API v0+)
    pub max_wait_ms: i32,

    /// The minimum bytes to accumulate in the response. (API v0+)
    pub min_bytes: i32,

    /// The maximum bytes to fetch. (API v3+, default: `0x7fffffff`)
    ///
    /// See KIP-74 for cases where this limit may not be honored.
    pub max_bytes: i32,

    /// This setting controls the visibility of transactional records.
    ///
    /// Using READ_UNCOMMITTED (isolation_level = 0) makes all records visible.
    ///
    /// With READ_COMMITTED (isolation_level = 1), non-transactional and COMMITTED transactional
    /// records are visible. To be more concrete, READ_COMMITTED returns all data from offsets
    /// smaller than the current LSO (last stable offset), and enables the inclusion of the list of
    /// aborted transactions in the result, which allows consumers to discard ABORTED transactional
    /// records.
    ///
    /// (API v4+, default: 0)
    pub isolation_level: i8,

    /// The fetch session ID. (API v7+, default: 0)
    pub session_id: i32,

    /// The fetch session epoch, which is used for ordering requests in a session.
    ///
    /// API v7+ (default: -1)
    pub session_epoch: i32,

    /// The topics to fetch. (API v0+)
    ///
    /// ### Encoding
    ///  - v0-11: [`Array`]
    ///  - v12+: [`CompactArray`]
    pub topics: Vec<FetchTopic>,

    /// In an incremental fetch request, the partitions to remove. (API v7+)
    ///
    /// ### Encoding
    ///  - v7-11: [`Array`]
    ///  - v12+: [`CompactArray`]
    pub forgotten_topics_data: Vec<ForgottenTopic>,

    /// Rack ID of the consumer making this request. (API v11+)
    ///
    /// ### Encoding
    ///  - v11: [`Str`]
    ///  - v12+: [`CompactStr`]
    pub rack_id: StrBytes,

    /// Other tagged fields (API v12+)
    pub tagged_fields: TagBuffer,
}

impl Deserialize for Fetch {
    fn read_from<B: Buf>(buf: &mut B, version: i16) -> Result<(Self, usize)> {
        let mut body_bytes = 0;

        let replica_id = match version {
            0..=14 => {
                ensure!(buf.remaining() >= 4, "not enough bytes left: replica id");
                body_bytes += 4;
                buf.get_i32()
            }
            _ => -1,
        };

        ensure!(buf.remaining() >= 4, "not enough bytes left: max wait ms");
        let max_wait_ms = buf.get_i32();
        body_bytes += 4;

        ensure!(buf.remaining() >= 4, "not enough bytes left: min bytes");
        let min_bytes = buf.get_i32();
        body_bytes += 4;

        let max_bytes = if version >= 3 {
            ensure!(buf.remaining() >= 4, "not enough bytes left: max bytes");
            body_bytes += 4;
            buf.get_i32()
        } else {
            0x7fffffff
        };

        let isolation_level = if version >= 4 {
            ensure!(
                buf.has_remaining(),
                "not enough bytes left: isolation level"
            );
            body_bytes += 1;
            buf.get_i8()
        } else {
            0
        };

        let session_id = if version >= 7 {
            ensure!(buf.remaining() >= 4, "not enough bytes left: session id");
            body_bytes += 4;
            buf.get_i32()
        } else {
            0
        };

        let session_epoch = if version >= 7 {
            ensure!(buf.remaining() >= 4, "not enough bytes left: session epoch");
            body_bytes += 4;
            buf.get_i32()
        } else {
            -1
        };

        // TODO: make a macro or a trait for this
        let topics = match version {
            0..=11 => {
                let (Array(items), n) = Deserialize::read_from(buf, version).context("topics")?;
                body_bytes += n;
                items
            }
            12.. => {
                let (CompactArray(items), n) =
                    Deserialize::read_from(buf, version).context("topics")?;
                body_bytes += n;
                items
            }
            v => bail!("invalid API version: v{v}"),
        };

        let forgotten_topics_data = match version {
            7..=11 => {
                let (Array(items), n) =
                    Deserialize::read_from(buf, version).context("forgotten topics data")?;
                body_bytes += n;
                items
            }

            12.. => {
                let (CompactArray(items), n) =
                    Deserialize::read_from(buf, version).context("forgotten topics data")?;
                body_bytes += n;
                items
            }

            _ => Default::default(),
        };

        let rack_id = match version {
            11 => {
                let (id, n) = Option::<Str>::read_from(buf, version).context("rack id")?;
                body_bytes += n;
                id.into()
            }
            12.. => {
                let (id, n) = CompactStr::read_from(buf, version).context("rack id")?;
                body_bytes += n;
                id.into()
            }
            v => bail!("reading rack id is supported from v11+, got v{v}"),
        };

        // tagged fields
        let cluster_id = Default::default();
        let replica_state = ReplicaState::default();
        let mut tagged_fields = TagBuffer::default();

        if version >= 12 {
            if version >= 15 {
                // TODO: overwrite replica_state from tagged fields (tag=1)
            }

            let (tag_buf, n) = TagBuffer::read_from(buf, version)?;
            body_bytes += n;

            tagged_fields = tag_buf;
        };

        ensure!(
            !buf.has_remaining(),
            "Fetch: buffer contains leftover bytes"
        );

        let body = Self {
            cluster_id,
            replica_id,
            replica_state,
            max_wait_ms,
            min_bytes,
            max_bytes,
            isolation_level,
            session_id,
            session_epoch,
            topics,
            forgotten_topics_data,
            rack_id,
            tagged_fields,
        };

        Ok((body, body_bytes))
    }
}

/// API v15+
#[allow(dead_code)]
#[derive(Debug)]
pub struct ReplicaState {
    /// The replica ID of the follower, or -1 if this request is from a consumer.
    pub replica_id: i32,
    /// The epoch of this follower, or -1 if not available.
    pub repolica_epoch: i64,
}

impl Default for ReplicaState {
    #[inline]
    fn default() -> Self {
        Self {
            replica_id: -1,
            repolica_epoch: -1,
        }
    }
}

impl Deserialize for ReplicaState {
    fn read_from<B: Buf>(buf: &mut B, version: i16) -> Result<(Self, usize)> {
        if version < 15 {
            return Ok((Self::default(), 0));
        }

        let byte_size = 12;

        ensure!(buf.remaining() >= byte_size, "not enough bytes left");

        let state = Self {
            replica_id: buf.get_i32(),
            repolica_epoch: buf.get_i64(),
        };

        Ok((state, byte_size))
    }
}

#[derive(Debug)]
pub struct FetchTopic {
    /// The name of the topic to fetch. (API v0-12)
    ///
    /// ### Encoding
    ///  - v0-11: [`Str`]
    ///  - v12+: [`CompactStr`]
    pub topic: StrBytes,

    /// The unique topic ID (API v13+)
    pub topic_id: Uuid,

    /// The partitions to fetch (API v0+)
    #[allow(dead_code)]
    pub partitions: Vec<FetchPartition>,

    /// Other tagged fields (API v12+)
    #[allow(dead_code)]
    pub tagged_fields: TagBuffer,
}

impl Deserialize for FetchTopic {
    fn read_from<B: Buf>(buf: &mut B, version: i16) -> Result<(Self, usize)> {
        let mut size = 0;

        let topic = match version {
            0..=11 => {
                let (topic, n) = Str::read_from(buf, version).context("partitions")?;
                size += n;
                topic.into()
            }
            12 => {
                let (topic, n) = CompactStr::read_from(buf, version).context("partitions")?;
                size += n;
                topic.into()
            }
            _ => Default::default(),
        };

        let topic_id = if version >= 13 {
            let (id, n) = Uuid::read_from(buf, version).context("topic id")?;
            size += n;
            id
        } else {
            Default::default()
        };

        let partitions = match version {
            0..=11 => {
                let (Array(items), n) =
                    Deserialize::read_from(buf, version).context("partitions")?;
                size += n;
                items
            }
            12.. => {
                let (CompactArray(items), n) =
                    Deserialize::read_from(buf, version).context("partitions")?;
                size += n;
                items
            }
            // XXX: UNSUPPORTED_VERSION
            v => bail!("invalid API version: v{v}"),
        };

        let tagged_fields = if version >= 12 {
            let (tag_buf, n) = TagBuffer::read_from(buf, version)?;
            size += n;
            tag_buf
        } else {
            Default::default()
        };

        let this = Self {
            topic,
            topic_id,
            partitions,
            tagged_fields,
        };

        Ok((this, size))
    }
}

#[allow(dead_code)]
#[derive(Debug)]
pub struct FetchPartition {
    /// The partition index. (API v0+)
    pub partition: i32,

    /// The current leader epoch of the partition. (API v9+, default: -1)
    pub current_leader_epoch: i32,

    /// The message offset. (API v0+)
    pub fetch_offset: i64,

    /// The epoch of the last fetched record or -1 if there is none. (API v12+)
    pub last_fetched_epoch: i32,

    /// The earliest available offset of the follower replica. (API v5+, default: -1)
    ///
    /// The field is only used when the request is sent by the follower.
    pub log_start_offset: i64,

    /// The maximum bytes to fetch from this partition. (API v0+)
    ///
    /// See KIP-74 for cases where this limit may not be honored.
    pub partition_max_bytes: i32,

    /// Other tagged fields (API v12+)
    pub tagged_fields: TagBuffer,
}

impl Deserialize for FetchPartition {
    fn read_from<B: Buf>(buf: &mut B, version: i16) -> Result<(Self, usize)> {
        let mut size = 0;

        ensure!(buf.remaining() >= 4, "not enough bytes left");
        let partition = buf.get_i32();
        size += 4;

        let current_leader_epoch = if version >= 9 {
            ensure!(buf.remaining() >= 4, "not enough bytes left");
            size += 4;
            buf.get_i32()
        } else {
            -1
        };

        ensure!(buf.remaining() >= 8, "not enough bytes left");
        let fetch_offset = buf.get_i64();
        size += 8;

        let last_fetched_epoch = if version >= 12 {
            ensure!(buf.remaining() >= 4, "not enough bytes left");
            size += 4;
            buf.get_i32()
        } else {
            -1
        };

        let log_start_offset = if version >= 12 {
            ensure!(buf.remaining() >= 8, "not enough bytes left");
            size += 8;
            buf.get_i64()
        } else {
            -1
        };

        ensure!(buf.remaining() >= 4, "not enough bytes left");
        let partition_max_bytes = buf.get_i32();
        size += 4;

        let tagged_fields = if version >= 12 {
            let (tag_buf, n) = TagBuffer::read_from(buf, version)?;
            size += n;
            tag_buf
        } else {
            Default::default()
        };

        let this = Self {
            partition,
            current_leader_epoch,
            fetch_offset,
            last_fetched_epoch,
            log_start_offset,
            partition_max_bytes,
            tagged_fields,
        };

        Ok((this, size))
    }
}

#[allow(dead_code)]
#[derive(Debug, Default)]
pub struct ForgottenTopic {
    /// The topic name. (API v7-12)
    ///
    /// ### Encoding
    ///  - v7-11: [`Str`]
    ///  - v12: [`CompactStr`]
    pub topic: StrBytes,

    /// The unique topic ID (API v13+)
    pub topic_id: Uuid,

    /// The partitions indexes to forget. (API v7+)
    ///
    /// ### Encoding
    ///  - v7-11: [`Array`]
    ///  - v12+: [`CompactArray`]
    pub partitions: Vec<i32>,

    /// Other tagged fields (API v12+)
    pub tagged_fields: TagBuffer,
}

impl Deserialize for ForgottenTopic {
    fn read_from<B: Buf>(buf: &mut B, version: i16) -> Result<(Self, usize)> {
        let mut byte_size = 0;

        let topic = match version {
            7..=11 => {
                let (topic, n) = Option::<Str>::read_from(buf, version).context("topic")?;
                byte_size += n;
                topic.into()
            }
            12.. => {
                let (topic, n) = CompactStr::read_from(buf, version).context("topic")?;
                byte_size += n;
                topic.into()
            }
            _ => Default::default(),
        };

        let topic_id = if version >= 13 {
            let (id, n) = Uuid::read_from(buf, version).context("topic id")?;
            byte_size += n;
            id
        } else {
            Default::default()
        };

        let partitions = match version {
            7..=11 => {
                let (Array(ps), n) =
                    Array::<Vec<i32>>::read_from(buf, version).context("partitions")?;
                byte_size += n;
                ps
            }
            12.. => {
                let (CompactArray(ps), n) =
                    CompactArray::<Vec<i32>>::read_from(buf, version).context("partitions")?;
                byte_size += n;
                ps
            }
            _ => Default::default(),
        };

        let tagged_fields = if version >= 12 {
            let (tag_buf, n) = TagBuffer::read_from(buf, version)?;
            byte_size += n;

            tag_buf
        } else {
            TagBuffer::default()
        };

        let this = Self {
            topic,
            topic_id,
            partitions,
            tagged_fields,
        };

        Ok((this, byte_size))
    }
}
