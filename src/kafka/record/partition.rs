use anyhow::{ensure, Context as _, Result};
use bytes::Buf;

use crate::kafka::types::{CompactArray, TagBuffer, Uuid};
use crate::kafka::Deserialize;

/// # PartitionRecord
///
/// [Record schema][schema]
///
/// Version 1 adds Directories for KIP-858
/// Version 2 implements Eligible Leader Replicas and LastKnownElr as described in KIP-966.
///
/// [schema]: https://github.com/apache/kafka/blob/5b3027dfcbcb62d169d4b4421260226e620459af/metadata/src/main/resources/common/metadata/PartitionRecord.json
#[derive(Debug, PartialEq)]
pub struct PartitionRecord {
    pub frame_version: i8,
    pub record_type: i8,
    pub version: i8,
    /// The partition id. (API v0+, default: -1)
    pub partition_id: i32,
    /// The unique ID of this topic. (API v0+)
    pub topic_id: Uuid,
    /// The replicas of this partition, sorted by preferred order. (API v0+)
    pub replicas: Vec<i32>,
    /// The in-sync replicas of this partition. (API v0+)
    pub isr: Vec<i32>,
    /// The replicas that we are in the process of removing. (API v0+)
    pub removing_replicas: Vec<i32>,
    /// The replicas that we are in the process of adding. (API v0+)
    pub adding_replicas: Vec<i32>,
    /// The lead replica, or -1 if there is no leader. (API v0+, default: -1)
    pub leader_id: i32,
    /// The epoch of the partition leader. (API v0+, default: -1)
    pub leader_epoch: i32,
    /// An epoch that gets incremented each time we change anything in the partition.
    /// (API v0+, default: -1)
    pub partition_epoch: i32,
    /// The log directory hosting each replica, sorted in the same exact order as the Replicas
    /// field. (API v1+)
    pub directories: Vec<Uuid>,
    /// Other tagged fields
    pub tagged_fields: TagBuffer,
}

impl Deserialize for PartitionRecord {
    fn decode<B: Buf>(buf: &mut B, version: i16) -> Result<(Self, usize)> {
        let (frame_version, mut size) = i8::decode(buf, version).context("frame_version")?;

        let (record_type, n) = i8::decode(buf, version).context("type")?;
        size += n;

        ensure!(record_type == 3, "invalid record type");

        let (ver, n) = i8::decode(buf, version).context("version")?;
        size += n;

        let (partition_id, n) = i32::decode(buf, version).context("partition_id")?;
        size += n;

        let (topic_id, n) = Uuid::decode(buf, version).context("topic_id")?;
        size += n;

        let (CompactArray(replicas), n) = Deserialize::decode(buf, version).context("replicas")?;
        size += n;

        let (CompactArray(isr), n) = Deserialize::decode(buf, version).context("isr")?;
        size += n;

        let (CompactArray(removing_replicas), n) =
            Deserialize::decode(buf, version).context("removing_replicas")?;
        size += n;

        let (CompactArray(adding_replicas), n) =
            Deserialize::decode(buf, version).context("adding_replicas")?;
        size += n;

        let (leader_id, n) = i32::decode(buf, version).context("leader_id")?;
        size += n;

        let (leader_epoch, n) = i32::decode(buf, version).context("leader_epoch")?;
        size += n;

        let (partition_epoch, n) = i32::decode(buf, version).context("partition_epoch")?;
        size += n;

        let (CompactArray(directories), n) =
            Deserialize::decode(buf, version).context("directories")?;
        size += n;

        let (tagged_fields, n) = TagBuffer::decode(buf, version).context("tagged_fields")?;
        size += n;

        let record = Self {
            frame_version,
            record_type,
            version: ver,
            partition_id,
            topic_id,
            replicas,
            isr,
            removing_replicas,
            adding_replicas,
            leader_id,
            leader_epoch,
            partition_epoch,
            directories,
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
        let mut buf = Vec::with_capacity(65);

        buf.extend_from_slice(b"\x01"); // frame version
        buf.extend_from_slice(b"\x03"); // type
        buf.extend_from_slice(b"\x01"); // version
        buf.extend_from_slice(b"\x00\x00\x00\x01"); // partition_id
                                                    // topic_id (0x00000000-0000-4000-8000-000000000091)
        buf.extend_from_slice(b"\x00\x00\x00\x00\x00\x00\x40\x00\x80\x00\x00\x00\x00\x00\x00\x91");

        buf.extend_from_slice(b"\x02"); // length of replica array
        buf.extend_from_slice(b"\x00\x00\x00\x01"); // replica array

        buf.extend_from_slice(b"\x02"); // length of ISR array
        buf.extend_from_slice(b"\x00\x00\x00\x01"); // ISR array

        buf.extend_from_slice(b"\x01"); // length of removing replicas array
        buf.extend_from_slice(b"\x01"); // length of adding replicas array

        buf.extend_from_slice(b"\x00\x00\x00\x01"); // leader_id
        buf.extend_from_slice(b"\x00\x00\x00\x00"); // leader_epoch
        buf.extend_from_slice(b"\x00\x00\x00\x00"); // partition_epoch

        buf.extend_from_slice(b"\x02"); // length of directories array
                                        // directory UUID (0x10000000-0000-4000-8000-000000000001)
        buf.extend_from_slice(b"\x10\x00\x00\x00\x00\x00\x40\x00\x80\x00\x00\x00\x00\x00\x00\x01");

        buf.extend_from_slice(b"\x00"); // tagged_fields

        let topic_id =
            Uuid::from_static(b"\x00\x00\x00\x00\x00\x00\x40\x00\x80\x00\x00\x00\x00\x00\x00\x91");

        let directory =
            Uuid::from_static(b"\x10\x00\x00\x00\x00\x00\x40\x00\x80\x00\x00\x00\x00\x00\x00\x01");

        let expected = PartitionRecord {
            frame_version: 1,
            record_type: 3,
            version: 1,
            partition_id: 1,
            topic_id,
            replicas: vec![1],
            isr: vec![1],
            removing_replicas: vec![],
            adding_replicas: vec![],
            leader_id: 1,
            leader_epoch: 0,
            partition_epoch: 0,
            directories: vec![directory],
            tagged_fields: TagBuffer::default(),
        };

        let mut buf = std::io::Cursor::new(buf);

        let (actual, size) =
            PartitionRecord::deserialize(&mut buf).expect("valid partition record");

        assert_eq!(size, 65);
        assert_eq!(expected, actual);
    }
}
