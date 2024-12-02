use anyhow::{bail, ensure, Context as _, Result};
use bytes::{Buf, Bytes};

use crate::kafka::types::{Array, CompactStr, StrBytes, TagBuffer, Uuid, VarInt, VarLong};
use crate::kafka::Deserialize;

//#[derive(Debug)]
//pub struct EncodeOptions {
//    pub version: i8,
//    pub compression: Compression,
//}
//
//impl Default for EncodeOptions {
//    #[inline]
//    fn default() -> Self {
//        Self {
//            version: 2,
//            compression: Compression::default(),
//        }
//    }
//}
//
//pub fn encode<'a, B, R>(buf: &mut B, records: R, ops: EncodeOptions) -> Result<usize>
//where
//    B: BufMut,
//    R: IntoIterator<Item = &'a Record>,
//{
//    match ops.version {
//        0..=1 => unimplemented!("legacy message set format is not supported"),
//        2 => {}
//        v => bail!("unsupported record batch format v{v}"),
//    }
//
//    let mut records = records.into_iter().peekable();
//
//    ensure!(
//        records.peek().is_some(),
//        "record batch must contain at lease one record"
//    );
//
//    let batch_lenght = 0;
//
//    Ok(batch_lenght)
//}

pub fn decode<B: Buf + std::fmt::Debug>(buf: &mut B) -> Result<Vec<RecordBatch>> {
    let mut batches = Vec::new();

    while buf.has_remaining() {
        let batch = decode_batch(buf).context("decode record batch")?;
        batches.push(batch);
    }

    Ok(batches)
}

// TODO: async version
pub fn decode_batch<B: Buf + std::fmt::Debug>(buf: &mut B) -> Result<RecordBatch> {
    let (base_offset, _) = i64::deserialize(buf).context("base_offset")?;
    let (batch_length, _) = i32::deserialize(buf).context("batch_length")?;
    ensure!(batch_length > 0, "record batch length must be positive i32");

    let (partition_leader_epoch, _) = i32::deserialize(buf).context("partition_leader_epoch")?;

    let (version, _) = i8::deserialize(buf).context("magic")?;

    match version {
        // TODO: support reading record batch format v0 and v1
        0..=1 => unimplemented!("legacy record batch format is unsupported"),
        2 => {}
        version => bail!("unknown record batch version: {version}"),
    }

    let (crc, _) = u32::deserialize(buf).context("crc")?;
    // TODO: validate CRC from the record batch against the batch buffer
    //  - compute incrementally while reading the rest of the fields

    let (attributes, _) = i16::deserialize(buf).context("attributes")?;
    let attributes = RecordBatchAttrs::try_from(attributes).context("invalid batch attributes")?;

    let (last_offset_delta, _) = i32::deserialize(buf).context("last_offset_delta")?;

    let (base_timestamp, _) = i64::deserialize(buf).context("base_timestamp")?;
    let (max_timestamp, _) = i64::deserialize(buf).context("max_timestamp")?;

    let (producer_id, _) = i64::deserialize(buf).context("producer_id")?;
    let (producer_epoch, _) = i16::deserialize(buf).context("producer_epoch")?;

    let (base_sequence, _) = i32::deserialize(buf).context("base_sequence")?;

    // TODO: decompress the rest of the buffer (based on `attributes.compression`)
    debug_assert_eq!(
        attributes.compression,
        Compression::None,
        "compression is not yet supported"
    );

    let (Array(records), _) = Deserialize::deserialize(buf).context("records")?;

    Ok(RecordBatch {
        base_offset,
        batch_length,
        partition_leader_epoch,
        magic: version,
        crc,
        attributes,
        last_offset_delta,
        base_timestamp,
        max_timestamp,
        producer_id,
        producer_epoch,
        base_sequence,
        records,
    })
}

/// See official docs for [Record Batch](https://kafka.apache.org/documentation/#recordbatch).
#[derive(Debug, PartialEq)]
pub struct RecordBatch {
    pub base_offset: i64,
    pub batch_length: i32,
    pub partition_leader_epoch: i32,
    /// format version (current magic value is 2)
    pub magic: i8,
    pub crc: u32,

    pub attributes: RecordBatchAttrs,

    pub last_offset_delta: i32,
    pub base_timestamp: i64,
    pub max_timestamp: i64,
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub base_sequence: i32,

    pub records: Vec<Record>,
}

#[derive(Debug, PartialEq)]
pub struct RecordBatchAttrs {
    /// bit 0~2
    pub compression: Compression,

    /// bit 3
    pub timestamp_type: TimestampType,

    /// bit 4
    pub is_transactional: bool,

    /// bit 5
    pub is_control: bool,

    /// bit 6
    ///
    /// `false` means [`base_timestamp`](RecordBatch::base_timestamp) is not set as the delete
    /// horizon for compaction.
    pub has_delete_horizon_ms: bool,
    // bit 7~15: unused
}

impl TryFrom<i16> for RecordBatchAttrs {
    type Error = anyhow::Error;

    fn try_from(attributes: i16) -> Result<Self> {
        Ok(Self {
            compression: Compression::try_from(attributes).context("compression")?,
            timestamp_type: TimestampType::from(attributes),
            is_transactional: (attributes & (1 << 4)) != 0,
            is_control: (attributes & (1 << 5)) != 0,
            has_delete_horizon_ms: (attributes & (1 << 6)) != 0,
        })
    }
}

/// See official docs for [Record](https://kafka.apache.org/documentation/#record).
#[derive(Debug, PartialEq)]
pub struct Record {
    pub length: usize,
    /// bit 0~7: unused
    pub attributes: RecordAttrs,
    // varlong
    pub timestamp_delta: i64,
    // varint
    pub offset_delta: i32,
    // XXX: enum RecordKey { Key(Option<Bytes>), Control { version: i16, type_: i16 } }
    //  - https://kafka.apache.org/documentation/#controlbatch
    // length: varint
    pub key: Option<Bytes>,
    // length: varint
    pub value: Option<Bytes>,
    pub headers: Vec<Header>,
}

impl Deserialize for Record {
    fn read_from<B: Buf>(buf: &mut B, _version: i16) -> Result<(Self, usize)> {
        let mut byte_size = 0;

        let (length, n) = VarInt::deserialize(buf).context("record length")?;
        let length = usize::try_from(length).context("record length")?;
        byte_size += n;

        ensure!(
            buf.remaining() >= length,
            "buffer has not enough bytes left for a record"
        );

        let (attributes, n) = RecordAttrs::deserialize(buf).context("record attributes")?;
        byte_size += n;

        let (VarLong(timestamp_delta), n) = VarLong::deserialize(buf).context("timestamp_delta")?;
        byte_size += n;

        let (VarInt(offset_delta), n) = VarInt::deserialize(buf).context("offset_delta")?;
        byte_size += n;

        let (RecordBytes(key), n) = RecordBytes::deserialize(buf).context("key")?;
        byte_size += n;

        let (RecordBytes(value), n) = RecordBytes::deserialize(buf).context("value")?;
        byte_size += n;

        let (Headers(headers), n) = Headers::deserialize(buf).context("headers")?;
        byte_size += n;

        let record = Self {
            length,
            attributes,
            timestamp_delta,
            offset_delta,
            key,
            value,
            headers,
        };

        Ok((record, byte_size))
    }
}

#[derive(Debug, PartialEq)]
#[repr(transparent)]
pub struct RecordAttrs(pub i8);

impl Deserialize for RecordAttrs {
    fn read_from<B: Buf>(buf: &mut B, _version: i16) -> Result<(Self, usize)> {
        ensure!(buf.has_remaining(), "not enough bytes left");
        Ok((Self(buf.get_i8()), 1))
    }
}

#[derive(Clone, Debug, PartialEq)]
#[repr(transparent)]
pub struct RecordBytes(Option<Bytes>);

impl Deserialize for RecordBytes {
    fn read_from<B: Buf>(buf: &mut B, _version: i16) -> Result<(Self, usize)> {
        let (VarInt(length), size) = VarInt::deserialize(buf).context("length")?;

        let (bytes, size) = match length {
            -1 => (None, size),
            n if n < -1 => bail!("invalid bytes length: {n}"),
            n => {
                let n = n as usize;

                ensure!(
                    buf.remaining() >= n,
                    "expected at least {n} additional bytes"
                );

                (Some(buf.copy_to_bytes(n)), size + n)
            }
        };

        Ok((Self(bytes), size))
    }
}

#[derive(Debug, PartialEq)]
#[repr(transparent)]
struct Headers(Vec<Header>);

// NOTE: headers have VarInt array length, so cannot use `Array<Vec<Header>>` instance
impl Deserialize for Headers {
    fn read_from<B: Buf>(buf: &mut B, _version: i16) -> Result<(Self, usize)> {
        let (VarInt(n), mut byte_size) = VarInt::deserialize(buf).context("header count")?;

        let mut headers = Vec::with_capacity(n as usize);

        for i in 0..n {
            let (header, n) = Header::deserialize(buf).with_context(|| format!("header {i}"))?;
            headers.push(header);
            byte_size += n;
        }

        Ok((Self(headers), byte_size))
    }
}

/// See official docs for [Record Header](https://kafka.apache.org/documentation/#recordheader).
#[derive(Debug, PartialEq)]
pub struct Header {
    key: StrBytes,
    val: Option<Bytes>,
}

impl Deserialize for Header {
    fn read_from<B: Buf>(buf: &mut B, _version: i16) -> Result<(Self, usize)> {
        let (VarInt(n), mut byte_size) = VarInt::deserialize(buf).context("header key length")?;

        ensure!(n >= 0, "invalid header key length: {n}");
        let key = StrBytes::try_from(buf.copy_to_bytes(n as usize)).context("header key")?;
        byte_size += n as usize;

        let (RecordBytes(val), n) = RecordBytes::deserialize(buf).context("header value")?;
        byte_size += n;

        Ok((Self { key, val }, byte_size))
    }
}

#[derive(Debug, Default, PartialEq, Eq)]
pub enum Compression {
    #[default]
    None = 0,
    Gzip = 1,
    Snappy = 2,
    Lz4 = 3,
    Zstd = 4,
}

impl TryFrom<i16> for Compression {
    type Error = anyhow::Error;

    fn try_from(attributes: i16) -> Result<Self> {
        Ok(match attributes & 0x7 {
            0 => Self::None,
            1 => Self::Gzip,
            2 => Self::Snappy,
            3 => Self::Lz4,
            4 => Self::Zstd,
            other => bail!("unsupported compression: {other}"),
        })
    }
}

#[derive(Debug, PartialEq)]
pub enum TimestampType {
    Create = 0,
    LogAppend = 1,
}

impl From<i16> for TimestampType {
    #[inline]
    fn from(attributes: i16) -> Self {
        if (attributes & (1 << 3)) != 0 {
            Self::LogAppend
        } else {
            Self::Create
        }
    }
}

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

#[derive(Debug, PartialEq)]
pub struct TopicRecord {
    pub frame_version: i8,
    pub type_: i8,
    pub version: i8,
    pub topic_name: StrBytes,
    pub topic_id: Uuid,
    pub tagged_fields: TagBuffer,
}

impl Deserialize for TopicRecord {
    fn read_from<B: Buf>(buf: &mut B, version: i16) -> Result<(Self, usize)> {
        let (frame_version, mut size) = i8::read_from(buf, version).context("frame_version")?;

        let (type_, n) = i8::read_from(buf, version).context("type")?;
        size += n;

        let (ver, n) = i8::read_from(buf, version).context("version")?;
        size += n;

        let (topic_name, n) = CompactStr::read_from(buf, version).context("topic_name")?;
        size += n;

        let (topic_id, n) = Uuid::read_from(buf, version).context("topic_id")?;
        size += n;

        let (tagged_fields, n) = TagBuffer::read_from(buf, version).context("tagged_fields")?;
        size += n;

        let record = Self {
            frame_version,
            type_,
            version: ver,
            topic_name: topic_name.into(),
            topic_id,
            tagged_fields,
        };

        Ok((record, size))
    }
}

#[derive(Default, PartialEq)]
pub struct FeatureLevelRecord {
    pub frame_version: i8,
    pub type_: i8,
    pub version: i8,
    // CompactStr
    pub name: StrBytes,
    pub feature_level: i16,
    pub tagged_fields: TagBuffer,
}

#[derive(Default, PartialEq)]
pub struct PartitionRecord {
    pub frame_version: i8,
    pub type_: i8,
    pub version: i8,
    pub partition_id: i32,
    pub topic_id: Uuid,
    // CompactArray
    pub replicas: Vec<i32>,
    // CompactArray
    pub isr: Vec<i32>,
    // CompactArray
    pub removing_replicas: Vec<i32>,
    // CompactArray
    pub adding_replicas: Vec<i32>,
    pub leader_id: i32,
    pub leader_epoch: i32,
    pub partition_epoch: i32,
    // CompactArray
    pub directories: Vec<Uuid>,
    pub tagged_fields: TagBuffer,
}

//#[derive(Clone, Copy, Debug, PartialEq)]
//#[repr(i8)]
//pub enum RecordType {
//    Topic = 2,
//    Partition = 3,
//    FeatureLevel = 12,
//}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_record_batch() {
        let mut buf = Vec::new();

        // Batch #1
        buf.extend_from_slice(b"\x00\x00\x00\x00\x00\x00\x00\x00"); // base_offset
        buf.extend_from_slice(b"\x00\x00\x00\x4f"); // batch_length
        buf.extend_from_slice(b"\x00\x00\x00\x01"); // partition_leader_epoch
        buf.extend_from_slice(b"\x02"); // magic
        buf.extend_from_slice(b"\xb0\x69\x45\x7c"); // crc
        buf.extend_from_slice(b"\x00\x00"); // attributes
        buf.extend_from_slice(b"\x00\x00\x00\x00"); // last_offset_delta
        buf.extend_from_slice(b"\x00\x00\x01\x91\xe0\x5a\xf8\x18"); // base_timestamp
        buf.extend_from_slice(b"\x00\x00\x01\x91\xe0\x5a\xf8\x18"); // max_timestamp
        buf.extend_from_slice(b"\xff\xff\xff\xff\xff\xff\xff\xff"); // producer_id
        buf.extend_from_slice(b"\xff\xff"); // producer_epoch
        buf.extend_from_slice(b"\xff\xff\xff\xff"); // base_sequence
        buf.extend_from_slice(b"\x00\x00\x00\x01"); // records length

        // Batch #1 > Record #1
        buf.extend_from_slice(b"\x3a"); // length
        buf.extend_from_slice(b"\x00"); // attributes
        buf.extend_from_slice(b"\x00"); // timestamp_delta
        buf.extend_from_slice(b"\x00"); // offset_delta
        buf.extend_from_slice(b"\x01"); // key length
        buf.extend_from_slice(b"\x2e"); // value length

        let value1 = {
            // Batch #1 > Record #1 > Value (Feature Level Record)
            let mut val = Vec::with_capacity(23);
            val.extend_from_slice(b"\x01"); // frame version
            val.extend_from_slice(b"\x0c"); // type
            val.extend_from_slice(b"\x00"); // version
            val.extend_from_slice(b"\x11"); // name length & contents
            val.extend_from_slice(
                b"\x6d\x65\x74\x61\x64\x61\x74\x61\x2e\x76\x65\x72\x73\x69\x6f\x6e",
            );
            val.extend_from_slice(b"\x00\x14"); // feature_level
            val.extend_from_slice(b"\x00"); // tagged_fields
            Bytes::from(val)
        };

        buf.extend_from_slice(&value1); // value contents
        buf.extend_from_slice(b"\x00"); // headers array count

        //let value1 = FeatureLevelRecord {
        //    frame_version: 1,
        //    type_: 12,
        //    version: 0,
        //    name: "metadata.version".into(),
        //    feature_level: 20,
        //    tagged_fields: TagBuffer::default(),
        //};

        let batch1 = RecordBatch {
            base_offset: 0,
            batch_length: 79,
            partition_leader_epoch: 1,
            magic: 2,
            crc: 2959689084,
            attributes: RecordBatchAttrs {
                compression: Compression::None,
                timestamp_type: TimestampType::Create,
                is_transactional: false,
                is_control: false,
                has_delete_horizon_ms: false,
            },
            last_offset_delta: 0,
            base_timestamp: 1726045943832,
            max_timestamp: 1726045943832,
            producer_id: -1,
            producer_epoch: -1,
            base_sequence: -1,
            records: vec![Record {
                length: 29,
                attributes: RecordAttrs(0),
                timestamp_delta: 0,
                offset_delta: 0,
                key: None,
                value: Some(value1),
                headers: vec![],
            }],
        };

        // Batch #2
        buf.extend_from_slice(b"\x00\x00\x00\x00\x00\x00\x00\x01"); // base_offset
        buf.extend_from_slice(b"\x00\x00\x00\xe4"); // batch_length
        buf.extend_from_slice(b"\x00\x00\x00\x01"); // partition_leader_epoch
        buf.extend_from_slice(b"\x02"); // magic
        buf.extend_from_slice(b"\x24\xdb\x12\xdd"); // crc
        buf.extend_from_slice(b"\x00\x00"); // attributes
        buf.extend_from_slice(b"\x00\x00\x00\x02"); // last_offset_delta
        buf.extend_from_slice(b"\x00\x00\x01\x91\xe0\x5b\x2d\x15"); // base_timestamp
        buf.extend_from_slice(b"\x00\x00\x01\x91\xe0\x5b\x2d\x15"); // max_timestamp
        buf.extend_from_slice(b"\xff\xff\xff\xff\xff\xff\xff\xff"); // producer_id
        buf.extend_from_slice(b"\xff\xff"); // producer_epoch
        buf.extend_from_slice(b"\xff\xff\xff\xff"); // base_sequence
        buf.extend_from_slice(b"\x00\x00\x00\x03"); // records length

        // Batch #2 > Record #1
        buf.extend_from_slice(b"\x3c"); // length
        buf.extend_from_slice(b"\x00"); // attributes
        buf.extend_from_slice(b"\x00"); // timestamp_delta
        buf.extend_from_slice(b"\x00"); // offset_delta
        buf.extend_from_slice(b"\x01"); // key length
        buf.extend_from_slice(b"\x30"); // value length

        let value1 = {
            // Batch #2 > Record #1 > Value (Topic Record)
            let mut val = Vec::with_capacity(24);
            val.extend_from_slice(b"\x01"); // frame version
            val.extend_from_slice(b"\x02"); // type
            val.extend_from_slice(b"\x00"); // version
            val.extend_from_slice(b"\x04"); // topic name length
            val.extend_from_slice(b"\x73\x61\x7a"); // topic name contents
                                                    // topic UUID
                                                    // 0x00-0000-4000-8000-000000000091
            val.extend_from_slice(
                b"\x00\x00\x00\x00\x00\x00\x40\x00\x80\x00\x00\x00\x00\x00\x00\x91",
            );
            val.extend_from_slice(b"\x00"); // tagged_fields
            Bytes::from(val)
        };

        buf.extend_from_slice(&value1); // value contents
        buf.extend_from_slice(b"\x00"); // headers array count

        // Batch #2 > Record #2
        buf.extend_from_slice(b"\x90\x01"); // length
        buf.extend_from_slice(b"\x00"); // attributes
        buf.extend_from_slice(b"\x00"); // timestamp_delta
        buf.extend_from_slice(b"\x02"); // offset_delta
        buf.extend_from_slice(b"\x01"); // key length
        buf.extend_from_slice(b"\x82\x01"); // value length

        let value2 = {
            // Batch #2 > Record #2 > Value (Partition Record)
            let mut val = Vec::with_capacity(65);

            val.extend_from_slice(b"\x01"); // frame version
            val.extend_from_slice(b"\x03"); // type
            val.extend_from_slice(b"\x01"); // version
            val.extend_from_slice(b"\x00\x00\x00\x00"); // partition_id
                                                        // topic_id (0x00-0000-4000-8000-000000000091)
            val.extend_from_slice(
                b"\x00\x00\x00\x00\x00\x00\x40\x00\x80\x00\x00\x00\x00\x00\x00\x91",
            );

            val.extend_from_slice(b"\x02"); // length of replica array
            val.extend_from_slice(b"\x00\x00\x00\x01"); // replica array

            val.extend_from_slice(b"\x02"); // length of ISR array
            val.extend_from_slice(b"\x00\x00\x00\x01"); // ISR array

            val.extend_from_slice(b"\x01"); // length of removing replicas array
            val.extend_from_slice(b"\x01"); // length of adding replicas array

            val.extend_from_slice(b"\x00\x00\x00\x01"); // leader_id
            val.extend_from_slice(b"\x00\x00\x00\x00"); // leader_epoch
            val.extend_from_slice(b"\x00\x00\x00\x00"); // partition_epoch

            val.extend_from_slice(b"\x02"); // length of directories array
                                            // directory UUID (0x00000000-0000-4000-8000-000000000001)
            val.extend_from_slice(
                b"\x10\x00\x00\x00\x00\x00\x40\x00\x80\x00\x00\x00\x00\x00\x00\x01",
            );

            val.extend_from_slice(b"\x00"); // tagged_fields
            Bytes::from(val)
        };

        buf.extend_from_slice(&value2); // value contents
        buf.extend_from_slice(b"\x00"); // headers array count

        // Batch #2 > Record #3
        buf.extend_from_slice(b"\x90\x01"); // length
        buf.extend_from_slice(b"\x00"); // attributes
        buf.extend_from_slice(b"\x00"); // timestamp_delta
        buf.extend_from_slice(b"\x04"); // offset_delta
        buf.extend_from_slice(b"\x01"); // key length
        buf.extend_from_slice(b"\x82\x01"); // value length

        let value3 = {
            // Batch #2 > Record #3 > Value (Partition Record)
            let mut val = Vec::with_capacity(65);

            val.extend_from_slice(b"\x01"); // frame version
            val.extend_from_slice(b"\x03"); // type
            val.extend_from_slice(b"\x01"); // version
            val.extend_from_slice(b"\x00\x00\x00\x01"); // partition_id
                                                        // topic_id (0x00000000-0000-4000-8000-000000000091)
            val.extend_from_slice(
                b"\x00\x00\x00\x00\x00\x00\x40\x00\x80\x00\x00\x00\x00\x00\x00\x91",
            );

            val.extend_from_slice(b"\x02"); // length of replica array
            val.extend_from_slice(b"\x00\x00\x00\x01"); // replica array

            val.extend_from_slice(b"\x02"); // length of ISR array
            val.extend_from_slice(b"\x00\x00\x00\x01"); // ISR array

            val.extend_from_slice(b"\x01"); // length of removing replicas array
            val.extend_from_slice(b"\x01"); // length of adding replicas array

            val.extend_from_slice(b"\x00\x00\x00\x01"); // leader_id
            val.extend_from_slice(b"\x00\x00\x00\x00"); // leader_epoch
            val.extend_from_slice(b"\x00\x00\x00\x00"); // partition_epoch

            val.extend_from_slice(b"\x02"); // length of directories array
                                            // directory UUID (0x10000000-0000-4000-8000-000000000001)
            val.extend_from_slice(
                b"\x10\x00\x00\x00\x00\x00\x40\x00\x80\x00\x00\x00\x00\x00\x00\x01",
            );

            val.extend_from_slice(b"\x00"); // tagged_fields
            Bytes::from(val)
        };

        buf.extend_from_slice(&value3); // value contents
        buf.extend_from_slice(b"\x00"); // headers array count

        let batch2 = RecordBatch {
            base_offset: 1,
            batch_length: 228,
            partition_leader_epoch: 1,
            magic: 2,
            crc: 618336989,
            attributes: RecordBatchAttrs {
                compression: Compression::None,
                timestamp_type: TimestampType::Create,
                is_transactional: false,
                is_control: false,
                has_delete_horizon_ms: false,
            },
            last_offset_delta: 2,
            base_timestamp: 1726045957397,
            max_timestamp: 1726045957397,
            producer_id: -1,
            producer_epoch: -1,
            base_sequence: -1,
            records: vec![
                Record {
                    length: 30,
                    attributes: RecordAttrs(0),
                    timestamp_delta: 0,
                    offset_delta: 0,
                    key: None,
                    value: Some(value1),
                    headers: vec![],
                },
                Record {
                    length: 72,
                    attributes: RecordAttrs(0),
                    timestamp_delta: 0,
                    offset_delta: 1,
                    key: None,
                    value: Some(value2),
                    headers: vec![],
                },
                Record {
                    length: 72,
                    attributes: RecordAttrs(0),
                    timestamp_delta: 0,
                    offset_delta: 2,
                    key: None,
                    value: Some(value3),
                    headers: vec![],
                },
            ],
        };

        let mut buf = std::io::Cursor::new(buf);

        let expected = vec![batch1, batch2];
        let actual = decode(&mut buf).expect("decode request batches");
        assert_eq!(expected, actual);
    }
}
