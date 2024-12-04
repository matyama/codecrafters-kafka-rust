use std::io::Cursor;

use anyhow::{bail, ensure, Context as _, Result};
use bytes::{Buf, Bytes};
use tokio::io::AsyncReadExt;

use crate::kafka::types::{Array, StrBytes, VarInt, VarLong};
use crate::kafka::{AsyncDeserialize, AsyncSerialize, Deserialize, Serialize};

pub(crate) use self::topic::{Topic, TopicRecord};

pub(crate) mod feature_level;
pub(crate) mod partition;
pub(crate) mod topic;

/// See official docs for [Record Batch](https://kafka.apache.org/documentation/#recordbatch).
#[derive(Debug, PartialEq)]
pub struct RecordBatchHeader {
    pub base_offset: i64,
    pub batch_length: i32,
    pub partition_leader_epoch: i32,
    pub magic: i8,
    pub crc: u32,
    pub attributes: RecordBatchAttrs,
    pub last_offset_delta: i32,
    pub base_timestamp: i64,
    pub max_timestamp: i64,
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub base_sequence: i32,
}

impl RecordBatchHeader {
    #[inline]
    pub fn records_len(&self) -> usize {
        (self.batch_length as usize).saturating_sub(Self::SIZE)
    }

    #[inline]
    pub fn contains(&self, offset: i64) -> bool {
        (self.base_offset..=self.base_offset + self.last_offset_delta as i64).contains(&offset)
    }
}

impl Serialize for RecordBatchHeader {
    const SIZE: usize = 8 + 4 + 4 + 1 + 4 + 2 + 4 + 8 + 8 + 8 + 2 + 4;

    #[inline]
    fn encode_size(&self, _version: i16) -> usize {
        Self::SIZE
    }
}

impl Deserialize for RecordBatchHeader {
    const DEFAULT_VERSION: i16 = 2;

    fn decode<B: Buf>(buf: &mut B, version: i16) -> Result<(Self, usize)> {
        let mut size = 0;

        let (base_offset, n) = i64::deserialize(buf).context("base_offset")?;
        size += n;

        let (batch_length, n) = i32::deserialize(buf).context("batch_length")?;
        size += n;
        ensure!(batch_length > 0, "record batch length must be positive i32");

        let (partition_leader_epoch, n) =
            i32::deserialize(buf).context("partition_leader_epoch")?;
        size += n;

        let (magic, n) = i8::deserialize(buf).context("magic")?;
        size += n;

        ensure!(magic as i16 == version, "record batch: magic != version");

        match magic {
            // TODO: support reading record batch format v0 and v1
            0..=1 => unimplemented!("legacy record batch format is unsupported"),
            2 => {}
            version => bail!("unknown record batch version: {version}"),
        }

        let (crc, n) = u32::deserialize(buf).context("crc")?;
        size += n;

        let (attributes, n) = i16::deserialize(buf).context("attributes")?;
        size += n;

        let attributes =
            RecordBatchAttrs::try_from(attributes).context("invalid batch attributes")?;

        let (last_offset_delta, n) = i32::deserialize(buf).context("last_offset_delta")?;
        size += n;

        let (base_timestamp, n) = i64::deserialize(buf).context("base_timestamp")?;
        size += n;

        let (max_timestamp, n) = i64::deserialize(buf).context("max_timestamp")?;
        size += n;

        let (producer_id, n) = i64::deserialize(buf).context("producer_id")?;
        size += n;

        let (producer_epoch, n) = i16::deserialize(buf).context("producer_epoch")?;
        size += n;

        let (base_sequence, n) = i32::deserialize(buf).context("base_sequence")?;
        size += n;

        // TODO: decompress the rest of the buffer (based on `attributes.compression`)
        debug_assert_eq!(
            attributes.compression,
            Compression::None,
            "compression is not yet supported"
        );

        let header = RecordBatchHeader {
            base_offset,
            batch_length,
            partition_leader_epoch,
            magic,
            crc,
            attributes,
            last_offset_delta,
            base_timestamp,
            max_timestamp,
            producer_id,
            producer_epoch,
            base_sequence,
        };

        Ok((header, size))
    }
}

impl AsyncDeserialize for RecordBatchHeader {
    const DEFAULT_VERSION: i16 = 2;

    /// This implementation is not cancellation safe.
    async fn read_from<R>(reader: &mut R, version: i16) -> Result<(Self, usize)>
    where
        R: AsyncReadExt + Send + Unpin,
    {
        let mut buf = [0; Self::SIZE];

        let n = reader
            .read_exact(&mut buf)
            .await
            .context("reading batch header")?;

        let mut buf = Cursor::new(&mut buf[..n]);

        Self::decode(&mut buf, version).context("record batch header")
    }
}

impl Deserialize for Vec<RecordBatch> {
    const DEFAULT_VERSION: i16 = <RecordBatch as Deserialize>::DEFAULT_VERSION;

    fn decode<B: Buf>(buf: &mut B, version: i16) -> Result<(Self, usize)> {
        let mut batches = Vec::new();
        let mut size = 0;

        while buf.has_remaining() {
            let (batch, n) = RecordBatch::decode(buf, version).context("decode record batch")?;
            batches.push(batch);
            size += n;
        }

        Ok((batches, size))
    }
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

impl RecordBatch {
    #[inline]
    pub fn new(header: RecordBatchHeader, records: Vec<Record>) -> Self {
        Self {
            base_offset: header.base_offset,
            batch_length: header.batch_length,
            partition_leader_epoch: header.partition_leader_epoch,
            magic: header.magic,
            crc: header.crc,
            attributes: header.attributes,
            last_offset_delta: header.last_offset_delta,
            base_timestamp: header.base_timestamp,
            max_timestamp: header.max_timestamp,
            producer_id: header.producer_id,
            producer_epoch: header.producer_epoch,
            base_sequence: header.base_sequence,
            records,
        }
    }
}

impl Deserialize for RecordBatch {
    const DEFAULT_VERSION: i16 = <RecordBatchHeader as Deserialize>::DEFAULT_VERSION;

    fn decode<B: Buf>(buf: &mut B, version: i16) -> Result<(Self, usize)> {
        let (header, header_size) =
            RecordBatchHeader::decode(buf, version).context("record batch header")?;

        let (Array(records), records_size) =
            Deserialize::decode(buf, version).context("record batch records")?;

        Ok((Self::new(header, records), header_size + records_size))
    }
}

impl AsyncDeserialize for RecordBatch {
    const DEFAULT_VERSION: i16 = <RecordBatchHeader as AsyncDeserialize>::DEFAULT_VERSION;

    /// This implementation is not cancellation safe.
    async fn read_from<R>(reader: &mut R, version: i16) -> Result<(Self, usize)>
    where
        R: AsyncReadExt + Send + Unpin,
    {
        let (header, header_size) = RecordBatchHeader::read_from(reader, version)
            .await
            .context("record batch header")?;

        // TODO: impl AsyncDeserialize for BatchRecords
        let mut buf = vec![0; header.records_len()];

        reader
            .read_exact(&mut buf)
            .await
            .context("reading batch records")?;

        let mut buf = Cursor::new(buf);

        let (Array(records), records_size) =
            Deserialize::deserialize(&mut buf).context("record batch records")?;

        let size = header.batch_length as usize;
        debug_assert_eq!(size, header_size + records_size);

        Ok((Self::new(header, records), size))
    }
}

#[derive(Clone, Debug, PartialEq)]
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

impl From<RecordBatchAttrs> for i16 {
    fn from(attrs: RecordBatchAttrs) -> Self {
        let mut attributes = attrs.compression as i16;

        attributes |= (attrs.timestamp_type as i16) << 3;

        if attrs.is_transactional {
            attributes |= 1 << 4;
        }

        if attrs.is_control {
            attributes |= 1 << 5;
        }

        if attrs.has_delete_horizon_ms {
            attributes |= 1 << 6;
        }

        attributes
    }
}

/// See official docs for [Record](https://kafka.apache.org/documentation/#record).
#[derive(Debug, PartialEq)]
pub struct Record {
    /// The length of the record calculated from the attributes field to the end of the record.
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
    fn decode<B: Buf>(buf: &mut B, _version: i16) -> Result<(Self, usize)> {
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

impl AsyncDeserialize for Record {
    const DEFAULT_VERSION: i16 = <RecordBatch as AsyncDeserialize>::DEFAULT_VERSION;

    async fn read_from<R>(reader: &mut R, version: i16) -> Result<(Self, usize)>
    where
        R: AsyncReadExt + Send + Unpin,
    {
        let (length, n) = VarInt::read_from(reader, version)
            .await
            .context("record length")?;

        let length = usize::try_from(length).context("record length")?;

        let mut buf = vec![0; n + length];

        // write the length to the buffer for the final decode to work
        // TODO: need to async serialize for this
        let mut length_buf = Cursor::new(&mut buf[..n]);
        VarInt(length as i32)
            .write_into(&mut length_buf, version)
            .await?;

        let n = reader
            .read_exact(&mut buf[n..])
            .await
            .context("record contents")?;

        debug_assert_eq!(n, length);

        let mut buf = Cursor::new(buf);
        Self::decode(&mut buf, version).context("decode record")
    }
}

#[derive(Debug, PartialEq)]
#[repr(transparent)]
pub struct RecordAttrs(pub i8);

impl Deserialize for RecordAttrs {
    fn decode<B: Buf>(buf: &mut B, _version: i16) -> Result<(Self, usize)> {
        ensure!(buf.has_remaining(), "not enough bytes left");
        Ok((Self(buf.get_i8()), 1))
    }
}

#[derive(Clone, Debug, PartialEq)]
#[repr(transparent)]
pub struct RecordBytes(Option<Bytes>);

impl Deserialize for RecordBytes {
    fn decode<B: Buf>(buf: &mut B, _version: i16) -> Result<(Self, usize)> {
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
    fn decode<B: Buf>(buf: &mut B, _version: i16) -> Result<(Self, usize)> {
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
    fn decode<B: Buf>(buf: &mut B, _version: i16) -> Result<(Self, usize)> {
        let (VarInt(n), mut byte_size) = VarInt::deserialize(buf).context("header key length")?;

        ensure!(n >= 0, "invalid header key length: {n}");
        let key = StrBytes::try_from(buf.copy_to_bytes(n as usize)).context("header key")?;
        byte_size += n as usize;

        let (RecordBytes(val), n) = RecordBytes::deserialize(buf).context("header value")?;
        byte_size += n;

        Ok((Self { key, val }, byte_size))
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
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

#[derive(Clone, Copy, Debug, PartialEq)]
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn batch_attributes() {
        let attrs = RecordBatchAttrs {
            compression: Compression::Lz4,
            timestamp_type: TimestampType::LogAppend,
            is_transactional: true,
            is_control: false,
            has_delete_horizon_ms: false,
        };

        let encoded = i16::from(attrs.clone());
        let decoded = RecordBatchAttrs::try_from(encoded).expect("valid attributes");
        assert_eq!(decoded, attrs);

        let attrs = RecordBatchAttrs {
            compression: Compression::None,
            timestamp_type: TimestampType::Create,
            is_transactional: false,
            is_control: false,
            has_delete_horizon_ms: false,
        };

        let encoded = i16::from(attrs.clone());
        let decoded = RecordBatchAttrs::try_from(encoded).expect("valid attributes");
        assert_eq!(decoded, attrs);
    }

    #[tokio::test]
    async fn read_record() {
        let mut buf = Vec::new();

        buf.extend_from_slice(b"\x3a"); // length
        buf.extend_from_slice(b"\x00"); // attributes
        buf.extend_from_slice(b"\x00"); // timestamp_delta
        buf.extend_from_slice(b"\x00"); // offset_delta
        buf.extend_from_slice(b"\x01"); // key length
        buf.extend_from_slice(b"\x2e"); // value length

        let value = {
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

        buf.extend_from_slice(&value); // value contents
        buf.extend_from_slice(b"\x00"); // headers array count

        let expected_len = buf.len();

        let expected = Record {
            length: 29,
            attributes: RecordAttrs(0),
            timestamp_delta: 0,
            offset_delta: 0,
            key: None,
            value: Some(value),
            headers: vec![],
        };

        let mut buf = Cursor::new(buf);

        let (actual, actual_len) = Record::read(&mut buf).await.expect("valid record");

        assert_eq!(expected_len, actual_len);
        assert_eq!(expected, actual);
    }

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

        let mut buf = Cursor::new(buf);

        let expected = vec![batch1, batch2];
        let (actual, _) =
            <Vec<RecordBatch>>::deserialize(&mut buf).expect("decode request batches");
        assert_eq!(expected, actual);
    }
}
