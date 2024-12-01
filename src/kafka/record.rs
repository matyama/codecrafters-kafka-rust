use anyhow::{bail, ensure, Context as _, Result};
use bytes::{Buf, Bytes};

use crate::kafka::types::{Array, StrBytes, Uuid, VarInt, VarLong};
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
#[allow(dead_code)]
#[derive(Debug)]
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

#[allow(dead_code)]
#[derive(Debug)]
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
#[allow(dead_code)]
#[derive(Debug)]
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

#[derive(Debug)]
#[repr(transparent)]
pub struct RecordAttrs(pub i8);

impl Deserialize for RecordAttrs {
    fn read_from<B: Buf>(buf: &mut B, _version: i16) -> Result<(Self, usize)> {
        ensure!(buf.has_remaining(), "not enough bytes left");
        Ok((Self(buf.get_i8()), 1))
    }
}

#[derive(Clone, Debug)]
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

#[derive(Debug)]
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
#[allow(dead_code)]
#[derive(Debug)]
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

#[derive(Debug)]
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

#[derive(Clone, Debug)]
pub struct Topic {
    name: StrBytes,
    topic_id: Uuid,
}

impl Topic {
    #[inline]
    pub(crate) fn name(&self) -> &str {
        self.name.as_str()
    }

    #[inline]
    pub(crate) fn topic_id(&self) -> Uuid {
        self.topic_id.clone()
    }
}

impl Deserialize for Topic {
    fn read_from<B: Buf>(buf: &mut B, version: i16) -> Result<(Self, usize)> {
        // XXX: what exactly are these two bytes
        let (_, mut size) = i16::deserialize(buf).context("topic header")?;

        let (len, n) = i16::deserialize(buf).context("topic name length")?;
        let len = len.saturating_sub(1) as usize;
        size += n;

        ensure!(buf.remaining() >= len, "not enough bytes");
        let name = StrBytes::try_from(buf.copy_to_bytes(len)).context("topic name")?;
        size += len;

        //    //// XXX: topic names seem to be null byte terminated
        //    let (name, n) = Str::deserialize(buf).context("topic name")?;
        //    size += n;

        let (topic_id, n) = Uuid::read_from(buf, version).context("topic id")?;
        size += n;

        // XXX: what exactly are these two bytes
        ensure!(buf.has_remaining(), "not enough bytes left");
        let (_, n) = i8::deserialize(buf).context("topic footer")?;
        size += n;

        Ok((Self { name, topic_id }, size))
    }
}
