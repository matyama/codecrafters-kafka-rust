use anyhow::{ensure, Context as _, Result};
use bytes::Buf;

use crate::kafka::types::{CompactStr, StrBytes, TagBuffer};
use crate::kafka::Deserialize;

/// # FeatureLevelRecord
///
/// [Record schema][schema]
///
/// [schema]: https://github.com/apache/kafka/blob/5b3027dfcbcb62d169d4b4421260226e620459af/metadata/src/main/resources/common/metadata/FeatureLevelRecord.json
#[derive(Debug, PartialEq)]
pub struct FeatureLevelRecord {
    pub frame_version: i8,
    pub record_type: i8,
    pub version: i8,
    /// The feature name. (API v0+)
    pub name: StrBytes,
    /// The current finalized feature level of this feature for the cluster, a value of 0 means
    /// feature not supported. (API v0+)
    pub feature_level: i16,
    /// Other tagged fields
    pub tagged_fields: TagBuffer,
}

impl Deserialize for FeatureLevelRecord {
    fn decode<B: Buf>(buf: &mut B, version: i16) -> Result<(Self, usize)> {
        let (frame_version, mut size) = i8::decode(buf, version).context("frame_version")?;

        let (record_type, n) = i8::decode(buf, version).context("type")?;
        size += n;

        ensure!(record_type == 12, "invalid record type");

        let (ver, n) = i8::decode(buf, version).context("version")?;
        size += n;

        let (name, n) = CompactStr::decode(buf, version).context("name")?;
        size += n;

        let (feature_level, n) = i16::decode(buf, version).context("feature_level")?;
        size += n;

        let (tagged_fields, n) = TagBuffer::decode(buf, version).context("tagged_fields")?;
        size += n;

        let record = Self {
            frame_version,
            record_type,
            version: ver,
            name: name.into(),
            feature_level,
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
        let mut buf = Vec::with_capacity(23);
        buf.extend_from_slice(b"\x01"); // frame version
        buf.extend_from_slice(b"\x0c"); // type
        buf.extend_from_slice(b"\x00"); // version
        buf.extend_from_slice(b"\x11"); // name length & contents
        buf.extend_from_slice(b"\x6d\x65\x74\x61\x64\x61\x74\x61\x2e\x76\x65\x72\x73\x69\x6f\x6e");
        buf.extend_from_slice(b"\x00\x14"); // feature_level
        buf.extend_from_slice(b"\x00"); // tagged_fields

        let expected = FeatureLevelRecord {
            frame_version: 1,
            record_type: 12,
            version: 0,
            name: "metadata.version".into(),
            feature_level: 20,
            tagged_fields: TagBuffer::default(),
        };

        let mut buf = std::io::Cursor::new(buf);

        let (actual, size) =
            FeatureLevelRecord::deserialize(&mut buf).expect("valid feature level record");

        assert_eq!(size, 23);
        assert_eq!(expected, actual);
    }
}
