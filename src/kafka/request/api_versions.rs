use anyhow::{ensure, Result};
use bytes::Buf;

use crate::kafka::types::{CompactStr, TagBuffer};
use crate::kafka::Deserialize;

/// # ApiVersions Request
///
/// [Request schema][schema]
///
/// Versions 0 through 2 of ApiVersionsRequest are the same.
///
/// Version 3 is the first flexible version and adds ClientSoftwareName and ClientSoftwareVersion.
///
/// Version 4 fixes KAFKA-17011, which blocked SupportedFeatures.MinVersion in the response from
/// being 0.
///
/// [schema]: https://github.com/apache/kafka/blob/trunk/clients/src/main/resources/common/message/ApiVersionsRequest.json
#[derive(Debug)]
pub struct ApiVersions {
    // XXX: Since<Str, 3>
    /// The name of the client. (API v3+)
    pub client_software_name: CompactStr,
    /// The version of the client. (API v3+)
    pub client_software_version: CompactStr,
    /// Other tagged fields. (API v3+)
    pub tagged_fields: TagBuffer,
}

impl Deserialize for ApiVersions {
    fn read_from<B: Buf>(buf: &mut B, version: i16) -> Result<(Self, usize)> {
        let mut body_bytes = 0;

        let client_software_name = if version >= 3 {
            let (name, n) = CompactStr::read_from(buf, version)?;
            body_bytes += n;
            name
        } else {
            CompactStr::empty()
        };

        let client_software_version = if version >= 3 {
            let (ver, n) = CompactStr::read_from(buf, version)?;
            body_bytes += n;
            ver
        } else {
            CompactStr::empty()
        };

        let tagged_fields = if version >= 3 {
            let (tag_buf, n) = TagBuffer::read_from(buf, version)?;
            body_bytes += n;
            tag_buf
        } else {
            TagBuffer::default()
        };

        ensure!(
            !buf.has_remaining(),
            "ApiVersions: buffer contains leftover bytes"
        );

        let body = Self {
            client_software_name,
            client_software_version,
            tagged_fields,
        };

        Ok((body, body_bytes))
    }
}
