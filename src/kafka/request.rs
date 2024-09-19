use anyhow::{bail, ensure, Result};
use bytes::{Buf, Bytes};

use crate::kafka::api::{ApiKey, ApiVersion};
use crate::kafka::error::{ErrorCode, KafkaError};
use crate::kafka::types::{CompactStr, Str, TagBuffer};
use crate::kafka::{Deserialize, HeaderVersion};

#[derive(Debug)]
pub struct RequestHeader {
    /// An integer identifying the request type. (API v0+)
    pub request_api_key: ApiKey,

    /// The version of the API to use for the request. (API v0+)
    pub request_api_version: ApiVersion,

    /// A unique identifier for the request. (API v0+)
    pub correlation_id: i32,

    // XXX: enum Since<T, const VERSION: i16> { Just(T), Nothing }
    //  - "unlock" by providing specific version or even based on a specific impl block
    /// A string identifying the client that sent the request. (API v1+)
    pub client_id: Option<Str>,

    /// Optional tagged fields. (API v2+)
    pub tagged_fields: TagBuffer,
}

impl RequestHeader {
    // NOTE: This is intentionally not impl Deserialize, because that one depends on given version.
    pub fn read_from<B: Buf>(buf: &mut B) -> Result<(Self, usize)> {
        ensure!(
            buf.remaining() >= 8,
            "request header has at least 8B, got {}B",
            buf.remaining()
        );

        // read header fields (v0)
        let api_key = buf.get_i16();
        let api_version = buf.get_i16();
        let correlation_id = buf.get_i32();

        let mut header_bytes = 2 + 2 + 4;

        let kafka_err = |error_code| KafkaError {
            error_code,
            api_key,
            api_version,
            correlation_id,
        };

        // parse header fields (v0+)
        let request_api_key = ApiKey::try_from(api_key).map_err(kafka_err)?;
        let request_api_version =
            ApiVersion::parse(request_api_key, api_version).map_err(kafka_err)?;

        // header version is generally different, but derivable from the API version
        let header_version = request_api_key.header_version(api_version);

        // parse header fields depending on version

        let client_id = match header_version {
            0 => None,

            v if v >= 1 && buf.has_remaining() => {
                let (client_id, n) = Deserialize::read_from(buf, v)
                    .map_err(|_| kafka_err(ErrorCode::INVALID_REQUEST))?;

                header_bytes += n;

                client_id
            }

            _ => bail!(kafka_err(ErrorCode::INVALID_REQUEST)),
        };

        let mut tagged_fields = TagBuffer::default();

        if header_version >= 2 {
            let (fields, n) = Deserialize::read_from(buf, header_version)
                .map_err(|_| kafka_err(ErrorCode::INVALID_REQUEST))?;

            tagged_fields = fields;
            header_bytes += n;
        }

        let header = Self {
            request_api_key,
            request_api_version,
            correlation_id,
            client_id,
            tagged_fields,
        };

        Ok((header, header_bytes))
    }
}

#[derive(Debug)]
pub struct RequestMessage {
    pub size: i32,
    pub header: RequestHeader,
    pub body: RequestBody,
}

#[derive(Debug)]
pub enum RequestBody {
    ApiVersions(ApiVersions),
    #[allow(dead_code)]
    Fetch(Fetch),
}

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

// TODO: implement
#[derive(Debug)]
#[repr(transparent)]
pub struct Fetch(Bytes);

impl Deserialize for Fetch {
    fn read_from<B: Buf>(buf: &mut B, _version: i16) -> Result<(Self, usize)> {
        let size = buf.remaining();

        let data = if buf.has_remaining() {
            buf.copy_to_bytes(size)
        } else {
            Bytes::default()
        };

        Ok((Self(data), size))
    }
}
