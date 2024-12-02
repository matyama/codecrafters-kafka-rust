use anyhow::{bail, ensure, Result};
use bytes::Buf;

use crate::kafka::api::{ApiKey, ApiVersion};
use crate::kafka::error::{ErrorCode, KafkaError};
use crate::kafka::types::{Str, TagBuffer};
use crate::kafka::{Deserialize, HeaderVersion};

pub(crate) use api_versions::ApiVersions;
pub(crate) use describe_topic_partitions::DescribeTopicPartitions;
pub(crate) use fetch::Fetch;

pub mod api_versions;
pub mod describe_topic_partitions;
pub mod fetch;

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
                let (client_id, n) = Deserialize::decode(buf, v)
                    .map_err(|_| kafka_err(ErrorCode::INVALID_REQUEST))?;

                header_bytes += n;

                client_id
            }

            _ => bail!(kafka_err(ErrorCode::INVALID_REQUEST)),
        };

        let mut tagged_fields = TagBuffer::default();

        if header_version >= 2 {
            let (fields, n) = Deserialize::decode(buf, header_version)
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
    Fetch(Fetch),
    DescribeTopicPartitions(DescribeTopicPartitions),
}
