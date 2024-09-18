use anyhow::{Context as _, Result};
use tokio::io::AsyncWriteExt;

use crate::kafka::api::ApiKey;
use crate::kafka::error::ErrorCode;
use crate::kafka::types::{Array, CompactArray, TagBuffer};
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
}

impl HeaderVersion for ResponseBody {
    fn header_version(&self, api_version: i16) -> i16 {
        match self {
            // ApiVersions responses always have headers without tagged fields (i.e., v0)
            // Tagged fields are only supported in the body but not in the header.
            Self::ApiVersions(_) => 0,
            #[allow(unreachable_patterns)]
            _ => api_version,
        }
    }
}

impl Serialize for ResponseBody {
    async fn write_into<W>(self, writer: &mut W, version: i16) -> Result<()>
    where
        W: AsyncWriteExt + Send + Unpin,
    {
        match self {
            ResponseBody::ApiVersions(body) => body.write_into(writer, version).await,
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
