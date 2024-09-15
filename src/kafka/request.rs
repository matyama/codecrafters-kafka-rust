use std::ops::RangeInclusive;

use anyhow::{ensure, Result};
use bytes::{Buf, Bytes};

use crate::kafka::error::{ErrorCode, KafkaError};

/// Request header v0
#[derive(Debug)]
pub struct RequestHeader {
    pub request_api_key: ApiKey,
    pub request_api_version: ApiVersion,
    pub correlation_id: i32,
    // TODO: v1
    //
    // NULLABLE_STRING
    //
    // Represents a sequence of characters or null. For non-null strings, first the length N is
    // given as an INT16. Then N bytes follow which are the UTF-8 encoding of the character
    // sequence. A null value is encoded with length of -1 and there are no following bytes.
    //
    //pub client_id: Option<String>,
    //
    // TODO: v2
    //
    // TAGGED_FIELDS
    //
    // https://cwiki.apache.org/confluence/display/KAFKA/KIP-482
    //
    //pub tagged_fields: ()
}

impl RequestHeader {
    pub(super) fn parse(mut buf: &[u8]) -> Result<(RequestHeader, usize)> {
        ensure!(
            buf.len() >= 8,
            "request header has at least 8B, got {}B",
            buf.len()
        );

        // read header fields (v0)
        let api_key = buf.get_i16();
        let api_version = buf.get_i16();
        let correlation_id = buf.get_i32();

        let kafka_err = |error_code| KafkaError {
            error_code,
            api_key,
            correlation_id,
        };

        // parse header fields (v0)
        let request_api_key = ApiKey::try_from(api_key).map_err(kafka_err)?;
        let request_api_version =
            ApiVersion::parse(request_api_key, api_version).map_err(kafka_err)?;

        // TODO: read more headers (based on api key and version)

        let header = RequestHeader {
            request_api_key,
            request_api_version,
            correlation_id,
        };

        Ok((header, 8))
    }
}

macro_rules! repr_enum {
    (
        $repr:ty;
        $vis:vis enum $name:ident {
            $v0:ident = $i0:literal,
            $($v:ident = $i:literal,)*
        }
    ) => {
        #[derive(Clone, Copy, Debug)]
        #[repr($repr)]
        $vis enum $name {
            $v0 = $i0,
            $($v = $i,)*
        }

        impl TryFrom<$repr> for $name {
            type Error = ErrorCode;

            fn try_from(value: $repr) -> Result<Self, Self::Error> {
                match value {
                    $i0 => Ok($name::$v0),
                    $($i => Ok($name::$v),)*
                    _ => Err(ErrorCode::INVALID_REQUEST),
                }
            }
        }

    };
}

repr_enum! { i16;
    pub enum ApiKey {
        Produce = 0,
        Fetch = 1,
        ListOffsets = 2,
        Metadata = 3,
        LeaderAndIsr = 4,
        StopReplica = 5,
        UpdateMetadata = 6,
        ControlledShutdown = 7,
        OffsetCommit = 8,
        OffsetFetch = 9,
        FindCoordinator = 10,
        JoinGroup = 11,
        Heartbeat = 12,
        LeaveGroup = 13,
        SyncGroup = 14,
        DescribeGroups = 15,
        ListGroups = 16,
        SaslHandshake = 17,
        ApiVersions = 18,
        CreateTopics = 19,
        DeleteTopics = 20,
        DeleteRecords = 21,
        InitProducerId = 22,
        OffsetForLeaderEpoch = 23,
        AddPartitionsToTxn = 24,
        AddOffsetsToTxn = 25,
        EndTxn = 26,
        WriteTxnMarkers = 27,
        TxnOffsetCommit = 28,
        DescribeAcls = 29,
        CreateAcls = 30,
        DeleteAcls = 31,
        DescribeConfigs = 32,
        AlterConfigs = 33,
        AlterReplicaLogDirs = 34,
        DescribeLogDirs = 35,
        SaslAuthenticate = 36,
        CreatePartitions = 37,
        CreateDelegationToken = 38,
        RenewDelegationToken = 39,
        ExpireDelegationToken = 40,
        DescribeDelegationToken = 41,
        DeleteGroups = 42,
        ElectLeaders = 43,
        IncrementalAlterConfigs = 44,
        AlterPartitionReassignments = 45,
        ListPartitionReassignments = 46,
        OffsetDelete = 47,
        DescribeClientQuotas = 48,
        AlterClientQuotas = 49,
        DescribeUserScramCredentials = 50,
        AlterUserScramCredentials = 51,
        DescribeQuorum = 55,
        AlterPartition = 56,
        UpdateFeatures = 57,
        Envelope = 58,
        DescribeCluster = 60,
        DescribeProducers = 61,
        UnregisterBroker = 64,
        DescribeTransactions = 65,
        ListTransactions = 66,
        AllocateProducerIds = 67,
        ConsumerGroupHeartbeat = 68,
        ConsumerGroupDescribe = 69,
        GetTelemetrySubscriptions = 71,
        PushTelemetry = 72,
        ListClientMetricsResources = 74,
        DescribeTopicPartitions = 75,
    }
}

impl ApiKey {
    #[inline]
    pub(crate) fn api_versions(&self) -> RangeInclusive<ApiVersion> {
        match self {
            Self::ApiVersions => ApiVersion(0)..=ApiVersion(4),
            key => unimplemented!("{key:?}"),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[repr(transparent)]
pub struct ApiVersion(i16);

impl ApiVersion {
    pub fn parse(key: ApiKey, version: i16) -> Result<Self, ErrorCode> {
        let version = ApiVersion(version);

        if !key.api_versions().contains(&version) {
            Err(ErrorCode::UNSUPPORTED_VERSION)
        } else {
            Ok(version)
        }
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
    // TODO: structural data
    ApiVersions { data: Bytes },
}
