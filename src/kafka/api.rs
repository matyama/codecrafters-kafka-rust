use std::ops::RangeInclusive;

use crate::kafka::error::ErrorCode;
use crate::kafka::{HeaderVersion, WireSize};

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

        impl $name {
            $vis fn iter() -> impl Iterator<Item = $name> {
                use std::sync::OnceLock;
                static VARIANTS: OnceLock<Vec<$name>> = OnceLock::new();
                VARIANTS.get_or_init(|| vec![$name::$v0, $($name::$v,)*]).iter().copied()
            }
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
    /// Return range of available API versions and `None` if API is unsupported.
    #[inline]
    pub(crate) fn api_versions(&self) -> Option<RangeInclusive<ApiVersion>> {
        match self {
            //Self::Produce => Some(ApiVersion(0)..=ApiVersion(11)),
            //Self::Fetch => Some(ApiVersion(0)..=ApiVersion(16)),
            Self::ApiVersions => Some(ApiVersion(0)..=ApiVersion(4)),
            _ => None,
        }
    }
}

impl HeaderVersion for ApiKey {
    fn header_version(&self, api_version: i16) -> i16 {
        match self {
            Self::ApiVersions if api_version >= 3 => 2,
            Self::ApiVersions => 1,
            _ => 0,
        }
    }
}

impl WireSize for ApiKey {
    // INT16 repr
    const SIZE: usize = 2;

    #[inline]
    fn size(&self, _version: i16) -> usize {
        Self::SIZE
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[repr(transparent)]
pub struct ApiVersion(i16);

impl ApiVersion {
    pub fn parse(key: ApiKey, version: i16) -> Result<Self, ErrorCode> {
        let version = ApiVersion(version);
        match key.api_versions() {
            Some(versions) if versions.contains(&version) => Ok(version),
            _ => Err(ErrorCode::UNSUPPORTED_VERSION),
        }
    }

    #[inline]
    pub fn into_inner(self) -> i16 {
        self.0
    }
}
