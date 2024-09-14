use bytes::Bytes;

/// Request Header v0
#[derive(Debug)]
pub struct Header {
    pub request_api_key: ApiKey,
    pub request_api_version: i16,
    pub correlation_id: i32,
    // TODO: v1
    //pub client_id: Option<String>,
    // TODO: v2
    //pub tagged_fields: ()
}

macro_rules! repr_enum {
    ($repr:ty; $vis:vis enum $name:ident { $v0:ident = $i0:literal, $($v:ident = $i:literal,)* }) => {
        #[repr($repr)]
        #[derive(Debug)]
        $vis enum $name {
            $v0 = $i0,
            $($v = $i,)*
        }

        impl TryFrom<$repr> for $name {
            type Error = String;

            fn try_from(value: $repr) -> Result<Self, Self::Error> {
                match value {
                    $i0 => Ok($name::$v0),
                    $($i => Ok($name::$v),)*
                    v => Err(format!("unsupported {} variant: {v}", stringify!($name))),
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

#[derive(Debug)]
pub enum Request {
    // TODO: structural data
    ApiVersions {
        size: i32,
        header: Header,
        data: Bytes,
    },
}

impl Request {
    pub(crate) fn header(&self) -> &Header {
        match self {
            Self::ApiVersions { header, .. } => header,
        }
    }
}
