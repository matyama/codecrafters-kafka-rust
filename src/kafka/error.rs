#[derive(Debug, thiserror::Error)]
#[error("KafkaError: {error_code:?}, api key {api_key} correlation id {correlation_id:?}")]
pub struct KafkaError {
    pub(crate) error_code: ErrorCode,
    pub(crate) api_key: i16,
    pub(crate) api_version: i16,
    pub(crate) correlation_id: i32,
}

// XXX: impl retriable via a bitset

// FIXME: camel case enum variants
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Debug, Default)]
#[repr(i16)]
pub enum ErrorCode {
    /// The server experienced an unexpected error when processing the request.
    UNKNOWN_SERVER_ERROR = -1,

    #[allow(clippy::upper_case_acronyms)]
    #[default]
    NONE = 0,

    /// The requested offset is not within the range of offsets maintained by the server.
    OFFSET_OUT_OF_RANGE = 1,

    /// This message has failed its CRC checksum, exceeds the valid size, has a null key for a
    /// compacted topic, or is otherwise corrupt.
    CORRUPT_MESSAGE = 2,

    /// This server does not host this topic-partition.
    UNKNOWN_TOPIC_OR_PARTITION = 3,

    /// The requested fetch size is invalid.
    INVALID_FETCH_SIZE = 4,

    /// There is no leader for this topic-partition as we are in the middle of a leadership
    /// election.
    LEADER_NOT_AVAILABLE = 5,

    /// For requests intended only for the leader, this error indicates that the broker is not the
    /// current leader. For requests intended for any replica, this error indicates that the broker
    /// is not a replica of the topic partition.
    NOT_LEADER_OR_FOLLOWER = 6,

    /// The request timed out.
    REQUEST_TIMED_OUT = 7,

    /// The broker is not available.
    BROKER_NOT_AVAILABLE = 8,

    /// The replica is not available for the requested topic-partition. Produce/Fetch requests and
    /// other requests intended only for the leader or follower return NOT_LEADER_OR_FOLLOWER if
    /// the broker is not a replica of the topic-partition.
    REPLICA_NOT_AVAILABLE = 9,

    /// The request included a message larger than the max message size the server will accept.
    MESSAGE_TOO_LARGE = 10,

    /// The controller moved to another broker.
    STALE_CONTROLLER_EPOCH = 11,

    /// The metadata field of the offset request was too large.
    OFFSET_METADATA_TOO_LARGE = 12,

    /// The server disconnected before a response was received.
    NETWORK_EXCEPTION = 13,

    /// The coordinator is loading and hence can't process requests.
    COORDINATOR_LOAD_IN_PROGRESS = 14,

    /// The coordinator is not available.
    COORDINATOR_NOT_AVAILABLE = 15,

    /// This is not the correct coordinator.
    NOT_COORDINATOR = 16,

    /// The request attempted to perform an operation on an invalid topic.
    INVALID_TOPIC_EXCEPTION = 17,

    /// The request included message batch larger than the configured segment size on the server.
    RECORD_LIST_TOO_LARGE = 18,

    /// Messages are rejected since there are fewer in-sync replicas than required.
    NOT_ENOUGH_REPLICAS = 19,

    /// Messages are written to the log, but to fewer in-sync replicas than required.
    NOT_ENOUGH_REPLICAS_AFTER_APPEND = 20,

    /// Produce request specified an invalid value for required acks.
    INVALID_REQUIRED_ACKS = 21,

    /// Specified group generation id is not valid.
    ILLEGAL_GENERATION = 22,

    /// The group member's supported protocols are incompatible with those of existing members or
    /// first group member tried to join with empty protocol type or empty protocol list.
    INCONSISTENT_GROUP_PROTOCOL = 23,

    /// The configured groupId is invalid.
    INVALID_GROUP_ID = 24,

    /// The coordinator is not aware of this member.
    UNKNOWN_MEMBER_ID = 25,

    /// The session timeout is not within the range allowed by the broker
    /// (as configured by group.min.session.timeout.ms and group.max.session.timeout.ms).
    INVALID_SESSION_TIMEOUT = 26,

    /// The group is rebalancing, so a rejoin is needed.
    REBALANCE_IN_PROGRESS = 27,

    /// The committing offset data size is not valid.
    INVALID_COMMIT_OFFSET_SIZE = 28,

    /// Topic authorization failed.
    TOPIC_AUTHORIZATION_FAILED = 29,

    /// Group authorization failed.
    GROUP_AUTHORIZATION_FAILED = 30,

    /// Cluster authorization failed.
    CLUSTER_AUTHORIZATION_FAILED = 31,

    /// The timestamp of the message is out of acceptable range.
    INVALID_TIMESTAMP = 32,

    /// The broker does not support the requested SASL mechanism.
    UNSUPPORTED_SASL_MECHANISM = 33,

    /// Request is not valid given the current SASL state.
    ILLEGAL_SASL_STATE = 34,

    /// The version of API is not supported.
    UNSUPPORTED_VERSION = 35,

    /// Topic with this name already exists.
    TOPIC_ALREADY_EXISTS = 36,

    /// Number of partitions is below 1.
    INVALID_PARTITIONS = 37,

    /// Replication factor is below 1 or larger than the number of available brokers.
    INVALID_REPLICATION_FACTOR = 38,

    /// Replica assignment is invalid.
    INVALID_REPLICA_ASSIGNMENT = 39,

    /// Configuration is invalid.
    INVALID_CONFIG = 40,

    /// This is not the correct controller for this cluster.
    NOT_CONTROLLER = 41,

    /// This most likely occurs because of a request being malformed by the client library or the
    /// message was sent to an incompatible broker. See the broker logs for more details.
    INVALID_REQUEST = 42,

    /// The message format version on the broker does not support the request.
    UNSUPPORTED_FOR_MESSAGE_FORMAT = 43,

    /// Request parameters do not satisfy the configured policy.
    POLICY_VIOLATION = 44,

    /// The broker received an out of order sequence number.
    OUT_OF_ORDER_SEQUENCE_NUMBER = 45,

    /// The broker received a duplicate sequence number.
    DUPLICATE_SEQUENCE_NUMBER = 46,

    /// Producer attempted to produce with an old epoch.
    INVALID_PRODUCER_EPOCH = 47,

    /// The producer attempted a transactional operation in an invalid state.
    INVALID_TXN_STATE = 48,

    /// The producer attempted to use a producer id which is not currently assigned to its
    /// transactional id.
    INVALID_PRODUCER_ID_MAPPING = 49,

    /// The transaction timeout is larger than the maximum value allowed by the broker
    /// (as configured by transaction.max.timeout.ms).
    INVALID_TRANSACTION_TIMEOUT = 50,

    /// The producer attempted to update a transaction while another concurrent operation on the
    /// same transaction was ongoing.
    CONCURRENT_TRANSACTIONS = 51,

    /// Indicates that the transaction coordinator sending a WriteTxnMarker is no longer the
    /// current coordinator for a given producer.
    TRANSACTION_COORDINATOR_FENCED = 52,

    /// Transactional Id authorization failed.
    TRANSACTIONAL_ID_AUTHORIZATION_FAILED = 53,

    /// Security features are disabled.
    SECURITY_DISABLED = 54,

    /// The broker did not attempt to execute this operation. This may happen for batched RPCs
    /// where some operations in the batch failed, causing the broker to respond without trying the
    /// rest.
    OPERATION_NOT_ATTEMPTED = 55,

    /// Disk error when trying to access log file on the disk.
    KAFKA_STORAGE_ERROR = 56,

    /// The user-specified log directory is not found in the broker config.
    LOG_DIR_NOT_FOUND = 57,

    /// SASL Authentication failed.
    SASL_AUTHENTICATION_FAILED = 58,

    /// This exception is raised by the broker if it could not locate the producer metadata
    /// associated with the producerId in question. This could happen if, for instance, the
    /// producer's records were deleted because their retention time had elapsed. Once the last
    /// records of the producerId are removed, the producer's metadata is removed from the broker,
    /// and future appends by the producer will return this exception.
    UNKNOWN_PRODUCER_ID = 59,

    /// A partition reassignment is in progress.
    REASSIGNMENT_IN_PROGRESS = 60,

    /// Delegation Token feature is not enabled.
    DELEGATION_TOKEN_AUTH_DISABLED = 61,

    /// Delegation Token is not found on server.
    DELEGATION_TOKEN_NOT_FOUND = 62,

    /// Specified Principal is not valid Owner/Renewer.
    DELEGATION_TOKEN_OWNER_MISMATCH = 63,

    /// Delegation Token requests are not allowed on PLAINTEXT/1-way SSL channels and on delegation
    /// token authenticated channels.
    DELEGATION_TOKEN_REQUEST_NOT_ALLOWED = 64,

    /// Delegation Token authorization failed.
    DELEGATION_TOKEN_AUTHORIZATION_FAILED = 65,

    /// Delegation Token is expired.
    DELEGATION_TOKEN_EXPIRED = 66,

    /// Supplied principalType is not supported.
    INVALID_PRINCIPAL_TYPE = 67,

    /// The group is not empty.
    NON_EMPTY_GROUP = 68,

    /// The group id does not exist.
    GROUP_ID_NOT_FOUND = 69,

    /// The fetch session ID was not found.
    FETCH_SESSION_ID_NOT_FOUND = 70,

    /// The fetch session epoch is invalid.
    INVALID_FETCH_SESSION_EPOCH = 71,

    /// There is no listener on the leader broker that matches the listener on which metadata
    /// request was processed.
    LISTENER_NOT_FOUND = 72,

    /// Topic deletion is disabled.
    TOPIC_DELETION_DISABLED = 73,

    /// The leader epoch in the request is older than the epoch on the broker.
    FENCED_LEADER_EPOCH = 74,

    /// The leader epoch in the request is newer than the epoch on the broker.
    UNKNOWN_LEADER_EPOCH = 75,

    /// The requesting client does not support the compression type of given partition.
    UNSUPPORTED_COMPRESSION_TYPE = 76,

    /// Broker epoch has changed.
    STALE_BROKER_EPOCH = 77,

    /// The leader high watermark has not caught up from a recent leader election so the offsets
    /// cannot be guaranteed to be monotonically increasing.
    OFFSET_NOT_AVAILABLE = 78,

    /// The group member needs to have a valid member id before actually entering a consumer group.
    MEMBER_ID_REQUIRED = 79,

    /// The preferred leader was not available.
    PREFERRED_LEADER_NOT_AVAILABLE = 80,

    /// The consumer group has reached its max size.
    GROUP_MAX_SIZE_REACHED = 81,

    /// The broker rejected this static consumer since another consumer with the same
    /// group.instance.id has registered with a different member.id.
    FENCED_INSTANCE_ID = 82,

    /// Eligible topic partition leaders are not available.
    ELIGIBLE_LEADERS_NOT_AVAILABLE = 83,

    /// Leader election not needed for topic partition.
    ELECTION_NOT_NEEDED = 84,

    /// No partition reassignment is in progress.
    NO_REASSIGNMENT_IN_PROGRESS = 85,

    /// Deleting offsets of a topic is forbidden while the consumer group is actively subscribed to
    /// it.
    GROUP_SUBSCRIBED_TO_TOPIC = 86,

    /// This record has failed the validation on broker and hence will be rejected.
    INVALID_RECORD = 87,

    /// There are unstable offsets that need to be cleared.
    UNSTABLE_OFFSET_COMMIT = 88,

    /// The throttling quota has been exceeded.
    THROTTLING_QUOTA_EXCEEDED = 89,

    /// There is a newer producer with the same transactionalId which fences the current one.
    PRODUCER_FENCED = 90,

    /// A request illegally referred to a resource that does not exist.
    RESOURCE_NOT_FOUND = 91,

    /// A request illegally referred to the same resource twice.
    DUPLICATE_RESOURCE = 92,

    /// Requested credential would not meet criteria for acceptability.
    UNACCEPTABLE_CREDENTIAL = 93,

    /// Indicates that the either the sender or recipient of a voter-only request is not one of the
    /// expected voters.
    INCONSISTENT_VOTER_SET = 94,

    /// The given update version was invalid.
    INVALID_UPDATE_VERSION = 95,

    /// Unable to update finalized features due to an unexpected server error.
    FEATURE_UPDATE_FAILED = 96,

    /// Request principal deserialization failed during forwarding. This indicates an internal
    /// error on the broker cluster security setup.
    PRINCIPAL_DESERIALIZATION_FAILURE = 97,

    /// Requested snapshot was not found.
    SNAPSHOT_NOT_FOUND = 98,

    /// Requested position is not greater than or equal to zero, and less than the size of the
    /// snapshot.
    POSITION_OUT_OF_RANGE = 99,

    /// This server does not host this topic ID.
    UNKNOWN_TOPIC_ID = 100,

    /// This broker ID is already in use.
    DUPLICATE_BROKER_REGISTRATION = 101,

    /// The given broker ID was not registered.
    BROKER_ID_NOT_REGISTERED = 102,

    /// The log's topic ID did not match the topic ID in the request.
    INCONSISTENT_TOPIC_ID = 103,

    /// The clusterId in the request does not match that found on the server.
    INCONSISTENT_CLUSTER_ID = 104,

    /// The transactionalId could not be found.
    TRANSACTIONAL_ID_NOT_FOUND = 105,

    /// The fetch session encountered inconsistent topic ID usage.
    FETCH_SESSION_TOPIC_ID_ERROR = 106,

    /// The new ISR contains at least one ineligible replica.
    INELIGIBLE_REPLICA = 107,

    /// The AlterPartition request successfully updated the partition state but the leader has
    /// changed.
    NEW_LEADER_ELECTED = 108,

    /// The requested offset is moved to tiered storage.
    OFFSET_MOVED_TO_TIERED_STORAGE = 109,

    /// The member epoch is fenced by the group coordinator. The member must abandon all its
    /// partitions and rejoin.
    FENCED_MEMBER_EPOCH = 110,

    /// The instance ID is still used by another member in the consumer group. That member must
    /// leave first.
    UNRELEASED_INSTANCE_ID = 111,

    /// The assignor or its version range is not supported by the consumer group.
    UNSUPPORTED_ASSIGNOR = 112,

    /// The member epoch is stale. The member must retry after receiving its updated member epoch
    /// via the ConsumerGroupHeartbeat API.
    STALE_MEMBER_EPOCH = 113,

    /// The request was sent to an endpoint of the wrong type.
    MISMATCHED_ENDPOINT_TYPE = 114,

    /// This endpoint type is not supported yet.
    UNSUPPORTED_ENDPOINT_TYPE = 115,

    /// This controller ID is not known.
    UNKNOWN_CONTROLLER_ID = 116,

    /// Client sent a push telemetry request with an invalid or outdated subscription ID.
    UNKNOWN_SUBSCRIPTION_ID = 117,

    /// Client sent a push telemetry request larger than the maximum size the broker will accept.
    TELEMETRY_TOO_LARGE = 118,

    /// The controller has considered the broker registration to be invalid.
    INVALID_REGISTRATION = 119,

    /// The server encountered an error with the transaction. The client can abort the transaction
    /// to continue using this transactional ID.
    TRANSACTION_ABORTABLE = 120,
}

impl crate::kafka::Serialize for ErrorCode {
    const SIZE: usize = 2;

    #[inline]
    fn encode_size(&self, _version: i16) -> usize {
        Self::SIZE
    }
}
