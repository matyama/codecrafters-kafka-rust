use std::sync::Arc;

use anyhow::Result;

use crate::kafka::error::{ErrorCode, KafkaError};
use crate::kafka::request::{self, RequestHeader};
use crate::kafka::response::{self, describe_topic_partitions::*, ResponseBody};
use crate::kafka::types::Uuid;
use crate::kafka::{ApiKey, WireSize as _};
use crate::storage::Storage;

use super::Handler;

pub struct DescribeTopicPartitionsHandler {
    #[allow(dead_code)]
    storage: Arc<Storage>,
}

impl DescribeTopicPartitionsHandler {
    #[inline]
    pub fn new(storage: Arc<Storage>) -> Self {
        Self { storage }
    }
}

impl Handler for DescribeTopicPartitionsHandler {
    const API_KEY: ApiKey = ApiKey::DescribeTopicPartitions;

    type RequestBody = request::describe_topic_partitions::DescribeTopicPartitions;

    async fn handle_message(
        &self,
        header: &RequestHeader,
        body: Self::RequestBody,
    ) -> Result<(usize, ResponseBody)> {
        let version = header.request_api_version.into_inner();

        let mut topics = Vec::with_capacity(body.topics.len());

        // TODO: respect body.response_partition_limit and use Cursor
        for topic in body.topics {
            let topic = match self.storage.describe_topic(&topic.name).await {
                Some((topic_id, partititons)) => {
                    let mut topic =
                        DescribeTopicPartitionsResponseTopic::new(topic_id).with_name(topic.name);

                    topic.partitions = partititons
                        .into_iter()
                        .map(DescribeTopicPartitionsResponsePartition::new)
                        .collect();

                    topic
                }
                None => DescribeTopicPartitionsResponseTopic::new(Uuid::zero())
                    .with_name(topic.name)
                    .with_err(ErrorCode::UNKNOWN_TOPIC_OR_PARTITION),
            };

            topics.push(topic);
        }

        let body = response::DescribeTopicPartitions {
            throttle_time_ms: 0,
            topics,
            cursor: None,
            ..Default::default()
        };

        let size = body.size(version);
        Ok((size, ResponseBody::DescribeTopicPartitions(body)))
    }

    async fn handle_error(&self, err: KafkaError) -> Result<(usize, ResponseBody)> {
        // TODO: use err.error_code
        let body = response::DescribeTopicPartitions::default();
        let size = body.size(err.api_version);
        Ok((size, ResponseBody::DescribeTopicPartitions(body)))
    }
}
