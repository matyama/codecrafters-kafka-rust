use std::ops::ControlFlow;
use std::sync::Arc;

use anyhow::{bail, Result};

use crate::kafka::common::Cursor;
use crate::kafka::error::{ErrorCode, KafkaError};
use crate::kafka::request::{self, RequestHeader};
use crate::kafka::response::{self, describe_topic_partitions::*, ResponseBody};
use crate::kafka::{ApiKey, Serialize as _};
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
        let mut cursor = None;
        let mut partition_limit = body.response_partition_limit as usize;

        let (skip_cnt, mut start_partition) = if let Some(cursor) = body.cursor {
            let topic = &cursor.topic_name;
            let Some(pos) = body.topics.iter().position(|t| &t.name == topic) else {
                bail!("unknown topic {topic:?} in DescribeTopicPartitions cursor");
            };
            (pos.saturating_sub(1), Some(cursor.partition_index))
        } else {
            (0, Some(0))
        };

        let mut query_topics = body.topics.into_iter().skip(skip_cnt);

        while let Some(topic) = query_topics.next() {
            // for topics following `cursor.topic_name`, `cursor.partition_index` should be ignored
            let min_partition = start_partition.take().unwrap_or(0);

            let (topic, next) = match self
                .storage
                .describe_topic(&topic.name, min_partition, partition_limit)
                .await
            {
                Some((topic_id, partititons, next)) => {
                    let mut topic =
                        DescribeTopicPartitionsResponseTopic::new(topic_id).with_name(topic.name);

                    topic.partitions = partititons
                        .into_iter()
                        .map(DescribeTopicPartitionsResponsePartition::new)
                        .collect();

                    (topic, next)
                }
                None => {
                    let error_code = ErrorCode::UNKNOWN_TOPIC_OR_PARTITION;

                    let topic =
                        DescribeTopicPartitionsResponseTopic::err(error_code).with_name(topic.name);

                    // NOTE: UNKNOWN_TOPIC_OR_PARTITION adds one partition
                    let next = if partition_limit > 1 {
                        ControlFlow::Continue(())
                    } else {
                        ControlFlow::Break(None)
                    };

                    (topic, next)
                }
            };

            partition_limit = partition_limit.saturating_sub(topic.partitions.len());
            topics.push(topic);

            match next {
                ControlFlow::Continue(()) => continue,

                // limit exceeded with some partition left => continue where we ended
                ControlFlow::Break(Some((topic_name, partition_index))) => {
                    debug_assert_eq!(partition_limit, 0);

                    let _ = cursor.insert(Cursor {
                        topic_name,
                        partition_index,
                        tagged_fields: Default::default(),
                    });

                    break;
                }

                // limit exceeded at the very last partition of a topic => take next topic
                ControlFlow::Break(None) => {
                    debug_assert_eq!(partition_limit, 0);

                    if let Some(topic_name) = query_topics.next().map(|t| t.name) {
                        // NOTE: partitions always start from zero
                        let _ = cursor.insert(Cursor {
                            topic_name,
                            partition_index: 0,
                            tagged_fields: Default::default(),
                        });
                    }

                    break;
                }
            }
        }

        let body = response::DescribeTopicPartitions {
            throttle_time_ms: 0,
            topics,
            cursor,
            ..Default::default()
        };

        let size = body.encode_size(version);
        Ok((size, ResponseBody::DescribeTopicPartitions(body)))
    }

    async fn handle_error(&self, err: KafkaError) -> Result<(usize, ResponseBody)> {
        let topic = DescribeTopicPartitionsResponseTopic::err(err.error_code);
        let body = response::DescribeTopicPartitions::default().with_topic(topic);
        let size = body.encode_size(err.api_version);
        Ok((size, ResponseBody::DescribeTopicPartitions(body)))
    }
}
