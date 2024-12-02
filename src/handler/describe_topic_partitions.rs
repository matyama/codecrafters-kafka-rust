use std::sync::Arc;

use anyhow::Result;

use crate::kafka::error::KafkaError;
use crate::kafka::request::{self, RequestHeader};
use crate::kafka::response::{self, ResponseBody};
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
        _body: Self::RequestBody,
    ) -> Result<(usize, ResponseBody)> {
        let version = header.request_api_version.into_inner();

        // TODO
        //let mut topics = Vec::new();

        //for _ in 0..10 {
        //    let topic_id = todo!();
        //    let topic = DescribeTopicPartitionsResponseTopic::new(topic_id);
        //    topics.push(topic);
        //}

        let body = response::DescribeTopicPartitions {
            throttle_time_ms: 0,
            topics: Vec::new(),
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
