use std::sync::Arc;

use anyhow::Result;

use crate::kafka::error::{ErrorCode, KafkaError};
use crate::kafka::request::{self, fetch::*, RequestHeader};
use crate::kafka::response::{self, fetch::*, ResponseBody};
use crate::kafka::types::Uuid;
use crate::kafka::{ApiKey, WireSize as _};
use crate::storage::{self, Storage};

use super::Handler;

pub struct FetchHandler {
    storage: Arc<Storage>,
}

impl FetchHandler {
    #[inline]
    pub fn new(storage: Arc<Storage>) -> Self {
        Self { storage }
    }

    async fn fetch_topic(&self, version: i16, ft: FetchTopic) -> FetchableTopicResponse {
        let Some(topic) = ft.topic_id(version) else {
            return unknonw_topic_response(ft);
        };

        // TODO: fetch partitions concurrently (tokio::spawn)
        // XXX: should this be grouped by partition first for better storage access?
        //  - but also return (in response) with partitions in the original order
        let mut partitions = Vec::with_capacity(ft.partitions.len());

        for fp in ft.partitions {
            let partition = self.fetch_partition(version, topic.clone(), fp).await;
            partitions.push(partition);
        }

        FetchableTopicResponse {
            topic: ft.topic,
            topic_id: ft.topic_id,
            partitions,
            tagged_fields: Default::default(),
        }
    }

    async fn fetch_partition(
        &self,
        _version: i16,
        topic: Uuid,
        fp: FetchPartition,
    ) -> PartitionData {
        println!(
            "Fetching ({topic}, {}): {:?}",
            fp.partition, fp.fetch_offset
        );

        // FIXME: concurrently this fetches the whole segment, given the fetch offset as the base
        let records = self
            .storage
            .fetch_records(topic, fp.partition, fp.fetch_offset)
            .await;

        use storage::FetchResult::*;

        match records {
            Ok(Records(records)) => {
                // TODO: fill in actual partition data
                PartitionData::new(fp.partition, ErrorCode::NONE, 0).with_records(records)
            }
            Ok(UnknownTopic) => PartitionData::new(fp.partition, ErrorCode::UNKNOWN_TOPIC_ID, 0),
            Ok(OffsetOutOfRange) => {
                PartitionData::new(fp.partition, ErrorCode::OFFSET_OUT_OF_RANGE, 0)
            }
            Err(_) => PartitionData::new(fp.partition, ErrorCode::KAFKA_STORAGE_ERROR, 0),
        }
    }
}

impl Handler for FetchHandler {
    const API_KEY: ApiKey = ApiKey::Fetch;

    type RequestBody = request::fetch::Fetch;

    async fn handle_message(
        &self,
        header: &RequestHeader,
        body: Self::RequestBody,
    ) -> Result<(usize, ResponseBody)> {
        let version = header.request_api_version.into_inner();

        let mut responses = Vec::with_capacity(body.topics.len());

        for topic in body.topics {
            // TODO: tokio::spawn / JoinSet => Arc<Storage>
            let response = self.fetch_topic(version, topic).await;
            responses.push(response);
        }

        let body = response::Fetch {
            throttle_time_ms: 0,
            error_code: ErrorCode::NONE,
            session_id: 0,
            responses,
            ..Default::default()
        };

        Ok((body.size(version), ResponseBody::Fetch(body)))
    }

    async fn handle_error(&self, err: KafkaError) -> Result<(usize, ResponseBody)> {
        // TODO: throttle_time_ms, session_id for Fetch errors
        let body = response::Fetch::error(0, err.error_code, 0);
        Ok((body.size(err.api_version), ResponseBody::Fetch(body)))
    }
}

fn unknonw_topic_response(ft: FetchTopic) -> FetchableTopicResponse {
    let partitions = ft
        .partitions
        .into_iter()
        .map(|fp| PartitionData::new(fp.partition, ErrorCode::UNKNOWN_TOPIC_ID, 0))
        .collect();

    FetchableTopicResponse {
        topic: ft.topic,
        topic_id: ft.topic_id,
        partitions,
        tagged_fields: Default::default(),
    }
}
