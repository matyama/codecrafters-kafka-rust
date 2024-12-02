use std::panic;
use std::sync::Arc;

use anyhow::Result;
use tokio::task::{self, JoinSet};
use tokio::time::{self, Duration};

use crate::kafka::error::{ErrorCode, KafkaError};
use crate::kafka::request::{self, fetch::*, RequestHeader};
use crate::kafka::response::{self, fetch::*, ResponseBody};
use crate::kafka::types::Uuid;
use crate::kafka::{ApiKey, Serialize as _};
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
}

// TODO: separate Network and I/O (Storage) layers request/response channels
//  - https://www.confluent.io/blog/kafka-producer-and-consumer-internals-4-consumer-fetch-requests
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
        let mut tasks = JoinSet::new();

        for topic in body.topics {
            tasks.spawn({
                let storage = Arc::clone(&self.storage);
                fetch_topic(topic, version, storage)
            });
        }

        let timeout = if body.max_wait_ms > 0 {
            Duration::from_millis(body.max_wait_ms as u64)
        } else {
            Duration::ZERO
        };

        let sleep = time::sleep(timeout);
        tokio::pin!(sleep);

        loop {
            tokio::select! {
                _ = &mut sleep, if !timeout.is_zero() => break,

                maybe_result = tasks.join_next() => {
                    let Some(result) = maybe_result else { break };
                    match result {
                        Ok(response) => responses.push(response),
                        Err(e) if e.is_cancelled() => task::yield_now().await,
                        Err(e) if e.is_panic() => panic::resume_unwind(e.into_panic()),
                        Err(e) => unreachable!("task neither panicked nor was canceled: {e:?}"),
                    }
                },

                else => task::yield_now().await,
            }
        }

        let body = response::Fetch {
            throttle_time_ms: 0,
            error_code: ErrorCode::NONE,
            session_id: body.session_id,
            responses,
            tagged_fields: Default::default(),
        };

        Ok((body.encode_size(version), ResponseBody::Fetch(body)))
    }

    async fn handle_error(&self, err: KafkaError) -> Result<(usize, ResponseBody)> {
        // TODO: throttle_time_ms, session_id for Fetch errors
        let body = response::Fetch::error(0, err.error_code, 0);
        Ok((body.encode_size(err.api_version), ResponseBody::Fetch(body)))
    }
}

async fn fetch_topic(
    ft: FetchTopic,
    version: i16,
    storage: Arc<Storage>,
) -> FetchableTopicResponse {
    let Some(topic) = ft.topic_id(version) else {
        return unknown_topic_response(ft);
    };

    // TODO: (debug) assert this assumption
    // NOTE: we assume that FetchTopic only contains one offset per unique partition

    let mut partitions = Vec::with_capacity(ft.partitions.len());
    let mut tasks = JoinSet::new();

    for fp in ft.partitions {
        tasks.spawn({
            let topic = topic.clone();
            let storage = Arc::clone(&storage);
            fetch_partition(topic, fp, storage)
        });
    }

    while let Some(result) = tasks.join_next().await {
        match result {
            Ok(data) => partitions.push(data),
            Err(e) if e.is_cancelled() => task::yield_now().await,
            Err(e) if e.is_panic() => panic::resume_unwind(e.into_panic()),
            Err(e) => unreachable!("task neither panicked nor was canceled: {e:?}"),
        }
    }

    FetchableTopicResponse {
        topic: ft.topic,
        topic_id: ft.topic_id,
        partitions,
        tagged_fields: Default::default(),
    }
}

async fn fetch_partition(topic: Uuid, fp: FetchPartition, storage: Arc<Storage>) -> PartitionData {
    println!(
        "Fetching ({topic}, {}): {:?}",
        fp.partition, fp.fetch_offset
    );

    let records = storage
        .fetch_records(topic, fp.partition, fp.fetch_offset)
        .await;

    use storage::FetchResult::*;

    match records {
        Ok(Records(records)) => {
            // TODO: fill in actual partition data
            PartitionData::new(fp.partition, ErrorCode::NONE, 0).with_records(records)
        }
        Ok(UnknownTopic) => PartitionData::new(fp.partition, ErrorCode::UNKNOWN_TOPIC_ID, 0),
        Ok(OffsetOutOfRange) => PartitionData::new(fp.partition, ErrorCode::OFFSET_OUT_OF_RANGE, 0),
        Err(_) => PartitionData::new(fp.partition, ErrorCode::KAFKA_STORAGE_ERROR, 0),
    }
}

fn unknown_topic_response(ft: FetchTopic) -> FetchableTopicResponse {
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
