use anyhow::Result;

use crate::kafka::error::{ErrorCode, KafkaError};
use crate::kafka::request::{self, fetch::*, RequestHeader};
use crate::kafka::response::{self, fetch::*, ResponseBody};
use crate::kafka::types::Uuid;
use crate::kafka::{ApiKey, WireSize as _};

use super::Handler;

pub struct FetchHandler;

impl FetchHandler {
    // TODO: test "known topic" based on current state / stored partition metadata
    // NOTE: known topic is currently 00000000-0000-4000-0000-000000000000 and above
    const KNOWN_TOPIC: Uuid = Uuid::from_static(&[0, 0, 0, 0, 0, 0, 0, 64, 0, 0, 0, 0, 0, 0, 0, 0]);

    fn is_known_topic(&self, version: i16, topic: &FetchTopic) -> bool {
        match version {
            0..=12 => Uuid::try_from(topic.topic.clone()).map_or(false, |t| t >= Self::KNOWN_TOPIC),
            _ => topic.topic_id >= Self::KNOWN_TOPIC,
        }
    }

    fn fetch_topic(&self, version: i16, ft: FetchTopic) -> FetchableTopicResponse {
        // TODO: handle old API versions (i.e., `ft.topic -> Uuid`)
        let topic = ft.topic_id.clone();

        let partitions = if self.is_known_topic(version, &ft) {
            // TODO: fetch partitions concurrently
            // XXX: should this be grouped by partition first for better storage access?
            ft.partitions
                .into_iter()
                .map(|fp| self.fetch_partition(version, topic.clone(), fp))
                .collect()
        } else {
            ft.partitions
                .into_iter()
                .map(|fp| PartitionData::new(fp.partition, ErrorCode::UNKNOWN_TOPIC_ID, 0))
                .collect()
        };

        FetchableTopicResponse {
            topic: ft.topic,
            topic_id: ft.topic_id,
            partitions,
            tagged_fields: Default::default(),
        }
    }

    fn fetch_partition(&self, _version: i16, topic: Uuid, fp: FetchPartition) -> PartitionData {
        println!(
            "Fetching ({topic}, {}): {:?}",
            fp.partition, fp.fetch_offset
        );

        // TODO: fill in actual partition data
        //let mut data = PartitionData::new(fp.partition, ErrorCode::NONE, 0);
        //data.last_start_offset = fp.fetch_offset;
        //data.last_stable_offset = fp.fetch_offset;

        // TODO: properly serialized records (seek and memcpy from fs)
        // https://kafka.apache.org/documentation/#recordbatch
        //data.records.replace(bytes::Bytes::new());

        PartitionData::new(fp.partition, ErrorCode::NONE, 0)
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

        // TODO: actual impl
        let responses = body
            .topics
            .into_iter()
            .map(|topic| self.fetch_topic(version, topic))
            .collect();

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
