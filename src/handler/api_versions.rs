use anyhow::Result;

use crate::kafka::error::{ErrorCode, KafkaError};
use crate::kafka::request::{self, RequestHeader};
use crate::kafka::response::{self, api_versions::*, ResponseBody};
use crate::kafka::{ApiKey, Serialize as _};

use super::Handler;

pub struct ApiVersionsHandler;

impl Handler for ApiVersionsHandler {
    const API_KEY: ApiKey = ApiKey::ApiVersions;

    type RequestBody = request::api_versions::ApiVersions;

    async fn handle_message(
        &self,
        header: &RequestHeader,
        _body: Self::RequestBody,
    ) -> Result<(usize, ResponseBody)> {
        let version = header.request_api_version.into_inner();

        let body = ApiVersions {
            api_keys: ApiKey::iter().filter_map(ApiVersion::new).collect(),
            ..Default::default()
        };

        Ok((body.encode_size(version), ResponseBody::ApiVersions(body)))
    }

    async fn handle_error(&self, err: KafkaError) -> Result<(usize, ResponseBody)> {
        // Starting from Apache Kafka 2.4 (KIP-511), ApiKeys field is populated with the
        // supported versions of the ApiVersionsRequest when an UNSUPPORTED_VERSION error is
        // returned.
        let body = match err.error_code {
            error_code @ ErrorCode::UNSUPPORTED_VERSION => response::ApiVersions {
                error_code,
                api_keys: [Self::API_KEY]
                    .into_iter()
                    .filter_map(ApiVersion::new)
                    .collect(),
                ..Default::default()
            },
            error_code => response::ApiVersions {
                error_code,
                ..Default::default()
            },
        };

        Ok((
            body.encode_size(err.api_version),
            ResponseBody::ApiVersions(body),
        ))
    }
}
