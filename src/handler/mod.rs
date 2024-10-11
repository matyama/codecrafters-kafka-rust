use anyhow::Result;

use crate::kafka::api::ApiKey;
use crate::kafka::error::KafkaError;
use crate::kafka::request::RequestHeader;
use crate::kafka::response::ResponseBody;

pub use api_versions::ApiVersionsHandler;
pub use fetch::FetchHandler;

mod api_versions;
mod fetch;

// TODO: resolve async_fn_in_trait lint and remove the allow
#[allow(async_fn_in_trait)]
pub trait Handler {
    const API_KEY: ApiKey;

    type RequestBody;

    async fn handle_message(
        &self,
        header: &RequestHeader,
        body: Self::RequestBody,
    ) -> Result<(usize, ResponseBody)>;

    async fn handle_error(&self, err: KafkaError) -> Result<(usize, ResponseBody)>;
}
