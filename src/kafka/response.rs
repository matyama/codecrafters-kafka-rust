use crate::kafka::error::ErrorCode;

/// Response header v0
#[derive(Debug)]
pub struct ResponseHeader {
    pub correlation_id: i32,
    // TODO: Response Header v1 => correlation_id TAG_BUFFER
}

#[derive(Debug)]
pub struct ResponseMessage {
    pub size: i32,
    pub header: ResponseHeader,
    pub body: ResponseBody,
}

impl ResponseMessage {
    #[inline]
    pub fn new(size: i32, correlation_id: i32, body: ResponseBody) -> Self {
        Self {
            size,
            header: ResponseHeader { correlation_id },
            body,
        }
    }
}

#[derive(Debug)]
pub enum ResponseBody {
    // TODO: other fields
    //
    // ApiVersions Response (Version: 0) => error_code [api_keys]
    //  error_code => INT16
    //  api_keys => api_key min_version max_version
    //    api_key => INT16
    //    min_version => INT16
    //    max_version => INT16
    ApiVersions { error_code: ErrorCode },
}

// XXX: ideally use something like strum or derive-more for this
impl std::fmt::Display for ResponseBody {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ApiVersions { .. } => write!(f, "ApiVersions"),
        }
    }
}
