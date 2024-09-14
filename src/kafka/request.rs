use bytes::Bytes;

#[derive(Debug)]
pub enum Request {
    // TODO: structural data
    ApiVersions { size: i32, data: Bytes },
}
