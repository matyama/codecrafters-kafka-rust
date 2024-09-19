use anyhow::Result;
use bytes::{Buf, Bytes};

use crate::kafka::Deserialize;

// TODO: implement
#[derive(Debug)]
#[repr(transparent)]
pub struct Fetch(Bytes);

impl Deserialize for Fetch {
    fn read_from<B: Buf>(buf: &mut B, _version: i16) -> Result<(Self, usize)> {
        let size = buf.remaining();

        let data = if buf.has_remaining() {
            buf.copy_to_bytes(size)
        } else {
            Bytes::default()
        };

        Ok((Self(data), size))
    }
}
