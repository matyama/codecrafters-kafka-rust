use std::collections::BTreeMap;

use anyhow::{bail, ensure, Context as _, Result};
use bytes::{Buf, Bytes, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::kafka::{AsyncDeserialize, AsyncSerialize, Deserialize, Serialize};

const EMPTY: Bytes = Bytes::from_static(&[]);

impl Serialize for bool {
    const SIZE: usize = 1;

    #[inline]
    fn encode_size(&self, _version: i16) -> usize {
        Self::SIZE
    }
}

impl AsyncSerialize for bool {
    #[inline]
    async fn write_into<W>(self, writer: &mut W, _version: i16) -> Result<()>
    where
        W: AsyncWriteExt + Send + Unpin + ?Sized,
    {
        writer.write_u8(self as u8).await.context("bool")
    }
}

/// BOOLEAN (`bool`)
///
/// Represents a boolean value in a byte. Values 0 and 1 are used to represent false and true
/// respectively. When reading a boolean value, any non-zero value is considered true.
impl Deserialize for bool {
    fn decode<B: Buf>(buf: &mut B, _version: i16) -> Result<(Self, usize)> {
        ensure!(buf.has_remaining(), "not enough bytes left");
        Ok((buf.get_u8() > 0, 1))
    }
}

impl AsyncDeserialize for bool {
    async fn read_from<R>(reader: &mut R, _version: i16) -> Result<(Self, usize)>
    where
        R: AsyncReadExt + Send + Unpin,
    {
        Ok((reader.read_u8().await? > 0, 1))
    }
}

// TODO: deduplicate these primitive impls via a macro
impl Deserialize for i8 {
    fn decode<B: Buf>(buf: &mut B, _version: i16) -> Result<(Self, usize)> {
        ensure!(buf.remaining() >= 1, "not enough bytes left");
        Ok((buf.get_i8(), 1))
    }
}

impl AsyncDeserialize for i8 {
    async fn read_from<R>(reader: &mut R, _version: i16) -> Result<(Self, usize)>
    where
        R: AsyncReadExt + Send + Unpin,
    {
        Ok((reader.read_i8().await?, 1))
    }
}

impl Deserialize for i16 {
    fn decode<B: Buf>(buf: &mut B, _version: i16) -> Result<(Self, usize)> {
        ensure!(buf.remaining() >= 2, "not enough bytes left");
        Ok((buf.get_i16(), 2))
    }
}

impl AsyncDeserialize for i16 {
    async fn read_from<R>(reader: &mut R, _version: i16) -> Result<(Self, usize)>
    where
        R: AsyncReadExt + Send + Unpin,
    {
        Ok((reader.read_i16().await?, 2))
    }
}

impl Deserialize for u32 {
    fn decode<B: Buf>(buf: &mut B, _version: i16) -> Result<(Self, usize)> {
        ensure!(buf.remaining() >= 4, "not enough bytes left");
        Ok((buf.get_u32(), 4))
    }
}

impl AsyncDeserialize for u32 {
    async fn read_from<R>(reader: &mut R, _version: i16) -> Result<(Self, usize)>
    where
        R: AsyncReadExt + Send + Unpin,
    {
        Ok((reader.read_u32().await?, 4))
    }
}

impl Deserialize for i32 {
    fn decode<B: Buf>(buf: &mut B, _version: i16) -> Result<(Self, usize)> {
        ensure!(buf.remaining() >= 4, "not enough bytes left");
        Ok((buf.get_i32(), 4))
    }
}

impl AsyncDeserialize for i32 {
    async fn read_from<R>(reader: &mut R, _version: i16) -> Result<(Self, usize)>
    where
        R: AsyncReadExt + Send + Unpin,
    {
        Ok((reader.read_i32().await?, 4))
    }
}

impl AsyncSerialize for i32 {
    #[inline]
    async fn write_into<W>(self, writer: &mut W, _version: i16) -> Result<()>
    where
        W: AsyncWriteExt + Send + Unpin + ?Sized,
    {
        writer.write_i32(self).await.context("i32")
    }
}

impl Serialize for i32 {
    const SIZE: usize = 4;

    #[inline]
    fn encode_size(&self, _version: i16) -> usize {
        Self::SIZE
    }
}

impl Deserialize for i64 {
    fn decode<B: Buf>(buf: &mut B, _version: i16) -> Result<(Self, usize)> {
        ensure!(buf.remaining() >= 8, "not enough bytes left");
        Ok((buf.get_i64(), 8))
    }
}

impl AsyncDeserialize for i64 {
    async fn read_from<R>(reader: &mut R, _version: i16) -> Result<(Self, usize)>
    where
        R: AsyncReadExt + Send + Unpin,
    {
        Ok((reader.read_i64().await?, 8))
    }
}

/// VARINT
///
/// Represents a 32-bit integer. Encoding follows the variable-length zig-zag encoding from Google
/// Protocol Buffers.
#[derive(Debug, Copy, Clone, Default)]
#[repr(transparent)]
pub struct VarInt(pub(crate) i32);

impl Serialize for VarInt {
    #[inline]
    fn encode_size(&self, version: i16) -> usize {
        UnsignedVarInt::from(*self).encode_size(version)
    }
}

impl AsyncSerialize for VarInt {
    async fn write_into<W>(self, writer: &mut W, version: i16) -> Result<()>
    where
        W: AsyncWriteExt + Send + Unpin + ?Sized,
    {
        UnsignedVarInt::from(self).write_into(writer, version).await
    }
}

impl Deserialize for VarInt {
    fn decode<B: Buf>(buf: &mut B, version: i16) -> Result<(Self, usize)> {
        UnsignedVarInt::decode(buf, version).map(|(zigzag, bytes)| (Self::from(zigzag), bytes))
    }
}

impl AsyncDeserialize for VarInt {
    async fn read_from<R>(reader: &mut R, version: i16) -> Result<(Self, usize)>
    where
        R: AsyncReadExt + Send + Unpin,
    {
        let (zigzag, bytes) = UnsignedVarInt::read_from(reader, version).await?;
        Ok((Self::from(zigzag), bytes))
    }
}

impl From<UnsignedVarInt> for VarInt {
    #[inline]
    fn from(UnsignedVarInt(zigzag): UnsignedVarInt) -> Self {
        Self(((zigzag >> 1) as i32) ^ (-((zigzag & 1) as i32)))
    }
}

impl From<VarInt> for UnsignedVarInt {
    #[inline]
    fn from(VarInt(value): VarInt) -> Self {
        Self(((value << 1) ^ (value >> 31)) as u32)
    }
}

impl TryFrom<VarInt> for usize {
    type Error = anyhow::Error;

    fn try_from(VarInt(i): VarInt) -> Result<Self> {
        ensure!(i >= 0, "VARINT {i} cannot be converted to usize");
        Ok(i as usize)
    }
}

/// UNSIGNED_VARINT
///
/// Represents an unsigned 32-bit integer. Encoding follows the variable-length zig-zag encoding
/// from Google Protocol Buffers.
#[derive(Debug, Copy, Clone, Default)]
#[repr(transparent)]
pub struct UnsignedVarInt(u32);

impl Serialize for UnsignedVarInt {
    fn encode_size(&self, _version: i16) -> usize {
        match self.0 {
            0x0..=0x7f => 1,
            0x80..=0x3fff => 2,
            0x4000..=0x1fffff => 3,
            0x200000..=0xfffffff => 4,
            0x10000000..=0xffffffff => 5,
        }
    }
}

impl AsyncSerialize for UnsignedVarInt {
    async fn write_into<W>(self, writer: &mut W, _version: i16) -> Result<()>
    where
        W: AsyncWriteExt + Send + Unpin + ?Sized,
    {
        // TODO: use local [0; MAX_SIZE] buf and write just once

        let mut value = self.0;

        while value >= 0x80 {
            // buf.write_u8(...)
            writer.write_u8((value as u8) | 0x80).await?;
            value >>= 7;
        }

        // buf.write_u8(...)
        writer.write_u8(value as u8).await?;

        Ok(())
    }
}

impl Deserialize for UnsignedVarInt {
    fn decode<B: Buf>(buf: &mut B, _version: i16) -> Result<(Self, usize)> {
        let mut value = 0;
        let mut bytes = 0;
        for i in 0..5 {
            ensure!(buf.has_remaining(), "not enough bytes left");
            let b = buf.get_u8() as u32;
            value |= (b & 0x7F) << (i * 7);
            bytes += 1;
            if b < 0x80 {
                break;
            }
        }
        Ok((Self(value), bytes))
    }
}

impl AsyncDeserialize for UnsignedVarInt {
    async fn read_from<R>(reader: &mut R, _version: i16) -> Result<(Self, usize)>
    where
        R: AsyncReadExt + Send + Unpin,
    {
        let mut value = 0;
        let mut bytes = 0;
        for i in 0..5 {
            let b = reader.read_u8().await? as u32;
            value |= (b & 0x7F) << (i * 7);
            bytes += 1;
            if b < 0x80 {
                break;
            }
        }
        Ok((Self(value), bytes))
    }
}

/// VARLONG
///
/// Represents a 64-bit integer. Encoding follows the variable-length zig-zag encoding from Google
/// Protocol Buffers.
#[derive(Debug, Copy, Clone, Default)]
#[repr(transparent)]
pub struct VarLong(pub(crate) i64);

impl Serialize for VarLong {
    #[inline]
    fn encode_size(&self, version: i16) -> usize {
        UnsignedVarLong::from(*self).encode_size(version)
    }
}

impl AsyncSerialize for VarLong {
    async fn write_into<W>(self, writer: &mut W, version: i16) -> Result<()>
    where
        W: AsyncWriteExt + Send + Unpin + ?Sized,
    {
        UnsignedVarLong::from(self)
            .write_into(writer, version)
            .await
    }
}

impl Deserialize for VarLong {
    fn decode<B: Buf>(buf: &mut B, version: i16) -> Result<(Self, usize)> {
        UnsignedVarLong::decode(buf, version).map(|(zigzag, bytes)| (Self::from(zigzag), bytes))
    }
}

impl AsyncDeserialize for VarLong {
    async fn read_from<R>(reader: &mut R, version: i16) -> Result<(Self, usize)>
    where
        R: AsyncReadExt + Send + Unpin,
    {
        let (zigzag, bytes) = UnsignedVarLong::read_from(reader, version).await?;
        Ok((Self::from(zigzag), bytes))
    }
}

impl From<UnsignedVarLong> for VarLong {
    #[inline]
    fn from(UnsignedVarLong(zigzag): UnsignedVarLong) -> Self {
        Self(((zigzag >> 1) as i64) ^ (-((zigzag & 1) as i64)))
    }
}

impl From<VarLong> for UnsignedVarLong {
    #[inline]
    fn from(VarLong(value): VarLong) -> Self {
        Self(((value << 1) ^ (value >> 63)) as u64)
    }
}

/// UNSIGNED_VARLONG
///
/// Represents an unsigned 64-bit integer. Encoding follows the variable-length zig-zag encoding
/// from Google Protocol Buffers.
#[derive(Debug, Copy, Clone, Default)]
#[repr(transparent)]
pub struct UnsignedVarLong(u64);

impl Serialize for UnsignedVarLong {
    fn encode_size(&self, _version: i16) -> usize {
        match self.0 {
            0x0..=0x7f => 1,
            0x80..=0x3fff => 2,
            0x4000..=0x1fffff => 3,
            0x200000..=0xfffffff => 4,
            0x10000000..=0x7ffffffff => 5,
            0x800000000..=0x3ffffffffff => 6,
            0x40000000000..=0x1ffffffffffff => 7,
            0x2000000000000..=0xffffffffffffff => 8,
            0x100000000000000..=0x7fffffffffffffff => 9,
            0x8000000000000000..=0xffffffffffffffff => 10,
        }
    }
}

impl AsyncSerialize for UnsignedVarLong {
    async fn write_into<W>(self, writer: &mut W, _version: i16) -> Result<()>
    where
        W: AsyncWriteExt + Send + Unpin + ?Sized,
    {
        // TODO: use local [0; MAX_SIZE] buf and write just once

        let mut value = self.0;

        while value >= 0x80 {
            // buf.write_u8(...)
            writer.write_u8((value as u8) | 0x80).await?;
            value >>= 7;
        }

        // buf.write_u8(...)
        writer.write_u8(value as u8).await?;

        Ok(())
    }
}

impl Deserialize for UnsignedVarLong {
    fn decode<B: Buf>(buf: &mut B, _version: i16) -> Result<(Self, usize)> {
        let mut value = 0;
        let mut bytes = 0;
        for i in 0..10 {
            ensure!(buf.has_remaining(), "not enough bytes left");
            let b = buf.get_u8() as u64;
            value |= (b & 0x7F) << (i * 7);
            bytes += 1;
            if b < 0x80 {
                break;
            }
        }
        Ok((Self(value), bytes))
    }
}

impl AsyncDeserialize for UnsignedVarLong {
    async fn read_from<R>(reader: &mut R, _version: i16) -> Result<(Self, usize)>
    where
        R: AsyncReadExt + Send + Unpin,
    {
        let mut value = 0;
        let mut bytes = 0;
        for i in 0..10 {
            let b = reader.read_u8().await? as u64;
            value |= (b & 0x7F) << (i * 7);
            bytes += 1;
            if b < 0x80 {
                break;
            }
        }
        Ok((Self(value), bytes))
    }
}

impl Serialize for Bytes {
    const SIZE: usize = 4;

    #[inline]
    fn encode_size(&self, _version: i16) -> usize {
        Self::SIZE + self.len()
    }
}

impl Serialize for Option<Bytes> {
    const SIZE: usize = Bytes::SIZE;

    #[inline]
    fn encode_size(&self, version: i16) -> usize {
        self.as_ref().map_or(Self::SIZE, |b| b.encode_size(version))
    }
}

impl AsyncSerialize for Bytes {
    async fn write_into<W>(self, writer: &mut W, _version: i16) -> Result<()>
    where
        W: AsyncWriteExt + Send + Unpin + ?Sized,
    {
        writer
            .write_i32(self.len() as i32)
            .await
            .context("bytes length")?;

        writer.write_all(&self).await.context("raw bytes")
    }
}

impl AsyncSerialize for Option<Bytes> {
    async fn write_into<W>(self, writer: &mut W, version: i16) -> Result<()>
    where
        W: AsyncWriteExt + Send + Unpin + ?Sized,
    {
        if let Some(bytes) = self {
            bytes.write_into(writer, version).await
        } else {
            writer.write_i32(-1).await.context("bytes length")
        }
    }
}

#[derive(Clone, Debug)]
#[repr(transparent)]
pub struct CompactBytes(pub Bytes);

impl CompactBytes {
    pub(crate) fn enc_len(&self) -> UnsignedVarInt {
        UnsignedVarInt((self.0.len() as u32) + 1)
    }
}

impl Serialize for CompactBytes {
    #[inline]
    fn encode_size(&self, version: i16) -> usize {
        self.enc_len().encode_size(version) + self.0.len()
    }
}

impl Serialize for Option<CompactBytes> {
    #[inline]
    fn encode_size(&self, version: i16) -> usize {
        self.as_ref().map_or(1, |b| b.encode_size(version))
    }
}

impl AsyncSerialize for CompactBytes {
    async fn write_into<W>(self, writer: &mut W, version: i16) -> Result<()>
    where
        W: AsyncWriteExt + Send + Unpin + ?Sized,
    {
        self.enc_len()
            .write_into(writer, version)
            .await
            .context("compact bytes length")?;

        writer.write_all(&self.0).await.context("compact bytes")
    }
}

impl AsyncSerialize for Option<CompactBytes> {
    async fn write_into<W>(self, writer: &mut W, version: i16) -> Result<()>
    where
        W: AsyncWriteExt + Send + Unpin + ?Sized,
    {
        if let Some(b) = self {
            b.write_into(writer, version).await
        } else {
            UnsignedVarInt(0)
                .write_into(writer, version)
                .await
                .context("compact bytes length")
        }
    }
}

/// [`Uuid`] bytes wrapper for displaying in (lowercase) hexadecimal format.
#[derive(Clone, Debug)]
#[repr(transparent)]
pub(crate) struct UuidHex<'a>(&'a [u8]);

impl std::fmt::Display for UuidHex<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (i, b) in self.0.iter().enumerate() {
            match i {
                4 | 6 | 8 | 10 => write!(f, "-{b:02x}")?,
                _ => write!(f, "{b:02x}")?,
            }
        }
        Ok(())
    }
}

/// UUID v4 bytes wrapper
#[derive(Clone, Debug, Default, PartialOrd, Ord, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct Uuid(Bytes);

impl Uuid {
    #[inline]
    pub const fn from_static(bytes: &'static [u8; Self::SIZE]) -> Self {
        Self(Bytes::from_static(bytes))
    }

    #[inline]
    pub const fn zero() -> Self {
        Self::from_static(&[0; Self::SIZE])
    }

    #[inline]
    pub(crate) fn as_hex(&self) -> UuidHex<'_> {
        UuidHex(&self.0)
    }
}

impl Serialize for Uuid {
    const SIZE: usize = 16;

    #[inline]
    fn encode_size(&self, _version: i16) -> usize {
        Self::SIZE
    }
}

impl AsyncSerialize for Uuid {
    async fn write_into<W>(self, writer: &mut W, _version: i16) -> Result<()>
    where
        W: AsyncWriteExt + Send + Unpin + ?Sized,
    {
        writer.write_all(&self.0).await.context("UUID bytes")
    }
}

impl Deserialize for Uuid {
    fn decode<B: Buf>(buf: &mut B, _version: i16) -> Result<(Self, usize)> {
        ensure!(buf.remaining() >= 16, "not enough bytes left");
        Ok((Self(buf.copy_to_bytes(16)), 16))
    }
}

impl AsyncDeserialize for Uuid {
    async fn read_from<R>(reader: &mut R, _version: i16) -> Result<(Self, usize)>
    where
        R: AsyncReadExt + Send + Unpin,
    {
        let mut buf = BytesMut::with_capacity(16);
        buf.resize(16, 0);

        let bytes = reader.read_exact(&mut buf).await?;

        Ok((Self(buf.freeze()), bytes))
    }
}

impl std::fmt::Display for Uuid {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.as_hex().fmt(f)
    }
}

impl TryFrom<Bytes> for Uuid {
    type Error = anyhow::Error;

    fn try_from(bytes: Bytes) -> std::result::Result<Self, Self::Error> {
        ensure!(
            Self::SIZE == bytes.len(),
            "UUID v4 has {}B, got {}B",
            Self::SIZE,
            bytes.len()
        );
        Ok(Self(bytes))
    }
}

impl TryFrom<StrBytes> for Uuid {
    type Error = anyhow::Error;

    #[inline]
    fn try_from(StrBytes(bytes): StrBytes) -> std::result::Result<Self, Self::Error> {
        Self::try_from(bytes)
    }
}

/// [`Bytes`] wrapper that guarantees UTF-8 encoding.
///
/// Note that this is rather a helper type. For wire-serialized types use either [`Str`] or
/// [`CompactStr`].
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct StrBytes(Bytes);

impl StrBytes {
    #[inline]
    pub fn as_str(&self) -> &str {
        // SAFETY: StrBytes as UTF-8 encoded by construction
        unsafe { std::str::from_utf8_unchecked(&self.0) }
    }
}

impl From<String> for StrBytes {
    fn from(s: String) -> Self {
        let mut bytes = s.into_bytes();
        bytes.shrink_to_fit();
        Self(Bytes::from(bytes))
    }
}

impl From<&str> for StrBytes {
    #[inline]
    fn from(s: &str) -> Self {
        Self::from(s.to_string())
    }
}

impl TryFrom<Bytes> for StrBytes {
    type Error = anyhow::Error;

    fn try_from(bytes: Bytes) -> Result<Self, Self::Error> {
        std::str::from_utf8(&bytes).with_context(|| format!("invalid data: {bytes:x?}"))?;
        Ok(Self(bytes))
    }
}

impl From<Str> for StrBytes {
    #[inline]
    fn from(Str(bytes): Str) -> Self {
        Self(bytes)
    }
}

impl From<Option<Str>> for StrBytes {
    #[inline]
    fn from(s: Option<Str>) -> Self {
        Self(s.map_or(EMPTY, |Str(bytes)| bytes))
    }
}

impl From<CompactStr> for StrBytes {
    #[inline]
    fn from(CompactStr(bytes): CompactStr) -> Self {
        Self(bytes)
    }
}

impl TryFrom<&StrBytes> for u128 {
    type Error = anyhow::Error;

    fn try_from(StrBytes(s): &StrBytes) -> Result<Self, Self::Error> {
        ensure!(s.len() >= 16, "cannot convert StrBytes into u128");
        let mut bytes = [0; 16];
        bytes.copy_from_slice(s);
        Ok(u128::from_le_bytes(bytes))
    }
}

/// STRING (`Str`)
///
/// Represents a sequence of characters. First the length N is given as an INT16. Then N bytes
/// follow which are the UTF-8 encoding of the character sequence. Length must not be negative.
///
/// NULLABLE_STRING (`Option<Str>`)
///
/// Represents a sequence of characters or null. For non-null strings, first the length N is
/// given as an INT16. Then N bytes follow which are the UTF-8 encoding of the character
/// sequence. A null value is encoded with length of -1 and there are no following bytes.
#[derive(Clone, Debug)]
#[repr(transparent)]
pub struct Str(Bytes);

impl From<StrBytes> for Str {
    #[inline]
    fn from(StrBytes(bytes): StrBytes) -> Self {
        Self(bytes)
    }
}

impl From<&StrBytes> for Str {
    #[inline]
    fn from(s: &StrBytes) -> Self {
        s.clone().into()
    }
}

impl Serialize for Str {
    // length encoded as INT16
    const SIZE: usize = 2;

    #[inline]
    fn encode_size(&self, _version: i16) -> usize {
        Self::SIZE + self.0.len()
    }
}

impl Serialize for Option<Str> {
    #[inline]
    fn encode_size(&self, version: i16) -> usize {
        // NOTE: null strings still encode as -1 (INT16)
        self.as_ref()
            .map(move |s| Serialize::encode_size(s, version))
            .unwrap_or(2)
    }
}

impl AsyncSerialize for Str {
    async fn write_into<W>(self, writer: &mut W, _version: i16) -> Result<()>
    where
        W: AsyncWriteExt + Send + Unpin + ?Sized,
    {
        // Since STRING is non-empty, empty (default) values should be skipped
        //if self.0.is_empty() {
        //    return Ok(());
        //}

        writer
            .write_i16(self.0.len() as i16)
            .await
            .context("string length")?;

        writer.write_all(&self.0).await.context("string chars")
    }
}

impl AsyncSerialize for Option<Str> {
    async fn write_into<W>(self, writer: &mut W, version: i16) -> Result<()>
    where
        W: AsyncWriteExt + Send + Unpin + ?Sized,
    {
        if let Some(s) = self {
            s.write_into(writer, version).await
        } else {
            writer.write_i16(-1).await.context("string length")
        }
    }
}

impl Deserialize for Str {
    fn decode<B: Buf>(buf: &mut B, version: i16) -> Result<(Self, usize)> {
        let (Some(s), len) = Deserialize::decode(buf, version)? else {
            bail!("empty string");
        };
        Ok((s, len))
    }
}

impl Deserialize for Option<Str> {
    fn decode<B: Buf>(buf: &mut B, _version: i16) -> Result<(Self, usize)> {
        ensure!(buf.remaining() > 2, "not enough bytes left");

        let len = buf.get_i16();

        Ok(if len > 0 {
            let len = len as usize;
            ensure!(buf.remaining() >= len, "not enough bytes left");

            let bytes = buf.copy_to_bytes(len);

            // validate UTF-8
            std::str::from_utf8(&bytes).with_context(|| format!("invalid data: {bytes:x?}"))?;

            (Some(Str(bytes)), 2 + len)
        } else {
            (None, 2)
        })
    }
}

impl AsyncDeserialize for Str {
    async fn read_from<R>(reader: &mut R, version: i16) -> Result<(Self, usize)>
    where
        R: AsyncReadExt + Send + Unpin,
    {
        let (Some(s), len) = <Option<Str>>::read_from(reader, version).await? else {
            bail!("empty string");
        };
        Ok((s, len))
    }
}

impl AsyncDeserialize for Option<Str> {
    async fn read_from<R>(reader: &mut R, _version: i16) -> Result<(Self, usize)>
    where
        R: AsyncReadExt + Send + Unpin,
    {
        let len = reader.read_i16().await?;

        Ok(if len > 0 {
            let len = len as usize;

            let mut buf = BytesMut::with_capacity(len);
            buf.resize(len, 0);

            reader.read_exact(&mut buf).await?;

            let bytes = buf.freeze();

            // validate UTF-8
            std::str::from_utf8(&bytes).with_context(|| format!("invalid data: {bytes:x?}"))?;

            (Some(Str(bytes)), 2 + len)
        } else {
            (None, 2)
        })
    }
}

#[derive(Clone, Debug, PartialEq)]
#[repr(transparent)]
pub struct CompactStr(Bytes);

impl CompactStr {
    #[inline]
    pub(crate) fn empty() -> Self {
        Self(EMPTY)
    }

    #[inline]
    pub(crate) fn enc_len(&self) -> UnsignedVarInt {
        UnsignedVarInt((self.0.len() as u32) + 1)
    }
}

impl From<StrBytes> for CompactStr {
    #[inline]
    fn from(StrBytes(bytes): StrBytes) -> Self {
        Self(bytes)
    }
}

impl From<&StrBytes> for CompactStr {
    #[inline]
    fn from(s: &StrBytes) -> Self {
        s.clone().into()
    }
}

// TODO: try to unify with impl WireSize for Str
impl Serialize for CompactStr {
    #[inline]
    fn encode_size(&self, version: i16) -> usize {
        self.enc_len().encode_size(version) + self.0.len()
    }
}

impl Serialize for Option<CompactStr> {
    #[inline]
    fn encode_size(&self, version: i16) -> usize {
        self.as_ref().map_or(1, |s| s.encode_size(version))
    }
}

impl AsyncSerialize for CompactStr {
    async fn write_into<W>(self, writer: &mut W, version: i16) -> Result<()>
    where
        W: AsyncWriteExt + Send + Unpin + ?Sized,
    {
        self.enc_len()
            .write_into(writer, version)
            .await
            .context("compact string length")?;

        writer
            .write_all(&self.0)
            .await
            .context("compact string chars")
    }
}

impl AsyncSerialize for Option<CompactStr> {
    async fn write_into<W>(self, writer: &mut W, version: i16) -> Result<()>
    where
        W: AsyncWriteExt + Send + Unpin + ?Sized,
    {
        if let Some(s) = self {
            s.write_into(writer, version).await
        } else {
            UnsignedVarInt(0)
                .write_into(writer, version)
                .await
                .context("compact string lenght")
        }
    }
}

impl Deserialize for CompactStr {
    fn decode<B: Buf>(buf: &mut B, version: i16) -> Result<(Self, usize)> {
        let (UnsignedVarInt(len), n) =
            UnsignedVarInt::decode(buf, version).context("compact string length")?;

        let len = (len - 1) as usize;

        ensure!(buf.remaining() >= len, "not enough bytes left");
        let bytes = buf.copy_to_bytes(len);

        // validate UTF-8
        std::str::from_utf8(&bytes).with_context(|| format!("invalid data: {bytes:x?}"))?;

        Ok((Self(bytes), n + len))
    }
}

impl AsyncDeserialize for CompactStr {
    async fn read_from<R>(reader: &mut R, version: i16) -> Result<(Self, usize)>
    where
        R: AsyncReadExt + Send + Unpin,
    {
        let (UnsignedVarInt(len), n) = UnsignedVarInt::read_from(reader, version)
            .await
            .context("compact string length")?;

        let len = (len - 1) as usize;

        let mut buf = BytesMut::with_capacity(len);
        buf.resize(len, 0);

        reader.read_exact(&mut buf).await?;

        let bytes = buf.freeze();

        // validate UTF-8
        std::str::from_utf8(&bytes).with_context(|| format!("invalid data: {bytes:x?}"))?;

        Ok((Self(bytes), n + len))
    }
}

// Array item helpers
impl<T: Serialize> Serialize for &[T] {
    #[inline]
    fn encode_size(&self, version: i16) -> usize {
        self.iter()
            .map(|x| Serialize::encode_size(x, version))
            .sum::<usize>()
    }
}

impl<T: AsyncSerialize> AsyncSerialize for Vec<T> {
    async fn write_into<W>(self, writer: &mut W, version: i16) -> Result<()>
    where
        W: AsyncWriteExt + Send + Unpin + ?Sized,
    {
        for (i, item) in self.into_iter().enumerate() {
            item.write_into(writer, version)
                .await
                .with_context(|| format!("array item {i}"))?;
        }

        Ok(())
    }
}

/// ARRAY
///
/// Represents a sequence of objects of a given type T. Type T can be either a primitive type
/// (e.g. STRING) or a structure. First, the length N is given as an INT32. Then N instances of
/// type T follow. A null array is represented with a length of -1.
///
/// In protocol documentation an array of T instances is referred to as [T].
#[derive(Debug)]
#[repr(transparent)]
pub struct Array<T>(pub T);

impl<T> Default for Array<Vec<T>> {
    #[inline]
    fn default() -> Self {
        Self(Vec::default())
    }
}

impl<T> std::ops::Deref for Array<Vec<T>> {
    type Target = [T];

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<A> FromIterator<A> for Array<Vec<A>> {
    #[inline]
    fn from_iter<T: IntoIterator<Item = A>>(iter: T) -> Self {
        Self(iter.into_iter().collect())
    }
}

impl<T: Serialize> Serialize for Array<&[T]> {
    // array length encoded as INT32
    const SIZE: usize = 4;

    #[inline]
    fn encode_size(&self, version: i16) -> usize {
        Self::SIZE + self.0.encode_size(version)
    }
}

impl<T: Serialize> Serialize for Array<Option<&[T]>> {
    const SIZE: usize = 4;

    #[inline]
    fn encode_size(&self, version: i16) -> usize {
        self.0
            .map_or(Self::SIZE, |array| Array(array).encode_size(version))
    }
}

impl<T: Serialize> Serialize for Array<&Option<Vec<T>>> {
    #[inline]
    fn encode_size(&self, version: i16) -> usize {
        Array(self.0.as_ref().map(|array| array.as_slice())).encode_size(version)
    }
}

impl<T: AsyncSerialize> AsyncSerialize for Array<Vec<T>> {
    async fn write_into<W>(self, writer: &mut W, version: i16) -> Result<()>
    where
        W: AsyncWriteExt + Send + Unpin + ?Sized,
    {
        writer
            .write_i32(self.len() as i32)
            .await
            .context("array len")?;

        // write items
        self.0.write_into(writer, version).await
    }
}

impl<T: AsyncSerialize> AsyncSerialize for Array<Option<Vec<T>>> {
    async fn write_into<W>(self, writer: &mut W, version: i16) -> Result<()>
    where
        W: AsyncWriteExt + Send + Unpin + ?Sized,
    {
        if let Some(array) = self.0 {
            Array(array).write_into(writer, version).await
        } else {
            writer.write_i32(-1).await.context("array len")?;
            Ok(())
        }
    }
}

impl<T: Deserialize> Deserialize for Array<Vec<T>> {
    fn decode<B: Buf>(buf: &mut B, version: i16) -> Result<(Self, usize)> {
        let (Array(Some(items)), size) = Deserialize::decode(buf, version)? else {
            bail!("invalid array length");
        };
        Ok((Array(items), size))
    }
}

impl<T: Deserialize> Deserialize for Array<Option<Vec<T>>> {
    fn decode<B: Buf>(buf: &mut B, version: i16) -> Result<(Self, usize)> {
        let (len, mut size) = i32::decode(buf, version).context("array length")?;
        ensure!(len >= -1, "invalid array length");

        if len == -1 {
            return Ok((Self(None), size));
        }

        let mut items = Vec::with_capacity(len as usize);
        for i in 0..len {
            let (i, n) = T::decode(buf, version).with_context(|| format!("array item {i}"))?;
            items.push(i);
            size += n;
        }

        Ok((Self(Some(items)), size))
    }
}

impl<T: AsyncDeserialize> AsyncDeserialize for Array<Vec<T>> {
    async fn read_from<R>(reader: &mut R, version: i16) -> Result<(Self, usize)>
    where
        R: AsyncReadExt + Send + Unpin,
    {
        let (Array(Some(items)), size) =
            <Array<Option<Vec<T>>>>::read_from(reader, version).await?
        else {
            bail!("invalid array length");
        };
        Ok((Array(items), size))
    }
}

impl<T: AsyncDeserialize> AsyncDeserialize for Array<Option<Vec<T>>> {
    async fn read_from<R>(reader: &mut R, version: i16) -> Result<(Self, usize)>
    where
        R: AsyncReadExt + Send + Unpin,
    {
        let (len, mut size) = i32::read_from(reader, version)
            .await
            .context("array length")?;
        ensure!(len >= -1, "invalid array length");

        if len == -1 {
            return Ok((Self(None), size));
        }

        let mut items = Vec::with_capacity(len as usize);
        for i in 0..len {
            let (i, n) = T::read_from(reader, version)
                .await
                .with_context(|| format!("array item {i}"))?;
            items.push(i);
            size += n;
        }

        Ok((Self(Some(items)), size))
    }
}

/// COMPACT_ARRAY
///
/// Represents a sequence of objects of a given type T. Type T can be either a primitive type
/// (e.g. STRING) or a structure. First, the length N + 1 is given as an UNSIGNED_VARINT. Then N
/// instances of type T follow. A null array is represented with a length of 0.
///
/// In protocol documentation an array of T instances is referred to as [T].
#[derive(Debug)]
#[repr(transparent)]
pub struct CompactArray<T>(pub T);

impl<T> Default for CompactArray<Vec<T>> {
    #[inline]
    fn default() -> Self {
        Self(Vec::default())
    }
}

impl<T> std::ops::Deref for CompactArray<Vec<T>> {
    type Target = [T];

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<A> FromIterator<A> for CompactArray<Vec<A>> {
    #[inline]
    fn from_iter<T: IntoIterator<Item = A>>(iter: T) -> Self {
        Self(iter.into_iter().collect())
    }
}

impl<T: Serialize> Serialize for CompactArray<&[T]> {
    #[inline]
    fn encode_size(&self, version: i16) -> usize {
        UnsignedVarInt(self.0.len() as u32 + 1).encode_size(version) + self.0.encode_size(version)
    }
}

impl<T: Serialize> Serialize for CompactArray<Option<&[T]>> {
    #[inline]
    fn encode_size(&self, version: i16) -> usize {
        self.0
            .map_or(1, |array| CompactArray(array).encode_size(version))
    }
}

impl<T: Serialize> Serialize for CompactArray<&Option<Vec<T>>> {
    #[inline]
    fn encode_size(&self, version: i16) -> usize {
        CompactArray(self.0.as_ref().map(|array| array.as_slice())).encode_size(version)
    }
}

impl<T: AsyncSerialize> AsyncSerialize for CompactArray<Vec<T>> {
    async fn write_into<W>(self, writer: &mut W, version: i16) -> Result<()>
    where
        W: AsyncWriteExt + Send + Unpin + ?Sized,
    {
        ensure!(
            self.len() < u32::MAX as usize,
            "exceeded max COMPACT_ARRAY size",
        );

        UnsignedVarInt(self.len() as u32 + 1)
            .write_into(writer, version)
            .await
            .context("compact array len")?;

        // write items
        self.0.write_into(writer, version).await
    }
}

impl<T: AsyncSerialize> AsyncSerialize for CompactArray<Option<Vec<T>>> {
    async fn write_into<W>(self, writer: &mut W, version: i16) -> Result<()>
    where
        W: AsyncWriteExt + Send + Unpin + ?Sized,
    {
        if let Some(array) = self.0 {
            CompactArray(array).write_into(writer, version).await
        } else {
            UnsignedVarInt(0)
                .write_into(writer, version)
                .await
                .context("array len")?;
            Ok(())
        }
    }
}

impl<T: Deserialize> Deserialize for CompactArray<Vec<T>> {
    fn decode<B: Buf>(buf: &mut B, version: i16) -> Result<(Self, usize)> {
        let (CompactArray(Some(items)), size) = Deserialize::decode(buf, version)? else {
            bail!("invalid compact array length");
        };
        Ok((Self(items), size))
    }
}

impl<T: Deserialize> Deserialize for CompactArray<Option<Vec<T>>> {
    fn decode<B: Buf>(buf: &mut B, version: i16) -> Result<(Self, usize)> {
        let (UnsignedVarInt(len), mut size) =
            UnsignedVarInt::decode(buf, version).context("compact array length")?;

        if len == 0 {
            return Ok((Self(None), size));
        }

        let mut items = Vec::with_capacity((len - 1) as usize);
        for i in 0..(len - 1) {
            let (i, n) =
                T::decode(buf, version).with_context(|| format!("compact array item {i}"))?;
            items.push(i);
            size += n;
        }

        Ok((Self(Some(items)), size))
    }
}

impl<T: AsyncDeserialize> AsyncDeserialize for CompactArray<Vec<T>> {
    async fn read_from<R>(reader: &mut R, version: i16) -> Result<(Self, usize)>
    where
        R: AsyncReadExt + Send + Unpin,
    {
        let (CompactArray(Some(items)), size) =
            <CompactArray<Option<Vec<T>>>>::read_from(reader, version).await?
        else {
            bail!("invalid compact array length");
        };
        Ok((Self(items), size))
    }
}

impl<T: AsyncDeserialize> AsyncDeserialize for CompactArray<Option<Vec<T>>> {
    async fn read_from<R>(reader: &mut R, version: i16) -> Result<(Self, usize)>
    where
        R: AsyncReadExt + Send + Unpin,
    {
        let (UnsignedVarInt(len), mut size) = UnsignedVarInt::read_from(reader, version)
            .await
            .context("compact array length")?;

        if len == 0 {
            return Ok((Self(None), size));
        }

        let mut items = Vec::with_capacity((len - 1) as usize);
        for i in 0..(len - 1) {
            let (i, n) = T::read_from(reader, version)
                .await
                .with_context(|| format!("compact array item {i}"))?;
            items.push(i);
            size += n;
        }

        Ok((Self(Some(items)), size))
    }
}

/// TAG_BUFFER [KIP-482][KIP]
///
/// [KIP]: https://cwiki.apache.org/confluence/display/KAFKA/KIP-482%3A+The+Kafka+Protocol+should+Support+Optional+Tagged+Fields
#[derive(Debug, PartialEq)]
#[repr(transparent)]
pub struct TagBuffer(BTreeMap<i32, Bytes>);

impl Default for TagBuffer {
    #[inline]
    fn default() -> Self {
        Self(BTreeMap::default())
    }
}

impl Serialize for TagBuffer {
    fn encode_size(&self, version: i16) -> usize {
        self.0.iter().fold(
            UnsignedVarInt(self.0.len() as u32).encode_size(version),
            |size, (&k, v)| {
                size + UnsignedVarInt(k as u32).encode_size(version)
                    + UnsignedVarInt(v.len() as u32).encode_size(version)
                    + v.len()
            },
        )
    }
}

impl AsyncSerialize for TagBuffer {
    async fn write_into<W>(self, writer: &mut W, version: i16) -> Result<()>
    where
        W: AsyncWriteExt + Send + Unpin + ?Sized,
    {
        UnsignedVarInt(self.0.len() as u32)
            .write_into(writer, version)
            .await
            .context("number of tagged fields")?;

        for (k, v) in self.0.into_iter() {
            UnsignedVarInt(k as u32)
                .write_into(writer, version)
                .await
                .with_context(|| format!("tagged field {k} - tag"))?;

            UnsignedVarInt(v.len() as u32)
                .write_into(writer, version)
                .await
                .with_context(|| format!("tagged field {k} - value length"))?;

            writer
                .write_all(&v)
                .await
                .with_context(|| format!("tagged field {k} - value"))?;
        }

        Ok(())
    }
}

impl Deserialize for TagBuffer {
    fn decode<B: Buf>(buf: &mut B, version: i16) -> Result<(Self, usize)> {
        let (UnsignedVarInt(count), mut bytes) = UnsignedVarInt::decode(buf, version)?;

        let mut tag_buf = BTreeMap::new();

        for _ in 0..count {
            let (UnsignedVarInt(tag), n) = UnsignedVarInt::decode(buf, version)?;
            bytes += n;

            let (UnsignedVarInt(val_len), n) = UnsignedVarInt::decode(buf, version)?;
            bytes += n;

            ensure!(buf.remaining() > val_len as usize, "not enough bytes left");

            let val = buf.copy_to_bytes(val_len as usize);
            tag_buf.insert(tag as i32, val);
        }

        Ok((Self(tag_buf), bytes))
    }
}

impl AsyncDeserialize for TagBuffer {
    async fn read_from<R>(reader: &mut R, version: i16) -> Result<(Self, usize)>
    where
        R: AsyncReadExt + Send + Unpin,
    {
        let (UnsignedVarInt(count), mut bytes) = UnsignedVarInt::read_from(reader, version).await?;

        let mut tag_buf = BTreeMap::new();

        for _ in 0..count {
            let (UnsignedVarInt(tag), n) = UnsignedVarInt::read_from(reader, version).await?;
            bytes += n;

            let (UnsignedVarInt(val_len), n) = UnsignedVarInt::read_from(reader, version).await?;
            bytes += n;

            let mut buf = BytesMut::with_capacity(val_len as usize);
            buf.resize(val_len as usize, 0);

            reader.read_exact(&mut buf).await?;

            let val = buf.freeze();
            tag_buf.insert(tag as i32, val);
        }

        Ok((Self(tag_buf), bytes))
    }
}
