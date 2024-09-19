use std::collections::BTreeMap;

use anyhow::{ensure, Context as _, Result};
use bytes::{Buf, Bytes};
use tokio::io::AsyncWriteExt;

use crate::kafka::{Deserialize, Serialize, WireSize};

const EMPTY: Bytes = Bytes::from_static(&[]);

impl WireSize for bool {
    const SIZE: usize = 1;

    #[inline]
    fn size(&self, _version: i16) -> usize {
        Self::SIZE
    }
}

impl Serialize for bool {
    #[inline]
    async fn write_into<W>(self, writer: &mut W, _version: i16) -> Result<()>
    where
        W: AsyncWriteExt + Send + Unpin,
    {
        writer.write_u8(self as u8).await.context("bool")
    }
}

/// BOOLEAN (`bool`)
///
/// Represents a boolean value in a byte. Values 0 and 1 are used to represent false and true
/// respectively. When reading a boolean value, any non-zero value is considered true.
impl Deserialize for bool {
    fn read_from<B: Buf>(buf: &mut B, _version: i16) -> Result<(Self, usize)> {
        ensure!(buf.has_remaining(), "not enough bytes left");
        Ok((buf.get_u8() > 0, 1))
    }
}

/// UNSIGNED_VARINT
///
/// Represents an unsigned 32-bit integer. Encoding follows the variable-length zig-zag encoding
/// from Google Protocol Buffers.
#[derive(Debug, Copy, Clone, Default)]
#[repr(transparent)]
pub struct UnsignedVarInt(u32);

impl WireSize for UnsignedVarInt {
    fn size(&self, _version: i16) -> usize {
        match self.0 {
            0x0..=0x7f => 1,
            0x80..=0x3fff => 2,
            0x4000..=0x1fffff => 3,
            0x200000..=0xfffffff => 4,
            0x10000000..=0xffffffff => 5,
        }
    }
}

impl Serialize for UnsignedVarInt {
    async fn write_into<W>(self, writer: &mut W, _version: i16) -> Result<()>
    where
        W: AsyncWriteExt + Send + Unpin,
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
    fn read_from<B: Buf>(buf: &mut B, _version: i16) -> Result<(Self, usize)> {
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

impl WireSize for Bytes {
    const SIZE: usize = 4;

    #[inline]
    fn size(&self, _version: i16) -> usize {
        Self::SIZE + self.len()
    }
}

impl WireSize for Option<Bytes> {
    const SIZE: usize = Bytes::SIZE;

    #[inline]
    fn size(&self, version: i16) -> usize {
        self.as_ref().map_or(Self::SIZE, |b| b.size(version))
    }
}

impl Serialize for Bytes {
    async fn write_into<W>(self, writer: &mut W, _version: i16) -> Result<()>
    where
        W: AsyncWriteExt + Send + Unpin,
    {
        writer
            .write_i32(self.len() as i32)
            .await
            .context("bytes length")?;

        writer.write_all(&self).await.context("raw bytes")
    }
}

impl Serialize for Option<Bytes> {
    async fn write_into<W>(self, writer: &mut W, version: i16) -> Result<()>
    where
        W: AsyncWriteExt + Send + Unpin,
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

impl WireSize for CompactBytes {
    #[inline]
    fn size(&self, version: i16) -> usize {
        self.enc_len().size(version) + self.0.len()
    }
}

impl WireSize for Option<CompactBytes> {
    #[inline]
    fn size(&self, version: i16) -> usize {
        self.as_ref().map_or(1, |b| b.size(version))
    }
}

impl Serialize for CompactBytes {
    async fn write_into<W>(self, writer: &mut W, version: i16) -> Result<()>
    where
        W: AsyncWriteExt + Send + Unpin,
    {
        self.enc_len()
            .write_into(writer, version)
            .await
            .context("compact bytes length")?;

        writer.write_all(&self.0).await.context("compact bytes")
    }
}

impl Serialize for Option<CompactBytes> {
    async fn write_into<W>(self, writer: &mut W, version: i16) -> Result<()>
    where
        W: AsyncWriteExt + Send + Unpin,
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

// TODO: TryFrom<Bytes> to validate size
/// UUID v4 bytes wrapper
#[derive(Clone, Debug)]
#[repr(transparent)]
pub struct Uuid(Bytes);

impl WireSize for Uuid {
    const SIZE: usize = 16;

    #[inline]
    fn size(&self, _version: i16) -> usize {
        Self::SIZE
    }
}

impl Serialize for Uuid {
    async fn write_into<W>(self, writer: &mut W, _version: i16) -> Result<()>
    where
        W: AsyncWriteExt + Send + Unpin,
    {
        writer.write_all(&self.0).await.context("UUID bytes")
    }
}

/// [`Bytes`] wrapper that guarantees UTF-8 encoding.
///
/// Note that this is rather a helper type. For wire-serialized types use either [`Str`] or
/// [`CompactStr`].
#[derive(Clone, Debug)]
#[repr(transparent)]
pub struct StrBytes(Bytes);

impl TryFrom<Bytes> for StrBytes {
    type Error = anyhow::Error;

    fn try_from(bytes: Bytes) -> Result<Self, Self::Error> {
        std::str::from_utf8(&bytes).with_context(|| format!("invalid data: {bytes:x?}"))?;
        Ok(Self(bytes))
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

impl WireSize for Str {
    // length encoded as INT16
    const SIZE: usize = 2;

    #[inline]
    fn size(&self, _version: i16) -> usize {
        Self::SIZE + self.0.len()
    }
}

impl WireSize for Option<Str> {
    #[inline]
    fn size(&self, version: i16) -> usize {
        // NOTE: null strings still encode as -1 (INT16)
        self.as_ref()
            .map(move |s| WireSize::size(s, version))
            .unwrap_or(2)
    }
}

impl Serialize for Str {
    async fn write_into<W>(self, writer: &mut W, _version: i16) -> Result<()>
    where
        W: AsyncWriteExt + Send + Unpin,
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

impl Serialize for Option<Str> {
    async fn write_into<W>(self, writer: &mut W, version: i16) -> Result<()>
    where
        W: AsyncWriteExt + Send + Unpin,
    {
        if let Some(s) = self {
            s.write_into(writer, version).await
        } else {
            writer.write_i16(-1).await.context("string length")
        }
    }
}

impl Deserialize for Option<Str> {
    fn read_from<B: Buf>(buf: &mut B, _version: i16) -> Result<(Self, usize)> {
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

#[derive(Clone, Debug)]
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
impl WireSize for CompactStr {
    #[inline]
    fn size(&self, version: i16) -> usize {
        self.enc_len().size(version) + self.0.len()
    }
}

impl WireSize for Option<CompactStr> {
    #[inline]
    fn size(&self, version: i16) -> usize {
        self.as_ref().map_or(1, |s| s.size(version))
    }
}

impl Serialize for CompactStr {
    async fn write_into<W>(self, writer: &mut W, version: i16) -> Result<()>
    where
        W: AsyncWriteExt + Send + Unpin,
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

impl Serialize for Option<CompactStr> {
    async fn write_into<W>(self, writer: &mut W, version: i16) -> Result<()>
    where
        W: AsyncWriteExt + Send + Unpin,
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
    fn read_from<B: Buf>(buf: &mut B, version: i16) -> Result<(Self, usize)> {
        let (UnsignedVarInt(len), n) =
            UnsignedVarInt::read_from(buf, version).context("compact string length")?;

        let len = (len - 1) as usize;

        ensure!(buf.remaining() >= len, "not enough bytes left");
        let bytes = buf.copy_to_bytes(len);

        // validate UTF-8
        std::str::from_utf8(&bytes).with_context(|| format!("invalid data: {bytes:x?}"))?;

        Ok((Self(bytes), n + len))
    }
}

// Array item helpers
impl<T: WireSize> WireSize for &[T] {
    #[inline]
    fn size(&self, version: i16) -> usize {
        self.iter()
            .map(|x| WireSize::size(x, version))
            .sum::<usize>()
    }
}

impl<T: Serialize> Serialize for Vec<T> {
    async fn write_into<W>(self, writer: &mut W, version: i16) -> Result<()>
    where
        W: AsyncWriteExt + Send + Unpin,
    {
        for (i, item) in self.into_iter().enumerate() {
            item.write_into(writer, version)
                .await
                .with_context(|| format!("array item {i}"))?;
        }

        Ok(())
    }
}

/// ARRAY | COMPACT_ARRAY
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

impl<T: WireSize> WireSize for Array<&[T]> {
    // array length encoded as INT32
    const SIZE: usize = 4;

    #[inline]
    fn size(&self, version: i16) -> usize {
        Self::SIZE + self.0.size(version)
    }
}

impl<T: WireSize> WireSize for Array<Option<&[T]>> {
    const SIZE: usize = 4;

    #[inline]
    fn size(&self, version: i16) -> usize {
        self.0
            .map_or(Self::SIZE, |array| Array(array).size(version))
    }
}

impl<T: WireSize> WireSize for Array<&Option<Vec<T>>> {
    #[inline]
    fn size(&self, version: i16) -> usize {
        Array(self.0.as_ref().map(|array| array.as_slice())).size(version)
    }
}

impl<T: Serialize> Serialize for Array<Vec<T>> {
    async fn write_into<W>(self, writer: &mut W, version: i16) -> Result<()>
    where
        W: AsyncWriteExt + Send + Unpin,
    {
        writer
            .write_i32(self.len() as i32)
            .await
            .context("array len")?;

        // write items
        self.0.write_into(writer, version).await
    }
}

impl<T: Serialize> Serialize for Array<Option<Vec<T>>> {
    async fn write_into<W>(self, writer: &mut W, version: i16) -> Result<()>
    where
        W: AsyncWriteExt + Send + Unpin,
    {
        if let Some(array) = self.0 {
            Array(array).write_into(writer, version).await
        } else {
            writer.write_i32(-1).await.context("array len")?;
            Ok(())
        }
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

impl<T: WireSize> WireSize for CompactArray<&[T]> {
    #[inline]
    fn size(&self, version: i16) -> usize {
        UnsignedVarInt(self.0.len() as u32 + 1).size(version) + self.0.size(version)
    }
}

impl<T: WireSize> WireSize for CompactArray<Option<&[T]>> {
    #[inline]
    fn size(&self, version: i16) -> usize {
        self.0.map_or(1, |array| CompactArray(array).size(version))
    }
}

impl<T: WireSize> WireSize for CompactArray<&Option<Vec<T>>> {
    #[inline]
    fn size(&self, version: i16) -> usize {
        CompactArray(self.0.as_ref().map(|array| array.as_slice())).size(version)
    }
}

impl<T: Serialize> Serialize for CompactArray<Vec<T>> {
    async fn write_into<W>(self, writer: &mut W, version: i16) -> Result<()>
    where
        W: AsyncWriteExt + Send + Unpin,
    {
        ensure!(
            self.len() < std::u32::MAX as usize,
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

impl<T: Serialize> Serialize for CompactArray<Option<Vec<T>>> {
    async fn write_into<W>(self, writer: &mut W, version: i16) -> Result<()>
    where
        W: AsyncWriteExt + Send + Unpin,
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

/// TAG_BUFFER [KIP-482][KIP]
///
/// [KIP]: https://cwiki.apache.org/confluence/display/KAFKA/KIP-482%3A+The+Kafka+Protocol+should+Support+Optional+Tagged+Fields
#[derive(Debug)]
#[repr(transparent)]
pub struct TagBuffer(BTreeMap<i32, Bytes>);

impl Default for TagBuffer {
    #[inline]
    fn default() -> Self {
        Self(BTreeMap::default())
    }
}

impl WireSize for TagBuffer {
    fn size(&self, version: i16) -> usize {
        self.0.iter().fold(
            UnsignedVarInt(self.0.len() as u32).size(version),
            |size, (&k, v)| {
                size + UnsignedVarInt(k as u32).size(version)
                    + UnsignedVarInt(v.len() as u32).size(version)
                    + v.len()
            },
        )
    }
}

impl Serialize for TagBuffer {
    async fn write_into<W>(self, writer: &mut W, version: i16) -> Result<()>
    where
        W: AsyncWriteExt + Send + Unpin,
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
    fn read_from<B: Buf>(buf: &mut B, version: i16) -> Result<(Self, usize)> {
        let (UnsignedVarInt(count), mut bytes) = UnsignedVarInt::read_from(buf, version)?;

        let mut tag_buf = BTreeMap::new();

        for _ in 0..count {
            let (UnsignedVarInt(tag), n) = UnsignedVarInt::read_from(buf, version)?;
            bytes += n;

            let (UnsignedVarInt(val_len), n) = UnsignedVarInt::read_from(buf, version)?;
            bytes += n;

            ensure!(buf.remaining() > val_len as usize, "not enough bytes left");

            let val = buf.copy_to_bytes(val_len as usize);
            tag_buf.insert(tag as i32, val);
        }

        Ok((Self(tag_buf), bytes))
    }
}
