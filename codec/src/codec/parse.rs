use crate::error::ParseError;
use crate::packet::*;
use bytes::{buf::ext::Take, Buf, Bytes};
use bytestring::ByteString;
use std::convert::TryFrom;
use std::num::{NonZeroU16, NonZeroU32};

pub(crate) trait ByteBuf: Buf {
    fn inner_mut(&mut self) -> &mut Bytes;
}

impl ByteBuf for Bytes {
    fn inner_mut(&mut self) -> &mut Bytes {
        self
    }
}

impl ByteBuf for Take<&mut Bytes> {
    fn inner_mut(&mut self) -> &mut Bytes {
        self.get_mut()
    }
}

pub(crate) trait Property {
    fn init() -> Self;
    fn read_value<B: ByteBuf>(&mut self, src: &mut B) -> Result<(), ParseError>;
}

impl<T: Parse> Property for Option<T> {
    fn init() -> Self {
        None
    }

    fn read_value<B: ByteBuf>(&mut self, src: &mut B) -> Result<(), ParseError> {
        ensure!(self.is_none(), ParseError::MalformedPacket); // property is set twice while not allowed
        *self = Some(T::parse(src)?);
        Ok(())
    }
}

impl<T: Parse> Property for Vec<T> {
    fn init() -> Self {
        Vec::new()
    }

    fn read_value<B: ByteBuf>(&mut self, src: &mut B) -> Result<(), ParseError> {
        self.push(T::parse(src)?);
        Ok(())
    }
}

pub(crate) trait Parse: Sized {
    fn parse<B: ByteBuf>(src: &mut B) -> Result<Self, ParseError>;
}

impl Parse for bool {
    fn parse<B: ByteBuf>(src: &mut B) -> Result<Self, ParseError> {
        ensure!(src.has_remaining(), ParseError::InvalidLength); // expected more data within the field
        let v = src.get_u8();
        ensure!(v <= 0x1, ParseError::MalformedPacket); // value is invalid
        Ok(v == 0x1)
    }
}

impl Parse for u16 {
    fn parse<B: ByteBuf>(src: &mut B) -> Result<Self, ParseError> {
        ensure!(src.remaining() >= 2, ParseError::InvalidLength);
        Ok(src.get_u16())
    }
}

impl Parse for u32 {
    fn parse<B: ByteBuf>(src: &mut B) -> Result<Self, ParseError> {
        ensure!(src.remaining() >= 4, ParseError::InvalidLength); // expected more data within the field
        let val = src.get_u32();
        Ok(val)
    }
}

impl Parse for NonZeroU32 {
    fn parse<B: ByteBuf>(src: &mut B) -> Result<Self, ParseError> {
        let val = NonZeroU32::new(u32::parse(src)?).ok_or(ParseError::MalformedPacket)?;
        Ok(val)
    }
}

impl Parse for NonZeroU16 {
    fn parse<B: ByteBuf>(src: &mut B) -> Result<Self, ParseError> {
        Ok(NonZeroU16::new(u16::parse(src)?).ok_or(ParseError::MalformedPacket)?)
    }
}

impl Parse for Bytes {
    fn parse<B: ByteBuf>(src: &mut B) -> Result<Self, ParseError> {
        let len = u16::parse(src)? as usize;
        ensure!(src.remaining() >= len, ParseError::InvalidLength);
        Ok(src.inner_mut().split_to(len))
    }
}

impl Parse for ByteStr {
    fn parse<B: ByteBuf>(src: &mut B) -> Result<Self, ParseError> {
        let bytes = Bytes::parse(src)?;
        Ok(ByteString::try_from(bytes)?)
    }
}

impl Parse for (ByteStr, ByteStr) {
    fn parse<B: ByteBuf>(src: &mut B) -> Result<Self, ParseError> {
        let key = ByteStr::parse(src)?;
        let val = ByteStr::parse(src)?;
        Ok((key, val))
    }
}
