use crate::{
    constants,
    rt_type::primitives::{self, AttributeKind, AttributeValue, TableAttribute},
};

pub struct LayoutReadWriter<'a> {
    layout: &'a primitives::TableLayout,
}

impl<'a> LayoutReadWriter<'a> {
    fn new(layout: &'a primitives::TableLayout) -> LayoutReadWriter<'a> {
        Self { layout }
    }

    fn find_offset(&self, attr: &TableAttribute) -> Option<u16> {
        for lay_attr in &self.layout.attr_layouts {
            if lay_attr.attr_name == attr.name {
                return Some(lay_attr.offset);
            }
        }
        None
    }

    pub fn read_attr(
        &self,
        attr: &TableAttribute,
        page: &[u8; constants::storage::DISK_PAGE_SIZE],
        base_offset: u16,
    ) -> Result<AttributeValue, errors::ReadAttrError> {
        let offset = base_offset
            + self
                .find_offset(attr)
                .ok_or(errors::ReadAttrError::BadSlice)?;

        let offset_us = offset as usize;
        let slice = &page[offset_us..offset_us + attr.kind.size_of()];

        match attr.kind {
            AttributeKind::U8 => Ok(AttributeValue::U8(u8::from_le_bytes(
                slice
                    .try_into()
                    .map_err(|_| errors::ReadAttrError::BadSlice)?,
            ))),
            AttributeKind::U16 => Ok(AttributeValue::U16(u16::from_le_bytes(
                slice
                    .try_into()
                    .map_err(|_| errors::ReadAttrError::BadSlice)?,
            ))),
            AttributeKind::U32 => Ok(AttributeValue::U32(u32::from_le_bytes(
                slice
                    .try_into()
                    .map_err(|_| errors::ReadAttrError::BadSlice)?,
            ))),
            AttributeKind::U64 => Ok(AttributeValue::U64(u64::from_le_bytes(
                slice
                    .try_into()
                    .map_err(|_| errors::ReadAttrError::BadSlice)?,
            ))),
            AttributeKind::U128 => Ok(AttributeValue::U128(u128::from_le_bytes(
                slice
                    .try_into()
                    .map_err(|_| errors::ReadAttrError::BadSlice)?,
            ))),
            AttributeKind::I8 => Ok(AttributeValue::I8(i8::from_le_bytes(
                slice
                    .try_into()
                    .map_err(|_| errors::ReadAttrError::BadSlice)?,
            ))),
            AttributeKind::I16 => Ok(AttributeValue::I16(i16::from_le_bytes(
                slice
                    .try_into()
                    .map_err(|_| errors::ReadAttrError::BadSlice)?,
            ))),
            AttributeKind::I32 => Ok(AttributeValue::I32(i32::from_le_bytes(
                slice
                    .try_into()
                    .map_err(|_| errors::ReadAttrError::BadSlice)?,
            ))),
            AttributeKind::I64 => Ok(AttributeValue::I64(i64::from_le_bytes(
                slice
                    .try_into()
                    .map_err(|_| errors::ReadAttrError::BadSlice)?,
            ))),
            AttributeKind::I128 => Ok(AttributeValue::I128(i128::from_le_bytes(
                slice
                    .try_into()
                    .map_err(|_| errors::ReadAttrError::BadSlice)?,
            ))),
            AttributeKind::F32 => Ok(AttributeValue::F32(f32::from_le_bytes(
                slice
                    .try_into()
                    .map_err(|_| errors::ReadAttrError::BadSlice)?,
            ))),
            AttributeKind::F64 => Ok(AttributeValue::F64(f64::from_le_bytes(
                slice
                    .try_into()
                    .map_err(|_| errors::ReadAttrError::BadSlice)?,
            ))),
            AttributeKind::Bool => {
                let arr: [u8; 1] = slice
                    .try_into()
                    .map_err(|_| errors::ReadAttrError::BadSlice)?;
                Ok(AttributeValue::Bool(arr[0] == 1))
            }
            AttributeKind::Char(max_size) => {
                if slice.is_empty() {
                    return Err(errors::ReadAttrError::BadSlice); // no length byte
                }
                let actual_len = slice[0] as usize;
                if actual_len > max_size {
                    return Err(errors::ReadAttrError::BadStringValue);
                }
                if slice.len() < 1 + actual_len {
                    return Err(errors::ReadAttrError::BadSlice); // invalid length
                }

                let str_bytes = &slice[1..1 + actual_len];
                let s = std::str::from_utf8(str_bytes)
                    .map_err(|_| errors::ReadAttrError::BadStringValue)?;

                Ok(AttributeValue::Char(s.to_string()))
            }
        }
    }

    pub fn write_attr(
        &self,
        attr: &TableAttribute,
        value: AttributeValue,
        page: &mut [u8; constants::storage::DISK_PAGE_SIZE],
        base_offset: u16,
    ) -> Result<(), errors::WriteAttrError> {
        let offset = base_offset
            + self
                .find_offset(attr)
                .ok_or(errors::WriteAttrError::BadAttribute)?;

        let offset_us = offset as usize;
        let size = attr.kind.size_of();
        let buf = &mut page[offset_us..offset_us + size];

        match (attr.kind, value) {
            (AttributeKind::U8, AttributeValue::U8(v)) => {
                buf.copy_from_slice(&v.to_le_bytes());
            }
            (AttributeKind::U16, AttributeValue::U16(v)) => {
                buf.copy_from_slice(&v.to_le_bytes());
            }
            (AttributeKind::U32, AttributeValue::U32(v)) => {
                buf.copy_from_slice(&v.to_le_bytes());
            }
            (AttributeKind::U64, AttributeValue::U64(v)) => {
                buf.copy_from_slice(&v.to_le_bytes());
            }
            (AttributeKind::U128, AttributeValue::U128(v)) => {
                buf.copy_from_slice(&v.to_le_bytes());
            }
            (AttributeKind::I8, AttributeValue::I8(v)) => {
                buf.copy_from_slice(&v.to_le_bytes());
            }
            (AttributeKind::I16, AttributeValue::I16(v)) => {
                buf.copy_from_slice(&v.to_le_bytes());
            }
            (AttributeKind::I32, AttributeValue::I32(v)) => {
                buf.copy_from_slice(&v.to_le_bytes());
            }
            (AttributeKind::I64, AttributeValue::I64(v)) => {
                buf.copy_from_slice(&v.to_le_bytes());
            }
            (AttributeKind::I128, AttributeValue::I128(v)) => {
                buf.copy_from_slice(&v.to_le_bytes());
            }
            (AttributeKind::F32, AttributeValue::F32(v)) => {
                buf.copy_from_slice(&v.to_le_bytes());
            }
            (AttributeKind::F64, AttributeValue::F64(v)) => {
                buf.copy_from_slice(&v.to_le_bytes());
            }
            (AttributeKind::Bool, AttributeValue::Bool(v)) => {
                buf[0] = if v { 1 } else { 0 };
            }
            (AttributeKind::Char(max_size), AttributeValue::Char(s)) => {
                let bytes = s.as_bytes();
                if bytes.len() > max_size {
                    return Err(errors::WriteAttrError::ValueKindMismatch);
                }

                buf[0] = bytes.len() as u8;
                buf[1..1 + bytes.len()].copy_from_slice(bytes);

                // zero-fill the rest
                for b in &mut buf[1 + bytes.len()..] {
                    *b = 0;
                }
            }
            _ => return Err(errors::WriteAttrError::ValueKindMismatch),
        }

        Ok(())
    }
}

pub mod errors {
    pub enum WriteAttrError {
        BadAttribute,
        ValueKindMismatch,
        OffsetOutOfBounds,
    }

    pub enum ReadAttrError {
        BadAttribute,
        BadSlice,
        BadStringValue,
    }
}
