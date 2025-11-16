use crate::{
    rt_type::primitives::{self, AttributeKind, AttributeValue, TableAttribute},
};
use std::convert::TryInto;

pub struct LayoutReadWriter<'a> {
    layout: &'a primitives::TableLayout,
}

impl<'a> LayoutReadWriter<'a> {
    pub fn new(layout: &'a primitives::TableLayout) -> LayoutReadWriter<'a> {
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
        buffer: &[u8],
        base_offset: u16,
    ) -> Result<AttributeValue, errors::ReadAttrError> {
        let offset = base_offset
            + self
                .find_offset(attr)
                .ok_or(errors::ReadAttrError::BadSlice)?;

        let offset_us = offset as usize;
        let fixed_size = attr.kind.size_of();

        if offset_us + fixed_size > buffer.len() {
            return Err(errors::ReadAttrError::BadSlice);
        }

        // We only create a slice for fixed types. Varchar must handle buffer directly.
        let slice = if fixed_size > 0 {
            &buffer[offset_us..offset_us + fixed_size]
        } else {
            &[] // Placeholder for Varchar
        };

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
                    return Err(errors::ReadAttrError::BadSlice);
                }
                let actual_len = slice[0] as usize;
                if actual_len > max_size {
                    return Err(errors::ReadAttrError::BadStringValue);
                }
                if slice.len() < 1 + actual_len {
                    return Err(errors::ReadAttrError::BadSlice);
                }

                let str_bytes = &slice[1..1 + actual_len];
                let s = std::str::from_utf8(str_bytes)
                    .map_err(|_| errors::ReadAttrError::BadStringValue)?;

                Ok(AttributeValue::Char(s.to_string()))
            }
            AttributeKind::Varchar => {
                // 1. Read Length (2 bytes, Big Endian preferred for network/disk standard, using BE here to match previous logic if any)
                if offset_us + 2 > buffer.len() {
                    return Err(errors::ReadAttrError::BadSlice);
                }
                let len_bytes: [u8; 2] = buffer[offset_us..offset_us + 2]
                    .try_into()
                    .map_err(|_| errors::ReadAttrError::BadSlice)?;

                // Use Big Endian (be_bytes) for length often avoids confusion,
                let str_len = u16::from_be_bytes(len_bytes) as usize;

                // 2. Read Data
                if offset_us + 2 + str_len > buffer.len() {
                    return Err(errors::ReadAttrError::BadSlice);
                }

                let str_bytes = &buffer[offset_us + 2..offset_us + 2 + str_len];
                let s = std::str::from_utf8(str_bytes)
                    .map_err(|_| errors::ReadAttrError::BadStringValue)?;

                Ok(AttributeValue::Varchar(s.to_string()))
            }
        }
    }

    pub fn write_attr(
        &self,
        attr: &TableAttribute,
        value: &AttributeValue,
        buffer: &mut [u8],
        base_offset: u16,
    ) -> Result<(), errors::WriteAttrError> {
        let offset = base_offset
            + self
                .find_offset(attr)
                .ok_or(errors::WriteAttrError::BadAttribute)?;

        let offset_us = offset as usize;
        let size = attr.kind.size_of();

        if size > 0 && offset_us + size > buffer.len() {
            return Err(errors::WriteAttrError::OffsetOutOfBounds);
        }

        match (attr.kind, value) {
            (AttributeKind::U8, AttributeValue::U8(v)) => {
                buffer[offset_us..offset_us + size].copy_from_slice(&v.to_le_bytes());
            }
            (AttributeKind::U16, AttributeValue::U16(v)) => {
                buffer[offset_us..offset_us + size].copy_from_slice(&v.to_le_bytes());
            }
            (AttributeKind::U32, AttributeValue::U32(v)) => {
                buffer[offset_us..offset_us + size].copy_from_slice(&v.to_le_bytes());
            }
            (AttributeKind::U64, AttributeValue::U64(v)) => {
                buffer[offset_us..offset_us + size].copy_from_slice(&v.to_le_bytes());
            }
            (AttributeKind::U128, AttributeValue::U128(v)) => {
                buffer[offset_us..offset_us + size].copy_from_slice(&v.to_le_bytes());
            }
            (AttributeKind::I8, AttributeValue::I8(v)) => {
                buffer[offset_us..offset_us + size].copy_from_slice(&v.to_le_bytes());
            }
            (AttributeKind::I16, AttributeValue::I16(v)) => {
                buffer[offset_us..offset_us + size].copy_from_slice(&v.to_le_bytes());
            }
            (AttributeKind::I32, AttributeValue::I32(v)) => {
                buffer[offset_us..offset_us + size].copy_from_slice(&v.to_le_bytes());
            }
            (AttributeKind::I64, AttributeValue::I64(v)) => {
                buffer[offset_us..offset_us + size].copy_from_slice(&v.to_le_bytes());
            }
            (AttributeKind::I128, AttributeValue::I128(v)) => {
                buffer[offset_us..offset_us + size].copy_from_slice(&v.to_le_bytes());
            }
            (AttributeKind::F32, AttributeValue::F32(v)) => {
                buffer[offset_us..offset_us + size].copy_from_slice(&v.to_le_bytes());
            }
            (AttributeKind::F64, AttributeValue::F64(v)) => {
                buffer[offset_us..offset_us + size].copy_from_slice(&v.to_le_bytes());
            }
            (AttributeKind::Bool, AttributeValue::Bool(v)) => {
                buffer[offset_us] = if *v { 1 } else { 0 };
            }
            (AttributeKind::Char(max_size), AttributeValue::Char(s)) => {
                let bytes = s.as_bytes();
                if bytes.len() > max_size {
                    return Err(errors::WriteAttrError::ValueKindMismatch);
                }

                buffer[offset_us] = bytes.len() as u8;
                buffer[offset_us + 1..offset_us + 1 + bytes.len()].copy_from_slice(bytes);
                for b in &mut buffer[offset_us + 1 + bytes.len()..offset_us + size] {
                    *b = 0;
                }
            }
            (AttributeKind::Varchar, AttributeValue::Varchar(s)) => {
                let bytes = s.as_bytes();
                let len = bytes.len();
                if len > u16::MAX as usize {
                    return Err(errors::WriteAttrError::ValueKindMismatch);
                }

                // Check capacity (2 bytes len + data)
                if offset_us + 2 + len > buffer.len() {
                    return Err(errors::WriteAttrError::OffsetOutOfBounds);
                }

                // Write Length (Big Endian)
                buffer[offset_us..offset_us + 2].copy_from_slice(&(len as u16).to_be_bytes());

                // Write Data
                buffer[offset_us + 2..offset_us + 2 + len].copy_from_slice(bytes);
            }

            _ => return Err(errors::WriteAttrError::ValueKindMismatch),
        }

        Ok(())
    }
}

pub mod errors {
    #[derive(Debug)]
    pub enum WriteAttrError {
        BadAttribute,
        ValueKindMismatch,
        OffsetOutOfBounds,
    }

    #[derive(Debug)]
    pub enum ReadAttrError {
        BadAttribute,
        BadSlice,
        BadStringValue,
    }
}
