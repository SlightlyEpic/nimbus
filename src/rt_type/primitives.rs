#[derive(Copy, Clone, PartialEq)]
pub enum AttributeKind {
    U8,
    U16,
    U32,
    U64,
    U128,

    I8,
    I16,
    I32,
    I64,
    I128,

    F32,
    F64,

    Bool,
    Char(usize),
}

pub enum AttributeValue {
    U8(u8),
    U16(u16),
    U32(u32),
    U64(u64),
    U128(u128),

    I8(i8),
    I16(i16),
    I32(i32),
    I64(i64),
    I128(i128),

    F32(f32),
    F64(f64),

    Bool(bool),
    Char(String),
}

#[derive(Clone)]
pub struct TableAttribute {
    pub kind: AttributeKind,
    pub name: String,
    pub nullable: bool,
    pub is_internal: bool,
}

#[derive(Clone)]
pub struct TableType {
    pub attributes: Vec<TableAttribute>,
    pub layout: TableLayout,
}

#[derive(Clone)]
pub struct LayoutAttrData {
    pub attr_name: String,
    pub offset: u16,
}

#[derive(Clone)]
pub struct TableLayout {
    pub size: usize,
    pub attr_layouts: Vec<LayoutAttrData>,
}

impl AttributeKind {
    pub fn size_of(self) -> usize {
        match self {
            AttributeKind::U8 => 1,
            AttributeKind::U16 => 2,
            AttributeKind::U32 => 4,
            AttributeKind::U64 => 8,
            AttributeKind::U128 => 16,
            AttributeKind::I8 => 1,
            AttributeKind::I16 => 2,
            AttributeKind::I32 => 4,
            AttributeKind::I64 => 8,
            AttributeKind::I128 => 16,
            AttributeKind::F32 => 4,
            AttributeKind::F64 => 8,
            AttributeKind::Bool => 1,
            AttributeKind::Char(size) => size,
        }
    }

    pub fn alignment(self) -> usize {
        match self {
            AttributeKind::U8 => 1,
            AttributeKind::U16 => 2,
            AttributeKind::U32 => 4,
            AttributeKind::U64 => 8,
            AttributeKind::U128 => 16,
            AttributeKind::I8 => 1,
            AttributeKind::I16 => 2,
            AttributeKind::I32 => 4,
            AttributeKind::I64 => 8,
            AttributeKind::I128 => 16,
            AttributeKind::F32 => 4,
            AttributeKind::F64 => 8,
            AttributeKind::Bool => 1,
            AttributeKind::Char(size) => {
                const SIZES: [usize; 5] = [1, 2, 4, 8, 16];
                for s in SIZES {
                    if size <= s {
                        return s;
                    }
                }
                16
            }
        }
    }

    pub fn from_le_bytes(self, bytes: &[u8]) -> AttributeValue {
        match self {
            AttributeKind::U8 => AttributeValue::U8(u8::from_le_bytes(bytes.try_into().unwrap())),
            AttributeKind::U16 => {
                AttributeValue::U16(u16::from_le_bytes(bytes.try_into().unwrap()))
            }
            AttributeKind::U32 => {
                AttributeValue::U32(u32::from_le_bytes(bytes.try_into().unwrap()))
            }
            AttributeKind::U64 => {
                AttributeValue::U64(u64::from_le_bytes(bytes.try_into().unwrap()))
            }
            AttributeKind::U128 => {
                AttributeValue::U128(u128::from_le_bytes(bytes.try_into().unwrap()))
            }
            AttributeKind::I8 => AttributeValue::I8(i8::from_le_bytes(bytes.try_into().unwrap())),
            AttributeKind::I16 => {
                AttributeValue::I16(i16::from_le_bytes(bytes.try_into().unwrap()))
            }
            AttributeKind::I32 => {
                AttributeValue::I32(i32::from_le_bytes(bytes.try_into().unwrap()))
            }
            AttributeKind::I64 => {
                AttributeValue::I64(i64::from_le_bytes(bytes.try_into().unwrap()))
            }
            AttributeKind::I128 => {
                AttributeValue::I128(i128::from_le_bytes(bytes.try_into().unwrap()))
            }
            AttributeKind::F32 => {
                AttributeValue::F32(f32::from_le_bytes(bytes.try_into().unwrap()))
            }
            AttributeKind::F64 => {
                AttributeValue::F64(f64::from_le_bytes(bytes.try_into().unwrap()))
            }
            AttributeKind::Bool => {
                let bytes_arr: [u8; 1] = bytes.try_into().unwrap();
                AttributeValue::Bool(bytes_arr[0] == 1u8)
            }
            AttributeKind::Char(max_size) => {
                // first byte stores actual length
                let actual_len = bytes[0] as usize;
                if actual_len > max_size - 1 {
                    panic!("Invalid string length: {}", actual_len);
                }

                // slice only the string part
                let str_bytes = &bytes[1..1 + actual_len];

                // decode UTF-8 safely
                let s = std::str::from_utf8(str_bytes)
                    .unwrap_or_else(|_| panic!("Invalid UTF-8 string data"));

                AttributeValue::Char(s.to_string())
            }
        }
    }
}
