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
    Varchar,
}

#[derive(Debug, PartialEq, Clone)]
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
    Varchar(String),
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
            AttributeKind::Char(size) => size + 1,
            AttributeKind::Varchar => 0,
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
            AttributeKind::Varchar => 1,
        }
    }

    pub fn to_u8(&self) -> u8 {
        match self {
            AttributeKind::U8 => 1,
            AttributeKind::U16 => 2,
            AttributeKind::U32 => 3,
            AttributeKind::U64 => 4,
            AttributeKind::U128 => 5,
            AttributeKind::I8 => 6,
            AttributeKind::I16 => 7,
            AttributeKind::I32 => 8,
            AttributeKind::I64 => 9,
            AttributeKind::I128 => 10,
            AttributeKind::F32 => 11,
            AttributeKind::F64 => 12,
            AttributeKind::Bool => 13,
            AttributeKind::Char(_) => 14,
            AttributeKind::Varchar => 15,
        }
    }
    pub fn from_u8(kind: u8, size: u16) -> Option<Self> {
        match kind {
            1 => Some(AttributeKind::U8),
            2 => Some(AttributeKind::U16),
            3 => Some(AttributeKind::U32),
            4 => Some(AttributeKind::U64),
            5 => Some(AttributeKind::U128),
            6 => Some(AttributeKind::I8),
            7 => Some(AttributeKind::I16),
            8 => Some(AttributeKind::I32),
            9 => Some(AttributeKind::I64),
            10 => Some(AttributeKind::I128),
            11 => Some(AttributeKind::F32),
            12 => Some(AttributeKind::F64),
            13 => Some(AttributeKind::Bool),
            14 => Some(AttributeKind::Char(size as usize)),
            15 => Some(AttributeKind::Varchar),
            _ => None,
        }
    }
}
