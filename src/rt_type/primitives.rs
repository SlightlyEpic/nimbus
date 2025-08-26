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
}
