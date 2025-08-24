#[derive(Copy, Clone)]
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
