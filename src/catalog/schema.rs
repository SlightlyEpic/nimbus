use crate::rt_type::primitives::{AttributeKind, TableAttribute, TableLayout, TableType};
pub const SYSTEM_TABLES_ID: u32 = 1;
pub const SYSTEM_COLUMNS_ID: u32 = 2;

/// Defines the schema for "system_tables"
/// Columns: [oid (U32), table_name (Varchar), root_page (U32)]
pub fn get_system_tables_schema() -> TableType {
    TableType {
        attributes: vec![
            TableAttribute {
                name: "oid".to_string(),
                kind: AttributeKind::U32,
                nullable: false,
                is_internal: true,
            },
            TableAttribute {
                name: "table_name".to_string(),
                kind: AttributeKind::Varchar,
                nullable: false,
                is_internal: true,
            },
            // NEW: Track where the table data starts
            TableAttribute {
                name: "root_page".to_string(),
                kind: AttributeKind::U32,
                nullable: false,
                is_internal: true,
            },
        ],
        layout: TableLayout {
            size: 0,
            attr_layouts: vec![],
        },
    }
}

/// Defines the schema for "system_columns"
/// Columns: [table_oid (U32), col_name (Varchar), col_type (U8), col_len (U16)]
pub fn get_system_columns_schema() -> TableType {
    TableType {
        attributes: vec![
            TableAttribute {
                name: "table_oid".to_string(),
                kind: AttributeKind::U32,
                nullable: false,
                is_internal: true,
            },
            TableAttribute {
                name: "col_name".to_string(),
                kind: AttributeKind::Varchar,
                nullable: false,
                is_internal: true,
            },
            TableAttribute {
                name: "col_type".to_string(),
                kind: AttributeKind::U8,
                nullable: false,
                is_internal: true,
            },
            TableAttribute {
                name: "col_max_len".to_string(),
                kind: AttributeKind::U16,
                nullable: false,
                is_internal: true,
            },
        ],
        layout: TableLayout {
            size: 0,
            attr_layouts: vec![],
        },
    }
}
