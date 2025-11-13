use crate::rt_type::primitives::TableType;
use crate::storage::buffer::BufferPool;
use crate::storage::heap::heap_file::HeapFile;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use super::schema::{
    SYSTEM_COLUMNS_ID, SYSTEM_TABLES_ID, get_system_columns_schema, get_system_tables_schema,
};

pub struct Catalog {
    bp: Arc<Mutex<BufferPool>>,
    // Cache: Table Name -> Table OID
    table_cache: HashMap<String, u32>,
    // Cache: Table OID -> Schema
    schema_cache: HashMap<u32, TableType>,
}

impl Catalog {
    pub fn new(bp: Arc<Mutex<BufferPool>>) -> Self {
        let mut catalog = Self {
            bp,
            table_cache: HashMap::new(),
            schema_cache: HashMap::new(),
        };

        // Ensure system tables exist (Bootstrapping)
        catalog.init_system_tables();
        catalog
    }

    fn init_system_tables(&mut self) {
        // TODO: Check if HeapFiles 1 and 2 exist.
        // If not, create them using the hardcoded schemas.
        // For now, we just populate the cache manually.

        self.table_cache
            .insert("system_tables".to_string(), SYSTEM_TABLES_ID);
        self.table_cache
            .insert("system_columns".to_string(), SYSTEM_COLUMNS_ID);

        self.schema_cache
            .insert(SYSTEM_TABLES_ID, get_system_tables_schema());
        self.schema_cache
            .insert(SYSTEM_COLUMNS_ID, get_system_columns_schema());
    }

    /// Returns the Schema for a given table name (if it exists)
    pub fn get_table_schema(&self, name: &str) -> Option<TableType> {
        let oid = self.table_cache.get(name)?;
        self.schema_cache.get(oid).cloned()
    }
}
