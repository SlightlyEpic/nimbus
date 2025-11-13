use crate::rt_type::primitives::{AttributeKind, AttributeValue, TableAttribute, TableType};
use crate::storage::buffer::BufferPool;
use crate::storage::heap::heap_file::HeapFile;
use crate::storage::heap::iterator::HeapIterator;
use crate::storage::heap::tuple::Tuple;
use crate::storage::page::base::PageKind;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

use super::schema::{
    SYSTEM_COLUMNS_ID, SYSTEM_TABLES_ID, get_system_columns_schema, get_system_tables_schema,
};

// Fixed Page IDs for system tables
const SYSTEM_TABLES_PAGE_ID: u32 = 1;
const SYSTEM_COLUMNS_PAGE_ID: u32 = 2;

pub struct Catalog {
    bp: Arc<Mutex<BufferPool>>,
    table_cache: HashMap<String, u32>,
    schema_cache: HashMap<u32, TableType>,
    next_oid: AtomicU32,
}

impl Catalog {
    pub fn new(bp: Arc<Mutex<BufferPool>>) -> Self {
        let mut catalog = Self {
            bp,
            table_cache: HashMap::new(),
            schema_cache: HashMap::new(),
            next_oid: AtomicU32::new(100),
        };

        catalog.init_system_tables();
        catalog
    }

    fn init_system_tables(&mut self) {
        self.table_cache
            .insert("system_tables".to_string(), SYSTEM_TABLES_ID);
        self.table_cache
            .insert("system_columns".to_string(), SYSTEM_COLUMNS_ID);

        self.schema_cache
            .insert(SYSTEM_TABLES_ID, get_system_tables_schema());
        self.schema_cache
            .insert(SYSTEM_COLUMNS_ID, get_system_columns_schema());

        if let Err(_) = self.load_state() {
            self.bootstrap_new_db();
        }
    }

    fn bootstrap_new_db(&self) {
        let mut bp_guard = self.bp.lock().expect("Lock poisoned");
        let mut pinned_bp = unsafe { Pin::new_unchecked(&mut *bp_guard) };

        // 1. Create Directory (Page 0)
        if pinned_bp.as_mut().fetch_page_at_offset(0).is_err() {
            pinned_bp
                .as_mut()
                .alloc_new_page(PageKind::Directory, 0)
                .expect("Bootstrap: Failed to allocate root directory");

            // 2. Create system_tables Root (Page 1)
            let frame_tabs = pinned_bp
                .as_mut()
                .alloc_new_page(PageKind::SlottedData, SYSTEM_TABLES_PAGE_ID)
                .expect("Bootstrap: Failed to alloc system_tables page");
            let fid_tabs = frame_tabs.fid();
            let offset_tabs = frame_tabs.file_offset();

            // Manually register in directory
            pinned_bp.as_mut().unpin_frame(fid_tabs).ok();
            pinned_bp
                .as_mut()
                .register_page_in_directory(SYSTEM_TABLES_PAGE_ID, offset_tabs, 4000)
                .unwrap();

            // 3. Create system_columns Root (Page 2)
            let frame_cols = pinned_bp
                .as_mut()
                .alloc_new_page(PageKind::SlottedData, SYSTEM_COLUMNS_PAGE_ID)
                .expect("Bootstrap: Failed to alloc system_columns page");
            let fid_cols = frame_cols.fid();
            let offset_cols = frame_cols.file_offset();

            pinned_bp.as_mut().unpin_frame(fid_cols).ok();
            pinned_bp
                .as_mut()
                .register_page_in_directory(SYSTEM_COLUMNS_PAGE_ID, offset_cols, 4000)
                .unwrap();

            // Flush all
            pinned_bp
                .as_mut()
                .flush_all()
                .expect("Bootstrap: Failed to flush");
        }
    }

    fn load_state(&mut self) -> Result<(), String> {
        let mut bp_guard = self.bp.lock().map_err(|_| "Lock poisoned")?;
        let mut pinned_bp = unsafe { Pin::new_unchecked(&mut *bp_guard) };

        // Check DB init
        if pinned_bp.as_mut().fetch_page_at_offset(0).is_err() {
            return Err("Database uninitialized".to_string());
        }

        // --- Step A: Recover Tables (Start at Page 1) ---
        let mut tables_iter = HeapIterator::new(pinned_bp.as_mut(), SYSTEM_TABLES_PAGE_ID);
        let mut max_oid = 99;

        while let Some(tuple_bytes_res) = tables_iter.next() {
            let tuple_bytes = tuple_bytes_res.map_err(|e| format!("Scan tables error: {:?}", e))?;
            let tuple = Tuple::from_bytes(&tuple_bytes, &get_system_tables_schema())?;

            let oid = match tuple.values.get(0) {
                Some(AttributeValue::U32(v)) => *v,
                _ => return Err("Corrupt system_tables".to_string()),
            };
            let name = match tuple.values.get(1) {
                Some(AttributeValue::Varchar(v)) => v.clone(),
                _ => return Err("Corrupt system_tables".to_string()),
            };

            self.table_cache.insert(name, oid);
            if oid > max_oid {
                max_oid = oid;
            }
        }
        self.next_oid.store(max_oid + 1, Ordering::SeqCst);

        // --- Step B: Recover Columns (Start at Page 2) ---
        // Reset iterator for columns
        let mut cols_iter = HeapIterator::new(pinned_bp.as_mut(), SYSTEM_COLUMNS_PAGE_ID);
        let mut table_attrs: HashMap<u32, Vec<TableAttribute>> = HashMap::new();

        while let Some(tuple_bytes_res) = cols_iter.next() {
            let tuple_bytes =
                tuple_bytes_res.map_err(|e| format!("Scan columns error: {:?}", e))?;
            let tuple = Tuple::from_bytes(&tuple_bytes, &get_system_columns_schema())?;

            // Unpack: [table_oid, col_name, col_type, col_len]
            let table_oid = match tuple.values.get(0) {
                Some(AttributeValue::U32(v)) => *v,
                _ => continue,
            };
            let col_name = match tuple.values.get(1) {
                Some(AttributeValue::Varchar(v)) => v.clone(),
                _ => continue,
            };
            let col_type = match tuple.values.get(2) {
                Some(AttributeValue::U8(v)) => *v,
                _ => continue,
            };
            let col_len = match tuple.values.get(3) {
                Some(AttributeValue::U16(v)) => *v,
                _ => continue,
            };

            let kind = AttributeKind::from_u8(col_type, col_len).ok_or("Unknown type")?;

            let attr = TableAttribute {
                name: col_name,
                kind,
                nullable: false,
                is_internal: false,
            };
            table_attrs.entry(table_oid).or_default().push(attr);
        }

        for (oid, attrs) in table_attrs {
            let schema = TableType {
                attributes: attrs,
                layout: crate::rt_type::primitives::TableLayout {
                    size: 0,
                    attr_layouts: vec![],
                },
            };
            self.schema_cache.insert(oid, schema);
        }

        Ok(())
    }

    pub fn get_table_oid(&self, name: &str) -> Option<u32> {
        self.table_cache.get(name).copied()
    }

    pub fn get_table_schema(&self, oid: u32) -> Option<TableType> {
        self.schema_cache.get(&oid).cloned()
    }

    pub fn create_table(&mut self, name: &str, schema: TableType) -> Result<u32, String> {
        if self.table_cache.contains_key(name) {
            return Err(format!("Table '{}' already exists", name));
        }

        let oid = self.next_oid.fetch_add(1, Ordering::SeqCst);
        self.table_cache.insert(name.to_string(), oid);
        self.schema_cache.insert(oid, schema.clone());

        let sys_tables_schema = get_system_tables_schema();
        let table_row = Tuple::new(vec![
            AttributeValue::U32(oid),
            AttributeValue::Varchar(name.to_string()),
        ]);

        self.insert_tuple(SYSTEM_TABLES_ID, &table_row, &sys_tables_schema)?;

        let sys_cols_schema = get_system_columns_schema();
        for col in &schema.attributes {
            let max_len = match col.kind {
                AttributeKind::Char(n) => n as u16,
                _ => 0,
            };
            let col_row = Tuple::new(vec![
                AttributeValue::U32(oid),
                AttributeValue::Varchar(col.name.clone()),
                AttributeValue::U8(col.kind.to_u8()),
                AttributeValue::U16(max_len),
            ]);
            self.insert_tuple(SYSTEM_COLUMNS_ID, &col_row, &sys_cols_schema)?;
        }

        Ok(oid)
    }

    fn insert_tuple(
        &self,
        table_oid: u32,
        tuple: &Tuple,
        schema: &TableType,
    ) -> Result<(), String> {
        let start_page = if table_oid == SYSTEM_TABLES_ID {
            SYSTEM_TABLES_PAGE_ID
        } else if table_oid == SYSTEM_COLUMNS_ID {
            SYSTEM_COLUMNS_PAGE_ID
        } else {
            0 // For user tables, dynamic
        };

        let mut heap = HeapFile::new(start_page, 0);
        let bytes = tuple.to_bytes(schema)?;

        let mut bp_guard = self.bp.lock().map_err(|_| "Lock poisoned")?;
        let txn_id = AtomicU32::new(0);
        let mut pinned_bp = unsafe { Pin::new_unchecked(&mut *bp_guard) };

        // If targeting system tables, we force using the specific page ID to avoid
        // "find_page_with_space" returning the wrong page (like returning page 2 for table 1).
        // Note: This is a simplification. Real DBs use separate files or strict linking.
        if start_page != 0 {
            // Attempt to insert into specific start page directly
            let frame = pinned_bp
                .as_mut()
                .fetch_page(start_page)
                .map_err(|e| format!("{:?}", e))?;
            let fid = frame.fid();

            // Scope for page view
            let inserted = {
                let mut view = frame.page_view();
                if let crate::storage::page::base::Page::SlottedData(slotted) = &mut view {
                    slotted.add_slot(&bytes).is_ok()
                } else {
                    false
                }
            };

            if inserted {
                pinned_bp.as_mut().mark_frame_dirty(fid);
                pinned_bp.as_mut().unpin_frame(fid).ok();
                return Ok(());
            }
            pinned_bp.as_mut().unpin_frame(fid).ok();
            // If failed (full), fall back to standard heap insert
        }

        heap.insert(pinned_bp, &txn_id, &bytes)
            .map_err(|e| format!("Heap insert failed: {:?}", e))?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::catalog::manager::Catalog;
    use crate::rt_type::primitives::{AttributeKind, TableAttribute, TableLayout, TableType};
    use crate::storage::buffer::BufferPool;
    use crate::storage::buffer::fifo_evictor::FifoEvictor;
    use crate::storage::disk::FileManager;
    use crate::storage::page_locator::locator::DirectoryPageLocator;
    use std::fs;
    use std::sync::{Arc, Mutex};

    #[test]
    fn test_create_table_persists_metadata() {
        let db_file = "test_db/test_catalog.db";
        let _ = fs::remove_file(db_file);

        let fm = FileManager::new(db_file.to_string()).unwrap();
        let bp = Arc::new(Mutex::new(BufferPool::new(
            fm,
            Box::new(FifoEvictor::new()),
            Box::new(DirectoryPageLocator::new()),
        )));

        let mut catalog = Catalog::new(bp.clone());

        let user_schema = TableType {
            attributes: vec![
                TableAttribute {
                    name: "username".into(),
                    kind: AttributeKind::Varchar,
                    nullable: false,
                    is_internal: false,
                },
                TableAttribute {
                    name: "age".into(),
                    kind: AttributeKind::U8,
                    nullable: false,
                    is_internal: false,
                },
            ],
            layout: TableLayout {
                size: 0,
                attr_layouts: vec![],
            },
        };

        let oid = catalog
            .create_table("users", user_schema.clone())
            .expect("Create failed");

        assert!(oid >= 100);
        assert_eq!(catalog.get_table_oid("users"), Some(oid));

        fs::remove_file(db_file).unwrap();
    }

    #[test]
    fn test_catalog_bootstrap() {
        let db_file = "test_db/test_bootstrap.db";

        let _ = fs::create_dir_all("test_db");
        let _ = fs::remove_file(db_file);

        {
            let fm = FileManager::new(db_file.to_string()).unwrap();
            let bp = Arc::new(Mutex::new(BufferPool::new(
                fm,
                Box::new(FifoEvictor::new()),
                Box::new(DirectoryPageLocator::new()),
            )));
            let mut catalog = Catalog::new(bp.clone());

            let schema = TableType {
                attributes: vec![TableAttribute {
                    name: "data".into(),
                    kind: AttributeKind::U64,
                    nullable: false,
                    is_internal: false,
                }],
                layout: TableLayout {
                    size: 0,
                    attr_layouts: vec![],
                },
            };

            catalog.create_table("saved_table", schema).unwrap();

            let mut bp_guard = bp.lock().unwrap();
            let mut pinned_bp = unsafe { std::pin::Pin::new_unchecked(&mut *bp_guard) };
            pinned_bp.flush_all().unwrap();
        }

        {
            let fm = FileManager::new(db_file.to_string()).unwrap();
            let bp = Arc::new(Mutex::new(BufferPool::new(
                fm,
                Box::new(FifoEvictor::new()),
                Box::new(DirectoryPageLocator::new()),
            )));
            let catalog = Catalog::new(bp.clone());

            let oid = catalog.get_table_oid("saved_table");
            assert!(
                oid.is_some(),
                "Catalog failed to load 'saved_table' from disk"
            );

            let loaded_schema = catalog.get_table_schema(oid.unwrap()).unwrap();
            assert_eq!(loaded_schema.attributes[0].name, "data");
        }

        fs::remove_file(db_file).unwrap();
        fs::remove_dir("test_db").unwrap();
    }
}
