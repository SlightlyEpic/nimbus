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

const SYSTEM_TABLES_PAGE_ID: u32 = 1;
const SYSTEM_COLUMNS_PAGE_ID: u32 = 2;

pub struct Catalog {
    bp: Arc<Mutex<BufferPool>>,
    table_cache: HashMap<String, u32>,     // Name -> OID
    root_page_cache: HashMap<u32, u32>,    // OID -> Root Page ID
    schema_cache: HashMap<u32, TableType>, // OID -> Schema
    next_oid: AtomicU32,
}

impl Catalog {
    pub fn new(bp: Arc<Mutex<BufferPool>>) -> Self {
        let mut catalog = Self {
            bp,
            table_cache: HashMap::new(),
            root_page_cache: HashMap::new(),
            schema_cache: HashMap::new(),
            next_oid: AtomicU32::new(100),
        };

        catalog.init_system_tables();
        catalog
    }

    fn init_system_tables(&mut self) {
        // Register System Tables (Fixed IDs)
        self.table_cache
            .insert("system_tables".to_string(), SYSTEM_TABLES_ID);
        self.table_cache
            .insert("system_columns".to_string(), SYSTEM_COLUMNS_ID);

        self.root_page_cache
            .insert(SYSTEM_TABLES_ID, SYSTEM_TABLES_PAGE_ID);
        self.root_page_cache
            .insert(SYSTEM_COLUMNS_ID, SYSTEM_COLUMNS_PAGE_ID);

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

        if pinned_bp.as_mut().fetch_page_at_offset(0).is_err() {
            // 1. Directory
            pinned_bp
                .as_mut()
                .alloc_new_page(PageKind::Directory, 0)
                .expect("Bootstrap dir");

            // 2. System Tables Root
            let frame_tabs = pinned_bp
                .as_mut()
                .alloc_new_page(PageKind::SlottedData, SYSTEM_TABLES_PAGE_ID)
                .expect("Bootstrap tabs");
            let off_tabs = frame_tabs.file_offset();
            let tabs_fid = frame_tabs.fid();
            pinned_bp.as_mut().unpin_frame(tabs_fid).ok();
            pinned_bp
                .as_mut()
                .register_page_in_directory(SYSTEM_TABLES_PAGE_ID, off_tabs, 4000)
                .unwrap();

            // 3. System Columns Root
            let frame_cols = pinned_bp
                .as_mut()
                .alloc_new_page(PageKind::SlottedData, SYSTEM_COLUMNS_PAGE_ID)
                .expect("Bootstrap cols");
            let off_cols = frame_cols.file_offset();

            let cols_fid = frame_cols.fid();
            pinned_bp.as_mut().unpin_frame(cols_fid).ok();
            pinned_bp
                .as_mut()
                .register_page_in_directory(SYSTEM_COLUMNS_PAGE_ID, off_cols, 4000)
                .unwrap();

            pinned_bp.as_mut().flush_all().expect("Flush");
        }
    }

    fn load_state(&mut self) -> Result<(), String> {
        let mut bp_guard = self.bp.lock().map_err(|_| "Lock poisoned")?;
        let mut pinned_bp = unsafe { Pin::new_unchecked(&mut *bp_guard) };

        if pinned_bp.as_mut().fetch_page_at_offset(0).is_err() {
            return Err("Uninitialized".to_string());
        }

        // --- Load Tables ---
        let mut tables_iter = HeapIterator::new(pinned_bp.as_mut(), SYSTEM_TABLES_PAGE_ID);
        let mut max_oid = 99;

        while let Some(tuple_res) = tables_iter.next() {
            let tuple = Tuple::from_bytes(
                &tuple_res.map_err(|e| format!("{:?}", e))?,
                &get_system_tables_schema(),
            )?;

            // Schema: [oid, name, root_page]
            let oid = match tuple.values.get(0) {
                Some(AttributeValue::U32(v)) => *v,
                _ => return Err("Bad OID".into()),
            };
            let name = match tuple.values.get(1) {
                Some(AttributeValue::Varchar(v)) => v.clone(),
                _ => return Err("Bad Name".into()),
            };
            let root = match tuple.values.get(2) {
                Some(AttributeValue::U32(v)) => *v,
                _ => return Err("Bad Root".into()),
            };

            self.table_cache.insert(name, oid);
            self.root_page_cache.insert(oid, root);
            if oid > max_oid {
                max_oid = oid;
            }
        }
        self.next_oid.store(max_oid + 1, Ordering::SeqCst);

        // --- Load Columns ---
        let mut cols_iter = HeapIterator::new(pinned_bp.as_mut(), SYSTEM_COLUMNS_PAGE_ID);
        let mut table_attrs: HashMap<u32, Vec<TableAttribute>> = HashMap::new();

        while let Some(tuple_res) = cols_iter.next() {
            let tuple = Tuple::from_bytes(
                &tuple_res.map_err(|e| format!("{:?}", e))?,
                &get_system_columns_schema(),
            )?;
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

            let kind = AttributeKind::from_u8(col_type, col_len).ok_or("Unknown kind")?;
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

    // NEW: Public accessor for SeqScan
    pub fn get_table_root_page(&self, oid: u32) -> Option<u32> {
        self.root_page_cache.get(&oid).copied()
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

        // 1. Allocate the Root Data Page for this table immediately
        let root_page_id = {
            let mut bp_guard = self.bp.lock().map_err(|_| "Lock poisoned")?;
            let mut pinned_bp = unsafe { Pin::new_unchecked(&mut *bp_guard) };

            let new_pid = self.next_oid.fetch_add(1, Ordering::SeqCst);

            let frame = pinned_bp
                .as_mut()
                .alloc_new_page(PageKind::SlottedData, new_pid)
                .map_err(|e| format!("Failed to alloc table root: {:?}", e))?;

            // Register in directory
            let offset = frame.file_offset();
            let fid = frame.fid();
            pinned_bp.as_mut().unpin_frame(fid).ok();
            pinned_bp
                .as_mut()
                .register_page_in_directory(new_pid, offset, 4000)
                .unwrap();

            new_pid
        };

        // 2. Update Metadata
        self.table_cache.insert(name.to_string(), oid);
        self.root_page_cache.insert(oid, root_page_id);
        self.schema_cache.insert(oid, schema.clone());

        // 3. Insert into system_tables: [oid, name, root_page]
        let sys_tables_schema = get_system_tables_schema();
        let table_row = Tuple::new(vec![
            AttributeValue::U32(oid),
            AttributeValue::Varchar(name.to_string()),
            AttributeValue::U32(root_page_id), // Storing the root
        ]);
        self.insert_tuple(SYSTEM_TABLES_ID, &table_row, &sys_tables_schema)?;

        // 4. Insert columns
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
        // Determine start page: Fixed for system tables, Dynamic (0) for user tables
        let start_page = if table_oid == SYSTEM_TABLES_ID {
            SYSTEM_TABLES_PAGE_ID
        } else if table_oid == SYSTEM_COLUMNS_ID {
            SYSTEM_COLUMNS_PAGE_ID
        } else {
            0
        };

        // HeapFile logic:
        // - If start_page is 0, it means "New/Unknown", so it will alloc a new page.
        // - If start_page is fixed (1 or 2), it tries that page first.

        let mut heap = HeapFile::new(start_page, start_page);
        let bytes = tuple.to_bytes(schema)?;

        let mut bp_guard = self.bp.lock().map_err(|_| "Lock poisoned")?;
        let txn_id = AtomicU32::new(0);
        let mut pinned_bp = unsafe { Pin::new_unchecked(&mut *bp_guard) };

        // Use the catalog's main OID counter for page allocation
        heap.insert(pinned_bp, &self.next_oid, &bytes)
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
