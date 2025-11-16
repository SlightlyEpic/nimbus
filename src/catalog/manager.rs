use crate::catalog::schema::SYSTEM_INDEXES_ID;
use crate::rt_type::primitives::{AttributeKind, AttributeValue, TableAttribute, TableType};
use crate::storage::bplus_tree::BPlusTree;
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
    SYSTEM_COLUMNS_ID, SYSTEM_TABLES_ID, get_system_columns_schema, get_system_indexes_schema,
    get_system_tables_schema,
};

// Fixed Page IDs for system tables
const SYSTEM_TABLES_PAGE_ID: u32 = 1;
const SYSTEM_COLUMNS_PAGE_ID: u32 = 2;
const SYSTEM_INDEXES_PAGE_ID: u32 = 3;

#[derive(Clone, Debug)]
pub struct IndexMeta {
    pub table_oid: u32,
    pub column_idx: usize,
    pub root_page_id: u32,
}

pub struct Catalog {
    bp: Arc<Mutex<BufferPool>>,
    table_cache: HashMap<String, u32>,
    schema_cache: HashMap<u32, TableType>,
    root_page_cache: HashMap<u32, u32>,
    index_name_cache: HashMap<String, u32>, // IndexName -> IndexOID
    index_meta_cache: HashMap<u32, IndexMeta>, // IndexOID -> Metadata
    table_indexes: HashMap<u32, Vec<u32>>,
    next_oid: AtomicU32,
}

impl Catalog {
    pub fn new(bp: Arc<Mutex<BufferPool>>) -> Self {
        let mut catalog = Self {
            bp,
            table_cache: HashMap::new(),
            schema_cache: HashMap::new(),
            root_page_cache: HashMap::new(),
            index_name_cache: HashMap::new(),
            index_meta_cache: HashMap::new(),
            table_indexes: HashMap::new(),
            next_oid: AtomicU32::new(100),
        };

        catalog.init_system_tables();
        catalog
    }

    fn init_system_tables(&mut self) {
        // Register Tables
        self.table_cache
            .insert("system_tables".to_string(), SYSTEM_TABLES_ID);
        self.table_cache
            .insert("system_columns".to_string(), SYSTEM_COLUMNS_ID);
        self.table_cache
            .insert("system_indexes".to_string(), SYSTEM_INDEXES_ID);

        self.root_page_cache
            .insert(SYSTEM_TABLES_ID, SYSTEM_TABLES_PAGE_ID);
        self.root_page_cache
            .insert(SYSTEM_COLUMNS_ID, SYSTEM_COLUMNS_PAGE_ID);
        self.root_page_cache
            .insert(SYSTEM_INDEXES_ID, SYSTEM_INDEXES_PAGE_ID);

        self.schema_cache
            .insert(SYSTEM_TABLES_ID, get_system_tables_schema());
        self.schema_cache
            .insert(SYSTEM_COLUMNS_ID, get_system_columns_schema());
        self.schema_cache
            .insert(SYSTEM_INDEXES_ID, get_system_indexes_schema());

        if let Err(_) = self.load_state() {
            self.bootstrap_new_db();
            self.bootstrap_system_metadata();
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

            // 2. System Tables
            let frame = pinned_bp
                .as_mut()
                .alloc_new_page(PageKind::SlottedData, SYSTEM_TABLES_PAGE_ID)
                .expect("Bootstrap tabs");
            let offset = frame.file_offset();
            let fid = frame.fid();
            pinned_bp.as_mut().unpin_frame(fid).ok();
            pinned_bp
                .as_mut()
                .register_page_in_directory(SYSTEM_TABLES_PAGE_ID, offset, 4000)
                .unwrap();

            // 3. System Columns
            let frame = pinned_bp
                .as_mut()
                .alloc_new_page(PageKind::SlottedData, SYSTEM_COLUMNS_PAGE_ID)
                .expect("Bootstrap cols");
            let offset = frame.file_offset();
            let fid = frame.fid();
            pinned_bp.as_mut().unpin_frame(fid).ok();
            pinned_bp
                .as_mut()
                .register_page_in_directory(SYSTEM_COLUMNS_PAGE_ID, offset, 4000)
                .unwrap();

            // 4. System Indexes
            let frame = pinned_bp
                .as_mut()
                .alloc_new_page(PageKind::SlottedData, SYSTEM_INDEXES_PAGE_ID)
                .expect("Bootstrap idx");
            let offset = frame.file_offset();
            let fid = frame.fid();
            pinned_bp.as_mut().unpin_frame(fid).ok();
            pinned_bp
                .as_mut()
                .register_page_in_directory(SYSTEM_INDEXES_PAGE_ID, offset, 4000)
                .unwrap();

            pinned_bp.as_mut().flush_all().expect("Flush");
        }
    }

    fn bootstrap_system_metadata(&self) {
        let sys_tables_schema = get_system_tables_schema();
        let sys_cols_schema = get_system_columns_schema();
        let sys_idxs_schema = get_system_indexes_schema();

        let mut bp_guard = self.bp.lock().expect("Lock poisoned");
        let mut pinned_bp = unsafe { Pin::new_unchecked(&mut *bp_guard) };

        let row = Tuple::new(vec![
            AttributeValue::U32(SYSTEM_TABLES_ID),
            AttributeValue::Varchar("system_tables".to_string()),
            AttributeValue::U32(SYSTEM_TABLES_PAGE_ID),
        ]);
        self.insert_tuple(
            SYSTEM_TABLES_ID,
            &row,
            &sys_tables_schema,
            pinned_bp.as_mut(),
        )
        .unwrap();

        let row = Tuple::new(vec![
            AttributeValue::U32(SYSTEM_COLUMNS_ID),
            AttributeValue::Varchar("system_columns".to_string()),
            AttributeValue::U32(SYSTEM_COLUMNS_PAGE_ID),
        ]);
        self.insert_tuple(
            SYSTEM_TABLES_ID,
            &row,
            &sys_tables_schema,
            pinned_bp.as_mut(),
        )
        .unwrap();

        let row = Tuple::new(vec![
            AttributeValue::U32(SYSTEM_INDEXES_ID),
            AttributeValue::Varchar("system_indexes".to_string()),
            AttributeValue::U32(SYSTEM_INDEXES_PAGE_ID),
        ]);
        self.insert_tuple(
            SYSTEM_TABLES_ID,
            &row,
            &sys_tables_schema,
            pinned_bp.as_mut(),
        )
        .unwrap();

        for col in &sys_tables_schema.attributes {
            self.insert_column_metadata(
                SYSTEM_TABLES_ID,
                col,
                &sys_cols_schema,
                pinned_bp.as_mut(),
            );
        }
        for col in &sys_cols_schema.attributes {
            self.insert_column_metadata(
                SYSTEM_COLUMNS_ID,
                col,
                &sys_cols_schema,
                pinned_bp.as_mut(),
            );
        }
        for col in &sys_idxs_schema.attributes {
            self.insert_column_metadata(
                SYSTEM_INDEXES_ID,
                col,
                &sys_cols_schema,
                pinned_bp.as_mut(),
            );
        }
    }

    fn insert_column_metadata(
        &self,
        table_oid: u32,
        col: &TableAttribute,
        schema: &TableType,
        bpm: Pin<&mut BufferPool>,
    ) {
        let max_len = match col.kind {
            AttributeKind::Char(n) => n as u16,
            _ => 0,
        };
        let row = Tuple::new(vec![
            AttributeValue::U32(table_oid),
            AttributeValue::Varchar(col.name.clone()),
            AttributeValue::U8(col.kind.to_u8()),
            AttributeValue::U16(max_len),
        ]);
        self.insert_tuple(SYSTEM_COLUMNS_ID, &row, schema, bpm)
            .unwrap();
    }

    fn load_state(&mut self) -> Result<(), String> {
        let mut bp_guard = self.bp.lock().map_err(|_| "Lock poisoned")?;
        let mut pinned_bp = unsafe { Pin::new_unchecked(&mut *bp_guard) };

        if pinned_bp.as_mut().fetch_page_at_offset(0).is_err() {
            return Err("Uninitialized".to_string());
        }

        // 1. Load Tables
        let mut iter = HeapIterator::new(pinned_bp.as_mut(), SYSTEM_TABLES_PAGE_ID);
        let mut max_oid = 99;
        while let Some(res) = iter.next() {
            let (_, bytes) = res.map_err(|e| format!("{:?}", e))?;
            let t = Tuple::from_bytes(&bytes, &get_system_tables_schema())?;
            let oid = match t.values.get(0) {
                Some(AttributeValue::U32(v)) => *v,
                _ => continue,
            };
            let name = match t.values.get(1) {
                Some(AttributeValue::Varchar(v)) => v.clone(),
                _ => continue,
            };
            let root = match t.values.get(2) {
                Some(AttributeValue::U32(v)) => *v,
                _ => continue,
            };

            self.table_cache.insert(name, oid);
            self.root_page_cache.insert(oid, root);
            if oid > max_oid {
                max_oid = oid;
            }
        }
        self.next_oid.store(max_oid + 1, Ordering::SeqCst);

        // 2. Load Columns
        let mut iter = HeapIterator::new(pinned_bp.as_mut(), SYSTEM_COLUMNS_PAGE_ID);
        let mut table_attrs: HashMap<u32, Vec<TableAttribute>> = HashMap::new();
        while let Some(res) = iter.next() {
            let (_, bytes) = res.map_err(|e| format!("{:?}", e))?;
            let t = Tuple::from_bytes(&bytes, &get_system_columns_schema())?;
            let tid = match t.values.get(0) {
                Some(AttributeValue::U32(v)) => *v,
                _ => continue,
            };
            let name = match t.values.get(1) {
                Some(AttributeValue::Varchar(v)) => v.clone(),
                _ => continue,
            };
            let typ = match t.values.get(2) {
                Some(AttributeValue::U8(v)) => *v,
                _ => continue,
            };
            let len = match t.values.get(3) {
                Some(AttributeValue::U16(v)) => *v,
                _ => continue,
            };
            let kind = AttributeKind::from_u8(typ, len).unwrap_or(AttributeKind::Varchar);

            table_attrs.entry(tid).or_default().push(TableAttribute {
                name,
                kind,
                nullable: false,
                is_internal: false,
            });
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

        // 3. Load Indexes
        let mut iter = HeapIterator::new(pinned_bp.as_mut(), SYSTEM_INDEXES_PAGE_ID);
        while let Some(res) = iter.next() {
            let (_, bytes) = res.map_err(|e| format!("{:?}", e))?;
            let t = Tuple::from_bytes(&bytes, &get_system_indexes_schema())?;

            let idx_oid = match t.values.get(0) {
                Some(AttributeValue::U32(v)) => *v,
                _ => continue,
            };
            let idx_name = match t.values.get(1) {
                Some(AttributeValue::Varchar(v)) => v.clone(),
                _ => continue,
            };
            let tbl_oid = match t.values.get(2) {
                Some(AttributeValue::U32(v)) => *v,
                _ => continue,
            };
            let col_idx = match t.values.get(3) {
                Some(AttributeValue::U8(v)) => *v,
                _ => continue,
            };
            let root = match t.values.get(4) {
                Some(AttributeValue::U32(v)) => *v,
                _ => continue,
            };

            self.index_name_cache.insert(idx_name, idx_oid);
            self.index_meta_cache.insert(
                idx_oid,
                IndexMeta {
                    table_oid: tbl_oid,
                    column_idx: col_idx as usize,
                    root_page_id: root,
                },
            );

            // UPDATE: Populate the table_indexes cache
            self.table_indexes.entry(tbl_oid).or_default().push(idx_oid);

            if idx_oid > max_oid {
                self.next_oid.store(idx_oid + 1, Ordering::SeqCst);
            }
        }

        Ok(())
    }

    pub fn get_table_root_page(&self, oid: u32) -> Option<u32> {
        self.root_page_cache.get(&oid).copied()
    }

    pub fn get_table_oid(&self, name: &str) -> Option<u32> {
        self.table_cache.get(name).copied()
    }

    pub fn get_table_schema(&self, oid: u32) -> Option<TableType> {
        self.schema_cache.get(&oid).cloned()
    }

    pub fn get_index_oid(&self, index_name: &str) -> Option<u32> {
        self.index_name_cache.get(index_name).copied()
    }

    pub fn get_index_meta(&self, index_oid: u32) -> Option<IndexMeta> {
        self.index_meta_cache.get(&index_oid).cloned()
    }

    pub fn find_index_for_column(&self, table_name: &str, col_name: &str) -> Option<u32> {
        let table_oid = self.get_table_oid(table_name)?;
        let schema = self.get_table_schema(table_oid)?;
        let col_idx = schema
            .attributes
            .iter()
            .position(|attr| attr.name == col_name)?;

        // Find an index that points to this table and this column index
        let table_indexes = self.table_indexes.get(&table_oid)?;

        for idx_oid in table_indexes {
            let meta = self.index_meta_cache.get(idx_oid)?;
            if meta.column_idx == col_idx {
                return Some(*idx_oid);
            }
        }

        None
    }

    pub fn create_table(&mut self, name: &str, schema: TableType) -> Result<u32, String> {
        if self.table_cache.contains_key(name) {
            return Err("Exists".into());
        }
        let oid = self.next_oid.fetch_add(1, Ordering::SeqCst);

        let root_page_id = {
            let mut bp_guard = self.bp.lock().map_err(|_| "Lock")?;
            let mut pinned_bp = unsafe { Pin::new_unchecked(&mut *bp_guard) };
            let new_pid = self.next_oid.fetch_add(1, Ordering::SeqCst);
            let frame = pinned_bp
                .as_mut()
                .alloc_new_page(PageKind::SlottedData, new_pid)
                .map_err(|e| format!("{:?}", e))?;
            let offset = frame.file_offset();
            let fid = frame.fid();
            pinned_bp.as_mut().unpin_frame(fid).ok();
            pinned_bp
                .as_mut()
                .register_page_in_directory(new_pid, offset, 4000)
                .unwrap();
            new_pid
        };

        self.table_cache.insert(name.to_string(), oid);
        self.root_page_cache.insert(oid, root_page_id);
        self.schema_cache.insert(oid, schema.clone());
        let mut bp_guard = self.bp.lock().map_err(|_| "Lock")?;
        let mut pinned_bp = unsafe { Pin::new_unchecked(&mut *bp_guard) };

        let row = Tuple::new(vec![
            AttributeValue::U32(oid),
            AttributeValue::Varchar(name.to_string()),
            AttributeValue::U32(root_page_id),
        ]);
        self.insert_tuple(
            SYSTEM_TABLES_ID,
            &row,
            &get_system_tables_schema(),
            pinned_bp.as_mut(),
        )?;

        for col in &schema.attributes {
            self.insert_column_metadata(oid, col, &get_system_columns_schema(), pinned_bp.as_mut());
        }
        Ok(oid)
    }

    // ... create_index ...
    pub fn create_index(
        &mut self,
        index_name: &str,
        table_name: &str,
        column_name: &str,
    ) -> Result<u32, String> {
        let table_oid = *self.table_cache.get(table_name).ok_or("Table not found")?;
        let schema = self
            .schema_cache
            .get(&table_oid)
            .ok_or("Schema not found")?;

        let (col_idx, col_attr) = schema
            .attributes
            .iter()
            .enumerate()
            .find(|(_, attr)| attr.name == column_name)
            .ok_or("Column not found")?;

        let index_oid = self.next_oid.fetch_add(1, Ordering::SeqCst);
        let mut bp_guard = self.bp.lock().map_err(|_| "Lock")?;
        let mut pinned_bp = unsafe { Pin::new_unchecked(&mut *bp_guard) };

        // Allocate B+ Tree
        let root_page_id = {
            let new_pid = self.next_oid.fetch_add(1, Ordering::SeqCst);

            let frame = pinned_bp
                .as_mut()
                .alloc_new_page(PageKind::BPlusLeaf, new_pid)
                .map_err(|e| format!("{:?}", e))?;
            {
                let mut view = frame.page_view();
                if let crate::storage::page::base::Page::BPlusLeaf(leaf) = &mut view {
                    let key_size = match col_attr.kind {
                        AttributeKind::U32 | AttributeKind::I32 => 4,
                        AttributeKind::U64 | AttributeKind::I64 => 8,
                        _ => return Err("Index only supports integers".into()),
                    };
                    leaf.init(new_pid, key_size);
                }
            }
            let offset = frame.file_offset();
            let fid = frame.fid();
            pinned_bp.as_mut().unpin_frame(fid).ok();
            pinned_bp
                .as_mut()
                .register_page_in_directory(new_pid, offset, 4000)
                .unwrap();
            new_pid
        };

        self.index_name_cache
            .insert(index_name.to_string(), index_oid);
        self.index_meta_cache.insert(
            index_oid,
            IndexMeta {
                table_oid,
                column_idx: col_idx,
                root_page_id,
            },
        );

        // UPDATE: Add to table_indexes
        self.table_indexes
            .entry(table_oid)
            .or_default()
            .push(index_oid);

        let row = Tuple::new(vec![
            AttributeValue::U32(index_oid),
            AttributeValue::Varchar(index_name.to_string()),
            AttributeValue::U32(table_oid),
            AttributeValue::U8(col_idx as u8),
            AttributeValue::U32(root_page_id),
        ]);
        self.insert_tuple(
            SYSTEM_INDEXES_ID,
            &row,
            &get_system_indexes_schema(),
            pinned_bp.as_mut(),
        )?;

        // Backfill
        let table_root = *self
            .root_page_cache
            .get(&table_oid)
            .ok_or("Table root missing")?;
        let mut rows_to_index = Vec::new();

        {
            let mut heap_iter = HeapIterator::new(pinned_bp.as_mut(), table_root);

            while let Some(Ok((rid, bytes))) = heap_iter.next() {
                if let Ok(tuple) = Tuple::from_bytes(&bytes, schema) {
                    let key_val = &tuple.values[col_idx];
                    let key_bytes = match key_val {
                        AttributeValue::U32(v) => v.to_be_bytes().to_vec(),
                        AttributeValue::I32(v) => v.to_be_bytes().to_vec(),
                        AttributeValue::U64(v) => v.to_be_bytes().to_vec(),
                        AttributeValue::I64(v) => v.to_be_bytes().to_vec(),
                        _ => vec![],
                    };
                    if !key_bytes.is_empty() {
                        rows_to_index.push((key_bytes, rid.to_u64()));
                    }
                }
            }
        }

        {
            let mut tree = BPlusTree::new(pinned_bp.as_mut(), root_page_id);

            for (key, val) in rows_to_index {
                tree.insert(&key, val, &self.next_oid)
                    .map_err(|e| format!("{:?}", e))?;
            }
        }

        Ok(index_oid)
    }

    pub fn list_user_tables(&self) -> Vec<(u32, String)> {
        let mut tables: Vec<(u32, String)> = self
            .table_cache
            .iter()
            .filter(|(_, oid)| **oid >= 100)
            .map(|(name, oid)| (*oid, name.clone()))
            .collect();

        tables.sort_by(|a, b| a.1.cmp(&b.1));
        tables
    }

    pub fn drop_table(&mut self, table_name: &str) -> Result<(), String> {
        // 1. Get table OID
        let table_oid = self
            .table_cache
            .get(table_name)
            .copied()
            .ok_or(format!("Table '{}' not found", table_name))?;

        // Prevent dropping system tables
        if table_oid < 100 {
            return Err("Cannot drop system tables".to_string());
        }

        let mut bp_guard = self.bp.lock().map_err(|_| "Lock poisoned")?;
        let mut pinned_bp = unsafe { Pin::new_unchecked(&mut *bp_guard) };

        // 2. Drop all indexes associated with this table
        if let Some(index_oids) = self.table_indexes.get(&table_oid).cloned() {
            for index_oid in index_oids {
                // Get index name for deletion
                let index_name = self
                    .index_name_cache
                    .iter()
                    .find(|(_, oid)| **oid == index_oid) // Fixed: dereference oid twice
                    .map(|(name, _)| name.clone());

                if let Some(idx_name) = index_name {
                    // Delete from system_indexes table
                    let mut iter = HeapIterator::new(pinned_bp.as_mut(), SYSTEM_INDEXES_PAGE_ID);
                    while let Some(Ok((rid, bytes))) = iter.next() {
                        if let Ok(tuple) = Tuple::from_bytes(&bytes, &get_system_indexes_schema()) {
                            if let AttributeValue::U32(oid) = tuple.values[0] {
                                if oid == index_oid {
                                    HeapFile::new(0, 0)
                                        .delete(pinned_bp.as_mut(), rid)
                                        .map_err(|e| {
                                            format!("Failed to delete index metadata: {:?}", e)
                                        })?;
                                    break;
                                }
                            }
                        }
                    }

                    // Remove from caches
                    self.index_name_cache.remove(&idx_name);
                    self.index_meta_cache.remove(&index_oid);
                }
            }
        }

        // 3. Delete column metadata from system_columns
        let mut rids_to_delete = Vec::new();
        {
            let mut iter = HeapIterator::new(pinned_bp.as_mut(), SYSTEM_COLUMNS_PAGE_ID);
            while let Some(Ok((rid, bytes))) = iter.next() {
                if let Ok(tuple) = Tuple::from_bytes(&bytes, &get_system_columns_schema()) {
                    if let AttributeValue::U32(tid) = tuple.values[0] {
                        if tid == table_oid {
                            rids_to_delete.push(rid);
                        }
                    }
                }
            }
        }

        for rid in rids_to_delete {
            HeapFile::new(0, 0)
                .delete(pinned_bp.as_mut(), rid)
                .map_err(|e| format!("Failed to delete column metadata: {:?}", e))?;
        }

        // 4. Delete table metadata from system_tables
        {
            let mut iter = HeapIterator::new(pinned_bp.as_mut(), SYSTEM_TABLES_PAGE_ID);
            while let Some(Ok((rid, bytes))) = iter.next() {
                if let Ok(tuple) = Tuple::from_bytes(&bytes, &get_system_tables_schema()) {
                    if let AttributeValue::U32(oid) = tuple.values[0] {
                        if oid == table_oid {
                            HeapFile::new(0, 0)
                                .delete(pinned_bp.as_mut(), rid)
                                .map_err(|e| format!("Failed to delete table metadata: {:?}", e))?;
                            break;
                        }
                    }
                }
            }
        }

        // 5. Remove from runtime caches
        self.table_cache.remove(table_name);
        self.schema_cache.remove(&table_oid);
        self.root_page_cache.remove(&table_oid);
        self.table_indexes.remove(&table_oid);

        Ok(())
    }

    pub fn insert_tuple(
        &self,
        table_oid: u32,
        tuple: &Tuple,
        schema: &TableType,
        mut bpm: Pin<&mut BufferPool>,
    ) -> Result<(), String> {
        let start_page = if table_oid == SYSTEM_TABLES_ID {
            SYSTEM_TABLES_PAGE_ID
        } else if table_oid == SYSTEM_COLUMNS_ID {
            SYSTEM_COLUMNS_PAGE_ID
        } else if table_oid == SYSTEM_INDEXES_ID {
            SYSTEM_INDEXES_PAGE_ID
        } else {
            *self
                .root_page_cache
                .get(&table_oid)
                .ok_or("Unknown table")?
        };

        let mut heap = HeapFile::new(start_page, start_page);
        let bytes = tuple.to_bytes(schema)?;

        // 1. Insert into Heap
        let rid = heap
            .insert(bpm.as_mut(), &self.next_oid, &bytes)
            .map_err(|e| format!("{:?}", e))?;

        // 2. Update Indexes
        if let Some(indexes) = self.table_indexes.get(&table_oid) {
            for &index_oid in indexes {
                if let Some(meta) = self.index_meta_cache.get(&index_oid) {
                    if meta.column_idx < tuple.values.len() {
                        let key_val = &tuple.values[meta.column_idx];
                        let key_bytes = match key_val {
                            AttributeValue::U32(v) => v.to_be_bytes().to_vec(),
                            AttributeValue::I32(v) => v.to_be_bytes().to_vec(),
                            AttributeValue::U64(v) => v.to_be_bytes().to_vec(),
                            AttributeValue::I64(v) => v.to_be_bytes().to_vec(),
                            _ => continue,
                        };

                        if !key_bytes.is_empty() {
                            let mut tree = BPlusTree::new(bpm.as_mut(), meta.root_page_id);
                            tree.insert(&key_bytes, rid.to_u64(), &self.next_oid)
                                .map_err(|e| format!("Index insert failed: {:?}", e))?;
                        }
                    }
                }
            }
        }

        Ok(())
    }

    pub fn delete_tuple(
        &self,
        table_oid: u32,
        rid: crate::storage::heap::row::RowId,
        mut bpm: Pin<&mut BufferPool>,
    ) -> Result<(), String> {
        // 1. Fetch tuple to get keys for index deletion
        let tuple_bytes = HeapFile::get(bpm.as_mut(), rid)
            .map_err(|e| format!("Failed to fetch tuple for delete: {:?}", e))?;

        let schema = self.get_table_schema(table_oid).ok_or("Schema not found")?;
        let tuple = Tuple::from_bytes(&tuple_bytes, &schema)?;

        // 2. Delete from Heap
        let mut heap = HeapFile::new(0, 0);
        heap.delete(bpm.as_mut(), rid)
            .map_err(|e| format!("Heap delete failed: {:?}", e))?;

        // 3. Delete from Indexes
        if let Some(indexes) = self.table_indexes.get(&table_oid) {
            for &index_oid in indexes {
                if let Some(meta) = self.index_meta_cache.get(&index_oid) {
                    if meta.column_idx < tuple.values.len() {
                        let key_val = &tuple.values[meta.column_idx];
                        let key_bytes = match key_val {
                            AttributeValue::U32(v) => v.to_be_bytes().to_vec(),
                            AttributeValue::I32(v) => v.to_be_bytes().to_vec(),
                            AttributeValue::U64(v) => v.to_be_bytes().to_vec(),
                            AttributeValue::I64(v) => v.to_be_bytes().to_vec(),
                            _ => continue,
                        };

                        if !key_bytes.is_empty() {
                            let mut tree = BPlusTree::new(bpm.as_mut(), meta.root_page_id);
                            // Ignore error if key not found (idempotent)
                            let _ = tree.delete(&key_bytes);
                        }
                    }
                }
            }
        }

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
        let db_file = "test_db/test_catalog_persist.db";
        // Ensure directory exists
        let _ = fs::create_dir_all("test_db");
        let _ = fs::remove_file(db_file);

        let fm = FileManager::new(db_file.to_string()).unwrap();
        let bp = Arc::new(Mutex::new(BufferPool::new(
            fm,
            Box::new(FifoEvictor::new()),
            Box::new(DirectoryPageLocator::new()),
        )));

        let mut catalog = Catalog::new(bp.clone());

        let user_schema = TableType {
            attributes: vec![TableAttribute {
                name: "username".into(),
                kind: AttributeKind::Varchar,
                nullable: false,
                is_internal: false,
            }],
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
            let pinned_bp = unsafe { std::pin::Pin::new_unchecked(&mut *bp_guard) };
            pinned_bp.flush_all().unwrap();
        }

        {
            let fm = FileManager::new(db_file.to_string()).unwrap();
            let bp = Arc::new(Mutex::new(BufferPool::new(
                fm,
                Box::new(FifoEvictor::new()),
                Box::new(DirectoryPageLocator::new()),
            )));
            let catalog = Catalog::new(bp.clone()); // FIX: was 'let'

            let oid = catalog.get_table_oid("saved_table");
            assert!(
                oid.is_some(),
                "Catalog failed to load 'saved_table' from disk"
            );
        }

        let _ = fs::remove_file(db_file);
        // FIX: Ignore error if directory not empty (used by other tests)
        let _ = fs::remove_dir("test_db");
    }
    #[test]
    fn test_create_index_metadata() {
        let db_file = "test_db/test_index_meta.db";
        let _ = fs::create_dir_all("test_db");
        let _ = fs::remove_file(db_file);

        let fm = FileManager::new(db_file.to_string()).unwrap();
        let bp = Arc::new(Mutex::new(BufferPool::new(
            fm,
            Box::new(FifoEvictor::new()),
            Box::new(DirectoryPageLocator::new()),
        )));
        let mut catalog = Catalog::new(bp.clone());

        // 1. Create Table
        let schema = TableType {
            attributes: vec![
                TableAttribute {
                    name: "id".into(),
                    kind: AttributeKind::U32,
                    nullable: false,
                    is_internal: false,
                },
                TableAttribute {
                    name: "score".into(),
                    kind: AttributeKind::U32,
                    nullable: false,
                    is_internal: false,
                },
            ],
            layout: TableLayout {
                size: 0,
                attr_layouts: vec![],
            },
        };
        let _ = catalog.create_table("scores", schema).unwrap();

        // 2. Create Index on "id"
        let idx_oid = catalog
            .create_index("idx_id", "scores", "id")
            .expect("Create index failed");

        // 3. Verify Metadata
        assert!(idx_oid > 100);
        assert_eq!(catalog.get_index_oid("idx_id"), Some(idx_oid));

        // 4. Verify Metadata Persistence (Restart)
        // Simulate crash/restart by dropping catalog and creating new one
        drop(catalog);

        let catalog_2 = Catalog::new(bp.clone());
        let recovered_oid = catalog_2.get_index_oid("idx_id");
        assert_eq!(
            recovered_oid,
            Some(idx_oid),
            "Index metadata lost after restart"
        );

        let _ = fs::remove_file(db_file);
        let _ = fs::remove_dir("test_db");
    }
}
