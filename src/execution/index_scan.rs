use super::executor::Executor;
use crate::catalog::manager::Catalog;
use crate::rt_type::primitives::TableType;
use crate::storage::bplus_tree::BPlusTree;
use crate::storage::buffer::BufferPool;
use crate::storage::heap::heap_file::HeapFile;
use crate::storage::heap::row::RowId;
use crate::storage::heap::tuple::Tuple;
use std::pin::Pin;

pub struct IndexScanExecutor<'a> {
    catalog: &'a Catalog,
    index_oid: u32,
    key: Vec<u8>,
    schema: TableType,
    done: bool,
}

impl<'a> IndexScanExecutor<'a> {
    pub fn new(catalog: &'a Catalog, index_oid: u32, key: Vec<u8>) -> Result<Self, String> {
        let idx_meta = catalog.get_index_meta(index_oid).ok_or("Index not found")?;
        let schema = catalog
            .get_table_schema(idx_meta.table_oid)
            .ok_or("Table schema missing")?;

        Ok(Self {
            catalog,
            index_oid,
            key,
            schema,
            done: false,
        })
    }
}

impl<'a> Executor for IndexScanExecutor<'a> {
    fn init(&mut self) {
        self.done = false;
    }

    fn next(&mut self, mut bpm: Pin<&mut BufferPool>) -> Option<Tuple> {
        // Added bpm
        if self.done {
            return None;
        }
        self.done = true;

        let meta = self.catalog.get_index_meta(self.index_oid)?;
        // Use passed-in bpm

        // 1. Look up RowId in B+ Tree
        let rid_val = {
            let mut tree = BPlusTree::new(bpm.as_mut(), meta.root_page_id);
            match tree.get_value(&self.key) {
                Ok(Some(v)) => v,
                _ => return None, // Key not found or error
            }
        };

        let rid = RowId::from_u64(rid_val);

        // 2. Fetch Tuple from Heap

        match HeapFile::get(bpm.as_mut(), rid) {
            Ok(bytes) => {
                if let Ok(mut tuple) = Tuple::from_bytes(&bytes, &self.schema) {
                    tuple.rid = Some(rid);
                    return Some(tuple);
                }
            }
            Err(_) => return None,
        }

        None
    }
}
