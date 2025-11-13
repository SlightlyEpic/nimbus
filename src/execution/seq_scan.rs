use super::executor::Executor;
use crate::catalog::manager::Catalog;
use crate::rt_type::primitives::TableType;
use crate::storage::buffer::BufferPool;
use crate::storage::heap::heap_file::HeapFile;
use crate::storage::heap::iterator::HeapIterator;
use crate::storage::heap::tuple::Tuple;
use std::pin::Pin;

pub struct SeqScanExecutor<'a> {
    // Dependencies
    bpm: Pin<&'a mut BufferPool>,
    catalog: &'a Catalog,

    // Configuration
    table_oid: u32,
    schema: TableType, // We need schema to deserialize tuples

    // Runtime State
    iterator: Option<HeapIterator<'a>>,
}

impl<'a> SeqScanExecutor<'a> {
    pub fn new(
        bpm: Pin<&'a mut BufferPool>,
        catalog: &'a Catalog,
        table_oid: u32,
    ) -> Result<Self, String> {
        // Lookup schema immediately to fail fast if table doesn't exist
        let schema = catalog
            .get_table_schema(table_oid)
            .ok_or_else(|| format!("Table OID {} not found", table_oid))?;

        Ok(Self {
            bpm,
            catalog,
            table_oid,
            schema,
            iterator: None,
        })
    }
}

impl<'a> Executor for SeqScanExecutor<'a> {
    fn init(&mut self) {
        // 1. Get the Root Page ID from Catalog
        // If table has no root page (empty/new), we might handle it gracefully,
        // but our current logic guarantees a root page on creation.
        let root_page = self.catalog.get_table_root_page(self.table_oid);

        if let Some(root) = root_page {
            // 2. Initialize the HeapIterator

            let iter = HeapIterator::new(self.bpm.as_mut(), root);
            self.iterator = Some(iter);
        } else {
            self.iterator = None; // Table empty or issue
        }
    }

    fn next(&mut self) -> Option<Tuple> {
        let iter = self.iterator.as_mut()?;

        // Loop until we find a valid tuple or exhaust iterator
        loop {
            match iter.next() {
                Some(Ok(tuple_bytes)) => {
                    // Deserialize
                    if let Ok(tuple) = Tuple::from_bytes(&tuple_bytes, &self.schema) {
                        return Some(tuple);
                    }
                    // If deserialization fails, we skip/log (or panic in debug)
                    continue;
                }
                Some(Err(_)) => return None, // Error reading page (e.g. IO), stop scan
                None => return None,         // End of scan
            }
        }
    }
}
