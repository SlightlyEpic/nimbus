use super::executor::Executor;
use crate::catalog::manager::Catalog;
use crate::rt_type::primitives::TableType;
use crate::storage::buffer::BufferPool;
use crate::storage::heap::iterator::HeapIterator;
use crate::storage::heap::tuple::Tuple;
use std::pin::Pin;

pub struct SeqScanExecutor<'a> {
    bpm: Option<Pin<&'a mut BufferPool>>,
    catalog: &'a Catalog,
    table_oid: u32,
    schema: TableType,
    iterator: Option<HeapIterator<'a>>,
}

impl<'a> SeqScanExecutor<'a> {
    pub fn new(
        bpm: Pin<&'a mut BufferPool>,
        catalog: &'a Catalog,
        table_oid: u32,
    ) -> Result<Self, String> {
        let schema = catalog
            .get_table_schema(table_oid)
            .ok_or_else(|| format!("Table OID {} not found", table_oid))?;

        Ok(Self {
            bpm: Some(bpm),
            catalog,
            table_oid,
            schema,
            iterator: None,
        })
    }
}

impl<'a> Executor for SeqScanExecutor<'a> {
    fn init(&mut self) {
        let root_page = self.catalog.get_table_root_page(self.table_oid);

        if let Some(root) = root_page {
            // We take the BPM out of the executor and give it to the iterator
            if let Some(bpm) = self.bpm.take() {
                let iter = HeapIterator::new(bpm, root);
                self.iterator = Some(iter);
            }
        } else {
            self.iterator = None;
        }
    }

    fn next(&mut self) -> Option<Tuple> {
        let iter = self.iterator.as_mut()?;

        loop {
            match iter.next() {
                Some(Ok(tuple_bytes)) => {
                    if let Ok(tuple) = Tuple::from_bytes(&tuple_bytes, &self.schema) {
                        return Some(tuple);
                    }
                    continue;
                }
                Some(Err(_)) => return None,
                None => return None,
            }
        }
    }
}
