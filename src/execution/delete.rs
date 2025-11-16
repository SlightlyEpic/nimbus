use super::executor::Executor;
use crate::catalog::manager::Catalog;
use crate::rt_type::primitives::AttributeValue;
use crate::storage::buffer::BufferPool;
use crate::storage::heap::tuple::Tuple;
use std::pin::Pin;

pub struct DeleteExecutor<'a> {
    child: Box<dyn Executor + 'a>,
    catalog: &'a Catalog,
    table_oid: u32,
    executed: bool,
}

impl<'a> DeleteExecutor<'a> {
    pub fn new(child: Box<dyn Executor + 'a>, catalog: &'a Catalog, table_oid: u32) -> Self {
        Self {
            child,
            catalog,
            table_oid,
            executed: false,
        }
    }
}

impl<'a> Executor for DeleteExecutor<'a> {
    fn init(&mut self) {
        self.child.init();
        self.executed = false;
    }

    fn next(&mut self, mut bpm: Pin<&mut BufferPool>) -> Option<Tuple> {
        // Added bpm
        if self.executed {
            return None;
        }

        let mut count = 0;

        while let Some(tuple) = self.child.next(bpm.as_mut()) {
            // Pass bpm to child
            if let Some(rid) = tuple.rid {
                if self
                    .catalog
                    .delete_tuple(self.table_oid, rid, bpm.as_mut()) // Pass bpm
                    .is_ok()
                {
                    count += 1;
                }
            }
        }

        self.executed = true;
        Some(Tuple::new(vec![AttributeValue::U32(count)]))
    }
}
