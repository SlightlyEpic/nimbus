use super::executor::Executor;
use crate::catalog::manager::Catalog;
use crate::rt_type::primitives::{AttributeValue, TableType};
use crate::storage::buffer::BufferPool;
use crate::storage::heap::tuple::Tuple;
use std::pin::Pin;

/// Executes an Update operation (Delete + Insert).
/// `F` is a closure that takes the Old Tuple and returns the New Tuple.
pub struct UpdateExecutor<'a, F>
where
    F: Fn(&Tuple) -> Tuple,
{
    child: Box<dyn Executor + 'a>,
    catalog: &'a Catalog,
    table_oid: u32,
    schema: TableType,
    update_fn: F,
    executed: bool,
}

impl<'a, F> UpdateExecutor<'a, F>
where
    F: Fn(&Tuple) -> Tuple,
{
    pub fn new(
        child: Box<dyn Executor + 'a>,
        catalog: &'a Catalog,
        table_oid: u32,
        update_fn: F,
    ) -> Result<Self, String> {
        let schema = catalog
            .get_table_schema(table_oid)
            .ok_or_else(|| format!("Table OID {} not found", table_oid))?;

        Ok(Self {
            child,
            catalog,
            table_oid,
            schema,
            update_fn,
            executed: false,
        })
    }
}

impl<'a, F> Executor for UpdateExecutor<'a, F>
where
    F: Fn(&Tuple) -> Tuple,
{
    fn init(&mut self) {
        self.child.init();
        self.executed = false;
    }

    fn next(&mut self, mut bpm: Pin<&mut BufferPool>) -> Option<Tuple> {
        if self.executed {
            return None;
        }

        let mut count = 0;
        while let Some(old_tuple) = self.child.next(bpm.as_mut()) {
            // Pass bpm
            if let Some(rid) = old_tuple.rid {
                // 1. Calculate New Tuple
                let new_tuple = (self.update_fn)(&old_tuple);

                // 2. Delete Old Tuple
                if self
                    .catalog
                    .delete_tuple(self.table_oid, rid, bpm.as_mut()) // Pass bpm
                    .is_ok()
                {
                    // 3. Insert New Tuple (Updates Heap + All Indexes)
                    if self
                        .catalog
                        .insert_tuple(self.table_oid, &new_tuple, &self.schema, bpm.as_mut()) // Pass bpm
                        .is_ok()
                    {
                        count += 1;
                    }
                }
            }
        }

        self.executed = true;
        Some(Tuple::new(vec![AttributeValue::U32(count)]))
    }
}
