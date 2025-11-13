use super::executor::Executor;
use crate::catalog::manager::Catalog;
use crate::rt_type::primitives::{AttributeValue, TableType};
use crate::storage::buffer::BufferPool;
use crate::storage::heap::tuple::Tuple;
use std::pin::Pin;

pub struct InsertExecutor<'a> {
    child: Box<dyn Executor + 'a>,
    catalog: &'a Catalog,
    table_oid: u32,
    schema: TableType,
    executed: bool,
}

impl<'a> InsertExecutor<'a> {
    pub fn new(
        child: Box<dyn Executor + 'a>,
        catalog: &'a Catalog,
        table_oid: u32,
    ) -> Result<Self, String> {
        let schema = catalog
            .get_table_schema(table_oid)
            .ok_or_else(|| format!("Table OID {} not found", table_oid))?;

        Ok(Self {
            child,
            catalog,
            table_oid,
            schema,
            executed: false,
        })
    }
}

impl<'a> Executor for InsertExecutor<'a> {
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
            // Pass bpm
            // Panic on error for now (Prototype phase)
            if let Err(e) =
                self.catalog
                    .insert_tuple(self.table_oid, &tuple, &self.schema, bpm.as_mut())
            // Pass bpm
            {
                panic!("Insert failed: {}", e);
            }
            count += 1;
        }

        self.executed = true;
        // Return the number of inserted rows as a single tuple
        Some(Tuple::new(vec![AttributeValue::U32(count)]))
    }
}

#[cfg(test)]
mod tests {
    use crate::catalog::manager::Catalog;
    use crate::execution::executor::Executor;
    use crate::execution::insert::InsertExecutor;
    use crate::execution::seq_scan::SeqScanExecutor;
    use crate::execution::values::ValuesExecutor;
    use crate::rt_type::primitives::{
        AttributeKind, AttributeValue, TableAttribute, TableLayout, TableType,
    };
    use crate::storage::buffer::BufferPool;
    use crate::storage::buffer::fifo_evictor::FifoEvictor;
    use crate::storage::disk::FileManager;
    use crate::storage::heap::tuple::Tuple;
    use crate::storage::page_locator::locator::DirectoryPageLocator;
    use std::fs;
    use std::pin::Pin;
    use std::sync::{Arc, Mutex};

    #[test]
    fn test_insert_execution() {
        let db_file = "test_insert_exec.db";
        let _ = fs::remove_file(db_file);

        let fm = FileManager::new(db_file.to_string()).unwrap();
        let bp = Arc::new(Mutex::new(BufferPool::new(
            fm,
            Box::new(FifoEvictor::new()),
            Box::new(DirectoryPageLocator::new()),
        )));
        let mut catalog = Catalog::new(bp.clone());

        // 1. Create Table "items"
        let schema = TableType {
            attributes: vec![TableAttribute {
                name: "val".into(),
                kind: AttributeKind::U32,
                nullable: false,
                is_internal: false,
            }],
            layout: TableLayout {
                size: 0,
                attr_layouts: vec![],
            },
        };
        let table_oid = catalog.create_table("items", schema).unwrap();

        // 2. Create Source Data (VALUES (10), (20), (30))
        let tuples = vec![
            Tuple::new(vec![AttributeValue::U32(10)]),
            Tuple::new(vec![AttributeValue::U32(20)]),
            Tuple::new(vec![AttributeValue::U32(30)]),
        ];
        let values_exec = Box::new(ValuesExecutor::new(tuples));

        // 3. Execute Insert
        let mut bp_guard = bp.lock().unwrap();
        let mut pinned_bp = unsafe { Pin::new_unchecked(&mut *bp_guard) };
        let mut insert_exec = InsertExecutor::new(values_exec, &catalog, table_oid).unwrap();
        insert_exec.init();

        let result = insert_exec
            .next(pinned_bp.as_mut())
            .expect("Should return count"); // Pass bpm

        // Verify count = 3
        if let AttributeValue::U32(count) = result.values[0] {
            assert_eq!(count, 3, "Should have inserted 3 rows");
        } else {
            panic!("Wrong result type from insert");
        }

        // 4. Verify Data via SeqScan

        let mut scan_exec = SeqScanExecutor::new(&catalog, table_oid).unwrap(); // Removed bpm from args
        scan_exec.init();

        let mut fetched_count = 0;
        while let Some(tuple) = scan_exec.next(pinned_bp.as_mut()) {
            // Pass bpm
            println!("Scanned: {:?}", tuple);
            fetched_count += 1;
        }
        assert_eq!(fetched_count, 3, "SeqScan should find 3 rows");

        fs::remove_file(db_file).unwrap();
    }
}
