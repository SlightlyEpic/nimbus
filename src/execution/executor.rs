use crate::storage::heap::tuple::Tuple;

/// The standard interface for all query execution operators.
pub trait Executor {
    /// Initializes the executor (e.g., sets up iterators).
    fn init(&mut self);

    /// Returns the next tuple from the operator, or None if exhausted.
    fn next(&mut self) -> Option<Tuple>;
}

#[cfg(test)]
mod tests {
    use crate::catalog::manager::Catalog;
    use crate::catalog::schema::SYSTEM_TABLES_ID;
    use crate::execution::executor::Executor;
    use crate::execution::seq_scan::SeqScanExecutor;
    use crate::storage::buffer::BufferPool;
    use crate::storage::buffer::fifo_evictor::FifoEvictor;
    use crate::storage::disk::FileManager;
    use crate::storage::page_locator::locator::DirectoryPageLocator;
    use std::fs;
    use std::pin::Pin;
    use std::sync::{Arc, Mutex};

    #[test]
    fn test_seq_scan_system_tables() {
        let db_file = "test_seq_scan.db";
        let _ = fs::remove_file(db_file);

        let fm = FileManager::new(db_file.to_string()).unwrap();
        let bp = Arc::new(Mutex::new(BufferPool::new(
            fm,
            Box::new(FifoEvictor::new()),
            Box::new(DirectoryPageLocator::new()),
        )));

        // 1. Initialize Catalog (Creates system tables)
        let catalog = Catalog::new(bp.clone());

        // 2. Setup Scan on SYSTEM_TABLES (ID 1)
        let mut bp_guard = bp.lock().unwrap();
        let mut pinned_bp = unsafe { Pin::new_unchecked(&mut *bp_guard) };

        let mut scan = SeqScanExecutor::new(pinned_bp.as_mut(), &catalog, SYSTEM_TABLES_ID)
            .expect("Failed to create scan");

        scan.init();

        // 3. Verify results
        // We expect at least 2 rows: 'system_tables' and 'system_columns'
        let mut count = 0;
        while let Some(tuple) = scan.next() {
            println!("Found table: {:?}", tuple);
            count += 1;
        }

        assert!(
            count >= 2,
            "Should have found system_tables and system_columns"
        );

        fs::remove_file(db_file).unwrap();
    }
}
