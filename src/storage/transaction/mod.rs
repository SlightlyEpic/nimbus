use crate::storage::page::base::{PageId, PageKind};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

/// Represents a single change operation intended to be applied to the database.
#[derive(Debug)]
pub enum WriteOperation {
    /// Mark a frame as dirty and copy its new data.
    PageUpdate { page_id: PageId, new_data: Vec<u8> },
    /// Register a new page with the file manager and load data.
    PageAllocation {
        page_id: PageId,
        page_kind: PageKind,
        new_data: Vec<u8>,
    },
    /// Mark a page as free/available for reuse (future work)
    PageDeallocation { page_id: PageId },
}

/// The central structure for maintaining transactional integrity.
pub struct Transaction {
    // Write Set: Collects all modifications during execution
    pub write_set: Vec<WriteOperation>,
    // Reference to the global counter for obtaining new OIDs during the TX
    pub next_oid_counter: Arc<AtomicU32>,
}

impl Transaction {
    pub fn new(next_oid_counter: Arc<AtomicU32>) -> Self {
        Self {
            write_set: Vec::new(),
            next_oid_counter,
        }
    }

    /// Allocates a new OID (PageId, IndexId, or TableId) guaranteed to be unique.
    pub fn allocate_oid(&self) -> u32 {
        self.next_oid_counter.fetch_add(1, Ordering::SeqCst) + 1
    }

    // NOTE: The `commit` and `abort` methods will be implemented in a later step
}
