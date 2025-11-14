use crate::storage::buffer::BufferPool;
use crate::storage::page::base::{PageId, PageKind};
use std::pin::Pin;
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

    /// Applies the deferred writes to the BufferPool and ensures disk persistence (Atomicity).
    /// Returns the highest PageId/OID allocated by this transaction for system updates.
    pub fn commit(mut self, mut bpm: Pin<&mut BufferPool>) -> Result<u32, String> {
        let mut max_oid = 0;

        // --- PHASE 1: APPLY WRITES TO MEMORY (Bypassing standard cycle) ---
        for op in self.write_set.into_iter() {
            let (page_id, new_data) = match op {
                WriteOperation::PageUpdate { page_id, new_data } => (page_id, new_data),
                WriteOperation::PageAllocation {
                    page_id, new_data, ..
                } => (page_id, new_data),
                WriteOperation::PageDeallocation { page_id } => {
                    // Log the max OID but skip application for now
                    if page_id > max_oid {
                        max_oid = page_id;
                    }
                    continue;
                }
            };

            // 1. Fetch the page (will load if not in cache, pinning it)
            let frame = bpm
                .as_mut()
                .fetch_page(page_id)
                .map_err(|e| format!("Commit failed (fetch page): {:?}", e))?;
            let fid = frame.fid();

            // 2. Overwrite the raw buffer directly (using unsafe access to the frame pointer)
            unsafe {
                // Assumes the Frame pointer is safe to use directly
                std::ptr::copy_nonoverlapping(new_data.as_ptr(), frame.buf_ptr, new_data.len());
            }

            // 3. Mark dirty and unpin (we assume the caller expects the frame to be unpinned after commit)
            bpm.as_mut().mark_frame_dirty(fid);
            bpm.as_mut().unpin_frame(fid).ok();

            if page_id > max_oid {
                max_oid = page_id;
            }
        }

        // --- PHASE 2: PERSISTENCE (Atomicity achieved by flush_all) ---
        bpm.as_mut()
            .flush_all()
            .map_err(|e| format!("Commit failed (flush): {:?}", e))?;

        // The maximum OID allocated by this TX is returned for the Catalog to update the global counter.
        Ok(max_oid)
    }
}
