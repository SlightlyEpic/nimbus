use super::row::RowId;
use crate::storage::buffer::BufferPool;
use crate::storage::heap::iterator::HeapIterator;
use crate::storage::page::{
    self,
    base::DiskPage,
    base::{PageId, PageKind},
};
use std::pin::Pin;
use std::sync::atomic::{AtomicU32, Ordering};

/// HeapFile manages a linked list of pages storing rows.
pub struct HeapFile {
    pub first_page_id: PageId,
    pub last_page_id: PageId,
}

#[derive(Debug)]
pub enum HeapError {
    FetchPage(String),
    AllocPage(String),
    UnpinPage(String),
    InvalidPage,
    AddSlot(String),
    RegisterPage(String),
    FindSpace(String),
    UpdateSpace(String),
}

impl HeapFile {
    pub fn new(first_page_id: PageId, last_page_id: PageId) -> Self {
        Self {
            first_page_id,
            last_page_id,
        }
    }

    pub fn scan<'a>(&self, bpm: Pin<&'a mut BufferPool>) -> HeapIterator<'a> {
        HeapIterator::new(bpm, self.first_page_id)
    }

    /// Inserts raw byte data into the heap file.
    /// Enforces isolation: Only attempts to insert into this specific heap's chain.
    pub fn insert(
        &mut self,
        mut bpm: Pin<&mut BufferPool>,
        page_id_counter: &AtomicU32,
        data: &[u8],
    ) -> Result<RowId, HeapError> {
        // 1. Try to insert into the last page of this specific heap
        if self.last_page_id != 0 {
            let frame = bpm
                .as_mut()
                .fetch_page(self.last_page_id)
                .map_err(|e| HeapError::FetchPage(format!("{:?}", e)))?;
            let frame_id = frame.fid();

            // Attempt insert
            let insert_result = {
                let mut page_view = frame.page_view();
                if let page::base::Page::SlottedData(slotted_page) = &mut page_view {
                    slotted_page.add_slot(data)
                } else {
                    bpm.as_mut().unpin_frame(frame_id).ok();
                    return Err(HeapError::InvalidPage);
                }
            };

            if let Ok(slot_num) = insert_result {
                // Success!
                bpm.as_mut().mark_frame_dirty(frame_id);
                bpm.as_mut()
                    .unpin_frame(frame_id)
                    .map_err(|e| HeapError::UnpinPage(format!("{:?}", e)))?;

                // We don't strictly need to update the global directory's free space
                // immediately for correctness, but it's good practice.
                // For now, we skip it to keep logic simple and isolated.

                return Ok(RowId::new(self.last_page_id, slot_num as u32));
            } else {
                // Page is full (or error), unpin and continue to allocate new page
                bpm.as_mut()
                    .unpin_frame(frame_id)
                    .map_err(|e| HeapError::UnpinPage(format!("{:?}", e)))?;
            }
        }

        // 2. Allocate a new page (Page is full or heap is empty)
        let new_page_id = page_id_counter.fetch_add(1, Ordering::SeqCst) + 1;

        let frame = bpm
            .as_mut()
            .alloc_new_page(PageKind::SlottedData, new_page_id)
            .map_err(|e| HeapError::AllocPage(format!("{:?}", e)))?;

        let new_frame_id = frame.fid();
        let new_file_offset = frame.file_offset();

        // Init and Insert
        let slot_num = {
            let mut page_view = frame.page_view();
            if let page::base::Page::SlottedData(slotted_page) = &mut page_view {
                slotted_page.header_mut().set_page_id(new_page_id);

                // Link backwards
                if self.last_page_id != 0 {
                    slotted_page
                        .header_mut()
                        .set_prev_page_id(self.last_page_id);
                }

                slotted_page
                    .add_slot(data)
                    .map_err(|e| HeapError::AddSlot(format!("{:?}", e)))?
            } else {
                bpm.as_mut().unpin_frame(new_frame_id).ok();
                return Err(HeapError::InvalidPage);
            }
        };

        bpm.as_mut().mark_frame_dirty(new_frame_id);
        bpm.as_mut()
            .unpin_frame(new_frame_id)
            .map_err(|e| HeapError::UnpinPage(format!("{:?}", e)))?;

        // 3. Link forward (Update old last page)
        if self.last_page_id != 0 {
            let prev_frame = bpm
                .as_mut()
                .fetch_page(self.last_page_id)
                .map_err(|e| HeapError::FetchPage(format!("{:?}", e)))?;
            let prev_fid = prev_frame.fid();

            {
                let mut prev_view = prev_frame.page_view();
                prev_view.header_mut().set_next_page_id(new_page_id);
            }

            bpm.as_mut().mark_frame_dirty(prev_fid);
            bpm.as_mut()
                .unpin_frame(prev_fid)
                .map_err(|e| HeapError::UnpinPage(format!("{:?}", e)))?;
        }

        // 4. Update HeapFile state
        if self.first_page_id == 0 {
            self.first_page_id = new_page_id;
        }
        self.last_page_id = new_page_id;

        // 5. Register new page in Directory (so we can find it later via PageID)
        // We calculate free space roughly
        let free_space = 4000; // Safe approximation for new page

        bpm.as_mut()
            .expand_directory_and_register(
                new_page_id,
                new_file_offset,
                free_space,
                page_id_counter,
            )
            .map_err(|e| HeapError::RegisterPage(e))?;

        Ok(RowId::new(new_page_id, slot_num as u32))
    }

    pub fn get(mut bpm: Pin<&mut BufferPool>, rid: RowId) -> Result<Vec<u8>, HeapError> {
        let page_id = rid.page_id();
        let slot_num = rid.slot_num() as usize;

        let offset = {
            let (core, locator) = bpm.as_mut().get_core_and_locator();
            locator
                .find_file_offset(page_id, core)
                .map_err(|e| HeapError::FetchPage(format!("PageLocator error: {:?}", e)))?
        };

        let frame = bpm
            .as_mut()
            .fetch_page_at_offset(offset)
            .map_err(|e| HeapError::FetchPage(format!("{:?}", e)))?;

        let frame_id = frame.fid();
        let mut page_view = frame.page_view();

        use crate::storage::page::base::DiskPage;
        let data = if let page::base::Page::SlottedData(slotted_page) = &mut page_view {
            if slotted_page.header().page_id() != page_id {
                bpm.as_mut().unpin_frame(frame_id).ok();
                return Err(HeapError::InvalidPage);
            }
            slotted_page
                .slot_data(slot_num)
                .map(|bytes| bytes.to_vec())
                .ok_or(HeapError::InvalidPage)
        } else {
            Err(HeapError::InvalidPage)
        };

        bpm.as_mut()
            .unpin_frame(frame_id)
            .map_err(|e| HeapError::UnpinPage(format!("{:?}", e)))?;

        data
    }

    pub fn delete(&mut self, mut bpm: Pin<&mut BufferPool>, rid: RowId) -> Result<(), HeapError> {
        let page_id = rid.page_id();
        let slot_num = rid.slot_num();

        let frame = bpm
            .as_mut()
            .fetch_page(page_id)
            .map_err(|e| HeapError::FetchPage(format!("{:?}", e)))?;
        let frame_id = frame.fid();

        let res = {
            let mut page_view = frame.page_view();
            if let page::base::Page::SlottedData(slotted) = &mut page_view {
                slotted
                    .mark_dead(slot_num as usize)
                    .map_err(|_| HeapError::InvalidPage)
            } else {
                Err(HeapError::InvalidPage)
            }
        };

        if res.is_ok() {
            bpm.as_mut().mark_frame_dirty(frame_id);
        }
        bpm.as_mut()
            .unpin_frame(frame_id)
            .map_err(|e| HeapError::UnpinPage(format!("{:?}", e)))?;

        res
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::buffer::fifo_evictor::FifoEvictor;
    use crate::storage::disk::FileManager;
    use crate::storage::page_locator::locator::DirectoryPageLocator;
    use std::fs;
    use std::path::PathBuf;

    fn setup_heap_test(test_name: &str) -> (PathBuf, Pin<Box<BufferPool>>, AtomicU32) {
        let file_name = format!("test_heap_reuse_{}.db", test_name);
        let _ = fs::remove_file(&file_name);

        let file_manager = FileManager::new(file_name.clone()).unwrap();
        let evictor = Box::new(FifoEvictor::new());
        let locator = Box::new(DirectoryPageLocator::new());
        let mut bp = Box::pin(BufferPool::new(file_manager, evictor, locator));

        let dir_page_id = 1;
        let frame = bp
            .as_mut()
            .alloc_new_page(PageKind::Directory, dir_page_id)
            .expect("Failed to allocate root directory page");

        let fid = frame.fid();
        bp.as_mut().unpin_frame(fid).unwrap();

        (PathBuf::from(file_name), bp, AtomicU32::new(1))
    }

    fn defer_delete(path: &PathBuf) {
        let _ = fs::remove_file(path);
    }

    #[test]
    fn test_heap_page_reuse() {
        let (path, mut bp, counter) = setup_heap_test("reuse");
        let mut heap = HeapFile::new(0, 0); // Start empty

        let data_small = vec![1u8; 100];

        // 1. Insert -> Allocates New Page (ID 2)
        let rid1 = heap
            .insert(bp.as_mut(), &counter, &data_small)
            .expect("Insert 1 failed");

        let page1_id = rid1.page_id();
        // With Directory(1), first alloc gets 2
        assert_eq!(page1_id, 2);

        // Update heap state manually for test since we don't have a Catalog wrapper here
        heap.last_page_id = page1_id;
        heap.first_page_id = page1_id;

        // 2. Insert again -> Should reuse Page 2
        let rid2 = heap
            .insert(bp.as_mut(), &counter, &data_small)
            .expect("Insert 2 failed");
        let page2_id = rid2.page_id();

        assert_eq!(
            page1_id, page2_id,
            "Should reuse the same page for small tuples"
        );
        assert_ne!(rid1.slot_num(), rid2.slot_num());

        defer_delete(&path);
    }
}
