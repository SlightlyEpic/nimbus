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

    // src/storage/heap/heap_file.rs

    pub fn insert(
        &mut self,
        mut bpm: Pin<&mut BufferPool>,
        page_id_counter: &AtomicU32,
        data: &[u8],
    ) -> Result<RowId, HeapError> {
        // 1. Calculate Required Space
        let required_space =
            data.len() as u32 + page::slotted_data::SlottedData::SLOT_META_SIZE as u32;

        let mut insert_page_id = 0;

        // --- A. Check Last Page (Prioritize last page in chain via direct fetch) ---
        if self.last_page_id != 0 {
            let last_page_id = self.last_page_id;
            let fetch_result = bpm.as_mut().fetch_page(last_page_id);

            if let Ok(frame) = fetch_result {
                let frame_id = frame.fid();
                let page_view = frame.page_view();

                let space = if let page::base::Page::SlottedData(slotted) = page_view {
                    slotted.free_space()
                } else {
                    0 // Invalid page type
                };

                bpm.as_mut().unpin_frame(frame_id).ok(); // Unpin after reading space

                if space >= required_space {
                    insert_page_id = last_page_id;
                }
            }
            // If fetch failed or page was full, insert_page_id remains 0
        }

        // --- B. Search Reusable Pages (Run 18 Logic) ---
        if insert_page_id == 0 {
            let (core, locator) = bpm.as_mut().get_core_and_locator();
            let reusable_id_opt: Option<PageId> = locator
                .find_page_with_space(required_space, core)
                .map_err(|e| HeapError::FindSpace(format!("{:?}", e)))?; // Yields Option<PageId> or returns HeapError

            if let Some(reusable_id) = reusable_id_opt {
                insert_page_id = reusable_id;
            }
        }

        // --- C. Execute Insert on Existing Page (Reusable Page or Last Page) ---
        if insert_page_id != 0 {
            let page_id = insert_page_id;
            let frame = bpm // Re-fetch, pins it again for insertion
                .as_mut()
                .fetch_page(page_id)
                .map_err(|e| HeapError::FetchPage(format!("{:?}", e)))?;
            let frame_id = frame.fid();

            let (insert_result, new_free_space) = {
                let mut page_view = frame.page_view();
                if let page::base::Page::SlottedData(slotted_page) = &mut page_view {
                    (slotted_page.add_slot(data), slotted_page.free_space())
                } else {
                    bpm.as_mut().unpin_frame(frame_id).ok();
                    return Err(HeapError::InvalidPage);
                }
            };

            let slot_num = insert_result.map_err(|e| HeapError::AddSlot(format!("{:?}", e)))?;

            // Success: Update Directory and mark dirty
            bpm.as_mut().mark_frame_dirty(frame_id);

            let (core, locator) = bpm.as_mut().get_core_and_locator();
            locator
                .update_page_free_space(page_id, new_free_space, core)
                .map_err(|e| HeapError::UpdateSpace(format!("{:?}", e)))?;

            bpm.as_mut().unpin_frame(frame_id).ok();
            return Ok(RowId::new(page_id, slot_num as u32));
        }

        // --- D. ALLOCATION FALLBACK (New Page) ---

        // Allocate a new page (Fallback - only reached if all existing pages are full)
        let new_page_id = page_id_counter.fetch_add(1, Ordering::SeqCst) + 1;

        let frame = bpm
            .as_mut()
            .alloc_new_page(PageKind::SlottedData, new_page_id)
            .map_err(|e| HeapError::AllocPage(format!("{:?}", e)))?;

        let new_frame_id = frame.fid();
        let new_file_offset = frame.file_offset();

        // Init and Insert
        let (slot_num, new_free_space) = {
            let mut page_view = frame.page_view();
            if let page::base::Page::SlottedData(slotted_page) = &mut page_view {
                slotted_page.header_mut().set_page_id(new_page_id);

                // Link backwards
                if self.last_page_id != 0 {
                    slotted_page
                        .header_mut()
                        .set_prev_page_id(self.last_page_id);
                }

                let slot_num = slotted_page
                    .add_slot(data)
                    .map_err(|e| HeapError::AddSlot(format!("{:?}", e)))?;
                (slot_num, slotted_page.free_space())
            } else {
                bpm.as_mut().unpin_frame(new_frame_id).ok();
                return Err(HeapError::InvalidPage);
            }
        };

        bpm.as_mut().mark_frame_dirty(new_frame_id);
        bpm.as_mut()
            .unpin_frame(new_frame_id)
            .map_err(|e| HeapError::UnpinPage(format!("{:?}", e)))?;

        // 4. Link forward (Update old last page)
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

        // 5. Update HeapFile state
        if self.first_page_id == 0 {
            self.first_page_id = new_page_id;
        }
        self.last_page_id = new_page_id;

        // 6. Register new page in Directory
        bpm.as_mut()
            .expand_directory_and_register(
                new_page_id,
                new_file_offset,
                new_free_space,
                page_id_counter,
            )
            .map_err(|e| HeapError::RegisterPage(e))?;

        Ok(RowId::new(new_page_id, slot_num as u32))
    }

    // --- HeapFile::delete update to include Directory update ---
    pub fn delete(&mut self, mut bpm: Pin<&mut BufferPool>, rid: RowId) -> Result<(), HeapError> {
        let page_id = rid.page_id();
        let slot_num = rid.slot_num();

        let frame = bpm
            .as_mut()
            .fetch_page(page_id)
            .map_err(|e| HeapError::FetchPage(format!("{:?}", e)))?;
        let frame_id = frame.fid();

        let (res, new_free_space) = {
            let mut page_view = frame.page_view();
            if let page::base::Page::SlottedData(slotted) = &mut page_view {
                let result = slotted
                    .mark_dead(slot_num as usize) // Mark slot dead (tombstone)
                    .map_err(|_| HeapError::InvalidPage);
                (result, slotted.free_space())
            } else {
                (Err(HeapError::InvalidPage), 0)
            }
        };

        if res.is_ok() {
            // 1. Mark Dirty
            bpm.as_mut().mark_frame_dirty(frame_id);

            // 2. Update Directory (New Step: makes space available for reuse)
            let (core, locator) = bpm.as_mut().get_core_and_locator();
            locator
                .update_page_free_space(page_id, new_free_space, core)
                .map_err(|e| HeapError::UpdateSpace(format!("{:?}", e)))?;
        }

        // 3. Unpin
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
