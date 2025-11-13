use crate::storage::buffer::BufferPool;
use crate::storage::heap::heap_file::HeapError;
use crate::storage::page::base::DiskPage;
use crate::storage::page::base::{Page, PageId};
use std::pin::Pin;

pub struct HeapIterator<'a> {
    bpm: Pin<&'a mut BufferPool>,
    current_page_id: PageId,
    current_slot_index: u16,
}

impl<'a> HeapIterator<'a> {
    /// Creates a new iterator starting at the given page_id (usually the first page of the heap file)
    pub fn new(bpm: Pin<&'a mut BufferPool>, start_page_id: PageId) -> Self {
        Self {
            bpm,
            current_page_id: start_page_id,
            current_slot_index: 0,
        }
    }

    /// Advances the iterator and returns the next tuple as bytes
    pub fn next(&mut self) -> Option<Result<Vec<u8>, HeapError>> {
        loop {
            // End of the linked list
            if self.current_page_id == 0 {
                return None;
            }

            // 1. Fetch the current page
            let frame_result = self.bpm.as_mut().fetch_page(self.current_page_id);
            if let Err(e) = frame_result {
                return Some(Err(HeapError::FetchPage(format!("{:?}", e))));
            }

            let frame = frame_result.unwrap();
            let frame_id = frame.fid();

            let mut next_page_id = 0;
            let mut found_data = None;

            // 2. Scope for Page View to ensure we drop the borrow before unpinning
            {
                let mut page_view = frame.page_view();

                if let Page::SlottedData(slotted) = &mut page_view {
                    let num_slots = slotted.num_slots();

                    // 3. Iterate through slots in the current page
                    while self.current_slot_index < num_slots {
                        let idx = self.current_slot_index as usize;
                        self.current_slot_index += 1;

                        // Check if the slot has valid data (not a tombstone)
                        if let Some(data) = slotted.slot_data(idx) {
                            found_data = Some(data.to_vec());
                            break;
                        }
                    }

                    // If we finished all slots, prepare to jump to the next page
                    if found_data.is_none() {
                        next_page_id = slotted.header().next_page_id();
                    }
                } else {
                    // We encountered a page that isn't a SlottedData page in the heap chain
                    let _ = self.bpm.as_mut().unpin_frame(frame_id);
                    return Some(Err(HeapError::InvalidPage));
                }
            }

            // 4. Always unpin the frame after reading
            if let Err(e) = self.bpm.as_mut().unpin_frame(frame_id) {
                return Some(Err(HeapError::UnpinPage(format!("{:?}", e))));
            }

            // 5. If we found data, return it
            if let Some(data) = found_data {
                return Some(Ok(data));
            }

            // 6. Otherwise, advance to the next page and reset slot index
            self.current_page_id = next_page_id;
            self.current_slot_index = 0;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::buffer::fifo_evictor::FifoEvictor;
    use crate::storage::disk::FileManager;
    use crate::storage::heap::heap_file::HeapFile;
    use crate::storage::page_locator::locator::DirectoryPageLocator;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::atomic::AtomicU32;

    fn setup_test_env(test_name: &str) -> (PathBuf, Pin<Box<BufferPool>>, AtomicU32) {
        let file_name = format!("test_heap_iter_{}.db", test_name);
        let _ = fs::remove_file(&file_name);

        let file_manager = FileManager::new(file_name.clone()).unwrap();
        let evictor = Box::new(FifoEvictor::new());
        let locator = Box::new(DirectoryPageLocator::new());
        let bp = Box::pin(BufferPool::new(file_manager, evictor, locator));

        (PathBuf::from(file_name), bp, AtomicU32::new(0))
    }
}
