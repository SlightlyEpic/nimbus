use crate::storage::bplus_tree::BPlusTree;
use crate::storage::buffer::BufferPool;
use crate::storage::heap::heap_file::HeapError;
use crate::storage::heap::row::RowId;
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
    pub fn next(&mut self) -> Option<Result<(RowId, Vec<u8>), HeapError>> {
        loop {
            if self.current_page_id == 0 {
                return None;
            }

            let frame_result = self.bpm.as_mut().fetch_page(self.current_page_id);
            if let Err(e) = frame_result {
                return Some(Err(HeapError::FetchPage(format!("{:?}", e))));
            }

            let frame = frame_result.unwrap();
            let frame_id = frame.fid();

            let mut next_page_id = 0;
            let mut found_data = None;
            let mut found_slot_num = 0; // Track the slot number

            {
                let mut page_view = frame.page_view();

                if let Page::SlottedData(slotted) = &mut page_view {
                    let num_slots = slotted.num_slots();

                    while self.current_slot_index < num_slots {
                        let idx = self.current_slot_index as usize;
                        self.current_slot_index += 1;

                        if let Some(data) = slotted.slot_data(idx) {
                            found_data = Some(data.to_vec());
                            found_slot_num = idx as u32; // Capture slot
                            break;
                        }
                    }

                    if found_data.is_none() {
                        next_page_id = slotted.header().next_page_id();
                    }
                } else {
                    let _ = self.bpm.as_mut().unpin_frame(frame_id);
                    return Some(Err(HeapError::InvalidPage));
                }
            }

            if let Err(e) = self.bpm.as_mut().unpin_frame(frame_id) {
                return Some(Err(HeapError::UnpinPage(format!("{:?}", e))));
            }

            if let Some(data) = found_data {
                // Construct RowId
                let rid = RowId::new(self.current_page_id, found_slot_num);
                return Some(Ok((rid, data)));
            }

            self.current_page_id = next_page_id;
            self.current_slot_index = 0;
        }
    }
}

pub struct BTreeIterator<'a> {
    bpm: Pin<&'a mut BufferPool>,
    current_page_id: PageId,
    current_idx: u16,
}

impl<'a> BTreeIterator<'a> {
    /// Initialize iterator starting at the leaf containing `start_key`.
    /// If `start_key` is None, it starts at the left-most leaf (Full Scan).
    pub fn new(tree: BPlusTree<'a>, start_key: Option<&[u8]>) -> Self {
        // 1. Destructure the tree to get raw access to BPM and Root
        // We do this to avoid borrowing `tree` (which would lock BPM) while trying to use BPM.
        let BPlusTree {
            mut bpm,
            root_page_id,
        } = tree;

        if root_page_id == 0 {
            return Self {
                bpm,
                current_page_id: 0,
                current_idx: 0,
            };
        }

        // 2. Find the starting PageId
        // We must manually traverse here because we decomposed the Tree struct.
        let mut current_page_id = root_page_id;

        // Traversal Loop (Root -> Leaf)
        loop {
            // Fetch Page
            let frame = bpm.as_mut().fetch_page(current_page_id);
            if frame.is_err() {
                // If we can't fetch the page (IO Error), we return an empty iterator
                // In a real impl, we might want to return Result<Self>, but Iterator::new is usually infallible
                return Self {
                    bpm,
                    current_page_id: 0,
                    current_idx: 0,
                };
            }
            let frame = frame.unwrap();
            let frame_id = frame.fid();
            let page_view = frame.page_view();

            let next_child = match page_view {
                Page::BPlusInner(inner) => {
                    if let Some(key) = start_key {
                        // Point Query / Range Scan: Find child for key
                        let idx = inner.find_child_for_key(key);
                        inner.get_child_at(idx)
                    } else {
                        // Full Scan: Always go Left (Child 0)
                        inner.get_child_at(0)
                    }
                }
                Page::BPlusLeaf(_) => {
                    // We reached the leaf
                    None
                }
                _ => None, // Invalid page type
            };

            // Unpin current page before moving down
            bpm.as_mut().unpin_frame(frame_id).ok();

            if let Some(child_id) = next_child {
                current_page_id = child_id;
            } else {
                // We are at the leaf (or error), break loop
                break;
            }
        }

        // 3. Find the Starting Index within the Leaf
        let mut idx = 0;
        if current_page_id != 0 {
            if let Ok(frame) = bpm.as_mut().fetch_page(current_page_id) {
                let view = frame.page_view();
                if let Page::BPlusLeaf(leaf) = view {
                    if let Some(key) = start_key {
                        for i in 0..leaf.num_entries() {
                            if leaf.get_key_at(i as usize) >= key {
                                idx = i;
                                break;
                            }
                        }

                        if idx == 0 && leaf.num_entries() > 0 && leaf.get_key_at(0) < key {
                            let mut found = false;
                            for i in 0..leaf.num_entries() {
                                if leaf.get_key_at(i as usize) >= key {
                                    idx = i;
                                    found = true;
                                    break;
                                }
                            }
                            if !found {
                                idx = leaf.num_entries();
                            }
                        }
                    } else {
                        // Full scan: start at 0
                        idx = 0;
                    }
                }
                let fid = frame.fid();
                bpm.as_mut().unpin_frame(fid).ok();
            }
        }

        Self {
            bpm,
            current_page_id,
            current_idx: idx,
        }
    }

    /// Returns the next (Key, Value) pair in the tree.
    pub fn next(&mut self) -> Option<(Vec<u8>, u64)> {
        loop {
            if self.current_page_id == 0 {
                return None;
            }

            let frame_res = self.bpm.as_mut().fetch_page(self.current_page_id);
            if frame_res.is_err() {
                return None;
            }
            let frame = frame_res.unwrap();
            let frame_id = frame.fid();

            let mut result = None;
            let mut jump_to_sibling = None;

            {
                let page_view = frame.page_view();
                if let Page::BPlusLeaf(leaf) = page_view {
                    if self.current_idx < leaf.num_entries() {
                        let key = leaf.get_key_at(self.current_idx as usize).to_vec();
                        let val = leaf.get_value(&key).unwrap();
                        result = Some((key, val));
                        self.current_idx += 1;
                    } else {
                        if let Some(next_id) = leaf.next_sibling() {
                            jump_to_sibling = Some(next_id);
                        } else {
                            self.current_page_id = 0;
                        }
                    }
                } else {
                    self.current_page_id = 0;
                }
            }

            self.bpm.as_mut().unpin_frame(frame_id).ok();

            if let Some(res) = result {
                return Some(res);
            }

            if let Some(next_id) = jump_to_sibling {
                self.current_page_id = next_id;
                self.current_idx = 0;
            } else if self.current_page_id == 0 {
                return None;
            }
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

    #[test]
    fn test_empty_heap_scan() {
        let (path, mut bp, _) = setup_test_env("empty");
        defer_delete(&path);

        // Scanning page 0 (which doesn't exist/is invalid) or a non-existent list should return None immediately
        let mut iter = HeapIterator::new(bp.as_mut(), 0);
        assert!(iter.next().is_none());
    }

    fn defer_delete(path: &PathBuf) {
        // Simple cleanup wrapper
        let _ = fs::remove_file(path);
    }
}
