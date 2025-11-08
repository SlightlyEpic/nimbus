use crate::storage::buffer::buffer_pool::{self, BufferPool, errors::FetchPageError};
use crate::storage::page::{
    self,
    base::{self, Page, PageId, PageKind},
    bplus_inner::{BPlusInner, BPlusInnerSplitData},
    bplus_leaf::BPlusLeaf,
};
use std::num::NonZeroU64;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug)]
pub struct SplitResult {
    pub split_key: Vec<u8>,
}

// return value for result of insert
#[derive(Debug)]
enum SplitData {
    // the insertion was absorbed by th child
    NoSplit,
    // the child node was split and returned a new Pageid
    Split { key: Vec<u8>, right_page_id: PageId },
}

// B+ tree struct
// access to api find and insert
// I/P: Pin<& mut Bufferpool>
#[derive(Debug)]
pub struct BplusTree {
    root_page_id: PageId,
    key_size: u32,
}

// Custom error type for B+ Tree operations
#[derive(Debug)]
pub enum BTreeError {
    FetchPage(FetchPageError),
    AllocPage(buffer_pool::errors::AllocNewPageError),
    UnpinPage(buffer_pool::errors::UnpinFrameError),
    InvalidPageType,
    PageNotFound,
    SplitError(String),
}

impl From<FetchPageError> for BTreeError {
    fn from(err: FetchPageError) -> Self {
        BTreeError::FetchPage(err)
    }
}
impl From<buffer_pool::errors::AllocNewPageError> for BTreeError {
    fn from(err: buffer_pool::errors::AllocNewPageError) -> Self {
        BTreeError::AllocPage(err)
    }
}
impl From<buffer_pool::errors::UnpinFrameError> for BTreeError {
    fn from(err: buffer_pool::errors::UnpinFrameError) -> Self {
        BTreeError::UnpinPage(err)
    }
}

impl BplusTree {
    pub fn new(root_page_id: PageId, key_size: u32) -> Self {
        Self {
            root_page_id,
            key_size,
        }
    }

    pub fn get_root_page_id(&self) -> PageId {
        self.root_page_id
    }

    /// Finds a value associated with a key.
    /// Manages pinning and unpinning of pages during traversal.
    pub fn find(
        &self,
        key: &[u8],
        mut bpm: Pin<&mut BufferPool>,
    ) -> Result<Option<u64>, BTreeError> {
        let mut current_page_id = self.root_page_id;

        loop {
            // 1. Fetch the page from the buffer pool
            let frame = bpm.as_mut().fetch_page(current_page_id)?;
            let frame_id = frame.fid();
            let mut page_view = frame.page_view();

            let result = match &mut page_view {
                // 2. If it's an inner page, find the next child to visit
                Page::BPlusInner(inner_page) => {
                    if let Some(child_page_id) = inner_page.find_child_page(key) {
                        // Set current_page_id for the next loop iteration
                        current_page_id = child_page_id;
                        Ok(None) // Indicate traversal should continue
                    } else {
                        // This should ideally not happen if the tree is valid
                        Err(BTreeError::PageNotFound)
                    }
                }
                // 3. If it's a leaf page, find the value
                Page::BPlusLeaf(leaf_page) => {
                    // We found the leaf, search for the key and return the value
                    Ok(Some(leaf_page.get_value(key)))
                }
                _ => {
                    // The page is not a valid B+ Tree page
                    Err(BTreeError::InvalidPageType)
                }
            };

            // 4. Unpin the current frame before the next loop or returning
            bpm.as_mut().unpin_frame(frame_id)?;

            // 5. Handle the result from the match
            match result {
                Ok(Some(value)) => return Ok(value), // Found in leaf, return value
                Ok(None) => continue,                // Continue traversal
                Err(e) => return Err(e),             // Error occurred
            }
        }
    }

    /// Inserts a key-value pair into the tree.
    pub fn insert(
        &mut self,
        key: &[u8],
        value: u64,
        mut bpm: Pin<&mut BufferPool>,
        page_id_counter: &AtomicU64,
    ) -> Result<(), BTreeError> {
        // Start the recursive insertion from the root
        let split_result =
            self.insert_internal(self.root_page_id, key, value, bpm.as_mut(), page_id_counter)?;

        // Check if the root itself was split
        if let SplitData::Split {
            key: new_key,
            right_page_id: new_child_page_id,
        } = split_result
        {
            // The root split. We must create a new root page.
            let old_root_id = self.root_page_id;

            // 1. Allocate a new page for the new root
            let new_root_id =
                PageId::new(page_id_counter.fetch_add(1, Ordering::SeqCst) + 1).unwrap();

            // Get level of old root *before* allocating new root
            let old_root_level = {
                let old_root_frame = bpm.as_mut().fetch_page(old_root_id)?;
                let level = match old_root_frame.page_view() {
                    Page::BPlusInner(inner) => inner.page_level(),
                    Page::BPlusLeaf(leaf) => leaf.page_level(),
                    _ => return Err(BTreeError::InvalidPageType),
                };
                let old_fid = old_root_frame.fid();
                bpm.as_mut().unpin_frame(old_fid)?;
                level
            };

            // 2. Allocate and initialize new root
            let new_root_frame_id = {
                let new_root_frame = bpm
                    .as_mut()
                    .alloc_new_page(PageKind::BPlusInner, new_root_id)?;
                let new_root_frame_id = new_root_frame.fid();
                let mut new_root_page_view = new_root_frame.page_view();
                let new_root_page = match &mut new_root_page_view {
                    Page::BPlusInner(inner) => inner,
                    _ => return Err(BTreeError::InvalidPageType),
                };

                new_root_page.init(new_root_id, old_root_level + 1);
                new_root_page.set_key_size(self.key_size);

                // 3. Set its first child pointer to the *old* root
                new_root_page.set_child_at(0, old_root_id);
                let free_space = new_root_page.free_space();
                new_root_page.set_free_space(free_space - 8); // Account for first child ptr

                // 4. Insert the new key and the new child pointer
                new_root_page.insert_sorted(&new_key, new_child_page_id);

                bpm.as_mut().mark_frame_dirty(new_root_frame_id);
                bpm.as_mut().unpin_frame(new_root_frame_id)?;
                new_root_frame_id
            };

            // 5. Update the tree's root ID
            self.root_page_id = new_root_id;
        }

        Ok(())
    }
    /// Recursive helper for insertion
    fn insert_internal(
        &mut self,
        current_page_id: PageId,
        key: &[u8],
        value: u64,
        mut bpm: Pin<&mut BufferPool>,
        page_id_counter: &AtomicU64,
    ) -> Result<SplitData, BTreeError> {
        // 1. Fetch page and check its type
        let (page_kind, is_full, level) = {
            let frame = bpm.as_mut().fetch_page(current_page_id)?;
            let frame_id = frame.fid();
            let mut page_view = frame.page_view();

            let (is_full, page_kind, level) = match &mut page_view {
                Page::BPlusLeaf(leaf) => (
                    !leaf.has_space_for_key(),
                    PageKind::BPlusLeaf,
                    leaf.page_level(),
                ),
                Page::BPlusInner(inner) => (
                    !inner.has_space_for_key(),
                    PageKind::BPlusInner,
                    inner.page_level(),
                ),
                _ => (false, PageKind::Invalid, 0),
            };
            bpm.as_mut().unpin_frame(frame_id)?;

            (page_kind, is_full, level)
        };

        // 2. Handle insertion or split based on page type
        match page_kind {
            //  BASE CASE: We are at a Leaf Page
            PageKind::BPlusLeaf => {
                if !is_full {
                    // Simple case: insert into leaf
                    let frame = bpm.as_mut().fetch_page(current_page_id)?;
                    let frame_id = frame.fid();
                    let mut page_view = frame.page_view();
                    match &mut page_view {
                        Page::BPlusLeaf(leaf_page) => {
                            leaf_page.insert_sorted(key, value);
                            bpm.as_mut().mark_frame_dirty(frame_id);
                        }
                        _ => return Err(BTreeError::InvalidPageType),
                    }
                    bpm.as_mut().unpin_frame(frame_id)?;
                    return Ok(SplitData::NoSplit);
                }

                // Hard case: Leaf page is full, must split
                // 1. Fetch old page, call split, get new entries, and unpin
                let (split_key, new_page_entries, old_next_sibling_id) = {
                    let frame = bpm.as_mut().fetch_page(current_page_id)?;
                    let frame_id = frame.fid();
                    let mut page_view = frame.page_view();
                    let leaf_page = match &mut page_view {
                        Page::BPlusLeaf(leaf) => leaf,
                        _ => return Err(BTreeError::InvalidPageType),
                    };

                    let (split_result, new_page_entries) = leaf_page
                        .split_and_get_new_entries(key, value)
                        .map_err(|e| BTreeError::SplitError(e.to_string()))?;

                    let old_next_sibling_id = leaf_page.next_sibling();

                    bpm.as_mut().mark_frame_dirty(frame_id);
                    bpm.as_mut().unpin_frame(frame_id)?;

                    (
                        split_result.split_key,
                        new_page_entries,
                        old_next_sibling_id,
                    )
                }; // --- old_frame borrow ends ---

                // 2. Allocate new page, fill it, and unpin
                let new_page_id =
                    PageId::new(page_id_counter.fetch_add(1, Ordering::SeqCst) + 1).unwrap();
                {
                    let new_frame = bpm
                        .as_mut()
                        .alloc_new_page(PageKind::BPlusLeaf, new_page_id)?;
                    let new_frame_id = new_frame.fid();
                    let mut new_page_view = new_frame.page_view();
                    let new_leaf = match &mut new_page_view {
                        Page::BPlusLeaf(leaf) => leaf,
                        _ => return Err(BTreeError::InvalidPageType),
                    };

                    new_leaf.init(new_page_id); // init sets level 0
                    new_leaf.set_key_size(self.key_size);

                    for (k, v) in new_page_entries {
                        new_leaf.insert_sorted(&k, v);
                    }

                    new_leaf.set_next_sibling(old_next_sibling_id);
                    new_leaf.set_prev_sibling(Some(current_page_id));

                    bpm.as_mut().mark_frame_dirty(new_frame_id);
                    bpm.as_mut().unpin_frame(new_frame_id)?;
                } // --- new_frame borrow ends ---

                // 3. Re-fetch old page to update its next_sibling pointer
                {
                    let old_frame = bpm.as_mut().fetch_page(current_page_id)?;
                    let old_frame_id = old_frame.fid();
                    let mut page_view = old_frame.page_view();
                    match &mut page_view {
                        Page::BPlusLeaf(old_leaf) => {
                            old_leaf.set_next_sibling(Some(new_page_id));
                            bpm.as_mut().mark_frame_dirty(old_frame_id);
                        }
                        _ => return Err(BTreeError::InvalidPageType),
                    }
                    bpm.as_mut().unpin_frame(old_frame_id)?;
                } // --- old_frame borrow ends ---

                // 4. Update the *next* sibling's prev pointer if it exists
                if let Some(next_id) = old_next_sibling_id {
                    let next_frame = bpm.as_mut().fetch_page(next_id)?;
                    let next_frame_id = next_frame.fid();
                    let mut page_view = next_frame.page_view();
                    match &mut page_view {
                        Page::BPlusLeaf(next_leaf) => {
                            next_leaf.set_prev_sibling(Some(new_page_id));
                            bpm.as_mut().mark_frame_dirty(next_frame_id);
                        }
                        _ => return Err(BTreeError::InvalidPageType),
                    }
                    bpm.as_mut().unpin_frame(next_frame_id)?;
                } // --- next_frame borrow ends ---

                // 7. Return split info to parent
                Ok(SplitData::Split {
                    key: split_key,
                    right_page_id: new_page_id,
                })
            }

            //  RECURSIVE CASE: We are at an Inner Page
            PageKind::BPlusInner => {
                // 1. Find the correct child to descend into
                let child_page_id = {
                    let frame = bpm.as_mut().fetch_page(current_page_id)?;
                    let frame_id = frame.fid();
                    let mut page_view = frame.page_view();
                    let inner_page = match &mut page_view {
                        Page::BPlusInner(inner) => inner,
                        _ => return Err(BTreeError::InvalidPageType),
                    };
                    let child_id = inner_page
                        .find_child_page(key)
                        .ok_or(BTreeError::PageNotFound)?;
                    bpm.as_mut().unpin_frame(frame_id)?;
                    child_id
                };

                // 2. Recursively call insert on that child
                let split_result =
                    self.insert_internal(child_page_id, key, value, bpm.as_mut(), page_id_counter)?;

                // 3. Handle the result from the child
                match split_result {
                    // Child did not split, we are done
                    SplitData::NoSplit => Ok(SplitData::NoSplit),

                    // Child *did* split, we must insert its new key and pointer
                    SplitData::Split {
                        key: new_key,
                        right_page_id: new_child_page_id,
                    } => {
                        if !is_full {
                            // Simple case: insert new key and child pointer
                            let frame = bpm.as_mut().fetch_page(current_page_id)?;
                            let frame_id = frame.fid();
                            let mut page_view = frame.page_view();
                            match &mut page_view {
                                Page::BPlusInner(inner_page) => {
                                    inner_page.insert_sorted(&new_key, new_child_page_id);
                                    bpm.as_mut().mark_frame_dirty(frame_id);
                                }
                                _ => return Err(BTreeError::InvalidPageType),
                            }
                            bpm.as_mut().unpin_frame(frame_id)?;
                            return Ok(SplitData::NoSplit);
                        }

                        // Hard case: This inner page is also full, must split
                        // 1. Fetch old page, call split, get new entries, and unpin
                        let (key_to_push_up, new_page_keys, new_page_children, old_next_sibling_id) = {
                            let frame = bpm.as_mut().fetch_page(current_page_id)?;
                            let frame_id = frame.fid();
                            let mut page_view = frame.page_view();
                            let inner_page = match &mut page_view {
                                Page::BPlusInner(inner) => inner,
                                _ => return Err(BTreeError::InvalidPageType),
                            };

                            let split_data =
                                inner_page.split_and_get_new_entries(&new_key, new_child_page_id);

                            let old_next_sibling_id = inner_page.next_sibling();

                            bpm.as_mut().mark_frame_dirty(frame_id);
                            bpm.as_mut().unpin_frame(frame_id)?;

                            (
                                split_data.key_to_push_up,
                                split_data.new_page_keys,
                                split_data.new_page_children,
                                old_next_sibling_id,
                            )
                        }; // --- old_frame borrow ends ---

                        // 2. Allocate new page, fill it, and unpin
                        let new_page_id =
                            PageId::new(page_id_counter.fetch_add(1, Ordering::SeqCst) + 1)
                                .unwrap();
                        {
                            let new_frame = bpm
                                .as_mut()
                                .alloc_new_page(PageKind::BPlusInner, new_page_id)?;
                            let new_frame_id = new_frame.fid();
                            let mut new_page_view = new_frame.page_view();
                            let new_inner = match &mut new_page_view {
                                Page::BPlusInner(inner) => inner,
                                _ => return Err(BTreeError::InvalidPageType),
                            };

                            new_inner.init(new_page_id, level);
                            new_inner.set_key_size(self.key_size);

                            // Set first child
                            new_inner.set_child_at(0, new_page_children[0]);
                            let free_space = new_inner.free_space();
                            new_inner.set_free_space(free_space - 8);

                            // Insert remaining keys and children
                            for i in 0..new_page_keys.len() {
                                new_inner
                                    .insert_sorted(&new_page_keys[i], new_page_children[i + 1]);
                            }

                            new_inner.set_next_sibling(old_next_sibling_id);
                            new_inner.set_prev_sibling(Some(current_page_id));

                            bpm.as_mut().mark_frame_dirty(new_frame_id);
                            bpm.as_mut().unpin_frame(new_frame_id)?;
                        } // --- new_frame borrow ends ---

                        // 3. Re-fetch old page to update its next_sibling pointer
                        {
                            let old_frame = bpm.as_mut().fetch_page(current_page_id)?;
                            let old_frame_id = old_frame.fid();
                            let mut page_view = old_frame.page_view();
                            match &mut page_view {
                                Page::BPlusInner(old_inner) => {
                                    old_inner.set_next_sibling(Some(new_page_id));
                                    bpm.as_mut().mark_frame_dirty(old_frame_id);
                                }
                                _ => return Err(BTreeError::InvalidPageType),
                            }
                            bpm.as_mut().unpin_frame(old_frame_id)?;
                        } // --- old_frame borrow ends ---

                        // 4. Update the *next* sibling's prev pointer if it exists
                        if let Some(next_id) = old_next_sibling_id {
                            let next_frame = bpm.as_mut().fetch_page(next_id)?;
                            let next_frame_id = next_frame.fid();
                            let mut page_view = next_frame.page_view();
                            match &mut page_view {
                                Page::BPlusInner(next_inner) => {
                                    next_inner.set_prev_sibling(Some(new_page_id));
                                    bpm.as_mut().mark_frame_dirty(next_frame_id);
                                }
                                _ => return Err(BTreeError::InvalidPageType),
                            }
                            bpm.as_mut().unpin_frame(next_frame_id)?;
                        } // --- next_frame borrow ends ---

                        // 7. Return split info to *our* parent
                        Ok(SplitData::Split {
                            key: key_to_push_up,
                            right_page_id: new_page_id,
                        })
                    }
                }
            }
            // This case is now handled by the main match
            PageKind::Invalid => Err(BTreeError::InvalidPageType),
            _ => Err(BTreeError::InvalidPageType), // Other page types
        }
    }
}

#[cfg(test)]
pub mod bplus_tests {
    // Add 'pub' to the module
    use super::*;
    use crate::constants;
    use crate::storage::bplus_tree::buffer_pool::errors::FlushFrameError;
    use crate::storage::buffer::BufferPoolCore;
    use crate::storage::buffer::fifo_evictor::FifoEvictor;
    use crate::storage::disk::FileManager;
    use crate::storage::page::base::{PageId, PageKind};
    use crate::storage::page_locator::locator::{self, PageLocator};
    use std::fs;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};
    const FRAME_COUNT: usize = 128;
    // Add 'pub'
    pub struct MockPageLocator;
    impl PageLocator for MockPageLocator {
        fn find_file_offset(
            &mut self,
            _page_id: PageId,
            _core: Pin<&mut BufferPoolCore>,
        ) -> Result<u64, locator::errors::FindOffsetError> {
            Err(locator::errors::FindOffsetError::NotFound)
        }
    }

    // Add 'pub'
    pub fn setup_buffer_pool_test(test_name: &str) -> (PathBuf, Pin<Box<BufferPool>>, AtomicU64) {
        let mut temp_dir = std::env::temp_dir();
        temp_dir.push(format!("nimbus_test_{}.db", test_name));
        let temp_file_path = temp_dir.clone();
        let temp_file_str = temp_file_path.to_str().expect("Invalid temp file path");

        let _ = fs::remove_file(&temp_file_path);

        let file_manager =
            FileManager::new(temp_file_str.to_string()).expect("Failed to create FileManager");

        let evictor = Box::new(FifoEvictor::new());

        let page_locator = Box::new(MockPageLocator);

        let buffer_pool = Box::pin(BufferPool::new(file_manager, evictor, page_locator));

        // Start counter at 1, since 0 is reserved for file manager
        // And PageId 1 will be the root.
        let page_id_cnt = AtomicU64::new(1);
        (temp_file_path, buffer_pool, page_id_cnt)
    }

    // Add 'pub'
    pub fn generate_test_page_id(counter: &AtomicU64) -> PageId {
        // ID start from 1.
        let next_id = counter.fetch_add(1, Ordering::SeqCst) + 1;
        PageId::new(next_id).expect("Page ID counter overflowed in test")
    }

    // Add 'pub'
    pub fn cleanup_temp_file(temp_file_path: &PathBuf) {
        let _ = fs::remove_file(temp_file_path);
    }

    #[test]
    fn test_fetch_page_at_offset_hit() {
        let (temp_path, mut buffer_pool, page_id_counter) =
            setup_buffer_pool_test("fetch_page_hit");

        let page_id = generate_test_page_id(&page_id_counter);
        // Allocate a page
        let page_kind = PageKind::SlottedData;
        let frame1 = buffer_pool
            .as_mut()
            .alloc_new_page(page_kind, page_id)
            .expect("Alloc page 1 failed");
        let offset1 = frame1.file_offset();
        let page_id1 = frame1.page_id();
        let frame_id1 = frame1.fid();

        let mut page_view = frame1.page_view();
        match &mut page_view {
            crate::storage::page::base::Page::SlottedData(page) => page.set_page_id(page_id1),
            _ => panic!("Expected SlottedData Page"),
        }
        assert_eq!(offset1, 0);

        // fetch the same page again by offset
        let frame2_result = buffer_pool.as_mut().fetch_page_at_offset(offset1);
        assert!(
            frame2_result.is_ok(),
            "Fetching page by offset (hit) failed"
        );
        let frame2 = frame2_result.unwrap();

        // verify it's the same frame and it's pinned
        assert_eq!(
            frame2.fid(),
            frame_id1,
            "Cache hit should return the same frame id"
        );
        assert_eq!(frame2.page_id(), page_id1, "Cache hit page id mismatch");
        assert!(
            frame2.pinned(),
            "Frame fetched via cache hit should be pinned"
        );

        //Unpin the frame (once is enough if no pin count)
        let unpin_res = buffer_pool.as_mut().unpin_frame(frame_id1);
        assert!(unpin_res.is_ok(), "Unpin failed");

        cleanup_temp_file(&temp_path);
    }

    #[test]
    fn test_fetch_page_at_offset_miss() {
        let (temp_path, mut buffer_pool, _) = setup_buffer_pool_test("fetch_page_miss");

        let page_id_on_disk = PageId::new(100).unwrap();
        let offset_on_disk: u64 = 0;
        let mut page_buf_disk = [0u8; constants::storage::PAGE_SIZE];

        {
            let mut fm_direct = FileManager::new(temp_path.to_str().unwrap().to_string()).unwrap();

            // Initialize buffer for SlottedData page
            page::base::init_page_buf(&mut page_buf_disk, PageKind::SlottedData);
            // Need to set PageId manually in the buffer
            page_buf_disk[8..16].copy_from_slice(&page_id_on_disk.get().to_le_bytes());

            // Write this buffer directly to the file
            fm_direct
                .write_block_from(offset_on_disk, &page_buf_disk)
                .expect("Failed to write initial page directly to disk");
        } // fm_direct goes out of scope, file is closed

        // fetch the page using the buffer pool, should be a cache miss
        let frame_result = buffer_pool.as_mut().fetch_page_at_offset(offset_on_disk);
        assert!(
            frame_result.is_ok(),
            "Fetching page from disk (miss) failed: {:?}",
            frame_result.err()
        );
        let frame = frame_result.unwrap();

        // Verify frame properties
        assert_eq!(frame.file_offset(), offset_on_disk, "Frame offset mismatch");
        assert_eq!(frame.page_id(), page_id_on_disk, "Frame PageId mismatch");
        assert!(frame.ready(), "Fetched frame should be ready");
        assert!(frame.pinned(), "Fetched frame should be pinned");
        assert!(!frame.dirty(), "Frame loaded from disk should not be dirty");
        assert_eq!(frame.fid(), 0, "First fetched frame should have fid 0");

        let frame_id = frame.fid();

        let unpin_result = buffer_pool.as_mut().unpin_frame(frame_id);
        assert!(unpin_result.is_ok(), "Unpinning fetched frame failed");

        cleanup_temp_file(&temp_path);
    }

    #[test]
    fn test_buffer_pool_full_eviction() {
        let (temp_path, mut buffer_pool, page_id_cnt) = setup_buffer_pool_test("eviction");

        let mut allocated_offsets = Vec::new();

        // Fill the buffer pool completely
        for i in 0..FRAME_COUNT {
            let page_id = generate_test_page_id(&page_id_cnt);
            let frame = buffer_pool
                .as_mut()
                .alloc_new_page(PageKind::SlottedData, page_id)
                .expect(&format!("Failed to alloc page {}", i));

            let mut page_view = frame.page_view();
            match &mut page_view {
                crate::storage::page::base::Page::SlottedData(page) => page.set_page_id(page_id),
                _ => panic!("Expected SlottedData Page"),
            }

            allocated_offsets.push(frame.file_offset());
        }

        // Flush all frames to ensure they can be evicted cleanly
        buffer_pool.as_mut().flush_all().expect("Flush all failed");

        let page_id2 = generate_test_page_id(&page_id_cnt);
        let extra_frame_result = buffer_pool
            .as_mut()
            .alloc_new_page(PageKind::SlottedData, page_id2);
        assert!(
            extra_frame_result.is_ok(),
            "Allocating page beyond capacity failed: {:?}",
            extra_frame_result.err()
        );
        let extra_frame = extra_frame_result.unwrap();

        // Set the page_id in the buffer
        let mut page_view = extra_frame.page_view();
        match &mut page_view {
            crate::storage::page::base::Page::SlottedData(page) => page.set_page_id(page_id2),
            _ => panic!("Expected SlottedData Page"),
        }

        let extra_offset = extra_frame.file_offset();

        // Check that the first allocated page was evicted
        // by fetching it again - it should be re-read from disk
        let first_offset = allocated_offsets[0];
        let frame_after_evict_result = buffer_pool.as_mut().fetch_page_at_offset(first_offset);

        assert!(
            frame_after_evict_result.is_ok(),
            "Fetching evicted page failed"
        );
        let frame_after_evict = frame_after_evict_result.unwrap();

        // The newly allocated frame's offset should be different from the first one
        assert_ne!(
            extra_offset, first_offset,
            "New page offset should differ from the evicted page offset"
        );

        // Cleanup - unpin the frame we fetched (fetch_page_at_offset pins it)
        let evicted_frame_id = frame_after_evict.fid();
        buffer_pool
            .as_mut()
            .unpin_frame(evicted_frame_id)
            .expect("Unpin failed");

        cleanup_temp_file(&temp_path);
    }

    #[test]
    fn test_pinned_frame_prevents_eviction() {
        let (temp_path, mut buffer_pool, page_id_cnt) = setup_buffer_pool_test("pinned_eviction");

        let mut allocated_offsets = Vec::new();

        let page_id0 = generate_test_page_id(&page_id_cnt);
        let frame0 = buffer_pool
            .as_mut()
            .alloc_new_page(PageKind::SlottedData, page_id0)
            .unwrap();

        let mut page_view = frame0.page_view();
        match &mut page_view {
            crate::storage::page::base::Page::SlottedData(page) => page.set_page_id(page_id0),
            _ => panic!("Expected SlottedData Page"),
        }

        let fid0 = frame0.fid();
        let offset0 = frame0.file_offset();
        let page_id0 = frame0.page_id();

        buffer_pool
            .as_mut()
            .pin_frame(fid0)
            .expect("Failed to pin frame 0");
        allocated_offsets.push(offset0);

        // Fill the rest of the buffer pool (these frames remain unpinned)
        for i in 1..FRAME_COUNT {
            let page_id1 = generate_test_page_id(&page_id_cnt);
            let frame = buffer_pool
                .as_mut()
                .alloc_new_page(PageKind::SlottedData, page_id1)
                .expect(&format!("Failed to alloc page {}", i));

            // Set the page_id in the actual page buffer (needed for flushing)
            let mut page_view = frame.page_view();
            match &mut page_view {
                crate::storage::page::base::Page::SlottedData(page) => page.set_page_id(page_id1),
                _ => panic!("Expected SlottedData Page"),
            }

            allocated_offsets.push(frame.file_offset());
        }

        buffer_pool.as_mut().flush_all().expect("Flush all failed");

        let page_id = generate_test_page_id(&page_id_cnt);
        let extra_frame_result = buffer_pool
            .as_mut()
            .alloc_new_page(PageKind::SlottedData, page_id);
        assert!(
            extra_frame_result.is_ok(),
            "Allocating page beyond capacity failed: {:?}",
            extra_frame_result.err()
        );

        let frame0_refetch_result = buffer_pool.as_mut().fetch_page_at_offset(offset0);
        assert!(
            frame0_refetch_result.is_ok(),
            "Fetching pinned frame failed"
        );
        let frame0_refetch = frame0_refetch_result.unwrap();
        assert_eq!(
            frame0_refetch.fid(),
            fid0,
            "Pinned frame fid changed after eviction attempts"
        );
        assert_eq!(
            frame0_refetch.page_id(),
            page_id0,
            "Pinned frame page_id changed"
        );
        assert!(
            frame0_refetch.pinned(),
            "Refetched frame 0 should still be pinned"
        );

        buffer_pool
            .as_mut()
            .unpin_frame(fid0)
            .expect("Failed to unpin frame 0 (first unpin)");
        buffer_pool
            .as_mut()
            .unpin_frame(fid0)
            .expect("Failed to unpin frame 0 (second unpin)");

        cleanup_temp_file(&temp_path);
    }

    #[test]
    fn test_mark_frame_dirty() {
        let (temp_path, mut buffer_pool, page_id_cnt) = setup_buffer_pool_test("mark_dirty");

        let fid1;
        let offset1;
        {
            let page_id1 = generate_test_page_id(&page_id_cnt);
            // Scope for frame1 borrow
            let frame1 = buffer_pool
                .as_mut()
                .alloc_new_page(PageKind::SlottedData, page_id1)
                .unwrap();
            fid1 = frame1.fid();
            offset1 = frame1.file_offset();
            assert!(frame1.dirty(), "Frame 1 should start dirty");
        }
        buffer_pool
            .as_mut()
            .flush_frame(fid1)
            .expect("Flush failed");
        let fid_after_flush;
        {
            // Scope for frame1_check borrow
            let frame1_check = buffer_pool.as_mut().fetch_page_at_offset(offset1).unwrap();
            fid_after_flush = frame1_check.fid();
            assert!(!frame1_check.dirty(), "Frame 1 should be clean after flush");
        }

        // Mark the clean frame as dirty
        let fid_to_mark;
        {
            // Scope for frame_to_mark borrow
            let frame_to_mark_ref = buffer_pool.as_mut().fetch_page_at_offset(offset1).unwrap();
            fid_to_mark = frame_to_mark_ref.fid();
            buffer_pool.as_mut().mark_frame_dirty(fid_to_mark);
        }

        let fid_dirty_check;
        {
            // Scope for frame1_dirty_check borrow
            let frame1_dirty_check_ref =
                buffer_pool.as_mut().fetch_page_at_offset(offset1).unwrap();
            fid_dirty_check = frame1_dirty_check_ref.fid();
            assert!(
                frame1_dirty_check_ref.dirty(),
                "Frame 1 should be dirty after mark_frame_dirty"
            );
        }

        buffer_pool
            .as_mut()
            .unpin_frame(fid_after_flush)
            .expect("Unpin step 2 failed");
        buffer_pool
            .as_mut()
            .unpin_frame(fid_to_mark)
            .expect("Unpin step 3 failed");
        buffer_pool
            .as_mut()
            .unpin_frame(fid_dirty_check)
            .expect("Unpin step 4 failed");

        cleanup_temp_file(&temp_path);
    }

    #[test]
    fn test_flush_clean_frame() {
        let (temp_path, mut buffer_pool, page_id_cnt) = setup_buffer_pool_test("flush_clean");

        let page_id = generate_test_page_id(&page_id_cnt);
        let fid;
        let offset;
        {
            // Scope for frame borrow
            let frame = buffer_pool
                .as_mut()
                .alloc_new_page(PageKind::SlottedData, page_id)
                .unwrap();
            fid = frame.fid();
            offset = frame.file_offset();
            assert!(frame.dirty());
        }

        // Flush it to make it clean
        buffer_pool
            .as_mut()
            .flush_frame(fid)
            .expect("First flush failed");
        let fid_check;
        {
            // Scope for frame_check borrow
            let frame_check_ref = buffer_pool.as_mut().fetch_page_at_offset(offset).unwrap();
            fid_check = frame_check_ref.fid();
            assert!(!frame_check_ref.dirty(), "Frame should be clean");
        }

        let flush_again_result = buffer_pool.as_mut().flush_frame(fid);
        assert!(flush_again_result.is_ok(), "Flushing a clean frame failed");

        buffer_pool
            .as_mut()
            .unpin_frame(fid_check)
            .expect("Unpin failed");

        cleanup_temp_file(&temp_path);
    }

    #[test]
    fn test_flush_non_existent_frame() {
        let (temp_path, mut buffer_pool, _) = setup_buffer_pool_test("flush_non_existent");
        let non_existent_fid = 999;
        let flush_result = buffer_pool.as_mut().flush_frame(non_existent_fid);
        assert!(
            flush_result.is_err(),
            "Flushing non-existent frame should fail"
        );
        assert!(
            matches!(flush_result, Err(FlushFrameError::FrameNotFound)),
            "Incorrect error type"
        );

        cleanup_temp_file(&temp_path);
    }

    #[test]
    #[should_panic]
    fn test_dealloc_pinned_frame_panics() {
        let (temp_path, mut buffer_pool, page_id_cnt) =
            setup_buffer_pool_test("dealloc_pinned_panics");

        let page_id = generate_test_page_id(&page_id_cnt);
        let frame = buffer_pool
            .as_mut()
            .alloc_new_page(PageKind::SlottedData, page_id)
            .unwrap();
        let fid = frame.fid();
        buffer_pool.as_mut().pin_frame(fid).expect("Pinning failed");

        // Directly call dealloc_frame_at (via BufferPoolCore) - This should panic
        let core = buffer_pool.as_mut().core();
        core.dealloc_frame_at(fid as usize); // This line should trigger the panic

        cleanup_temp_file(&temp_path);
    }
}
