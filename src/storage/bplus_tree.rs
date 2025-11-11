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

#[derive(Debug)]
enum DeleteResult {
    /// The key was not found.
    NotFound,
    /// The key was found and deleted.
    Deleted,
    /// The key was found and deleted, and it was the first key in its leaf.
    /// The parent's separator key must be updated.
    DeletedAndPromote(Vec<u8>),
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

    fn register_page(
        &self,
        page_id: PageId,
        offset: u64,
        mut bpm: Pin<&mut BufferPool>,
        page_id_counter: &AtomicU64,
    ) -> Result<(), BTreeError> {
        bpm.as_mut()
            .expand_directory_and_register(page_id, offset, 0, page_id_counter)
            .map_err(|_| BTreeError::PageNotFound)
    }

    pub fn find(
        &self,
        key: &[u8],
        mut bpm: Pin<&mut BufferPool>,
    ) -> Result<Option<u64>, BTreeError> {
        let mut current_page_id = self.root_page_id;

        loop {
            let frame = bpm.as_mut().fetch_page(current_page_id)?;
            let frame_id = frame.fid();
            let mut page_view = frame.page_view();

            let result = match &mut page_view {
                Page::BPlusInner(inner_page) => {
                    if let Some(child_page_id) = inner_page.find_child_page(key) {
                        current_page_id = child_page_id;
                        Ok(None)
                    } else {
                        Err(BTreeError::PageNotFound)
                    }
                }
                Page::BPlusLeaf(leaf_page) => Ok(Some(leaf_page.get_value(key))),
                _ => Err(BTreeError::InvalidPageType),
            };

            bpm.as_mut().unpin_frame(frame_id)?;

            match result {
                Ok(Some(value)) => return Ok(value),
                Ok(None) => continue,
                Err(e) => return Err(e),
            }
        }
    }

    pub fn insert(
        &mut self,
        key: &[u8],
        value: u64,
        mut bpm: Pin<&mut BufferPool>,
        page_id_counter: &AtomicU64,
    ) -> Result<(), BTreeError> {
        let split_result =
            self.insert_internal(self.root_page_id, key, value, bpm.as_mut(), page_id_counter)?;

        if let SplitData::Split {
            key: new_key,
            right_page_id: new_child_page_id,
        } = split_result
        {
            let old_root_id = self.root_page_id;
            let new_root_id =
                PageId::new(page_id_counter.fetch_add(1, Ordering::SeqCst) + 1).unwrap();

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

            let new_root_frame = bpm
                .as_mut()
                .alloc_new_page(PageKind::BPlusInner, new_root_id)?;
            let new_root_frame_id = new_root_frame.fid();
            let new_root_offset = new_root_frame.file_offset();

            let mut new_root_page_view = new_root_frame.page_view();
            let new_root_page = match &mut new_root_page_view {
                Page::BPlusInner(inner) => inner,
                _ => return Err(BTreeError::InvalidPageType),
            };

            new_root_page.init(new_root_id, old_root_level + 1);
            new_root_page.set_key_size(self.key_size);

            new_root_page.set_child_at(0, old_root_id);
            new_root_page.insert_sorted(&new_key, new_child_page_id);

            bpm.as_mut().mark_frame_dirty(new_root_frame_id);
            bpm.as_mut().unpin_frame(new_root_frame_id)?;

            self.register_page(new_root_id, new_root_offset, bpm.as_mut(), page_id_counter)?;

            self.root_page_id = new_root_id;
        }

        Ok(())
    }

    pub fn delete(
        &mut self,
        key: &[u8],
        mut bpm: Pin<&mut BufferPool>,
    ) -> Result<bool, BTreeError> {
        self.delete_internal(self.root_page_id, key, bpm.as_mut())
            .map(|_| true)
    }

    fn insert_internal(
        &mut self,
        current_page_id: PageId,
        key: &[u8],
        value: u64,
        mut bpm: Pin<&mut BufferPool>,
        page_id_counter: &AtomicU64,
    ) -> Result<SplitData, BTreeError> {
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

        match page_kind {
            PageKind::BPlusLeaf => {
                if !is_full {
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
                };

                let new_page_id =
                    PageId::new(page_id_counter.fetch_add(1, Ordering::SeqCst) + 1).unwrap();

                let new_page_offset = {
                    let new_frame = bpm
                        .as_mut()
                        .alloc_new_page(PageKind::BPlusLeaf, new_page_id)?;
                    let new_frame_id = new_frame.fid();
                    let new_offset = new_frame.file_offset();

                    let mut new_page_view = new_frame.page_view();
                    let new_leaf = match &mut new_page_view {
                        Page::BPlusLeaf(leaf) => leaf,
                        _ => return Err(BTreeError::InvalidPageType),
                    };

                    new_leaf.init(new_page_id);
                    new_leaf.set_key_size(self.key_size);

                    for (k, v) in new_page_entries {
                        new_leaf.insert_sorted(&k, v);
                    }

                    new_leaf.set_next_sibling(old_next_sibling_id);
                    new_leaf.set_prev_sibling(Some(current_page_id));

                    bpm.as_mut().mark_frame_dirty(new_frame_id);
                    bpm.as_mut().unpin_frame(new_frame_id)?;

                    new_offset
                };

                self.register_page(new_page_id, new_page_offset, bpm.as_mut(), page_id_counter)?;

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
                }

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
                }

                Ok(SplitData::Split {
                    key: split_key,
                    right_page_id: new_page_id,
                })
            }

            PageKind::BPlusInner => {
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

                let split_result =
                    self.insert_internal(child_page_id, key, value, bpm.as_mut(), page_id_counter)?;

                match split_result {
                    SplitData::NoSplit => Ok(SplitData::NoSplit),
                    SplitData::Split {
                        key: new_key,
                        right_page_id: new_child_page_id,
                    } => {
                        if !is_full {
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
                        };

                        let new_page_id =
                            PageId::new(page_id_counter.fetch_add(1, Ordering::SeqCst) + 1)
                                .unwrap();

                        let new_page_offset = {
                            let new_frame = bpm
                                .as_mut()
                                .alloc_new_page(PageKind::BPlusInner, new_page_id)?;
                            let new_frame_id = new_frame.fid();
                            let new_offset = new_frame.file_offset();

                            let mut new_page_view = new_frame.page_view();
                            let new_inner = match &mut new_page_view {
                                Page::BPlusInner(inner) => inner,
                                _ => return Err(BTreeError::InvalidPageType),
                            };

                            new_inner.init(new_page_id, level);
                            new_inner.set_key_size(self.key_size);
                            new_inner.set_child_at(0, new_page_children[0]);
                            let free_space = new_inner.free_space();
                            new_inner.set_free_space(free_space - 8);

                            for i in 0..new_page_keys.len() {
                                new_inner
                                    .insert_sorted(&new_page_keys[i], new_page_children[i + 1]);
                            }

                            new_inner.set_next_sibling(old_next_sibling_id);
                            new_inner.set_prev_sibling(Some(current_page_id));

                            bpm.as_mut().mark_frame_dirty(new_frame_id);
                            bpm.as_mut().unpin_frame(new_frame_id)?;

                            new_offset
                        };

                        self.register_page(
                            new_page_id,
                            new_page_offset,
                            bpm.as_mut(),
                            page_id_counter,
                        )?;

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
                        }

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
                        }

                        Ok(SplitData::Split {
                            key: key_to_push_up,
                            right_page_id: new_page_id,
                        })
                    }
                }
            }
            PageKind::Invalid => Err(BTreeError::InvalidPageType),
            _ => Err(BTreeError::InvalidPageType),
        }
    }

    fn delete_internal(
        &mut self,
        current_page_id: PageId,
        key: &[u8],
        mut bpm: Pin<&mut BufferPool>,
    ) -> Result<DeleteResult, BTreeError> {
        let frame = bpm.as_mut().fetch_page(current_page_id)?;
        let frame_id = frame.fid();
        let mut page_view = frame.page_view();

        match &mut page_view {
            Page::BPlusLeaf(leaf_page) => {
                let key_size = leaf_page.get_key_size() as usize;
                if key.len() != key_size {
                    bpm.as_mut().unpin_frame(frame_id)?;
                    return Err(BTreeError::SplitError("Key length mismatch".to_string()));
                }

                // Check if the key we are deleting is the first key
                let is_first_key = if let Some(first_key) = leaf_page.get_first_key() {
                    first_key == key
                } else {
                    false
                };

                // Try to remove the key
                if leaf_page.remove_key(key) {
                    let result = if is_first_key {
                        if let Some(new_first_key) = leaf_page.get_first_key() {
                            DeleteResult::DeletedAndPromote(new_first_key)
                        } else {
                            DeleteResult::Deleted // Page is empty
                        }
                    } else {
                        DeleteResult::Deleted
                    };

                    bpm.as_mut().mark_frame_dirty(frame_id);
                    bpm.as_mut().unpin_frame(frame_id)?;
                    Ok(result)
                } else {
                    // Key not found, just unpin.
                    bpm.as_mut().unpin_frame(frame_id)?;
                    Ok(DeleteResult::NotFound)
                }
            }
            Page::BPlusInner(inner_page) => {
                // 1. Find the correct child to descend into
                let child_index = inner_page.find_child_page_index(key);
                let child_page_id = inner_page
                    .get_child_at(child_index)
                    .ok_or(BTreeError::PageNotFound)?;

                // We unpin the parent *before* recursing to the child to avoid
                // deadlocking if the child needs to fetch a sibling
                bpm.as_mut().unpin_frame(frame_id)?;

                // 2. Recurse
                let delete_result = self.delete_internal(child_page_id, key, bpm.as_mut())?;

                // 3. Handle result: Check if we need to update a parent key
                match delete_result {
                    DeleteResult::DeletedAndPromote(new_key) => {
                        // The child's first key changed, so we must update
                        // the separator key in this node.

                        // The key to update is at child_index - 1
                        if child_index > 0 {
                            let key_index = child_index - 1;

                            // Re-fetch this inner page to modify it
                            let frame = bpm.as_mut().fetch_page(current_page_id)?;
                            let frame_id = frame.fid();
                            let mut page_view = frame.page_view();

                            if let Page::BPlusInner(inner_page) = &mut page_view {
                                // We replace the old key with the new promoted key
                                inner_page.set_entry(key_index, &new_key, child_page_id);
                                bpm.as_mut().mark_frame_dirty(frame_id);
                                bpm.as_mut().unpin_frame(frame_id)?;
                            } else {
                                bpm.as_mut().unpin_frame(frame_id)?;
                                return Err(BTreeError::InvalidPageType);
                            }
                        }

                        // This node's keys changed, but its *own* first key did not.
                        // So we just pass the result up.
                        Ok(DeleteResult::Deleted)
                    }
                    _ => {
                        // Child handled it, no changes to this node.
                        Ok(delete_result)
                    }
                }
            }
            _ => {
                bpm.as_mut().unpin_frame(frame_id)?;
                Err(BTreeError::InvalidPageType)
            }
        }
    }
}

#[cfg(test)]
pub mod bplus_tests {
    use super::*;
    use crate::constants;
    use crate::storage::bplus_tree::buffer_pool::errors::FlushFrameError;
    use crate::storage::buffer::fifo_evictor::FifoEvictor;
    use crate::storage::disk::FileManager;
    use crate::storage::page::base::Page;
    use crate::storage::page::base::{PageId, PageKind};
    use crate::storage::page_locator::locator::DirectoryPageLocator;
    use std::alloc::{Layout, alloc};
    use std::fs;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};
    const FRAME_COUNT: usize = 128;

    pub fn setup_buffer_pool_test(test_name: &str) -> (PathBuf, Pin<Box<BufferPool>>, AtomicU64) {
        let mut temp_dir = std::env::temp_dir();
        temp_dir.push(format!("nimbus_test_{}.db", test_name));
        let temp_file_path = temp_dir.clone();
        let temp_file_str = temp_file_path.to_str().expect("Invalid temp file path");

        let _ = fs::remove_file(&temp_file_path);

        let file_manager =
            FileManager::new(temp_file_str.to_string()).expect("Failed to create FileManager");

        let evictor = Box::new(FifoEvictor::new());
        let page_locator = Box::new(DirectoryPageLocator::new());

        let buffer_pool = Box::pin(BufferPool::new(file_manager, evictor, page_locator));

        let page_id_cnt = AtomicU64::new(0);
        (temp_file_path, buffer_pool, page_id_cnt)
    }

    fn setup_bplus_tree_test(
        test_name: &str,
        key_size: u32,
    ) -> (
        BplusTree,
        Pin<Box<BufferPool>>,
        std::path::PathBuf,
        std::sync::atomic::AtomicU64,
    ) {
        let (temp_path, mut buffer_pool, page_id_counter) = setup_buffer_pool_test(test_name);

        // Create directory page at offset 0
        let dir_page_id = PageId::new(page_id_counter.fetch_add(1, Ordering::SeqCst) + 1).unwrap();
        let dir_frame = buffer_pool
            .as_mut()
            .alloc_new_page(PageKind::Directory, dir_page_id)
            .expect("Failed to allocate directory page");

        let dir_frame_id = dir_frame.fid();
        let mut dir_page_view = dir_frame.page_view();
        if let Page::Directory(dir_page) = &mut dir_page_view {
            dir_page.set_page_id(dir_page_id);
        }
        buffer_pool.as_mut().mark_frame_dirty(dir_frame_id);
        buffer_pool.as_mut().unpin_frame(dir_frame_id).unwrap();

        // Create root page
        let root_page_id = PageId::new(page_id_counter.fetch_add(1, Ordering::SeqCst) + 1).unwrap();

        let mut root_frame = buffer_pool
            .as_mut()
            .alloc_new_page(PageKind::BPlusLeaf, root_page_id)
            .expect("Failed to allocate root page");

        let root_offset = root_frame.file_offset();
        let root_frame_id = root_frame.fid();
        let mut page_view = root_frame.page_view();
        let root_page = match &mut page_view {
            Page::BPlusLeaf(leaf) => leaf,
            _ => panic!("Root page was not a leaf page"),
        };

        root_page.init(root_page_id);
        root_page.set_key_size(key_size);

        buffer_pool.as_mut().mark_frame_dirty(root_frame_id);
        buffer_pool.as_mut().unpin_frame(root_frame_id).unwrap();

        // Register root page in directory
        buffer_pool
            .as_mut()
            .register_page_in_directory(root_page_id, root_offset, 0)
            .expect("Failed to register root page");

        let btree = BplusTree::new(root_page_id, key_size);

        (btree, buffer_pool, temp_path, page_id_counter)
    }

    pub fn generate_test_page_id(counter: &AtomicU64) -> PageId {
        let next_id = counter.fetch_add(1, Ordering::SeqCst) + 1;
        PageId::new(next_id).expect("Page ID counter overflowed in test")
    }

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
        let layout =
            Layout::from_size_align(constants::storage::PAGE_SIZE, constants::storage::PAGE_SIZE)
                .expect("Failed to create layout");
        let page_buf_disk_ptr = unsafe { alloc(layout) };
        if page_buf_disk_ptr.is_null() {
            panic!("Failed to allocate aligned memory for test");
        }

        let page_buf_disk =
            unsafe { &mut *(page_buf_disk_ptr as *mut [u8; constants::storage::PAGE_SIZE]) };

        {
            let mut fm_direct = FileManager::new(temp_path.to_str().unwrap().to_string()).unwrap();

            page::base::init_page_buf(page_buf_disk, PageKind::SlottedData);
            page_buf_disk[8..16].copy_from_slice(&page_id_on_disk.get().to_le_bytes());

            fm_direct
                .write_block_from(offset_on_disk, &page_buf_disk[..])
                .expect("Failed to write initial page directly to disk");
        }

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
            let fid = frame.fid();
            buffer_pool
                .as_mut()
                .unpin_frame(fid)
                .expect("Unpin failed in loop");
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
            let fid = frame.fid();
            allocated_offsets.push(frame.file_offset());
            buffer_pool
                .as_mut()
                .unpin_frame(fid)
                .expect("Unpin failed in loop");
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

    #[test]
    fn test_btree_simple_insert_and_find() {
        const KEY_SIZE: u32 = 8;
        let (mut btree, mut buffer_pool, temp_path, page_id_counter) =
            setup_bplus_tree_test("simple_insert_find", KEY_SIZE);

        let key = 100u64.to_le_bytes();
        let value = 12345u64;

        let insert_res = btree.insert(&key, value, buffer_pool.as_mut(), &page_id_counter);
        assert!(insert_res.is_ok(), "Insert failed: {:?}", insert_res.err());

        let find_res = btree.find(&key, buffer_pool.as_mut());
        assert!(find_res.is_ok(), "Find failed: {:?}", find_res.err());

        let found_val = find_res.unwrap();
        assert!(found_val.is_some(), "Key not found after insert");
        assert_eq!(found_val.unwrap(), value, "Value mismatch");

        let key_nonexist = 999u64.to_le_bytes();
        let find_res_none = btree.find(&key_nonexist, buffer_pool.as_mut());
        assert!(
            find_res_none.is_ok(),
            "Find (non-existent) failed: {:?}",
            find_res_none.err()
        );
        assert!(
            find_res_none.unwrap().is_none(),
            "Found a key that should not exist"
        );

        cleanup_temp_file(&temp_path);
    }

    #[test]
    fn test_btree_leaf_split() {
        const KEY_SIZE: u32 = 8;
        let (mut btree, mut buffer_pool, temp_path, page_id_counter) =
            setup_bplus_tree_test("leaf_split", KEY_SIZE);

        let max_keys = {
            let root_page_id = btree.get_root_page_id();
            let frame = buffer_pool.as_mut().fetch_page(root_page_id).unwrap();
            let mut page_view = frame.page_view();
            let max = match &mut page_view {
                Page::BPlusLeaf(leaf) => leaf.calculate_max_keys(),
                _ => panic!("Root is not leaf"),
            };
            let fr_id = frame.fid();
            buffer_pool.as_mut().unpin_frame(fr_id).unwrap();
            max as usize
        };

        let num_keys_to_insert = max_keys + 1;
        let mut keys = Vec::new();

        for i in 0..num_keys_to_insert {
            let key = (i as u64).to_le_bytes();
            let value = (i as u64) * 10;
            keys.push((key, value));

            let insert_res = btree.insert(&key, value, buffer_pool.as_mut(), &page_id_counter);
            assert!(
                insert_res.is_ok(),
                "Insert failed for key {}: {:?}",
                i,
                insert_res.err()
            );
        }

        let new_root_id = btree.get_root_page_id();
        assert_ne!(
            new_root_id.get(),
            2,
            "Root Page ID should have changed after a split"
        );

        for (key, value) in keys {
            let find_res = btree.find(&key, buffer_pool.as_mut());
            assert!(
                find_res.is_ok(),
                "Find failed for key {:?}: {:?}",
                key,
                find_res.err()
            );

            let found_val = find_res.unwrap();
            assert!(found_val.is_some(), "Key {:?} not found after split", key);
            assert_eq!(
                found_val.unwrap(),
                value,
                "Value mismatch for key {:?}",
                key
            );
        }

        cleanup_temp_file(&temp_path);
    }

    #[test]
    fn test_btree_delete_and_promote_key() {
        const KEY_SIZE: u32 = 8;
        let (mut btree, mut buffer_pool, temp_path, page_id_counter) =
            setup_bplus_tree_test("delete_and_promote", KEY_SIZE);

        // 1. Determine max keys to force a split
        let max_keys = {
            let root_page_id = btree.get_root_page_id();
            let frame = buffer_pool.as_mut().fetch_page(root_page_id).unwrap();
            let mut page_view = frame.page_view();
            let max = match &mut page_view {
                Page::BPlusLeaf(leaf) => leaf.calculate_max_keys(),
                _ => panic!("Root is not leaf"),
            };
            let fr_id = frame.fid();
            buffer_pool.as_mut().unpin_frame(fr_id).unwrap();
            max as usize
        };

        // 2. Insert keys to trigger a leaf split
        let num_keys_to_insert = max_keys + 1;
        for i in 0..num_keys_to_insert {
            let key = (i as u64).to_le_bytes();
            let value = (i as u64) * 10;
            btree
                .insert(&key, value, buffer_pool.as_mut(), &page_id_counter)
                .expect(&format!("Insert failed for key {}", i));
        }

        // After this, the tree has split.
        // The split point is `num_keys_to_insert / 2`.
        // e.g., if max_keys = 10, we insert 11 keys (0-10). Split point is 5.
        // Root (Inner): [Key(5)]
        // Left Leaf:  [0, 1, 2, 3, 4]
        // Right Leaf: [5, 6, 7, 8, 9, 10]
        let split_point = num_keys_to_insert / 2;
        let key_to_delete_val = split_point as u64; // This is the first key in the right leaf
        let new_promoted_key_val = (split_point + 1) as u64; // This will be the new first key
        let key_before_split_val = (split_point - 1) as u64; // A key in the left leaf

        let key_to_delete_bytes = key_to_delete_val.to_le_bytes();
        let new_promoted_key_bytes = new_promoted_key_val.to_le_bytes();
        let key_before_split_bytes = key_before_split_val.to_le_bytes();

        // 3. Delete the first key of the right leaf
        btree
            .delete(&key_to_delete_bytes, buffer_pool.as_mut())
            .expect("Delete failed");

        // 4. Verify the key is gone
        let find_res = btree
            .find(&key_to_delete_bytes, buffer_pool.as_mut())
            .unwrap();
        assert!(
            find_res.is_none(),
            "Deleted key {:?} was still found",
            key_to_delete_val
        );

        // 5. Verify other keys are still present
        let find_res_before = btree
            .find(&key_before_split_bytes, buffer_pool.as_mut())
            .unwrap();
        assert_eq!(
            find_res_before.unwrap(),
            key_before_split_val * 10,
            "Key before split was not found"
        );

        let find_res_after = btree
            .find(&new_promoted_key_bytes, buffer_pool.as_mut())
            .unwrap();
        assert_eq!(
            find_res_after.unwrap(),
            new_promoted_key_val * 10,
            "New first key was not found"
        );

        // 6. Verify the parent inner node's key was promoted and updated
        let root_id = btree.get_root_page_id();
        let root_frame = buffer_pool.as_mut().fetch_page(root_id).unwrap();
        let root_fid = root_frame.fid();
        let mut page_view = root_frame.page_view();

        if let Page::BPlusInner(inner_page) = &mut page_view {
            assert_eq!(
                inner_page.curr_vec_sz(),
                1,
                "Root should still have 1 separator key"
            );
            let separator_key = inner_page.get_key_at(0);
            assert_eq!(
                separator_key,
                &new_promoted_key_bytes[..],
                "Parent separator key was not correctly promoted"
            );
        } else {
            panic!("Root page was not an BPlusInner page after split");
        }
        buffer_pool.as_mut().unpin_frame(root_fid).unwrap();

        cleanup_temp_file(&temp_path);
    }

    #[test]
    fn test_btree_duplicate_insert() {
        const KEY_SIZE: u32 = 8;
        let (mut btree, mut buffer_pool, temp_path, page_id_counter) =
            setup_bplus_tree_test("duplicate_insert", KEY_SIZE);

        let key = 100u64.to_le_bytes();
        let value1 = 12345u64;
        let value2 = 67890u64;

        let insert_res1 = btree.insert(&key, value1, buffer_pool.as_mut(), &page_id_counter);
        assert!(
            insert_res1.is_ok(),
            "Insert 1 failed: {:?}",
            insert_res1.err()
        );

        let find_res1 = btree.find(&key, buffer_pool.as_mut()).unwrap().unwrap();
        assert_eq!(find_res1, value1);

        let insert_res2 = btree.insert(&key, value2, buffer_pool.as_mut(), &page_id_counter);
        assert!(
            insert_res2.is_ok(),
            "Insert 2 failed: {:?}",
            insert_res2.err()
        );

        let find_res2 = btree.find(&key, buffer_pool.as_mut()).unwrap().unwrap();
        assert_eq!(
            find_res2, value2,
            "Value was not updated on duplicate insert"
        );

        cleanup_temp_file(&temp_path);
    }

    fn test_btree_inner_page_split() {
        const KEY_SIZE: u32 = 8;
        let (mut btree, mut buffer_pool, temp_path, page_id_counter) =
            setup_bplus_tree_test("inner_split", KEY_SIZE);

        let (max_leaf_keys, max_inner_keys) = {
            let root_page_id = btree.get_root_page_id();
            let frame = buffer_pool.as_mut().fetch_page(root_page_id).unwrap();
            let mut page_view = frame.page_view();
            let leaf_max = match &mut page_view {
                Page::BPlusLeaf(leaf) => leaf.calculate_max_keys(),
                _ => panic!("Root is not leaf"),
            } as usize;

            let mut dummy_buf = [0u8; constants::storage::PAGE_SIZE];
            let mut inner_page = crate::storage::page::bplus_inner::BPlusInner::new(&mut dummy_buf);
            inner_page.set_key_size(KEY_SIZE);
            let inner_max = inner_page.calculate_max_keys() as usize;

            let fr_id = frame.fid();
            buffer_pool.as_mut().unpin_frame(fr_id).unwrap();
            (leaf_max, inner_max)
        };

        let keys_per_leaf_split = (max_leaf_keys / 2) + 1;
        let num_leaf_splits_needed = max_inner_keys + 1;
        let num_keys_to_insert = keys_per_leaf_split * num_leaf_splits_needed + 1;

        println!(
            "Test config: max_leaf_keys={}, max_inner_keys={}",
            max_leaf_keys, max_inner_keys
        );
        println!(
            "Inserting {} keys to trigger inner page split...",
            num_keys_to_insert
        );

        let mut keys = Vec::new();

        for i in 0..num_keys_to_insert {
            let key = (i as u64).to_le_bytes();
            let value = (i as u64) * 10;
            keys.push((key, value));

            let insert_res = btree.insert(&key, value, buffer_pool.as_mut(), &page_id_counter);
            if insert_res.is_err() {
                panic!("Insert failed for key {}: {:?}", i, insert_res.err());
            }
        }

        let root_id = btree.get_root_page_id();
        let root_frame = buffer_pool.as_mut().fetch_page(root_id).unwrap();
        let root_height = match root_frame.page_view() {
            Page::BPlusInner(inner) => inner.page_level(),
            _ => panic!("Root page is not an inner page after inner split"),
        };
        let fr_id = root_frame.fid();
        buffer_pool.as_mut().unpin_frame(fr_id).unwrap();

        assert_eq!(
            root_height, 2,
            "Tree height should be 2 after inner page split"
        );

        println!("Verifying {} keys...", keys.len());
        for (key, value) in keys {
            let find_res = btree.find(&key, buffer_pool.as_mut());
            let found_val = find_res.unwrap();
            assert!(
                found_val.is_some(),
                "Key {:?} not found after inner split",
                key
            );
            assert_eq!(
                found_val.unwrap(),
                value,
                "Value mismatch for key {:?}",
                key
            );
        }

        cleanup_temp_file(&temp_path);
    }

    #[test]
    fn test_btree_insert_sequential() {
        const KEY_SIZE: u32 = 8;
        let (mut btree, mut buffer_pool, temp_path, page_id_counter) =
            setup_bplus_tree_test("insert_sequential", KEY_SIZE);

        let num_keys = 1000;
        let mut keys = Vec::new();

        for i in 0..num_keys {
            let key = (i as u64).to_le_bytes();
            let value = (i as u64) * 10;
            keys.push((key, value));
            btree
                .insert(&key, value, buffer_pool.as_mut(), &page_id_counter)
                .unwrap();
        }

        for (key, value) in keys {
            let find_res = btree.find(&key, buffer_pool.as_mut()).unwrap();
            assert!(find_res.is_some(), "Key not found: {:?}", key);
            assert_eq!(find_res.unwrap(), value, "Value mismatch: {:?}", key);
        }

        cleanup_temp_file(&temp_path);
    }
}
