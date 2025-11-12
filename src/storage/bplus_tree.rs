use crate::storage::bplus_tree;
use crate::storage::buffer::Frame;
use crate::storage::buffer::buffer_pool::{self, BufferPool, errors::FetchPageError};
use crate::storage::page::{
    base::{Page, PageId, PageKind},
    bplus_inner::{BPlusInner, BPlusInnerSplitData},
    bplus_leaf::BPlusLeaf,
};
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
    /// Key was not found.
    NotFound,
    /// Key was deleted, no rebalancing needed.
    Deleted,
    /// Key was deleted, and the first key in the node was updated.
    /// The parent must update its separator key.
    DeletedAndPromoted(Vec<u8>),
    /// Key was deleted, and the node is now underflowed.
    /// The parent must fix this by borrowing or merging.
    DeletedAndUnderflow,

    /// Key was deleted, was the first key, AND the node is underflowed.
    /// Parent must fix this AND update its separator key.
    DeletedAndUnderflowAndPromote(Vec<u8>),
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

                            new_inner.populate_entries(&new_page_keys, &new_page_children[1..]);

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

    /// Public API for deleting a key.
    pub fn delete(
        &mut self,
        key: &[u8],
        mut bpm: Pin<&mut BufferPool>,
    ) -> Result<bool, BTreeError> {
        let result = self.delete_internal(self.root_page_id, key, bpm.as_mut())?;

        match result {
            DeleteResult::NotFound => Ok(false),
            _ => Ok(true),
        }
        // Note: After deletion, the root node might become empty and contain only
        // one child. A full implementation would then "pop" the root and make
        // its single child the new root, shortening the tree.
        // We are skipping that step for simplicity.
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

                let is_first_key = if let Some(first_key) = leaf_page.get_first_key() {
                    first_key == key
                } else {
                    false
                };

                if !leaf_page.remove_key(key) {
                    // Key not found
                    bpm.as_mut().unpin_frame(frame_id)?;
                    return Ok(DeleteResult::NotFound);
                }

                // 1. All logic using `leaf_page` must happen first.
                let is_underflow = leaf_page.is_underflow();
                let new_first_key = leaf_page.get_first_key();

                let result = match (is_first_key, is_underflow, new_first_key) {
                    (true, true, Some(new_key)) => {
                        DeleteResult::DeletedAndUnderflowAndPromote(new_key)
                    }
                    (true, true, None) => DeleteResult::DeletedAndUnderflow, // Page is now empty
                    (true, false, Some(new_key)) => DeleteResult::DeletedAndPromoted(new_key),
                    (true, false, None) => DeleteResult::Deleted, // Should not happen if not underflow
                    (false, true, _) => DeleteResult::DeletedAndUnderflow,
                    (false, false, _) => DeleteResult::Deleted,
                };

                // 2. Now that `leaf_page` is no longer used, we can
                //    make new mutable calls to `bpm`.
                bpm.as_mut().mark_frame_dirty(frame_id);
                bpm.as_mut().unpin_frame(frame_id)?;
                Ok(result)
            }
            Page::BPlusInner(inner_page) => {
                // 1. Find the child to descend into
                let child_index = inner_page.find_child_page_index(key);
                let child_page_id = inner_page
                    .get_child_at(child_index)
                    .ok_or(BTreeError::PageNotFound)?;

                // 2. Unpin parent before recursing
                bpm.as_mut().unpin_frame(frame_id)?;

                // 3. Recurse
                let delete_result = self.delete_internal(child_page_id, key, bpm.as_mut())?;

                // 4. Handle result from child

                // A. Child's first key was promoted. Update this node's key.
                let promote_key = match &delete_result {
                    DeleteResult::DeletedAndPromoted(new_key) => Some(new_key.clone()),
                    DeleteResult::DeletedAndUnderflowAndPromote(new_key) => Some(new_key.clone()),
                    _ => None,
                };

                if let Some(new_key) = promote_key {
                    if child_index > 0 {
                        let key_index = child_index - 1;
                        // Re-fetch parent to update it
                        let frame = bpm.as_mut().fetch_page(current_page_id)?;
                        let frame_id = frame.fid();
                        let mut page_view = frame.page_view();
                        if let Page::BPlusInner(parent_page) = &mut page_view {
                            let child_for_entry = parent_page
                                .get_child_at(key_index + 1)
                                .ok_or(BTreeError::PageNotFound)?;
                            parent_page.set_entry(key_index, &new_key, child_for_entry);
                            bpm.as_mut().mark_frame_dirty(frame_id);
                            bpm.as_mut().unpin_frame(frame_id)?;
                        } else {
                            bpm.as_mut().unpin_frame(frame_id)?;
                            return Err(BTreeError::InvalidPageType);
                        }
                    }
                }

                // B. Child is underflowed
                if matches!(
                    delete_result,
                    DeleteResult::DeletedAndUnderflow
                        | DeleteResult::DeletedAndUnderflowAndPromote(_)
                ) {
                    // 1. Fetch parent and get ALL info needed
                    let (
                        left_sibling_page_id,
                        right_sibling_page_id,
                        parent_sep_key_left,
                        parent_sep_key_right,
                        parent_is_root,
                    ) = {
                        let parent_frame = bpm.as_mut().fetch_page(current_page_id)?;
                        let parent_frame_id = parent_frame.fid();
                        let mut parent_page_view = parent_frame.page_view();
                        let parent_page = match &mut parent_page_view {
                            Page::BPlusInner(page) => page,
                            _ => return Err(BTreeError::InvalidPageType),
                        };

                        let left_sib_id = if child_index > 0 {
                            parent_page.get_child_at(child_index - 1)
                        } else {
                            None
                        };

                        let right_sib_id = if child_index < parent_page.curr_vec_sz() as usize {
                            parent_page.get_child_at(child_index + 1)
                        } else {
                            None
                        };

                        let sep_key_left = if child_index > 0 {
                            Some(parent_page.get_key_at(child_index - 1).to_vec())
                        } else {
                            None
                        };

                        let sep_key_right = if child_index < parent_page.curr_vec_sz() as usize {
                            Some(parent_page.get_key_at(child_index).to_vec())
                        } else {
                            None
                        };

                        let is_root = parent_page.page_id() == self.root_page_id;

                        // 2. Unpin parent *before* fetching siblings
                        bpm.as_mut().unpin_frame(parent_frame_id)?;

                        (
                            left_sib_id,
                            right_sib_id,
                            sep_key_left,
                            sep_key_right,
                            is_root,
                        )
                    };
                    // --- "Long borrow" on parent_frame is now over ---

                    // --- 3. Try to borrow from left sibling ---
                    if let Some(left_sibling_page_id) = left_sibling_page_id {
                        let can_borrow = {
                            let left_frame = bpm.as_mut().fetch_page(left_sibling_page_id)?;
                            let left_frame_id = left_frame.fid();
                            let mut left_page_view = left_frame.page_view();
                            let can_give = match &mut left_page_view {
                                Page::BPlusLeaf(left_leaf) => left_leaf.can_give_key(),
                                Page::BPlusInner(left_inner) => left_inner.can_give_key(),
                                _ => return Err(BTreeError::InvalidPageType),
                            };
                            bpm.as_mut().unpin_frame(left_frame_id)?;
                            can_give
                        };

                        if can_borrow {
                            // Perform the redistribution based on page type
                            let is_leaf = {
                                let child_frame = bpm.as_mut().fetch_page(child_page_id)?;
                                let child_fid = child_frame.fid();
                                let child_view = child_frame.page_view();
                                let is_leaf = matches!(&child_view, Page::BPlusLeaf(_));
                                bpm.as_mut().unpin_frame(child_fid)?;
                                is_leaf
                            };

                            if is_leaf {
                                let new_separator_key = unsafe {
                                    let left_frame_ptr =
                                        bpm.as_mut().fetch_page(left_sibling_page_id)?
                                            as *mut Frame;
                                    let child_frame_ptr =
                                        bpm.as_mut().fetch_page(child_page_id)? as *mut Frame;

                                    let left_frame = &mut *left_frame_ptr;
                                    let child_frame = &mut *child_frame_ptr;

                                    let left_fid = left_frame.fid();
                                    let child_fid = child_frame.fid();

                                    let mut left_view = left_frame.page_view();
                                    let mut child_view = child_frame.page_view();

                                    let new_key = if let (
                                        Page::BPlusLeaf(left_leaf),
                                        Page::BPlusLeaf(child_leaf),
                                    ) = (&mut left_view, &mut child_view)
                                    {
                                        // Move last key from left sibling to beginning of child
                                        // Returns: child's new first key (which becomes the separator)
                                        left_leaf.move_last_to_beginning_of(child_leaf)
                                    } else {
                                        return Err(BTreeError::InvalidPageType);
                                    };

                                    bpm.as_mut().mark_frame_dirty(left_fid);
                                    bpm.as_mut().mark_frame_dirty(child_fid);
                                    bpm.as_mut().unpin_frame(left_fid)?;
                                    bpm.as_mut().unpin_frame(child_fid)?;

                                    new_key
                                };

                                // Update parent separator between left sibling and child
                                let separator_index = child_index - 1;
                                let p_frame = bpm.as_mut().fetch_page(current_page_id)?;
                                let p_fid = p_frame.fid();
                                let mut p_view = p_frame.page_view();

                                if let Page::BPlusInner(p_page) = &mut p_view {
                                    let child_for_entry = p_page.get_child_at(child_index).unwrap();
                                    p_page.set_entry(
                                        separator_index,
                                        &new_separator_key,
                                        child_for_entry,
                                    );
                                    bpm.as_mut().mark_frame_dirty(p_fid);
                                }
                                bpm.as_mut().unpin_frame(p_fid)?;
                            } else {
                                // Inner redistribution
                                let new_separator_key = unsafe {
                                    let left_frame_ptr =
                                        bpm.as_mut().fetch_page(left_sibling_page_id)?
                                            as *mut Frame;
                                    let child_frame_ptr =
                                        bpm.as_mut().fetch_page(child_page_id)? as *mut Frame;

                                    let left_frame = &mut *left_frame_ptr;
                                    let child_frame = &mut *child_frame_ptr;

                                    let left_fid = left_frame.fid();
                                    let child_fid = child_frame.fid();

                                    let mut left_view = left_frame.page_view();
                                    let mut child_view = child_frame.page_view();

                                    let new_key = if let (
                                        Page::BPlusInner(left_inner),
                                        Page::BPlusInner(child_inner),
                                    ) = (&mut left_view, &mut child_view)
                                    {
                                        left_inner.move_last_to_beginning_of(
                                            child_inner,
                                            &parent_sep_key_left.unwrap(),
                                        )
                                    } else {
                                        return Err(BTreeError::InvalidPageType);
                                    };

                                    bpm.as_mut().mark_frame_dirty(left_fid);
                                    bpm.as_mut().mark_frame_dirty(child_fid);
                                    bpm.as_mut().unpin_frame(left_fid)?;
                                    bpm.as_mut().unpin_frame(child_fid)?;

                                    new_key
                                };

                                // Update parent separator
                                let p_frame = bpm.as_mut().fetch_page(current_page_id)?;
                                let p_fid = p_frame.fid();
                                let mut p_view = p_frame.page_view();

                                if let Page::BPlusInner(p_page) = &mut p_view {
                                    let child_for_entry = p_page.get_child_at(child_index).unwrap();
                                    p_page.set_entry(
                                        child_index - 1,
                                        &new_separator_key,
                                        child_for_entry,
                                    );
                                    bpm.as_mut().mark_frame_dirty(p_fid);
                                }
                                bpm.as_mut().unpin_frame(p_fid)?;
                            }

                            return Ok(DeleteResult::Deleted); // Rebalanced
                        }
                    }

                    // --- 4. Try to borrow from right sibling ---
                    if let Some(right_sibling_page_id) = right_sibling_page_id {
                        let can_borrow = {
                            let right_frame = bpm.as_mut().fetch_page(right_sibling_page_id)?;
                            let right_frame_id = right_frame.fid();
                            let mut right_page_view = right_frame.page_view();
                            let can_give = match &mut right_page_view {
                                Page::BPlusLeaf(right_leaf) => right_leaf.can_give_key(),
                                Page::BPlusInner(right_inner) => right_inner.can_give_key(),
                                _ => return Err(BTreeError::InvalidPageType),
                            };
                            bpm.as_mut().unpin_frame(right_frame_id)?;
                            can_give
                        };

                        if can_borrow {
                            // Perform the redistribution based on page type
                            let is_leaf = {
                                let child_frame = bpm.as_mut().fetch_page(child_page_id)?;
                                let child_fid = child_frame.fid();
                                let child_view = child_frame.page_view();
                                let is_leaf = matches!(&child_view, Page::BPlusLeaf(_));
                                bpm.as_mut().unpin_frame(child_fid)?;
                                is_leaf
                            };

                            if is_leaf {
                                // Leaf redistribution
                                let new_separator_key = unsafe {
                                    let right_frame_ptr =
                                        bpm.as_mut().fetch_page(right_sibling_page_id)?
                                            as *mut Frame;
                                    let child_frame_ptr =
                                        bpm.as_mut().fetch_page(child_page_id)? as *mut Frame;

                                    let right_frame = &mut *right_frame_ptr;
                                    let child_frame = &mut *child_frame_ptr;

                                    let right_fid = right_frame.fid();
                                    let child_fid = child_frame.fid();

                                    let mut right_view = right_frame.page_view();
                                    let mut child_view = child_frame.page_view();

                                    let new_key = if let (
                                        Page::BPlusLeaf(right_leaf),
                                        Page::BPlusLeaf(child_leaf),
                                    ) = (&mut right_view, &mut child_view)
                                    {
                                        right_leaf.move_first_to_end_of(child_leaf)
                                    } else {
                                        return Err(BTreeError::InvalidPageType);
                                    };

                                    bpm.as_mut().mark_frame_dirty(right_fid);
                                    bpm.as_mut().mark_frame_dirty(child_fid);
                                    bpm.as_mut().unpin_frame(right_fid)?;
                                    bpm.as_mut().unpin_frame(child_fid)?;

                                    new_key
                                };

                                // Update parent separator
                                let p_frame = bpm.as_mut().fetch_page(current_page_id)?;
                                let p_fid = p_frame.fid();
                                let mut p_view = p_frame.page_view();

                                if let Page::BPlusInner(p_page) = &mut p_view {
                                    let child_for_entry =
                                        p_page.get_child_at(child_index + 1).unwrap();
                                    p_page.set_entry(
                                        child_index,
                                        &new_separator_key,
                                        child_for_entry,
                                    );
                                    bpm.as_mut().mark_frame_dirty(p_fid);
                                }
                                bpm.as_mut().unpin_frame(p_fid)?;
                            } else {
                                // Inner redistribution
                                let new_separator_key = unsafe {
                                    let right_frame_ptr =
                                        bpm.as_mut().fetch_page(right_sibling_page_id)?
                                            as *mut Frame;
                                    let child_frame_ptr =
                                        bpm.as_mut().fetch_page(child_page_id)? as *mut Frame;

                                    let right_frame = &mut *right_frame_ptr;
                                    let child_frame = &mut *child_frame_ptr;

                                    let right_fid = right_frame.fid();
                                    let child_fid = child_frame.fid();

                                    let mut right_view = right_frame.page_view();
                                    let mut child_view = child_frame.page_view();

                                    let new_key = if let (
                                        Page::BPlusInner(right_inner),
                                        Page::BPlusInner(child_inner),
                                    ) = (&mut right_view, &mut child_view)
                                    {
                                        right_inner.move_first_to_end_of(
                                            child_inner,
                                            &parent_sep_key_right.unwrap(),
                                        )
                                    } else {
                                        return Err(BTreeError::InvalidPageType);
                                    };

                                    bpm.as_mut().mark_frame_dirty(right_fid);
                                    bpm.as_mut().mark_frame_dirty(child_fid);
                                    bpm.as_mut().unpin_frame(right_fid)?;
                                    bpm.as_mut().unpin_frame(child_fid)?;

                                    new_key
                                };

                                // Update parent separator
                                let p_frame = bpm.as_mut().fetch_page(current_page_id)?;
                                let p_fid = p_frame.fid();
                                let mut p_view = p_frame.page_view();

                                if let Page::BPlusInner(p_page) = &mut p_view {
                                    let child_for_entry =
                                        p_page.get_child_at(child_index + 1).unwrap();
                                    p_page.set_entry(
                                        child_index,
                                        &new_separator_key,
                                        child_for_entry,
                                    );
                                    bpm.as_mut().mark_frame_dirty(p_fid);
                                }
                                bpm.as_mut().unpin_frame(p_fid)?;
                            }

                            return Ok(DeleteResult::Deleted);
                        }
                    }

                    // --- 5. If we are here, we MUST merge ---

                    // Re-fetch parent to perform the merge
                    let parent_frame = bpm.as_mut().fetch_page(current_page_id)?;
                    let parent_frame_id = parent_frame.fid();
                    let mut parent_page_view = parent_frame.page_view();

                    // We must hold the parent lock while merging
                    if let Page::BPlusInner(parent_page) = &mut parent_page_view {
                        if child_index > 0 {
                            // Merge child into left sibling
                            let left_sibling_page_id = left_sibling_page_id.unwrap();
                            let separator_key_index = child_index - 1;
                            let separator_key =
                                parent_page.get_key_at(separator_key_index).to_vec();

                            // Drop parent borrow before fetching children
                            drop(parent_page_view);
                            bpm.as_mut().unpin_frame(parent_frame_id)?;

                            // Determine page type
                            let is_leaf = {
                                let child_frame = bpm.as_mut().fetch_page(child_page_id)?;
                                let child_fid = child_frame.fid();
                                let child_view = child_frame.page_view();
                                let is_leaf = matches!(&child_view, Page::BPlusLeaf(_));
                                bpm.as_mut().unpin_frame(child_fid)?;
                                is_leaf
                            };

                            if is_leaf {
                                // Leaf merge
                                let next_sib_id = unsafe {
                                    let left_frame_ptr =
                                        bpm.as_mut().fetch_page(left_sibling_page_id)?
                                            as *mut Frame;
                                    let child_frame_ptr =
                                        bpm.as_mut().fetch_page(child_page_id)? as *mut Frame;

                                    let left_frame = &mut *left_frame_ptr;
                                    let child_frame = &mut *child_frame_ptr;

                                    let left_fid = left_frame.fid();
                                    let child_fid = child_frame.fid();

                                    let mut left_view = left_frame.page_view();
                                    let mut child_view = child_frame.page_view();

                                    let next_id = if let (
                                        Page::BPlusLeaf(left_leaf),
                                        Page::BPlusLeaf(child_leaf),
                                    ) = (&mut left_view, &mut child_view)
                                    {
                                        left_leaf.merge_from(child_leaf);
                                        left_leaf.next_sibling()
                                    } else {
                                        return Err(BTreeError::InvalidPageType);
                                    };

                                    bpm.as_mut().mark_frame_dirty(left_fid);
                                    bpm.as_mut().mark_frame_dirty(child_fid);
                                    bpm.as_mut().unpin_frame(left_fid)?;
                                    bpm.as_mut().unpin_frame(child_fid)?;

                                    next_id
                                };

                                if let Some(next_sib_id) = next_sib_id {
                                    let mut next_sib_frame =
                                        bpm.as_mut().fetch_page(next_sib_id)?;
                                    let next_fid = next_sib_frame.fid();
                                    let mut next_view = next_sib_frame.page_view();

                                    if let Page::BPlusLeaf(next_leaf) = &mut next_view {
                                        next_leaf.set_prev_sibling(Some(left_sibling_page_id));
                                        bpm.as_mut().mark_frame_dirty(next_fid);
                                    }
                                    bpm.as_mut().unpin_frame(next_fid)?;
                                }
                            } else {
                                // Inner merge
                                let next_sib_id = unsafe {
                                    let left_frame_ptr =
                                        bpm.as_mut().fetch_page(left_sibling_page_id)?
                                            as *mut Frame;
                                    let child_frame_ptr =
                                        bpm.as_mut().fetch_page(child_page_id)? as *mut Frame;

                                    let left_frame = &mut *left_frame_ptr;
                                    let child_frame = &mut *child_frame_ptr;

                                    let left_fid = left_frame.fid();
                                    let child_fid = child_frame.fid();

                                    let mut left_view = left_frame.page_view();
                                    let mut child_view = child_frame.page_view();

                                    let next_id = if let (
                                        Page::BPlusInner(left_inner),
                                        Page::BPlusInner(child_inner),
                                    ) = (&mut left_view, &mut child_view)
                                    {
                                        left_inner.merge_from(child_inner, &separator_key);
                                        left_inner.next_sibling()
                                    } else {
                                        return Err(BTreeError::InvalidPageType);
                                    };

                                    bpm.as_mut().mark_frame_dirty(left_fid);
                                    bpm.as_mut().mark_frame_dirty(child_fid);
                                    bpm.as_mut().unpin_frame(left_fid)?;
                                    bpm.as_mut().unpin_frame(child_fid)?;

                                    next_id
                                };

                                if let Some(next_sib_id) = next_sib_id {
                                    let mut next_sib_frame =
                                        bpm.as_mut().fetch_page(next_sib_id)?;
                                    let next_fid = next_sib_frame.fid();
                                    let mut next_view = next_sib_frame.page_view();

                                    if let Page::BPlusInner(next_inner) = &mut next_view {
                                        next_inner.set_prev_sibling(Some(left_sibling_page_id));
                                        bpm.as_mut().mark_frame_dirty(next_fid);
                                    }
                                    bpm.as_mut().unpin_frame(next_fid)?;
                                }
                            }

                            // Re-fetch parent to remove entry
                            let parent_frame = bpm.as_mut().fetch_page(current_page_id)?;
                            let parent_frame_id = parent_frame.fid();
                            let mut parent_page_view = parent_frame.page_view();

                            if let Page::BPlusInner(parent_page) = &mut parent_page_view {
                                parent_page.remove_entry_at(separator_key_index);
                                let parent_underflow =
                                    parent_page.is_underflow() && !parent_is_root;

                                bpm.as_mut().mark_frame_dirty(parent_frame_id);
                                bpm.as_mut().unpin_frame(parent_frame_id)?;

                                if parent_underflow {
                                    return Ok(DeleteResult::DeletedAndUnderflow);
                                }
                            } else {
                                bpm.as_mut().unpin_frame(parent_frame_id)?;
                                return Err(BTreeError::InvalidPageType);
                            }
                        } else {
                            // Merge right sibling into child
                            let right_sibling_page_id = right_sibling_page_id.unwrap();
                            let separator_key_index = child_index;
                            let separator_key =
                                parent_page.get_key_at(separator_key_index).to_vec();

                            // Drop parent borrow before fetching children
                            drop(parent_page_view);
                            bpm.as_mut().unpin_frame(parent_frame_id)?;

                            // Determine page type
                            let is_leaf = {
                                let child_frame = bpm.as_mut().fetch_page(child_page_id)?;
                                let child_fid = child_frame.fid();
                                let child_view = child_frame.page_view();
                                let is_leaf = matches!(&child_view, Page::BPlusLeaf(_));
                                bpm.as_mut().unpin_frame(child_fid)?;
                                is_leaf
                            };

                            if is_leaf {
                                // Leaf merge
                                let next_sib_id = unsafe {
                                    let child_frame_ptr =
                                        bpm.as_mut().fetch_page(child_page_id)? as *mut Frame;
                                    let right_frame_ptr =
                                        bpm.as_mut().fetch_page(right_sibling_page_id)?
                                            as *mut Frame;

                                    let child_frame = &mut *child_frame_ptr;
                                    let right_frame = &mut *right_frame_ptr;

                                    let child_fid = child_frame.fid();
                                    let right_fid = right_frame.fid();

                                    let mut child_view = child_frame.page_view();
                                    let mut right_view = right_frame.page_view();

                                    let next_id = if let (
                                        Page::BPlusLeaf(child_leaf),
                                        Page::BPlusLeaf(right_leaf),
                                    ) = (&mut child_view, &mut right_view)
                                    {
                                        child_leaf.merge_from(right_leaf);
                                        child_leaf.next_sibling()
                                    } else {
                                        return Err(BTreeError::InvalidPageType);
                                    };

                                    bpm.as_mut().mark_frame_dirty(child_fid);
                                    bpm.as_mut().mark_frame_dirty(right_fid);
                                    bpm.as_mut().unpin_frame(child_fid)?;
                                    bpm.as_mut().unpin_frame(right_fid)?;

                                    next_id
                                };

                                if let Some(next_sib_id) = next_sib_id {
                                    let mut next_sib_frame =
                                        bpm.as_mut().fetch_page(next_sib_id)?;
                                    let next_fid = next_sib_frame.fid();
                                    let mut next_view = next_sib_frame.page_view();

                                    if let Page::BPlusLeaf(next_leaf) = &mut next_view {
                                        next_leaf.set_prev_sibling(Some(child_page_id));
                                        bpm.as_mut().mark_frame_dirty(next_fid);
                                    }
                                    bpm.as_mut().unpin_frame(next_fid)?;
                                }
                            } else {
                                // Inner merge
                                let next_sib_id = unsafe {
                                    let child_frame_ptr =
                                        bpm.as_mut().fetch_page(child_page_id)? as *mut Frame;
                                    let right_frame_ptr =
                                        bpm.as_mut().fetch_page(right_sibling_page_id)?
                                            as *mut Frame;

                                    let child_frame = &mut *child_frame_ptr;
                                    let right_frame = &mut *right_frame_ptr;

                                    let child_fid = child_frame.fid();
                                    let right_fid = right_frame.fid();

                                    let mut child_view = child_frame.page_view();
                                    let mut right_view = right_frame.page_view();

                                    let next_id = if let (
                                        Page::BPlusInner(child_inner),
                                        Page::BPlusInner(right_inner),
                                    ) = (&mut child_view, &mut right_view)
                                    {
                                        child_inner.merge_from(right_inner, &separator_key);
                                        child_inner.next_sibling()
                                    } else {
                                        return Err(BTreeError::InvalidPageType);
                                    };

                                    bpm.as_mut().mark_frame_dirty(child_fid);
                                    bpm.as_mut().mark_frame_dirty(right_fid);
                                    bpm.as_mut().unpin_frame(child_fid)?;
                                    bpm.as_mut().unpin_frame(right_fid)?;

                                    next_id
                                };

                                if let Some(next_sib_id) = next_sib_id {
                                    let mut next_sib_frame =
                                        bpm.as_mut().fetch_page(next_sib_id)?;
                                    let next_fid = next_sib_frame.fid();
                                    let mut next_view = next_sib_frame.page_view();

                                    if let Page::BPlusInner(next_inner) = &mut next_view {
                                        next_inner.set_prev_sibling(Some(child_page_id));
                                        bpm.as_mut().mark_frame_dirty(next_fid);
                                    }
                                    bpm.as_mut().unpin_frame(next_fid)?;
                                }
                            }

                            // Re-fetch parent to remove entry
                            let parent_frame = bpm.as_mut().fetch_page(current_page_id)?;
                            let parent_frame_id = parent_frame.fid();
                            let mut parent_page_view = parent_frame.page_view();

                            if let Page::BPlusInner(parent_page) = &mut parent_page_view {
                                parent_page.remove_entry_at(separator_key_index);
                                let parent_underflow =
                                    parent_page.is_underflow() && !parent_is_root;

                                bpm.as_mut().mark_frame_dirty(parent_frame_id);
                                bpm.as_mut().unpin_frame(parent_frame_id)?;

                                if parent_underflow {
                                    return Ok(DeleteResult::DeletedAndUnderflow);
                                }
                            } else {
                                bpm.as_mut().unpin_frame(parent_frame_id)?;
                                return Err(BTreeError::InvalidPageType);
                            }
                        }

                        return Ok(DeleteResult::Deleted);
                    } else {
                        // This was the last branch, but we need to unpin
                        bpm.as_mut().unpin_frame(parent_frame_id)?;
                        return Err(BTreeError::InvalidPageType);
                    }
                }

                // No underflow, or it was handled.
                Ok(DeleteResult::Deleted)
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
    use crate::storage::page::base::{self, Page, PageId, PageKind, page_kind_from_buf}; // Import page_kind_from_buf
    use crate::storage::page_locator::locator::DirectoryPageLocator; // Fixed import
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

            base::init_page_buf(page_buf_disk, PageKind::SlottedData);
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

    /// Helper function to get max leaf keys
    fn get_max_leaf_keys(mut bpm: Pin<&mut BufferPool>, btree: &BplusTree) -> usize {
        let root_page_id = btree.get_root_page_id();
        let frame = bpm.as_mut().fetch_page(root_page_id).unwrap();
        let mut page_view = frame.page_view();
        let max = match &mut page_view {
            Page::BPlusLeaf(leaf) => leaf.calculate_max_keys(),
            _ => panic!("Root is not leaf"),
        };
        let fr_id = frame.fid();
        bpm.as_mut().unpin_frame(fr_id).unwrap();
        max as usize
    }

    #[test]
    fn test_btree_delete_non_existent_key() {
        const KEY_SIZE: u32 = 8;
        let (mut btree, mut buffer_pool, temp_path, page_id_counter) =
            setup_bplus_tree_test("delete_non_existent", KEY_SIZE);

        let key1 = 10u64.to_le_bytes();
        let val1 = 100u64;
        let key2 = 20u64.to_le_bytes();
        let val2 = 200u64;
        let key_nonexist = 15u64.to_le_bytes();

        btree
            .insert(&key1, val1, buffer_pool.as_mut(), &page_id_counter)
            .unwrap();
        btree
            .insert(&key2, val2, buffer_pool.as_mut(), &page_id_counter)
            .unwrap();

        let delete_res = btree.delete(&key_nonexist, buffer_pool.as_mut());
        assert!(delete_res.is_ok(), "Delete failed");
        assert_eq!(
            delete_res.unwrap(),
            false,
            "Delete should return false for non-existent key"
        );

        // Verify other keys are still present
        let find_res1 = btree.find(&key1, buffer_pool.as_mut()).unwrap();
        assert_eq!(find_res1, Some(val1), "Key 1 missing after failed delete");

        let find_res2 = btree.find(&key2, buffer_pool.as_mut()).unwrap();
        assert_eq!(find_res2, Some(val2), "Key 2 missing after failed delete");

        cleanup_temp_file(&temp_path);
    }

    #[test]
    fn test_btree_insert_reverse_sequential() {
        const KEY_SIZE: u32 = 8;
        let (mut btree, mut buffer_pool, temp_path, page_id_counter) =
            setup_bplus_tree_test("insert_reverse_seq", KEY_SIZE);

        let num_keys = 1000;
        let mut keys = Vec::new();

        for i in (0..num_keys).rev() {
            // Insert in reverse order
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

    #[test]
    fn test_btree_delete_leaf_merge() {
        const KEY_SIZE: u32 = 8;
        let (mut btree, mut buffer_pool, temp_path, page_id_counter) =
            setup_bplus_tree_test("delete_leaf_merge", KEY_SIZE);

        let max_keys = get_max_leaf_keys(buffer_pool.as_mut(), &btree);
        let min_keys = (max_keys + 1) / 2;

        let num_keys_to_split = max_keys + 1;
        let mut keys = Vec::new();

        for i in 0..num_keys_to_split {
            let key = (i as u64).to_le_bytes();
            let val = (i as u64) * 10;
            keys.push((key, val));
            btree
                .insert(&key, val, buffer_pool.as_mut(), &page_id_counter)
                .unwrap();
        }

        // State after split (e.g., max=252, num=253, min=126, split_point=126):
        // Leaf_L has keys [0..125] (size 126 = min_keys)
        // Leaf_R has keys [126..252] (size 127 = min_keys+1)
        // Root has key [126]

        let split_point_val = (num_keys_to_split / 2) as u64;
        let leaf_r_size = num_keys_to_split - (split_point_val as usize);

        if leaf_r_size > min_keys {
            // Leaf_R has min_keys+1. Delete one from it to bring it to min_keys.
            let key_to_remove_from_r = (keys.last().unwrap().0).clone();
            btree
                .delete(&key_to_remove_from_r, buffer_pool.as_mut())
                .unwrap();
            keys.pop();
        }

        // Now, both Leaf_L [0..125] and Leaf_R [126..251] are at `min_keys` (126).
        // Root has key [126].

        let key_to_delete_val = 0u64;
        let key_to_delete_bytes = key_to_delete_val.to_le_bytes();

        let delete_res = btree.delete(&key_to_delete_bytes, buffer_pool.as_mut());
        assert!(delete_res.is_ok(), "Delete failed: {:?}", delete_res.err());
        assert_eq!(delete_res.unwrap(), true, "Delete should return true");

        // 4. Verify merge
        // Leaf_L underflows (125 keys). Leaf_R is at min (126) and cannot give.
        // Merge occurs. Root's key [126] is removed.
        // Root is now BPlusInner with 0 keys and 1 child (the merged leaf).
        let root_id = btree.get_root_page_id();
        let root_frame = buffer_pool.as_mut().fetch_page(root_id).unwrap();
        let root_fid = root_frame.fid();
        let mut page_view = root_frame.page_view();

        if let Page::BPlusInner(inner_page) = &mut page_view {
            assert_eq!(
                inner_page.curr_vec_sz(),
                0,
                "Root should have 0 separator keys after merge"
            );
        } else {
            // Panic with more info
            let page_kind = page_kind_from_buf(root_frame.page_view().raw());
            panic!(
                "Root page is not BPlusInner after merge, it is: {:?}",
                page_kind
            );
        }
        buffer_pool.as_mut().unpin_frame(root_fid).unwrap();

        // 5. Verify all other keys are still present
        for (key_bytes, val) in keys {
            if key_bytes == key_to_delete_bytes {
                let find_res = btree.find(&key_bytes, buffer_pool.as_mut()).unwrap();
                assert!(find_res.is_none(), "Deleted key was found");
            } else {
                let find_res = btree.find(&key_bytes, buffer_pool.as_mut()).unwrap();
                assert_eq!(
                    find_res,
                    Some(val),
                    "Key missing after merge: {:?}",
                    key_bytes
                );
            }
        }

        cleanup_temp_file(&temp_path);
    }

    #[test]
    fn test_btree_delete_borrow_from_left_leaf() {
        const KEY_SIZE: u32 = 8;
        let (mut btree, mut buffer_pool, temp_path, page_id_counter) =
            setup_bplus_tree_test("delete_borrow_left", KEY_SIZE);

        let max_keys = get_max_leaf_keys(buffer_pool.as_mut(), &btree);
        let min_keys = (max_keys + 1) / 2;

        // Build a simple 2-leaf tree by inserting exactly max_keys + 1 keys
        // This forces one split, creating two leaves
        let mut keys = Vec::new();

        for i in 0..=(max_keys + 1) {
            let key = (i as u64).to_le_bytes();
            let val = (i as u64) * 10;
            keys.push((key, val));
            btree
                .insert(&key, val, buffer_pool.as_mut(), &page_id_counter)
                .unwrap();
        }

        // After inserting max_keys + 1 keys, we have exactly 2 leaves
        // Split point is at (max_keys + 1) / 2
        let split_point = ((max_keys + 1) / 2) as u64;

        // Verify the tree structure
        let root_id = btree.get_root_page_id();
        let root_frame = buffer_pool.as_mut().fetch_page(root_id).unwrap();
        let root_fid = root_frame.fid();
        let mut page_view = root_frame.page_view();

        let separator = if let Page::BPlusInner(inner_page) = &mut page_view {
            assert_eq!(
                inner_page.curr_vec_sz(),
                1,
                "Should have exactly 1 separator"
            );
            u64::from_le_bytes(inner_page.get_key_at(0).try_into().unwrap())
        } else {
            panic!("Root should be inner after split");
        };
        buffer_pool.as_mut().unpin_frame(root_fid).unwrap();
        let key_to_remove_val = (max_keys + 1) as u64; // This is the last key
        let key_to_remove_bytes = key_to_remove_val.to_le_bytes();
        btree
            .delete(&key_to_remove_bytes, buffer_pool.as_mut())
            .expect("Pre-delete failed");
        keys.pop();
        assert_eq!(
            separator,
            ((max_keys + 1) / 2) as u64,
            "Separator should be at split point + 1"
        );
        // Now delete the separator key (first key of right leaf)
        // This causes right leaf to underflow
        let key_to_delete = separator.to_le_bytes();

        let delete_res = btree.delete(&key_to_delete, buffer_pool.as_mut());
        assert!(delete_res.is_ok(), "Delete failed: {:?}", delete_res.err());
        assert_eq!(delete_res.unwrap(), true);

        // After borrowing from left, new separator should be (separator - 1)
        // because left leaf's last key moves to become right leaf's first key
        let expected_separator = separator + 1;

        let root_frame = buffer_pool.as_mut().fetch_page(root_id).unwrap();
        let root_fid = root_frame.fid();
        let mut page_view = root_frame.page_view();

        if let Page::BPlusInner(inner_page) = &mut page_view {
            let actual_separator = u64::from_le_bytes(inner_page.get_key_at(0).try_into().unwrap());
            assert_eq!(
                actual_separator, expected_separator,
                "Parent separator key was not updated correctly"
            );
        }
        buffer_pool.as_mut().unpin_frame(root_fid).unwrap();

        // Verify all other keys
        for (key_bytes, val) in keys {
            if key_bytes == key_to_delete {
                assert!(
                    btree
                        .find(&key_bytes, buffer_pool.as_mut())
                        .unwrap()
                        .is_none()
                );
            } else {
                assert_eq!(
                    btree.find(&key_bytes, buffer_pool.as_mut()).unwrap(),
                    Some(val)
                );
            }
        }

        cleanup_temp_file(&temp_path);
    }

    #[test]
    fn test_btree_delete_borrow_from_right_leaf() {
        const KEY_SIZE: u32 = 8;
        let (mut btree, mut buffer_pool, temp_path, page_id_counter) =
            setup_bplus_tree_test("delete_borrow_right", KEY_SIZE);

        let max_keys = get_max_leaf_keys(buffer_pool.as_mut(), &btree);

        // Insert exactly max_keys + 1 keys to create 2 leaves
        let mut keys = Vec::new();

        for i in 0..=max_keys {
            let key = (i as u64).to_le_bytes();
            let val = (i as u64) * 10;
            keys.push((key, val));
            btree
                .insert(&key, val, buffer_pool.as_mut(), &page_id_counter)
                .unwrap();
        }

        // Get the separator
        let root_id = btree.get_root_page_id();
        let root_frame = buffer_pool.as_mut().fetch_page(root_id).unwrap();
        let root_fid = root_frame.fid();
        let mut page_view = root_frame.page_view();

        let separator = if let Page::BPlusInner(inner_page) = &mut page_view {
            assert_eq!(
                inner_page.curr_vec_sz(),
                1,
                "Should have exactly 1 separator"
            );
            u64::from_le_bytes(inner_page.get_key_at(0).try_into().unwrap())
        } else {
            panic!("Root should be inner after split");
        };
        buffer_pool.as_mut().unpin_frame(root_fid).unwrap();

        // Delete first key (key 0) from left leaf
        // This causes left leaf to underflow and borrow from right
        let key_to_delete = 0u64.to_le_bytes();

        let delete_res = btree.delete(&key_to_delete, buffer_pool.as_mut());
        assert!(delete_res.is_ok(), "Delete failed: {:?}", delete_res.err());
        assert_eq!(delete_res.unwrap(), true);

        // After borrowing from right, new separator should be (separator + 1)
        // because right leaf's first key moves to left, and right gets a new first key
        let expected_separator = separator + 1;

        let root_frame = buffer_pool.as_mut().fetch_page(root_id).unwrap();
        let root_fid = root_frame.fid();
        let mut page_view = root_frame.page_view();

        if let Page::BPlusInner(inner_page) = &mut page_view {
            let actual_separator = u64::from_le_bytes(inner_page.get_key_at(0).try_into().unwrap());
            assert_eq!(
                actual_separator, expected_separator,
                "Parent separator key was not updated correctly"
            );
        }
        buffer_pool.as_mut().unpin_frame(root_fid).unwrap();

        // Verify all other keys
        for (key_bytes, val) in keys {
            if key_bytes == key_to_delete {
                assert!(
                    btree
                        .find(&key_bytes, buffer_pool.as_mut())
                        .unwrap()
                        .is_none()
                );
            } else {
                assert_eq!(
                    btree.find(&key_bytes, buffer_pool.as_mut()).unwrap(),
                    Some(val)
                );
            }
        }

        cleanup_temp_file(&temp_path);
    }
}
