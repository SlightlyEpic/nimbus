use crate::storage::buffer::BufferPool;
use crate::storage::page::base::{Page, PageId, PageKind};
use std::pin::Pin;
use std::sync::atomic::{AtomicU32, Ordering};

#[derive(Debug)]
pub enum BTreeError {
    FetchPage(String),
    UnpinPage(String),
    AllocPage(String),
    InvalidPageType,
    InsertError(String),
    DeleteError(String),
}

pub struct BPlusTree<'a> {
    pub bpm: Pin<&'a mut BufferPool>,
    pub root_page_id: PageId,
}

impl<'a> BPlusTree<'a> {
    pub fn new(bpm: Pin<&'a mut BufferPool>, root_page_id: PageId) -> Self {
        Self { bpm, root_page_id }
    }

    /// Traverses the tree from Root -> Leaf for a given key.
    /// Returns the PageId of the leaf node that *should* contain the key.
    pub fn find_leaf_page_id(&mut self, key: &[u8]) -> Result<PageId, BTreeError> {
        if self.root_page_id == 0 {
            return Err(BTreeError::InvalidPageType);
        }

        let mut current_page_id = self.root_page_id;

        loop {
            let frame = self
                .bpm
                .as_mut()
                .fetch_page(current_page_id)
                .map_err(|e| BTreeError::FetchPage(format!("{:?}", e)))?;
            let frame_id = frame.fid();
            let page_view = frame.page_view();

            let next_page_id = match page_view {
                Page::BPlusInner(inner) => {
                    let idx = inner.find_child_for_key(key);
                    inner.get_child_at(idx)
                }
                Page::BPlusLeaf(_) => None, // Reached leaf
                _ => {
                    self.bpm.as_mut().unpin_frame(frame_id).ok();
                    return Err(BTreeError::InvalidPageType);
                }
            };

            self.bpm
                .as_mut()
                .unpin_frame(frame_id)
                .map_err(|e| BTreeError::UnpinPage(format!("{:?}", e)))?;

            match next_page_id {
                Some(id) => current_page_id = id,
                None => return Ok(current_page_id),
            }
        }
    }

    // ========================= DELETION LOGIC =========================

    pub fn delete(&mut self, key: &[u8]) -> Result<(), BTreeError> {
        if self.root_page_id == 0 {
            return Ok(()); // Empty tree
        }

        let leaf_page_id = self.find_leaf_page_id(key)?;
        self.delete_entry(leaf_page_id, key)
    }

    fn delete_entry(&mut self, page_id: PageId, key: &[u8]) -> Result<(), BTreeError> {
        // 1. Fetch Page and Remove Entry
        let (is_underflow, parent_id) = {
            let frame = self
                .bpm
                .as_mut()
                .fetch_page(page_id)
                .map_err(|e| BTreeError::FetchPage(format!("{:?}", e)))?;
            let frame_id = frame.fid();
            let mut page_view = frame.page_view();

            let (underflow, _parent) = match &mut page_view {
                Page::BPlusLeaf(leaf) => {
                    let removed = leaf.remove_key(key);
                    if !removed {
                        self.bpm.as_mut().unpin_frame(frame_id).ok();
                        return Ok(());
                    }
                    (leaf.is_underflow(), 0)
                }
                Page::BPlusInner(_inner) => {
                    // Inner node deletion is handled by coalesce/redistribute usually.
                    // This path is hit if we recursively call delete_entry on root.
                    (false, 0)
                }
                _ => {
                    self.bpm.as_mut().unpin_frame(frame_id).ok();
                    return Err(BTreeError::InvalidPageType);
                }
            };

            self.bpm.as_mut().mark_frame_dirty(frame_id);
            self.bpm.as_mut().unpin_frame(frame_id).ok();
            (underflow, _parent)
        };

        // 2. Check Root Adjustment
        if self.root_page_id == page_id {
            return self.adjust_root();
        }

        // 3. Handle Underflow
        if is_underflow {
            // Re-traverse to find parent since we don't store parent pointers in pages
            let parent_id = self.find_parent_of(page_id, key)?;
            if let Some(pid) = parent_id {
                self.handle_underflow(page_id, pid, key)?;
            }
        }

        Ok(())
    }

    fn find_parent_of(
        &mut self,
        target_page_id: PageId,
        key: &[u8],
    ) -> Result<Option<PageId>, BTreeError> {
        let mut curr = self.root_page_id;
        let mut parent = None;

        loop {
            if curr == target_page_id {
                return Ok(parent);
            }

            let frame = self
                .bpm
                .as_mut()
                .fetch_page(curr)
                .map_err(|e| BTreeError::FetchPage(format!("{:?}", e)))?;
            let fid = frame.fid();
            let view = frame.page_view();

            let next_id = match view {
                Page::BPlusInner(inner) => {
                    let idx = inner.find_child_for_key(key);
                    Some(inner.get_child_at(idx).unwrap())
                }
                _ => None,
            };

            self.bpm.as_mut().unpin_frame(fid).ok();

            if let Some(next) = next_id {
                parent = Some(curr);
                curr = next;
            } else {
                return Ok(None);
            }
        }
    }

    fn adjust_root(&mut self) -> Result<(), BTreeError> {
        let frame = self
            .bpm
            .as_mut()
            .fetch_page(self.root_page_id)
            .map_err(|e| BTreeError::FetchPage(format!("{:?}", e)))?;
        let fid = frame.fid();
        let view = frame.page_view();

        let should_update = match view {
            Page::BPlusLeaf(leaf) => leaf.num_entries() == 0,
            Page::BPlusInner(inner) => inner.num_entries() == 0,
            _ => false,
        };

        let new_root = if should_update {
            match frame.page_view() {
                Page::BPlusInner(inner) => Some(inner.get_child_at(0).unwrap()),
                _ => None,
            }
        } else {
            self.bpm.as_mut().unpin_frame(fid).ok();
            return Ok(());
        };

        self.bpm.as_mut().unpin_frame(fid).ok();

        if let Some(new_root_id) = new_root {
            self.root_page_id = new_root_id;
            let frame = self
                .bpm
                .as_mut()
                .fetch_page(new_root_id)
                .map_err(|e| BTreeError::FetchPage(format!("{:?}", e)))?;
            let fid = frame.fid();
            if let Page::BPlusInner(inner) = &mut frame.page_view() {
                inner.set_root(true);
            }
            self.bpm.as_mut().mark_frame_dirty(fid);
            self.bpm.as_mut().unpin_frame(fid).ok();
        } else {
            self.root_page_id = 0;
        }

        Ok(())
    }

    fn handle_underflow(
        &mut self,
        page_id: PageId,
        parent_id: PageId,
        key: &[u8],
    ) -> Result<(), BTreeError> {
        let (left_sib, right_sib, index_in_parent) = {
            let frame = self
                .bpm
                .as_mut()
                .fetch_page(parent_id)
                .map_err(|e| BTreeError::FetchPage(format!("{:?}", e)))?;
            let fid = frame.fid();
            let view = frame.page_view();

            if let Page::BPlusInner(inner) = view {
                let idx = inner
                    .lookup_child_index(page_id)
                    .ok_or(BTreeError::DeleteError("Parent child mismatch".into()))?;

                let left = if idx > 0 {
                    inner.get_child_at(idx - 1)
                } else {
                    None
                };
                let right = inner.get_child_at(idx + 1);

                self.bpm.as_mut().unpin_frame(fid).ok();
                (left, right, idx)
            } else {
                self.bpm.as_mut().unpin_frame(fid).ok();
                return Err(BTreeError::InvalidPageType);
            }
        };

        if let Some(left_id) = left_sib {
            if self.redistribute(left_id, page_id, parent_id, index_in_parent - 1, true)? {
                return Ok(());
            }
        }

        if let Some(right_id) = right_sib {
            if self.redistribute(right_id, page_id, parent_id, index_in_parent, false)? {
                return Ok(());
            }
        }

        if let Some(left_id) = left_sib {
            self.coalesce(left_id, page_id, parent_id, index_in_parent - 1)
        } else if let Some(right_id) = right_sib {
            self.coalesce(page_id, right_id, parent_id, index_in_parent)
        } else {
            Err(BTreeError::DeleteError("Cannot merge: no siblings".into()))
        }
    }

    fn redistribute(
        &mut self,
        sibling_id: PageId,
        node_id: PageId,
        parent_id: PageId,
        parent_key_idx: usize,
        is_left_sibling: bool,
    ) -> Result<bool, BTreeError> {
        // Check if sibling can give
        let can_give = {
            let frame = self
                .bpm
                .as_mut()
                .fetch_page(sibling_id)
                .map_err(|e| BTreeError::FetchPage(format!("{:?}", e)))?;
            let view = frame.page_view();
            let res = match view {
                Page::BPlusLeaf(leaf) => leaf.can_give_key(),
                Page::BPlusInner(inner) => inner.can_give_key(),
                _ => false,
            };
            let fid = frame.fid();
            self.bpm.as_mut().unpin_frame(fid).ok();
            res
        };

        if !can_give {
            return Ok(false);
        }

        // Fetch pages one by one to avoid borrowing self.bpm multiple times
        let (p_fid, p_ptr) = {
            let frame = self.bpm.as_mut().fetch_page(parent_id).unwrap();
            (
                frame.fid(),
                frame.page_view().raw_mut() as *mut crate::storage::page::base::PageBuf,
            )
        };

        let (s_fid, s_ptr) = {
            let frame = self.bpm.as_mut().fetch_page(sibling_id).unwrap();
            (
                frame.fid(),
                frame.page_view().raw_mut() as *mut crate::storage::page::base::PageBuf,
            )
        };

        let (n_fid, n_ptr) = {
            let frame = self.bpm.as_mut().fetch_page(node_id).unwrap();
            (
                frame.fid(),
                frame.page_view().raw_mut() as *mut crate::storage::page::base::PageBuf,
            )
        };

        unsafe {
            let mut parent_view = crate::storage::page::BPlusInner::new(&mut *p_ptr);
            // We need to interpret sibling and node as either Inner or Leaf
            // We'll use PageKind from header to decide
            let s_kind = crate::storage::page::base::page_kind_from_buf(&*s_ptr);
            let n_kind = crate::storage::page::base::page_kind_from_buf(&*n_ptr);

            match (s_kind, n_kind) {
                (PageKind::BPlusLeaf, PageKind::BPlusLeaf) => {
                    let mut sibling = crate::storage::page::BPlusLeaf::new(&mut *s_ptr);
                    let mut node = crate::storage::page::BPlusLeaf::new(&mut *n_ptr);

                    if is_left_sibling {
                        let moved_key = sibling.move_last_to_beginning_of(&mut node);
                        parent_view.set_entry(parent_key_idx, &moved_key, node_id);
                    } else {
                        let new_separator = sibling.move_first_to_end_of(&mut node);
                        parent_view.set_entry(parent_key_idx, &new_separator, sibling_id);
                    }
                }
                (PageKind::BPlusInner, PageKind::BPlusInner) => {
                    let mut sibling = crate::storage::page::BPlusInner::new(&mut *s_ptr);
                    let mut node = crate::storage::page::BPlusInner::new(&mut *n_ptr);
                    let separator_key = parent_view.get_key_at(parent_key_idx).to_vec();

                    if is_left_sibling {
                        let new_sep = sibling.move_last_to_beginning_of(&mut node, &separator_key);
                        parent_view.set_entry(parent_key_idx, &new_sep, node_id);
                    } else {
                        let new_sep = sibling.move_first_to_end_of(&mut node, &separator_key);
                        parent_view.set_entry(parent_key_idx, &new_sep, sibling_id);
                    }
                }
                _ => panic!("Mismatch types in redistribute"),
            }
        }

        self.bpm.as_mut().mark_frame_dirty(p_fid);
        self.bpm.as_mut().mark_frame_dirty(s_fid);
        self.bpm.as_mut().mark_frame_dirty(n_fid);

        self.bpm.as_mut().unpin_frame(p_fid).ok();
        self.bpm.as_mut().unpin_frame(s_fid).ok();
        self.bpm.as_mut().unpin_frame(n_fid).ok();

        Ok(true)
    }

    fn coalesce(
        &mut self,
        left_id: PageId,
        right_id: PageId,
        parent_id: PageId,
        parent_key_idx: usize,
    ) -> Result<(), BTreeError> {
        // Fetch pages one by one
        let (p_fid, p_ptr) = {
            let frame = self.bpm.as_mut().fetch_page(parent_id).unwrap();
            (
                frame.fid(),
                frame.page_view().raw_mut() as *mut crate::storage::page::base::PageBuf,
            )
        };

        let (l_fid, l_ptr) = {
            let frame = self.bpm.as_mut().fetch_page(left_id).unwrap();
            (
                frame.fid(),
                frame.page_view().raw_mut() as *mut crate::storage::page::base::PageBuf,
            )
        };

        let (r_fid, r_ptr) = {
            let frame = self.bpm.as_mut().fetch_page(right_id).unwrap();
            (
                frame.fid(),
                frame.page_view().raw_mut() as *mut crate::storage::page::base::PageBuf,
            )
        };

        unsafe {
            let mut parent_view = crate::storage::page::BPlusInner::new(&mut *p_ptr);
            let l_kind = crate::storage::page::base::page_kind_from_buf(&*l_ptr);

            match l_kind {
                PageKind::BPlusLeaf => {
                    let mut left = crate::storage::page::BPlusLeaf::new(&mut *l_ptr);
                    let mut right = crate::storage::page::BPlusLeaf::new(&mut *r_ptr);
                    left.merge_from(&mut right);
                }
                PageKind::BPlusInner => {
                    let mut left = crate::storage::page::BPlusInner::new(&mut *l_ptr);
                    let mut right = crate::storage::page::BPlusInner::new(&mut *r_ptr);
                    let separator = parent_view.get_key_at(parent_key_idx).to_vec();
                    left.merge_from(&mut right, &separator);
                }
                _ => panic!("Invalid page type in coalesce"),
            }

            // Remove from parent
            parent_view.remove_at(parent_key_idx);
        }

        self.bpm.as_mut().mark_frame_dirty(p_fid);
        self.bpm.as_mut().mark_frame_dirty(l_fid);
        self.bpm.as_mut().mark_frame_dirty(r_fid);

        self.bpm.as_mut().unpin_frame(p_fid).ok();
        self.bpm.as_mut().unpin_frame(l_fid).ok();
        self.bpm.as_mut().unpin_frame(r_fid).ok();

        // Recursively check if parent underflowed
        let (p_underflow, search_key) = {
            let frame = self.bpm.as_mut().fetch_page(parent_id).unwrap();
            let is_uf = match frame.page_view() {
                Page::BPlusInner(inner) => inner.is_underflow(),
                _ => false,
            };
            let fid = frame.fid();
            self.bpm.as_mut().unpin_frame(fid).ok();

            // Get a key to find the parent's parent
            let l_frame = self.bpm.as_mut().fetch_page(left_id).unwrap();
            let key = match l_frame.page_view() {
                Page::BPlusLeaf(l) => l.get_first_key(),
                Page::BPlusInner(i) => i.get_first_key(),
                _ => None,
            };
            let l_fid = l_frame.fid();
            self.bpm.as_mut().unpin_frame(l_fid).ok();

            (is_uf, key)
        };

        if p_underflow && parent_id != self.root_page_id {
            if let Some(key) = search_key {
                if let Some(grandparent_id) = self.find_parent_of(parent_id, &key)? {
                    self.handle_underflow(parent_id, grandparent_id, &key)?;
                } else if parent_id == self.root_page_id {
                    self.adjust_root()?;
                }
            }
        } else if parent_id == self.root_page_id {
            self.adjust_root()?;
        }

        Ok(())
    }

    pub fn get_value(&mut self, key: &[u8]) -> Result<Option<u64>, BTreeError> {
        if self.root_page_id == 0 {
            return Ok(None);
        }

        let leaf_page_id = self.find_leaf_page_id(key)?;
        let frame = self
            .bpm
            .as_mut()
            .fetch_page(leaf_page_id)
            .map_err(|e| BTreeError::FetchPage(format!("{:?}", e)))?;
        let frame_id = frame.fid();
        let page_view = frame.page_view();

        let result = if let Page::BPlusLeaf(leaf) = page_view {
            leaf.get_value(key)
        } else {
            self.bpm.as_mut().unpin_frame(frame_id).ok();
            return Err(BTreeError::InvalidPageType);
        };

        self.bpm
            .as_mut()
            .unpin_frame(frame_id)
            .map_err(|e| BTreeError::UnpinPage(format!("{:?}", e)))?;

        Ok(result)
    }

    pub fn insert(
        &mut self,
        key: &[u8],
        value: u64,
        page_id_counter: &AtomicU32,
    ) -> Result<(), BTreeError> {
        if self.root_page_id == 0 {
            let new_root_id = page_id_counter.fetch_add(1, Ordering::SeqCst) + 1;
            let frame = self
                .bpm
                .as_mut()
                .alloc_new_page(PageKind::BPlusLeaf, new_root_id)
                .map_err(|e| BTreeError::AllocPage(format!("{:?}", e)))?;

            {
                let mut view = frame.page_view();
                if let Page::BPlusLeaf(leaf) = &mut view {
                    leaf.init(new_root_id, key.len() as u32);
                    leaf.insert_sorted(key, value);
                }
            }
            self.root_page_id = new_root_id;
            let fid = frame.fid();
            self.bpm.as_mut().unpin_frame(fid).ok();
            return Ok(());
        }

        let path = self.find_path_to_leaf(key)?;
        let leaf_id = *path.last().unwrap();

        let split_result = self.insert_into_leaf(leaf_id, key, value, page_id_counter)?;

        if let Some((split_key, new_node_id)) = split_result {
            self.insert_into_parent(path, split_key, new_node_id, page_id_counter)?;
        }

        Ok(())
    }

    fn find_path_to_leaf(&mut self, key: &[u8]) -> Result<Vec<PageId>, BTreeError> {
        let mut path = Vec::new();
        let mut curr = self.root_page_id;

        loop {
            path.push(curr);
            let frame = self
                .bpm
                .as_mut()
                .fetch_page(curr)
                .map_err(|e| BTreeError::FetchPage(format!("{:?}", e)))?;
            let frame_id = frame.fid();
            let page_view = frame.page_view();

            let next = match page_view {
                Page::BPlusInner(inner) => {
                    let idx = inner.find_child_for_key(key);
                    inner.get_child_at(idx)
                }
                Page::BPlusLeaf(_) => None,
                _ => {
                    self.bpm.as_mut().unpin_frame(frame_id).ok();
                    return Err(BTreeError::InvalidPageType);
                }
            };

            self.bpm.as_mut().unpin_frame(frame_id).ok();

            match next {
                Some(id) => curr = id,
                None => return Ok(path),
            }
        }
    }

    fn insert_into_leaf(
        &mut self,
        page_id: PageId,
        key: &[u8],
        value: u64,
        counter: &AtomicU32,
    ) -> Result<Option<(Vec<u8>, PageId)>, BTreeError> {
        let (split_info, frame_id) = {
            let frame = self
                .bpm
                .as_mut()
                .fetch_page(page_id)
                .map_err(|e| BTreeError::FetchPage(format!("{:?}", e)))?;
            let frame_id = frame.fid();
            let mut page_view = frame.page_view();

            if let Page::BPlusLeaf(leaf) = &mut page_view {
                if leaf.has_space_for_key() {
                    leaf.insert_sorted(key, value);
                    (None, frame_id)
                } else {
                    let (split_res, new_entries) = leaf
                        .split_and_get_new_entries(key, value)
                        .map_err(|e| BTreeError::InsertError(e.to_string()))?;
                    (Some((split_res.split_key, new_entries)), frame_id)
                }
            } else {
                self.bpm.as_mut().unpin_frame(frame_id).ok();
                return Err(BTreeError::InvalidPageType);
            }
        };

        self.bpm.as_mut().mark_frame_dirty(frame_id);
        self.bpm
            .as_mut()
            .unpin_frame(frame_id)
            .map_err(|e| BTreeError::UnpinPage(format!("{:?}", e)))?;

        if split_info.is_none() {
            return Ok(None);
        }

        let (split_key, new_entries) = split_info.unwrap();

        let new_page_id = counter.fetch_add(1, Ordering::SeqCst) + 1;
        let new_frame = self
            .bpm
            .as_mut()
            .alloc_new_page(PageKind::BPlusLeaf, new_page_id)
            .map_err(|e| BTreeError::AllocPage(format!("{:?}", e)))?;
        let new_frame_id = new_frame.fid();

        {
            let mut new_view = new_frame.page_view();
            if let Page::BPlusLeaf(new_leaf) = &mut new_view {
                new_leaf.init(new_page_id, key.len() as u32);
                for (k, v) in new_entries {
                    new_leaf.insert_sorted(&k, v);
                }
                new_leaf.set_prev_sibling(Some(page_id));
            }
        }

        self.bpm.as_mut().mark_frame_dirty(new_frame_id);
        self.bpm.as_mut().unpin_frame(new_frame_id).ok();

        let old_frame = self
            .bpm
            .as_mut()
            .fetch_page(page_id)
            .map_err(|e| BTreeError::FetchPage(format!("{:?}", e)))?;
        let old_frame_id = old_frame.fid();
        let mut old_next_sibling_id = None;

        {
            let mut old_view = old_frame.page_view();
            if let Page::BPlusLeaf(old_leaf) = &mut old_view {
                old_next_sibling_id = old_leaf.next_sibling();
                old_leaf.set_next_sibling(Some(new_page_id));
            }
        }
        self.bpm.as_mut().mark_frame_dirty(old_frame_id);
        self.bpm.as_mut().unpin_frame(old_frame_id).ok();

        if let Some(next_sib_id) = old_next_sibling_id {
            let new_frame = self
                .bpm
                .as_mut()
                .fetch_page(new_page_id)
                .map_err(|e| BTreeError::FetchPage(format!("{:?}", e)))?;
            let new_fid = new_frame.fid();
            {
                let mut new_view = new_frame.page_view();
                if let Page::BPlusLeaf(new_leaf) = &mut new_view {
                    new_leaf.set_next_sibling(Some(next_sib_id));
                }
            }
            self.bpm.as_mut().mark_frame_dirty(new_fid);
            self.bpm.as_mut().unpin_frame(new_fid).ok();

            let sib_frame = self
                .bpm
                .as_mut()
                .fetch_page(next_sib_id)
                .map_err(|e| BTreeError::FetchPage(format!("{:?}", e)))?;
            let sib_fid = sib_frame.fid();
            {
                let mut sib_view = sib_frame.page_view();
                if let Page::BPlusLeaf(sib_leaf) = &mut sib_view {
                    sib_leaf.set_prev_sibling(Some(new_page_id));
                }
            }
            self.bpm.as_mut().mark_frame_dirty(sib_fid);
            self.bpm.as_mut().unpin_frame(sib_fid).ok();
        }

        Ok(Some((split_key, new_page_id)))
    }

    fn insert_into_parent(
        &mut self,
        mut path: Vec<PageId>,
        split_key: Vec<u8>,
        new_child_id: PageId,
        counter: &AtomicU32,
    ) -> Result<(), BTreeError> {
        let _child_id = path.pop();

        if let Some(parent_id) = path.last().copied() {
            let (split_data_opt, frame_id) = {
                let frame = self
                    .bpm
                    .as_mut()
                    .fetch_page(parent_id)
                    .map_err(|e| BTreeError::FetchPage(format!("{:?}", e)))?;
                let frame_id = frame.fid();
                let mut page_view = frame.page_view();

                if let Page::BPlusInner(inner) = &mut page_view {
                    if inner.has_space_for_key() {
                        let idx = inner.find_child_for_key(&split_key);
                        inner.insert_at(idx, &split_key, new_child_id);
                        (None, frame_id)
                    } else {
                        let split_res = inner.split_and_get_new_entries(&split_key, new_child_id);
                        (Some(split_res), frame_id)
                    }
                } else {
                    self.bpm.as_mut().unpin_frame(frame_id).ok();
                    return Err(BTreeError::InvalidPageType);
                }
            };

            self.bpm.as_mut().mark_frame_dirty(frame_id);
            self.bpm.as_mut().unpin_frame(frame_id).ok();

            if split_data_opt.is_none() {
                return Ok(());
            }

            let split_data = split_data_opt.unwrap();
            let new_inner_id = counter.fetch_add(1, Ordering::SeqCst) + 1;

            let new_frame = self
                .bpm
                .as_mut()
                .alloc_new_page(PageKind::BPlusInner, new_inner_id)
                .map_err(|e| BTreeError::AllocPage(format!("{:?}", e)))?;
            let new_fid = new_frame.fid();

            {
                let mut new_view = new_frame.page_view();
                if let Page::BPlusInner(new_inner) = &mut new_view {
                    new_inner.init(new_inner_id, 0, split_key.len() as u32);

                    let first_child = split_data.new_page_children[0];
                    new_inner.set_child_at(0, first_child);

                    for (i, key) in split_data.new_page_keys.iter().enumerate() {
                        let child = split_data.new_page_children[i + 1];
                        new_inner.insert_at(i, key, child);
                    }
                }
            }
            self.bpm.as_mut().mark_frame_dirty(new_fid);
            self.bpm.as_mut().unpin_frame(new_fid).ok();

            self.insert_into_parent(path, split_data.key_to_push_up, new_inner_id, counter)
        } else {
            self.create_new_root(self.root_page_id, split_key, new_child_id, counter)
        }
    }

    fn create_new_root(
        &mut self,
        left_child_id: PageId,
        key: Vec<u8>,
        right_child_id: PageId,
        counter: &AtomicU32,
    ) -> Result<(), BTreeError> {
        let new_root_id = counter.fetch_add(1, Ordering::SeqCst) + 1;

        let frame = self
            .bpm
            .as_mut()
            .alloc_new_page(PageKind::BPlusInner, new_root_id)
            .map_err(|e| BTreeError::AllocPage(format!("{:?}", e)))?;
        let frame_id = frame.fid();

        {
            let mut view = frame.page_view();
            if let Page::BPlusInner(inner) = &mut view {
                inner.init(new_root_id, 1, key.len() as u32);
                inner.set_root(true);
                inner.set_child_at(0, left_child_id);
                inner.insert_at(0, &key, right_child_id);
            }
        }

        self.bpm.as_mut().mark_frame_dirty(frame_id);
        self.bpm.as_mut().unpin_frame(frame_id).ok();

        self.root_page_id = new_root_id;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::buffer::fifo_evictor::FifoEvictor;
    use crate::storage::disk::FileManager;
    use crate::storage::page_locator;
    use std::fs;
    use std::path::PathBuf;

    fn setup_bp(test_name: &str) -> (PathBuf, Pin<Box<BufferPool>>, AtomicU32) {
        let file_name = format!("test_btree_full_{}.db", test_name);
        let _ = fs::remove_file(&file_name);
        let file_manager = FileManager::new(file_name.clone()).unwrap();
        let evictor = Box::new(FifoEvictor::new());
        let locator = Box::new(page_locator::locator::DirectoryPageLocator::new());
        let bp = Box::pin(BufferPool::new(file_manager, evictor, locator));
        (PathBuf::from(file_name), bp, AtomicU32::new(0))
    }

    #[test]
    fn test_btree_insert_and_split() {
        let (path, mut bp, counter) = setup_bp("split");
        let mut tree = BPlusTree::new(bp.as_mut(), 0);

        let n = 500;
        for i in 0..n {
            let key = (i as u32).to_be_bytes();
            tree.insert(&key, i as u64, &counter)
                .expect("Insert failed");
        }

        for i in 0..n {
            let key = (i as u32).to_be_bytes();
            let val = tree
                .get_value(&key)
                .expect("Get failed")
                .expect("Key not found");
            assert_eq!(val, i as u64);
        }

        assert!(tree.root_page_id > 1);
        let _ = fs::remove_file(&path);
    }
}
