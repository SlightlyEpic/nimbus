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

    /// Performs a Point Query. Returns the RowId (as u64) if the key exists.
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
        // Case 0: Empty Tree -> Create Root Leaf
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

        // 1. Find path to leaf (Stack of PageIds: Root -> ... -> Parent)
        let path = self.find_path_to_leaf(key)?;
        let leaf_id = *path.last().unwrap();

        // 2. Insert into Leaf
        let split_result = self.insert_into_leaf(leaf_id, key, value, page_id_counter)?;

        // 3. Propagate Splits upwards
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

    /// Helper: Insert into a specific leaf page. Handles splitting if necessary.
    /// Refactored to avoid double-mutable borrow of `self.bpm`.
    fn insert_into_leaf(
        &mut self,
        page_id: PageId,
        key: &[u8],
        value: u64,
        counter: &AtomicU32,
    ) -> Result<Option<(Vec<u8>, PageId)>, BTreeError> {
        // 1. Fetch and Attempt Insert/Split calculation
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
                    // Full: split in memory, modify old leaf (truncate), return new entries
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

        // 2. Unpin the old frame (Ending the first borrow)
        self.bpm.as_mut().mark_frame_dirty(frame_id);
        self.bpm
            .as_mut()
            .unpin_frame(frame_id)
            .map_err(|e| BTreeError::UnpinPage(format!("{:?}", e)))?;

        // 3. If no split, we are done
        if split_info.is_none() {
            return Ok(None);
        }

        let (split_key, new_entries) = split_info.unwrap();

        // 4. Allocate New Page (Starts second borrow)
        let new_page_id = counter.fetch_add(1, Ordering::SeqCst) + 1;
        let new_frame = self
            .bpm
            .as_mut()
            .alloc_new_page(PageKind::BPlusLeaf, new_page_id)
            .map_err(|e| BTreeError::AllocPage(format!("{:?}", e)))?;
        let new_frame_id = new_frame.fid();

        // 5. Init New Page
        {
            let mut new_view = new_frame.page_view();
            if let Page::BPlusLeaf(new_leaf) = &mut new_view {
                new_leaf.init(new_page_id, key.len() as u32);
                for (k, v) in new_entries {
                    new_leaf.insert_sorted(&k, v);
                }
                // Set Prev Sibling to Old Page
                new_leaf.set_prev_sibling(Some(page_id));
            }
        }

        // 6. Unpin New Frame (Ending second borrow)
        self.bpm.as_mut().mark_frame_dirty(new_frame_id);
        self.bpm.as_mut().unpin_frame(new_frame_id).ok();

        // 7. Linking: We need to update Old Page's Next pointer.
        // Re-fetch Old Page (Starts third borrow)
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

        // 8. Linking: If Old Page had a Next sibling, that Sibling's Prev must point to New Page
        if let Some(next_sib_id) = old_next_sibling_id {
            // Re-fetch New Page to link it to Next Sibling
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

            // Fetch Next Sibling to update Prev
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

    /// Helper: Propagate splits up the tree (Recursive)
    fn insert_into_parent(
        &mut self,
        mut path: Vec<PageId>,
        split_key: Vec<u8>,
        new_child_id: PageId,
        counter: &AtomicU32,
    ) -> Result<(), BTreeError> {
        let _child_id = path.pop(); // Remove child just processed

        if let Some(parent_id) = path.last().copied() {
            // 1. Fetch Parent and Determine Split
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
                        // Full: Split
                        let split_res = inner.split_and_get_new_entries(&split_key, new_child_id);
                        (Some(split_res), frame_id)
                    }
                } else {
                    self.bpm.as_mut().unpin_frame(frame_id).ok();
                    return Err(BTreeError::InvalidPageType);
                }
            };

            // 2. Unpin Parent
            self.bpm.as_mut().mark_frame_dirty(frame_id);
            self.bpm.as_mut().unpin_frame(frame_id).ok();

            if split_data_opt.is_none() {
                return Ok(());
            }

            // 3. Handle Split (Alloc New Inner)
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

                    // Initialize children of new node
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

            // 4. Recurse Up
            self.insert_into_parent(path, split_data.key_to_push_up, new_inner_id, counter)
        } else {
            // Root Split
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
