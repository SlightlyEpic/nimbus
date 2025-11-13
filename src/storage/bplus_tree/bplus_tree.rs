use crate::storage::buffer::BufferPool;
use crate::storage::page::base::{Page, PageId};
use std::pin::Pin;

#[derive(Debug)]
pub enum BTreeError {
    FetchPage(String),
    UnpinPage(String),
    InvalidPageType,
    KeyNotFound,
}

pub struct BPlusTree<'a> {
    bpm: Pin<&'a mut BufferPool>,
    root_page_id: PageId,
}

impl<'a> BPlusTree<'a> {
    /// Creates a new B+ Tree instance.
    /// Note: In a real DB, root_page_id would be fetched from a metadata page.
    pub fn new(bpm: Pin<&'a mut BufferPool>, root_page_id: PageId) -> Self {
        Self { bpm, root_page_id }
    }

    /// Traverses the tree from Root -> Leaf for a given key.
    /// Returns the PageId of the leaf node that *should* contain the key.
    fn find_leaf_page_id(&mut self, key: &[u8]) -> Result<PageId, BTreeError> {
        let mut current_page_id = self.root_page_id;

        loop {
            // 1. Fetch the current page
            let frame = self
                .bpm
                .as_mut()
                .fetch_page(current_page_id)
                .map_err(|e| BTreeError::FetchPage(format!("{:?}", e)))?;

            let frame_id = frame.fid();
            let mut page_view = frame.page_view();

            // 2. Determine the next step based on Page Kind
            let next_page_id_opt = match &page_view {
                Page::BPlusInner(inner) => {
                    // Binary search the inner node to find the correct child pointer
                    let child_idx = inner.find_child_for_key(key);
                    inner.get_child_at(child_idx)
                }
                Page::BPlusLeaf(_) => {
                    // We have reached a leaf
                    None
                }
                _ => {
                    self.bpm.as_mut().unpin_frame(frame_id).ok();
                    return Err(BTreeError::InvalidPageType);
                }
            };

            // 3. Unpin the current page
            self.bpm
                .as_mut()
                .unpin_frame(frame_id)
                .map_err(|e| BTreeError::UnpinPage(format!("{:?}", e)))?;

            // 4. Advance or Return
            match next_page_id_opt {
                Some(next_id) => current_page_id = next_id, // Traverse down
                None => return Ok(current_page_id),         // Found leaf
            }
        }
    }

    /// Performs a Point Query. Returns the RowId (as u64) if the key exists.
    pub fn get_value(&mut self, key: &[u8]) -> Result<Option<u64>, BTreeError> {
        if self.root_page_id == 0 {
            return Ok(None);
        }

        // 1. Find the correct leaf page
        let leaf_page_id = self.find_leaf_page_id(key)?;

        // 2. Fetch the leaf page
        let frame = self
            .bpm
            .as_mut()
            .fetch_page(leaf_page_id)
            .map_err(|e| BTreeError::FetchPage(format!("{:?}", e)))?;

        let frame_id = frame.fid();
        let page_view = frame.page_view();

        // 3. Search within the leaf page
        let result = if let Page::BPlusLeaf(leaf) = page_view {
            leaf.get_value(key)
        } else {
            self.bpm.as_mut().unpin_frame(frame_id).ok();
            return Err(BTreeError::InvalidPageType);
        };

        // 4. Unpin
        self.bpm
            .as_mut()
            .unpin_frame(frame_id)
            .map_err(|e| BTreeError::UnpinPage(format!("{:?}", e)))?;

        Ok(result)
    }
}
