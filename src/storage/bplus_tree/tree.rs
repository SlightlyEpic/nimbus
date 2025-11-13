use crate::storage::buffer::BufferPool;
use crate::storage::page::base::{Page, PageId};
use std::pin::Pin;

#[derive(Debug)]
pub enum BTreeError {
    FetchPage(String),
    UnpinPage(String),
    InvalidPageType,
    // Remove KeyNotFound if not used, or keep for future use
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::buffer::fifo_evictor::FifoEvictor;
    use crate::storage::disk::FileManager;
    use crate::storage::page::base::DiskPage;
    use crate::storage::page::base::PageKind;
    use crate::storage::page_locator;
    use crate::storage::page_locator::locator::DirectoryPageLocator;
    use std::fs;
    use std::path::PathBuf;

    fn setup_bp(test_name: &str) -> (PathBuf, Pin<Box<BufferPool>>) {
        let file_name = format!("test_btree_{}.db", test_name);
        let _ = fs::remove_file(&file_name);

        let file_manager = FileManager::new(file_name.clone()).unwrap();
        let evictor = Box::new(FifoEvictor::new());
        let locator = Box::new(DirectoryPageLocator::new());
        let bp = Box::pin(BufferPool::new(file_manager, evictor, locator));

        (PathBuf::from(file_name), bp)
    }

    #[test]
    fn test_point_query_manual_construction() {
        let (path, mut bp) = setup_bp("point_query");

        // 1. Manually Build Tree Structure
        // Structure:
        // Root (Inner, Page 3)
        //  |
        //  +-- Child 0: Leaf (Page 1) -> Keys [10, 20]
        //  +-- Child 1: Leaf (Page 2) -> Keys [50, 60]
        // Inner Node Keys: [50] (Separator)
        // If Key < 50 -> Go Child 0 (Page 1)
        // If Key >= 50 -> Go Child 1 (Page 2)

        // -- Create Leaf 1 (Page 1) --
        let frame1 = bp.as_mut().alloc_new_page(PageKind::BPlusLeaf, 1).unwrap();
        {
            let mut view = frame1.page_view();
            if let Page::BPlusLeaf(leaf) = &mut view {
                leaf.init(1, 4); // Key size 4 bytes
                leaf.insert_sorted(&10u32.to_be_bytes(), 100); // Key 10, RowId 100
                leaf.insert_sorted(&20u32.to_be_bytes(), 200); // Key 20, RowId 200
            }
        }
        let fid1 = frame1.fid();
        bp.as_mut().unpin_frame(fid1).unwrap();

        // -- Create Leaf 2 (Page 2) --
        let frame2 = bp.as_mut().alloc_new_page(PageKind::BPlusLeaf, 2).unwrap();
        {
            let mut view = frame2.page_view();
            if let Page::BPlusLeaf(leaf) = &mut view {
                leaf.init(2, 4);
                leaf.insert_sorted(&50u32.to_be_bytes(), 500);
                leaf.insert_sorted(&60u32.to_be_bytes(), 600);
            }
        }
        let fid2 = frame2.fid();
        bp.as_mut().unpin_frame(fid2).unwrap();

        // -- Create Root Inner (Page 3) --
        let frame3 = bp.as_mut().alloc_new_page(PageKind::BPlusInner, 3).unwrap();
        {
            let mut view = frame3.page_view();
            if let Page::BPlusInner(inner) = &mut view {
                inner.init(3, 1, 4); // PageId 3, Level 1, Key Size 4

                // Set first child to Page 1 (Left of key 50)
                inner.set_child_at(0, 1);

                // Insert Key 50, pointing to Page 2 (Right of key 50)
                inner.insert_at(0, &50u32.to_be_bytes(), 2);
            }
        }
        let fid3 = frame3.fid();
        bp.as_mut().unpin_frame(fid3).unwrap();

        // 2. Instantiate Tree and Query
        let mut tree = BPlusTree::new(bp.as_mut(), 3);

        // Case A: Find Key 10 (In Left Leaf)
        let res = tree.get_value(&10u32.to_be_bytes()).unwrap();
        assert_eq!(res, Some(100));

        // Case B: Find Key 50 (In Right Leaf)
        let res = tree.get_value(&50u32.to_be_bytes()).unwrap();
        assert_eq!(res, Some(500));

        // Case C: Find Key 60 (In Right Leaf)
        let res = tree.get_value(&60u32.to_be_bytes()).unwrap();
        assert_eq!(res, Some(600));

        // Case D: Non-existent key (In Left range)
        let res = tree.get_value(&15u32.to_be_bytes()).unwrap();
        assert_eq!(res, None);

        // Case E: Non-existent key (In Right range)
        let res = tree.get_value(&99u32.to_be_bytes()).unwrap();
        assert_eq!(res, None);

        let _ = fs::remove_file(&path);
    }
}
