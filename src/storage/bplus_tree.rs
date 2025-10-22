use crate::storage::buffer::buffer_pool::{self, BufferPool};
use crate::storage::page::{
    self,
    base::{self, PageId, PageKind},
    bplus_inner::BPlusInner,
    bplus_leaf::BPlusLeaf,
};
use std::num::NonZeroU64;
use std::pin::Pin;

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

    // pub fn find(
    //     &self,
    //     key: &[u8],
    //     mut bpm: Pin<&mut BufferPool>,
    // ) -> Result<Option<u64>, buffer_pool::errors::FetchPageError> {
    //     let mut current_page_id = self.root_page_id;

    //     loop {
    //         let mut bpm_ref = bpm.as_mut();

    //         let frame = bpm_ref.as_mut().fetch_page(current_page_id);
    //         let frame_id = frame.fid();
    //         let page_view = frame.page_view();
    //     }

    //     Ok("Done")
    // }
}
