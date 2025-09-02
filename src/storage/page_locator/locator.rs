use crate::storage::{
    buffer::buffer_pool::BufferPool,
    page::page_base::{self, Page},
};
use std::pin::Pin;

pub trait PageLocator {
    fn find_file_offset(
        &self,
        page_id: page_base::PageId,
        bp: Pin<&mut BufferPool>,
    ) -> Result<u64, errors::FindOffsetError>;
}

pub struct LLDirPageLocator {}

impl PageLocator for LLDirPageLocator {
    fn find_file_offset(
        &self,
        page_id: page_base::PageId,
        mut bp: Pin<&mut BufferPool>,
    ) -> Result<u64, errors::FindOffsetError> {
        let mut curr_dir_page = bp
            .as_mut()
            .fetch_page_at_offset(0u64)
            .map_err(|_| errors::FindOffsetError::PageFetchError)?;

        loop {
            let page_view = curr_dir_page.page_view();
            if let Page::Directory(page) = page_view {
                // search entries for page_id
                let num_entries = page.num_entries();
                for i in 0..num_entries {
                    if page.entry_page_id(i as usize).unwrap() == page_id {
                        return Ok(page.entry_file_offset(i as usize).unwrap().into());
                    }
                }

                // not found in this directory page, go to the next
                if let Some(next_page_id) = page.next_directory_page_id() {
                    curr_dir_page = bp
                        .as_mut()
                        .fetch_page(next_page_id)
                        .map_err(|_| errors::FindOffsetError::PageFetchError)?;
                } else {
                    // end of page directory linked list
                    break;
                }
            } else {
                return Err(errors::FindOffsetError::InvalidDirectory);
            }
        }

        Err(errors::FindOffsetError::NotFound)
    }
}

pub mod errors {
    #[derive(Debug)]
    pub enum FindOffsetError {
        NotFound,
        PageFetchError,
        InvalidDirectory,
    }
}
