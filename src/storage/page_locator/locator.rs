use crate::storage::{
    buffer::buffer_pool::BufferPoolCore,
    page::{self, Directory, base::Page},
};
use std::{num::NonZeroU64, pin::Pin};

pub trait PageLocator {
    fn find_file_offset(
        &mut self,
        page_id: page::base::PageId,
        bp: Pin<&mut BufferPoolCore>,
    ) -> Result<u64, errors::FindOffsetError>;
}

pub struct LLDirPageLocator {}

impl PageLocator for LLDirPageLocator {
    fn find_file_offset(
        &mut self,
        page_id: page::base::PageId,
        mut bp: Pin<&mut BufferPoolCore>,
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
                if let Some(_) = page.next_directory_page_id() {
                    let next_page_offset = self.next_page_offset(&page);
                    curr_dir_page = bp
                        .as_mut()
                        .fetch_page_at_offset(next_page_offset)
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

impl LLDirPageLocator {
    fn next_page_offset(&self, page: &Directory) -> u64 {
        let next_page_id = page
            .next_directory_page_id()
            .expect("next_page_id to be present");

        let num_entries = page.num_entries();
        for i in 0..num_entries {
            if page.entry_page_id(i as usize).unwrap() == next_page_id {
                return page.entry_file_offset(i as usize).unwrap().get();
            }
        }

        // The offset of the next page is guaranteed to be in this page
        unreachable!()
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
