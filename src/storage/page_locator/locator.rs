// src/storage/page_locator/locator.rs
use crate::storage::buffer::buffer_pool::{BufferPoolCore, errors as BPErrors};
use crate::storage::page::{
    self,
    base::{self, DiskPage, PageId},
    directory::{Directory, DirectoryEntry},
};
use std::num::NonZeroU64;
use std::pin::Pin;

pub mod errors {
    use crate::storage::buffer::buffer_pool;

    #[derive(Debug)]
    pub enum FindOffsetError {
        PageFetchError(buffer_pool::errors::FetchPageError),
        PageNotFoundError,
        UnpinError(buffer_pool::errors::UnpinFrameError),
    }
    impl Default for FindOffsetError {
        fn default() -> Self {
            FindOffsetError::PageFetchError(buffer_pool::errors::FetchPageError::default())
        }
    }
    #[derive(Debug)]
    pub enum RegisterPageError {
        DirectoryFull,
        InvalidOffset,
        PageFetchError(buffer_pool::errors::FetchPageError),
        AddEntryError,
        UnpinError(buffer_pool::errors::UnpinFrameError),
    }
}

pub trait PageLocator {
    /// Finds the physical file offset for a given logical page ID

    fn find_file_offset(
        &mut self,
        page_id: base::PageId,
        bp: Pin<&mut BufferPoolCore>,
    ) -> Result<u64, errors::FindOffsetError>;

    /// Registers a new logical page ID with its physical file offset

    fn register_page(
        &mut self,
        page_id: base::PageId,
        file_offset: u64,
        free_space: u32,
        bp: Pin<&mut BufferPoolCore>,
    ) -> Result<(), errors::RegisterPageError>;
}

pub struct DirectoryPageLocator {
    dir_page_1_offset: u64,
}

impl DirectoryPageLocator {
    pub fn new() -> Self {
        Self {
            dir_page_1_offset: 0, // Offset 0 is always the first directory page
        }
    }
}

impl PageLocator for DirectoryPageLocator {
    fn find_file_offset(
        &mut self,
        page_id: base::PageId,
        mut bp: Pin<&mut BufferPoolCore>,
    ) -> Result<u64, errors::FindOffsetError> {
        let mut curr_dir_offset = self.dir_page_1_offset;

        if page_id == 0 {
            return Err(errors::FindOffsetError::PageNotFoundError);
        }

        loop {
            let curr_frame = bp
                .as_mut()
                .fetch_page_at_offset(curr_dir_offset)
                .map_err(|e| errors::FindOffsetError::PageFetchError(e))?;

            let curr_frame_id = curr_frame.fid();
            let mut page_view = curr_frame.page_view();

            use crate::storage::page::base::DiskPage;
            if let page::base::Page::Directory(dir_page) = &mut page_view {
                let num_entries = dir_page.num_entries();
                for i in 0..num_entries {
                    // Use new entry_at() method
                    let entry = dir_page.entry_at(i as usize).unwrap();
                    if entry.page_id == page_id {
                        let offset = entry.file_offset;
                        bp.as_mut().unpin_frame(curr_frame_id).ok(); // Ignore unpin error on read
                        return Ok(offset);
                    }
                }

                // Not in this page, check next
                if let Some(next_page_id) = dir_page.next_directory_page_id() {
                    bp.as_mut().unpin_frame(curr_frame_id).ok(); // Ignore unpin error on read

                    // We must re-call find_file_offset to get the offset of the *next directory page*
                    curr_dir_offset = self.find_file_offset(next_page_id, bp.as_mut())?;
                } else {
                    // This is the last directory page and we didn't find it
                    bp.as_mut().unpin_frame(curr_frame_id).ok(); // Ignore unpin error on read
                    return Err(errors::FindOffsetError::PageNotFoundError);
                }
            } else {
                // Page at offset was not a directory
                bp.as_mut().unpin_frame(curr_frame_id).ok(); // Ignore unpin error on read
                return Err(errors::FindOffsetError::PageFetchError(Default::default()));
            }
        }
    }

    fn register_page(
        &mut self,
        page_id: base::PageId,
        file_offset: u64,
        free_space: u32,
        mut bp: Pin<&mut BufferPoolCore>,
    ) -> Result<(), errors::RegisterPageError> {
        if file_offset == 0 && page_id != 1 {
            // Offset 0 is reserved for the first directory page (PageId 1)
            return Err(errors::RegisterPageError::InvalidOffset);
        }

        let entry = DirectoryEntry {
            page_id,
            file_offset,
            free_space,
        };

        let mut curr_dir_offset = self.dir_page_1_offset;

        loop {
            let curr_frame = bp
                .as_mut()
                .fetch_page_at_offset(curr_dir_offset)
                .map_err(|e| errors::RegisterPageError::PageFetchError(e))?;

            let curr_frame_id = curr_frame.fid();
            let mut page_view = curr_frame.page_view();

            use crate::storage::page::base::DiskPage;
            if let page::base::Page::Directory(dir_page) = &mut page_view {
                if dir_page.free_space() >= (Directory::ENTRY_SIZE as u32) {
                    // Found space, add the entry
                    dir_page
                        .add_entry(entry)
                        .map_err(|_| errors::RegisterPageError::AddEntryError)?;

                    bp.as_mut().mark_frame_dirty(curr_frame_id);
                    bp.as_mut()
                        .unpin_frame(curr_frame_id)
                        .map_err(|e| errors::RegisterPageError::UnpinError(e))?;
                    return Ok(());
                }

                // No space, move to next directory page
                if let Some(next_page_id) = dir_page.next_directory_page_id() {
                    bp.as_mut()
                        .unpin_frame(curr_frame_id)
                        .map_err(|e| errors::RegisterPageError::UnpinError(e))?;

                    // Recursively find offset of next directory page
                    curr_dir_offset =
                        self.find_file_offset(next_page_id, bp.as_mut())
                            .map_err(|_| {
                                errors::RegisterPageError::PageFetchError(Default::default())
                            })?;
                } else {
                    // This is the last directory page and it's full
                    bp.as_mut()
                        .unpin_frame(curr_frame_id)
                        .map_err(|e| errors::RegisterPageError::UnpinError(e))?;
                    return Err(errors::RegisterPageError::DirectoryFull);
                }
            } else {
                bp.as_mut()
                    .unpin_frame(curr_frame_id)
                    .map_err(|e| errors::RegisterPageError::UnpinError(e))?;
                return Err(errors::RegisterPageError::PageFetchError(Default::default()));
            }
        }
    }
}
