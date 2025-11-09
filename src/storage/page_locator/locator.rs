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

    fn register_page(
        &mut self,
        page_id: page::base::PageId,
        file_offset: u64,
        free_space: u32,
        bp: Pin<&mut BufferPoolCore>,
    ) -> Result<(), errors::RegisterPageError>;
}

pub struct LLDirPageLocator {}

pub struct DirectoryPageLocator {
    first_dir_offset: u64,
}

impl DirectoryPageLocator {
    pub fn new() -> Self {
        Self {
            first_dir_offset: 0,
        }
    }
}

impl PageLocator for DirectoryPageLocator {
    fn find_file_offset(
        &mut self,
        page_id: page::base::PageId,
        mut bp: Pin<&mut BufferPoolCore>,
    ) -> Result<u64, errors::FindOffsetError> {
        let mut curr_dir_offset = self.first_dir_offset;

        loop {
            let curr_dir_frame = bp
                .as_mut()
                .fetch_page_at_offset(curr_dir_offset)
                .map_err(|_| errors::FindOffsetError::PageFetchError)?;

            let curr_frame_id = curr_dir_frame.fid();
            let mut page_view = curr_dir_frame.page_view();

            if let Page::Directory(page) = &mut page_view {
                let num_entries = page.num_entries();

                for i in 0..num_entries {
                    if let Some(entry_page_id) = page.entry_page_id(i as usize) {
                        if entry_page_id == page_id {
                            let offset = page
                                .entry_file_offset(i as usize)
                                .ok_or(errors::FindOffsetError::NotFound)?
                                .get();

                            bp.as_mut()
                                .unpin_frame(curr_frame_id)
                                .map_err(|_| errors::FindOffsetError::PageFetchError)?;

                            return Ok(offset);
                        }
                    }
                }

                if let Some(next_page_id) = page.next_directory_page_id() {
                    let mut next_offset = None;
                    for i in 0..num_entries {
                        if let Some(entry_page_id) = page.entry_page_id(i as usize) {
                            if entry_page_id == next_page_id {
                                next_offset = page.entry_file_offset(i as usize).map(|o| o.get());
                                break;
                            }
                        }
                    }

                    bp.as_mut()
                        .unpin_frame(curr_frame_id)
                        .map_err(|_| errors::FindOffsetError::PageFetchError)?;

                    if let Some(offset) = next_offset {
                        curr_dir_offset = offset;
                    } else {
                        return Err(errors::FindOffsetError::InvalidDirectory);
                    }
                } else {
                    bp.as_mut()
                        .unpin_frame(curr_frame_id)
                        .map_err(|_| errors::FindOffsetError::PageFetchError)?;
                    break;
                }
            } else {
                bp.as_mut()
                    .unpin_frame(curr_frame_id)
                    .map_err(|_| errors::FindOffsetError::PageFetchError)?;
                return Err(errors::FindOffsetError::InvalidDirectory);
            }
        }

        Err(errors::FindOffsetError::NotFound)
    }

    fn register_page(
        &mut self,
        page_id: page::base::PageId,
        file_offset: u64,
        free_space: u32,
        mut bp: Pin<&mut BufferPoolCore>,
    ) -> Result<(), errors::RegisterPageError> {
        use crate::storage::page::directory::{DirectoryEntry, errors::AddEntryError};

        let entry = DirectoryEntry {
            page_id,
            file_offset: NonZeroU64::new(file_offset)
                .ok_or(errors::RegisterPageError::InvalidOffset)?,
            free_space,
        };

        let mut curr_dir_offset = self.first_dir_offset;

        loop {
            let curr_dir_frame = bp
                .as_mut()
                .fetch_page_at_offset(curr_dir_offset)
                .map_err(|_| errors::RegisterPageError::PageFetchError)?;

            let curr_frame_id = curr_dir_frame.fid();
            let mut page_view = curr_dir_frame.page_view();

            if let Page::Directory(page) = &mut page_view {
                match page.add_entry(entry) {
                    Ok(()) => {
                        bp.as_mut().mark_frame_dirty(curr_frame_id);
                        bp.as_mut()
                            .unpin_frame(curr_frame_id)
                            .map_err(|_| errors::RegisterPageError::PageFetchError)?;
                        return Ok(());
                    }
                    Err(AddEntryError::InsufficientSpace) => {
                        if let Some(next_page_id) = page.next_directory_page_id() {
                            let num_entries = page.num_entries();
                            let mut next_offset = None;
                            for i in 0..num_entries {
                                if let Some(entry_page_id) = page.entry_page_id(i as usize) {
                                    if entry_page_id == next_page_id {
                                        next_offset =
                                            page.entry_file_offset(i as usize).map(|o| o.get());
                                        break;
                                    }
                                }
                            }

                            bp.as_mut()
                                .unpin_frame(curr_frame_id)
                                .map_err(|_| errors::RegisterPageError::PageFetchError)?;

                            if let Some(offset) = next_offset {
                                curr_dir_offset = offset;
                                continue;
                            } else {
                                return Err(errors::RegisterPageError::CorruptedDirectory);
                            }
                        } else {
                            bp.as_mut()
                                .unpin_frame(curr_frame_id)
                                .map_err(|_| errors::RegisterPageError::PageFetchError)?;
                            return Err(errors::RegisterPageError::DirectoryFull);
                        }
                    }
                }
            } else {
                bp.as_mut()
                    .unpin_frame(curr_frame_id)
                    .map_err(|_| errors::RegisterPageError::PageFetchError)?;
                return Err(errors::RegisterPageError::InvalidDirectory);
            }
        }
    }
}

pub mod errors {
    #[derive(Debug)]
    pub enum FindOffsetError {
        NotFound,
        PageFetchError,
        InvalidDirectory,
    }
    #[derive(Debug)]
    pub enum RegisterPageError {
        PageFetchError,
        InvalidDirectory,
        InvalidOffset,
        CorruptedDirectory,
        DirectoryFull,
    }
}
