use crate::storage::buffer::buffer_pool::BufferPoolCore;
use crate::storage::page::{
    self,
    base,
    directory::{Directory, DirectoryEntry},
};
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

    #[derive(Debug)]
    pub enum FindSpaceError {
        PageFetchError(buffer_pool::errors::FetchPageError),
        UnpinError(buffer_pool::errors::UnpinFrameError),
    }

    #[derive(Debug)]
    pub enum UpdateSpaceError {
        PageFetchError(buffer_pool::errors::FetchPageError),
        PageNotFoundError,
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

    /// Finds a page that has at least `required_space` bytes free.
    fn find_page_with_space(
        &mut self,
        required_space: u32,
        bp: Pin<&mut BufferPoolCore>,
    ) -> Result<Option<base::PageId>, errors::FindSpaceError>;

    /// Updates the recorded free space for a page.
    fn update_page_free_space(
        &mut self,
        page_id: base::PageId,
        new_free_space: u32,
        bp: Pin<&mut BufferPoolCore>,
    ) -> Result<(), errors::UpdateSpaceError>;
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

            if let page::base::Page::Directory(dir_page) = &mut page_view {
                let num_entries = dir_page.num_entries();
                for i in 0..num_entries {
                    let entry = dir_page.entry_at(i as usize).unwrap();
                    if entry.page_id == page_id {
                        let offset = entry.file_offset;
                        bp.as_mut().unpin_frame(curr_frame_id).ok();
                        return Ok(offset);
                    }
                }

                if let Some(next_page_id) = dir_page.next_directory_page_id() {
                    bp.as_mut().unpin_frame(curr_frame_id).ok();
                    curr_dir_offset = self.find_file_offset(next_page_id, bp.as_mut())?;
                } else {
                    bp.as_mut().unpin_frame(curr_frame_id).ok();
                    return Err(errors::FindOffsetError::PageNotFoundError);
                }
            } else {
                bp.as_mut().unpin_frame(curr_frame_id).ok();
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

            if let page::base::Page::Directory(dir_page) = &mut page_view {
                if dir_page.free_space() >= (Directory::ENTRY_SIZE as u32) {
                    dir_page
                        .add_entry(entry)
                        .map_err(|_| errors::RegisterPageError::AddEntryError)?;

                    bp.as_mut().mark_frame_dirty(curr_frame_id);
                    bp.as_mut()
                        .unpin_frame(curr_frame_id)
                        .map_err(|e| errors::RegisterPageError::UnpinError(e))?;
                    return Ok(());
                }

                if let Some(next_page_id) = dir_page.next_directory_page_id() {
                    bp.as_mut()
                        .unpin_frame(curr_frame_id)
                        .map_err(|e| errors::RegisterPageError::UnpinError(e))?;

                    curr_dir_offset =
                        self.find_file_offset(next_page_id, bp.as_mut())
                            .map_err(|_| {
                                errors::RegisterPageError::PageFetchError(Default::default())
                            })?;
                } else {
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

    fn find_page_with_space(
        &mut self,
        required_space: u32,
        mut bp: Pin<&mut BufferPoolCore>,
    ) -> Result<Option<base::PageId>, errors::FindSpaceError> {
        let mut curr_dir_offset = self.dir_page_1_offset;

        loop {
            let curr_frame = bp
                .as_mut()
                .fetch_page_at_offset(curr_dir_offset)
                .map_err(|e| errors::FindSpaceError::PageFetchError(e))?;

            let curr_frame_id = curr_frame.fid();
            let mut page_view = curr_frame.page_view();

            if let page::base::Page::Directory(dir_page) = &mut page_view {
                let num_entries = dir_page.num_entries();
                for i in 0..num_entries {
                    let entry = dir_page.entry_at(i as usize).unwrap();
                    // Skip directory pages (which are also registered usually)
                    // Ideally we check page kind, but here we rely on free_space being tracked for data pages
                    if entry.free_space >= required_space {
                        bp.as_mut().unpin_frame(curr_frame_id).ok();
                        return Ok(Some(entry.page_id));
                    }
                }

                if let Some(next_page_id) = dir_page.next_directory_page_id() {
                    bp.as_mut().unpin_frame(curr_frame_id).ok();
                    // Using find_file_offset might be slow if chain is long, but correct
                    match self.find_file_offset(next_page_id, bp.as_mut()) {
                        Ok(offset) => curr_dir_offset = offset,
                        Err(_) => return Ok(None), // Broken link or error
                    }
                } else {
                    bp.as_mut().unpin_frame(curr_frame_id).ok();
                    return Ok(None);
                }
            } else {
                bp.as_mut().unpin_frame(curr_frame_id).ok();
                return Err(errors::FindSpaceError::PageFetchError(Default::default()));
            }
        }
    }

    fn update_page_free_space(
        &mut self,
        page_id: base::PageId,
        new_free_space: u32,
        mut bp: Pin<&mut BufferPoolCore>,
    ) -> Result<(), errors::UpdateSpaceError> {
        let mut curr_dir_offset = self.dir_page_1_offset;

        loop {
            let curr_frame = bp
                .as_mut()
                .fetch_page_at_offset(curr_dir_offset)
                .map_err(|e| errors::UpdateSpaceError::PageFetchError(e))?;

            let curr_frame_id = curr_frame.fid();
            let mut page_view = curr_frame.page_view();

            if let page::base::Page::Directory(dir_page) = &mut page_view {
                let num_entries = dir_page.num_entries();
                for i in 0..num_entries {
                    let entry = dir_page.entry_at(i as usize).unwrap();
                    if entry.page_id == page_id {
                        dir_page.set_entry_free_space(i as usize, new_free_space);

                        bp.as_mut().mark_frame_dirty(curr_frame_id);
                        bp.as_mut().unpin_frame(curr_frame_id).ok();
                        return Ok(());
                    }
                }

                if let Some(next_page_id) = dir_page.next_directory_page_id() {
                    bp.as_mut().unpin_frame(curr_frame_id).ok();
                    match self.find_file_offset(next_page_id, bp.as_mut()) {
                        Ok(offset) => curr_dir_offset = offset,
                        Err(_) => return Err(errors::UpdateSpaceError::PageNotFoundError),
                    }
                } else {
                    bp.as_mut().unpin_frame(curr_frame_id).ok();
                    return Err(errors::UpdateSpaceError::PageNotFoundError);
                }
            } else {
                bp.as_mut().unpin_frame(curr_frame_id).ok();
                return Err(errors::UpdateSpaceError::PageFetchError(Default::default()));
            }
        }
    }
}
