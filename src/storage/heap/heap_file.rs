use super::row::RowId;
use crate::storage::buffer::BufferPool;
use crate::storage::page::{
    self,
    base::{PageId, PageKind},
};
use std::pin::Pin;
use std::sync::atomic::{AtomicU32, Ordering};

/// HeapFile is a collection of functions for managing row storage.
pub struct HeapFile;

#[derive(Debug)]
pub enum HeapError {
    FetchPage(String),
    AllocPage(String),
    UnpinPage(String),
    InvalidPage,
    AddSlot(String),
    RegisterPage(String),
}

impl HeapFile {
    /// Inserts raw byte data into a slotted page, returning a RowId.
    /// For this step, we always allocate a new page.
    pub fn insert(
        mut bpm: Pin<&mut BufferPool>,
        page_id_counter: &AtomicU32,
        data: &[u8],
    ) -> Result<RowId, HeapError> {
        let new_page_id = page_id_counter.fetch_add(1, Ordering::SeqCst) + 1;

        let frame = bpm
            .as_mut()
            .alloc_new_page(PageKind::SlottedData, new_page_id)
            .map_err(|e| HeapError::AllocPage(format!("{:?}", e)))?;

        let frame_id = frame.fid();
        let file_offset = frame.file_offset();
        let mut page_view = frame.page_view();

        let slot_num = if let page::base::Page::SlottedData(slotted_page) = &mut page_view {
            slotted_page.set_page_id(new_page_id);
            let slot_num = slotted_page
                .add_slot(data)
                .map_err(|e| HeapError::AddSlot(format!("{:?}", e)))?;

            let free_space = slotted_page.free_space();

            // We must unpin before calling register_page, as it will re-fetch pages.
            bpm.as_mut().mark_frame_dirty(frame_id);
            bpm.as_mut()
                .unpin_frame(frame_id)
                .map_err(|e| HeapError::UnpinPage(format!("{:?}", e)))?;

            // Now register this new page in the directory
            // We use the u32 page_id_counter for the AtomicU64 requirement
            let temp_page_id_counter_u64 = AtomicU64::new(new_page_id as u64);
            bpm.as_mut()
                .expand_directory_and_register(
                    new_page_id,
                    file_offset,
                    free_space,
                    &temp_page_id_counter_u64, // This is a temporary bridge
                )
                .map_err(|e| HeapError::RegisterPage(e))?;

            Ok(slot_num as u32)
        } else {
            bpm.as_mut()
                .unpin_frame(frame_id)
                .map_err(|e| HeapError::UnpinPage(format!("{:?}", e)))?;
            Err(HeapError::InvalidPage)
        }?;

        Ok(RowId::new(new_page_id, slot_num))
    }

    /// Retrieves raw byte data given a RowId.
    /// Note: This returns an owned Vec<u8> because the frame is unpinned.
    pub fn get(mut bpm: Pin<&mut BufferPool>, rid: RowId) -> Result<Vec<u8>, HeapError> {
        let page_id = rid.page_id();
        let slot_num = rid.slot_num() as usize;

        // Use the PageLocator to find the actual file offset
        let offset = {
            let (core, locator) = unsafe {
                let this = bpm.as_mut().get_unchecked_mut();
                (Pin::new_unchecked(&mut this.core), &mut this.page_locator)
            };
            locator
                .find_file_offset(page_id, core)
                .map_err(|e| HeapError::FetchPage(format!("PageLocator error: {:?}", e)))?
        };

        let frame = bpm
            .as_mut()
            .fetch_page_at_offset(offset)
            .map_err(|e| HeapError::FetchPage(format!("{:?}", e)))?;

        let frame_id = frame.fid();
        let mut page_view = frame.page_view();

        let data = if let page::base::Page::SlottedData(slotted_page) = &mut page_view {
            // Double-check we fetched the right page
            if slotted_page.page_id() != page_id {
                bpm.as_mut().unpin_frame(frame_id).ok(); // Best effort
                return Err(HeapError::InvalidPage);
            }
            slotted_page
                .slot_data(slot_num)
                .map(|bytes| bytes.to_vec()) // Copy the data
                .ok_or(HeapError::InvalidPage) // Slot not found
        } else {
            Err(HeapError::InvalidPage)
        };

        bpm.as_mut()
            .unpin_frame(frame_id)
            .map_err(|e| HeapError::UnpinPage(format!("{:?}", e)))?;

        data
    }
}
