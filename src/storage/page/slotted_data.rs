use crate::constants;
use crate::storage::page::base::DiskPage;
use crate::storage::page::{base, header::PageHeader};

pub struct SlottedData<'a> {
    raw: &'a mut base::PageBuf,
}

impl<'a> base::DiskPage for SlottedData<'a> {
    const PAGE_KIND: u8 = base::PageKind::SlottedData as u8;
    const DATA_START: usize = PageHeader::SIZE; // Data starts after the header

    fn raw(self: &Self) -> &[u8; constants::storage::PAGE_SIZE] {
        return &self.raw;
    }

    fn raw_mut(&mut self) -> &mut [u8; constants::storage::PAGE_SIZE] {
        return &mut self.raw;
    }
}

impl<'a> SlottedData<'a> {
    // Bytes:   | +0        | +1        | +2        | +3        |
    // ---------+-----------+-----------+-----------+-----------|
    // 0..31    |            PageHeader (32 bytes)              |
    // ---------+-----------+-----------+-----------+-----------|
    // 32..35   | Slot 0 offset(u16) | Slot 0 len(u16)          |
    // ---------+-----------+-----------+-----------+-----------|
    // 36..39   | Slot 1 offset(u16) | Slot 1 len(u16)          |
    // ---------+-----------+-----------+-----------+-----------|
    // ...      | (Slot array grows downwards)                  |
    // ---------+-----------------------------------------------|
    //          |          <<< FREE SPACE >>>                   |
    //          | (Gap between end of slot array and fsp)       |
    // ---------+-----------------------------------------------|
    // ...      | (Data grows upwards from the end)             |
    // ---------+-----------+-----------+-----------+-----------|
    // (fsp)..  |              Data for Slot 1                  |
    // ---------+-----------+-----------+-----------+-----------|
    // ...      |              Data for Slot 0                  |
    // ---------+-----------+-----------+-----------+-----------|
    // 4095     | (End of Page)                                 |
    // ---------------------------------------------------------|
    pub const SLOT_META_SIZE: usize = 4; // u16 offset + u16 len

    /// Creates a new SlottedData page view from a raw buffer.
    pub fn new<'b: 'a>(raw: &'b mut base::PageBuf) -> Self {
        Self { raw }
    }

    // === Direct Getters ===

    /// Gets the PageId from the header.
    pub fn page_id(&self) -> base::PageId {
        self.header().page_id()
    }

    /// Calculates the amount of free space.
    pub fn free_space(&self) -> u32 {
        let num_slots = self.num_slots() as usize;
        let meta_end = Self::DATA_START + (num_slots * Self::SLOT_META_SIZE);
        let data_start = self.header().free_space_pointer() as usize;

        if data_start < meta_end {
            0 // Should not happen
        } else {
            (data_start - meta_end) as u32
        }
    }

    /// Gets the number of slots from the header.
    pub fn num_slots(&self) -> u16 {
        self.header().num_entries()
    }

    /// Gets the raw pointer to the slot's offset metadata.
    fn slot_offset_ptr(&self, idx: usize) -> *const u16 {
        let base = Self::DATA_START + Self::SLOT_META_SIZE * idx;
        unsafe { self.raw.as_ptr().add(base) as *const u16 }
    }

    /// Gets the raw pointer to the slot's length metadata.
    fn slot_len_ptr(&self, idx: usize) -> *const u16 {
        let base = Self::DATA_START + Self::SLOT_META_SIZE * idx + 2;
        unsafe { self.raw.as_ptr().add(base) as *const u16 }
    }

    /// Gets the offset of the data for a given slot.
    pub fn slot_offset(&self, idx: usize) -> Option<u16> {
        if idx >= self.num_slots() as usize {
            return None;
        }
        unsafe { Some(u16::from_le(*self.slot_offset_ptr(idx))) }
    }

    /// Gets the size of the data for a given slot.
    pub fn slot_size(&self, idx: usize) -> Option<u16> {
        if idx >= self.num_slots() as usize {
            return None;
        }
        unsafe { Some(u16::from_le(*self.slot_len_ptr(idx))) }
    }

    // === Indirect Getters ===

    /// Gets an immutable slice to the data in the specified slot.
    pub fn slot_data(&self, idx: usize) -> Option<&[u8]> {
        let offset = self.slot_offset(idx)? as usize;
        let size = self.slot_size(idx)? as usize;

        // Check for tombstone (len 0) or invalid offset
        if size == 0 || offset == 0 {
            return None;
        }
        Some(&self.raw[offset..offset + size])
    }

    /// Gets a mutable slice to the data in the specified slot.
    pub fn slot_data_mut(&mut self, idx: usize) -> Option<&mut [u8]> {
        let offset = self.slot_offset(idx)? as usize;
        let size = self.slot_size(idx)? as usize;

        // Check for tombstone (len 0) or invalid offset
        if size == 0 || offset == 0 {
            return None;
        }
        Some(&mut self.raw[offset..offset + size])
    }

    // === Direct Setters ===

    /// Sets the PageId in the header.
    pub fn set_page_id(&mut self, id: base::PageId) {
        self.header_mut().set_page_id(id);
    }

    /// Writes the slot's offset metadata
    fn set_slot_offset_unchecked(&mut self, idx: usize, offset: u16) {
        let base = Self::DATA_START + Self::SLOT_META_SIZE * idx;
        unsafe {
            let ptr = self.raw.as_mut_ptr().add(base) as *mut u16;
            *ptr = offset.to_le();
        }
    }

    /// Writes the slot's size metadata
    fn set_slot_size_unchecked(&mut self, idx: usize, size: u16) {
        let base = Self::DATA_START + Self::SLOT_META_SIZE * idx + 2;
        unsafe {
            let ptr = self.raw.as_mut_ptr().add(base) as *mut u16;
            *ptr = size.to_le();
        }
    }

    // === Indirect Setters ===

    /// Adds a new data slot to the page.
    /// Returns the slot index (u16) if successful.
    pub fn add_slot(&mut self, data: &[u8]) -> Result<u16, errors::AddSlotError> {
        let data_len = data.len();
        if data_len == 0 {
            return Err(errors::AddSlotError::DataEmpty);
        }

        let total_needed = data_len + Self::SLOT_META_SIZE;
        if self.free_space() < total_needed as u32 {
            return Err(errors::AddSlotError::InsufficientSpace);
        }

        let num_slots = self.num_slots();
        let free_ptr = self.header().free_space_pointer();
        let new_data_offset = free_ptr - data_len as u16;

        // Write the data itself (from the end of the page)
        self.raw[new_data_offset as usize..free_ptr as usize].copy_from_slice(data);

        // Write the slot metadata (from the start of the data area)
        self.set_slot_offset_unchecked(num_slots as usize, new_data_offset);
        self.set_slot_size_unchecked(num_slots as usize, data_len as u16);

        // Update header
        self.header_mut().set_num_entries(num_slots + 1);
        self.header_mut().set_free_space_pointer(new_data_offset);

        Ok(num_slots)
    }

    /// Removes a slot by swapping it with the last slot.
    /// Note: This does not reclaim the data space (no compaction).
    pub fn remove_slot_at(&mut self, idx: usize) -> Result<(), errors::RemoveSlotError> {
        let num_slots = self.num_slots();
        if idx >= num_slots as usize {
            return Err(errors::RemoveSlotError::IndexOutOfBounds);
        }

        if idx != (num_slots - 1) as usize {
            // Not the last slot, so swap with last
            let last_slot_offset = self.slot_offset(num_slots as usize - 1).unwrap();
            let last_slot_size = self.slot_size(num_slots as usize - 1).unwrap();

            self.set_slot_offset_unchecked(idx, last_slot_offset);
            self.set_slot_size_unchecked(idx, last_slot_size);
        }

        // Update header (simply decrementing count effectively removes the last slot)
        self.header_mut().set_num_entries(num_slots - 1);

        // TODO: Add compaction logic to reclaim free_space_pointer
        // For now, space is "lost" until the page is rebuilt.

        Ok(())
    }
}

pub mod errors {
    #[derive(Debug)]
    pub enum AddSlotError {
        InsufficientSpace,
        DataEmpty,
    }

    #[derive(Debug)]
    pub enum RemoveSlotError {
        IndexOutOfBounds,
    }
}
