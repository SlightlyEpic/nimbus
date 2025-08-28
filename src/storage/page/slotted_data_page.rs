use crate::constants;
use crate::storage::page::page_base;

// Slotted page implementation
pub struct SlottedDataPage<'a> {
    raw: &'a mut page_base::PageBuf,
}

impl<'a> page_base::DiskPage for SlottedDataPage<'a> {
    const PAGE_KIND: u8 = page_base::PageKind::SlottedData as u8;

    fn raw(self: &Self) -> &[u8; constants::storage::DISK_PAGE_SIZE] {
        return &self.raw;
    }

    fn raw_mut(&mut self) -> &mut [u8; constants::storage::DISK_PAGE_SIZE] {
        return &mut self.raw;
    }
}

impl<'a> SlottedDataPage<'a> {
    // === Memory layout ===
    //   0..  1 -> Page Kind  (u8)         -|
    //   4..  8 -> Free space (u32)         | Header (64 bytes)
    //   8.. 16 -> Page Id    (u64)         |
    //  16.. 64 -> Reserved for future use -|
    //  64.. 66 -> # of slots (u16)
    //  66.. 68 -> Slot offset #1 (u16)
    //  68.. 70 -> Slot #1 len (u16)
    //  ...(slot offsets, slot lengths)
    //  ...data (from the end)

    const fn new<'b: 'a>(raw: &'b mut page_base::PageBuf) -> Self {
        let mut page = Self { raw };
        page.set_page_kind(page_base::PageKind::SlottedData);
        page.set_free_space(constants::storage::DISK_PAGE_SIZE as u32 - 64 - 2);

        page
    }

    // === Direct Getters ===

    pub const fn page_kind(&self) -> u8 {
        self.raw[0]
    }

    pub const fn free_space(&self) -> u32 {
        unsafe {
            let ptr = self.raw.as_ptr().add(4) as *const u32;
            u32::from_le(*ptr)
        }
    }

    pub const fn page_id(&self) -> page_base::PageId {
        unsafe {
            let ptr = self.raw.as_ptr().add(8) as *const u64;
            let val = u64::from_le(*ptr);
            page_base::PageId::new(val).unwrap()
        }
    }

    pub const fn num_slots(&self) -> u16 {
        unsafe {
            let ptr = self.raw.as_ptr().add(64) as *const u16;
            *ptr
        }
    }

    pub const fn slot_offset(&self, idx: usize) -> Option<u16> {
        let num_slots = self.num_slots() as usize;
        if idx >= num_slots {
            return None;
        }
        let base = 66 + 4 * idx;
        unsafe {
            let ptr = self.raw.as_ptr().add(base) as *const u16;
            Some(*ptr)
        }
    }

    pub const fn slot_size(&self, idx: usize) -> Option<u16> {
        let num_slots = self.num_slots() as usize;
        if idx >= num_slots {
            return None;
        }
        let base = 66 + 4 * idx + 2;
        unsafe {
            let ptr = self.raw.as_ptr().add(base) as *const u16;
            Some(*ptr)
        }
    }

    // === Indirect Getters ===

    pub fn slot_data(&self, idx: usize) -> Option<&[u8]> {
        let num_slots = self.num_slots() as usize;
        if idx >= num_slots {
            return None;
        }
        let offset = unsafe { self.slot_offset(idx).unwrap_unchecked() } as usize;
        let size = unsafe { self.slot_size(idx).unwrap_unchecked() } as usize;
        Some(&self.raw[offset..offset + size])
    }

    pub fn slot_data_mut(&mut self, idx: usize) -> Option<&mut [u8]> {
        let num_slots = self.num_slots() as usize;
        if idx >= num_slots {
            return None;
        }
        let offset = unsafe { self.slot_offset(idx).unwrap_unchecked() } as usize;
        let size = unsafe { self.slot_size(idx).unwrap_unchecked() } as usize;
        Some(&mut self.raw[offset..offset + size])
    }

    // === Direct Setters ===

    const fn set_page_kind(&mut self, kind: page_base::PageKind) {
        self.raw[0] = kind as u8;
    }

    const fn set_free_space(&mut self, free: u32) {
        unsafe {
            let ptr = self.raw.as_mut_ptr().add(4) as *mut u32;
            *ptr = free.to_le();
        }
    }

    pub const fn set_page_id(&mut self, id: page_base::PageId) {
        unsafe {
            let ptr = self.raw.as_mut_ptr().add(8) as *mut u64;
            *ptr = id.get().to_le();
        }
    }

    const fn set_num_slots(&mut self, num_slots: u16) {
        unsafe {
            let ptr = self.raw.as_mut_ptr().add(64) as *mut u16;
            *ptr = num_slots.to_le();
        }
    }

    const fn set_slot_offset_unchecked(&mut self, idx: usize, offset: u16) {
        let base = 66 + 4 * idx;
        unsafe {
            let ptr = self.raw.as_mut_ptr().add(base) as *mut u16;
            *ptr = offset.to_le();
        }
    }

    const fn set_slot_size_unchecked(&mut self, idx: usize, size: u16) {
        let base = 66 + 4 * idx + 2;
        unsafe {
            let ptr = self.raw.as_mut_ptr().add(base) as *mut u16;
            *ptr = size.to_le();
        }
    }

    // === Indirect Setters ===

    /// Result<slot_index, error>
    pub const fn add_slot(&mut self, data: &[u8]) -> Result<u16, errors::AddSlotError> {
        let free_space = self.free_space() as usize;
        let data_len = data.len();
        if free_space < data_len + 4 {
            return Err(errors::AddSlotError::InsufficientSpace);
        }

        let num_slots = self.num_slots() as usize;
        let last_offset = match num_slots {
            0 => constants::storage::DISK_PAGE_SIZE,
            _ => unsafe { self.slot_offset(num_slots - 1).unwrap_unchecked() as usize },
        };
        let slot_offset = last_offset - data_len;

        self.set_num_slots(num_slots as u16 + 1);
        self.set_free_space((free_space - data_len - 4) as u32);
        self.set_slot_offset_unchecked(num_slots, slot_offset as u16);
        self.set_slot_size_unchecked(num_slots, data_len as u16);

        Ok(num_slots as u16)
    }

    pub const fn remove_slot_at(&mut self, idx: usize) -> Result<(), errors::RemoveSlotError> {
        let num_slots = self.num_slots();
        if idx >= num_slots as usize {
            return Err(errors::RemoveSlotError::IndexOutOfBounds);
        }

        let rm_slot_offset = unsafe { self.slot_offset(idx).unwrap_unchecked() as usize };
        let rm_slot_size = unsafe { self.slot_size(idx).unwrap_unchecked() as usize };
        unsafe {
            std::ptr::write_bytes(
                self.raw.as_mut_ptr().add(rm_slot_offset as usize),
                0,
                rm_slot_size as usize,
            );
        }

        if idx != num_slots as usize - 1 {
            // Move slot data
            let last_slot_offset =
                unsafe { self.slot_offset(num_slots as usize - 1).unwrap_unchecked() as usize };
            let combined_data_size = rm_slot_offset - last_slot_offset;
            unsafe {
                std::ptr::copy(
                    self.raw.as_mut_ptr().add(last_slot_offset as usize),
                    self.raw.as_mut_ptr().add(last_slot_offset + rm_slot_size),
                    combined_data_size,
                )
            }

            // Move slot offsets and sizes
            let slot_meta_start = 66 + idx * 4 + 4;
            let slot_meta_end = 66 + idx * num_slots as usize + 4;
            let slot_meta_size = slot_meta_end - slot_meta_start;
            unsafe {
                std::ptr::copy(
                    self.raw.as_mut_ptr().add(slot_meta_start),
                    self.raw.as_mut_ptr().add(slot_meta_start - 4),
                    slot_meta_size,
                );
            }
        }

        Ok(())
    }
}

pub mod errors {
    pub enum AddSlotError {
        InsufficientSpace,
    }

    pub enum RemoveSlotError {
        IndexOutOfBounds,
    }
}
