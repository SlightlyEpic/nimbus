use crate::constants;
use crate::storage::page::base::{self, DiskPage, PageId};
use crate::storage::page::header::PageHeader;
use std::num::NonZeroU64;

// Stores the mapping from page_id -> file_offset
// Directory pages form a linked list
pub struct Directory<'a> {
    raw: &'a mut base::PageBuf,
}

#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub struct DirectoryEntry {
    pub page_id: base::PageId, // 4 bytes
    pub file_offset: u64,      // 8 bytes (0 means invalid/NonZero)
    pub free_space: u32,       // 4 bytes
}

impl<'a> DiskPage for Directory<'a> {
    const PAGE_KIND: u8 = base::PageKind::Directory as u8;
    const DATA_START: usize = PageHeader::SIZE; // Data starts after the header

    fn raw(self: &Self) -> &[u8; constants::storage::PAGE_SIZE] {
        return &self.raw;
    }

    fn raw_mut(&mut self) -> &mut [u8; constants::storage::PAGE_SIZE] {
        return &mut self.raw;
    }
}

impl<'a> Directory<'a> {
    // Bytes:   | +0        | +1        | +2        | +3        |
    // ---------+-----------+-----------+-----------+-----------|
    // 0..31    |              PageHeader (32 bytes)            |
    //          | (page_kind = Directory)                       |
    //          | (num_entries = N)                             |
    //          | (next_page_id = P)                            |
    // ---------+-----------+-----------+-----------+-----------|
    // 32..47   | Entry 0 (page_id: u32, offset: u64, free: u32)|
    // ---------+-----------+-----------+-----------+-----------|
    // 48..63   | Entry 1 (page_id: u32, offset: u64, free: u32)|
    // ---------+-----------+-----------+-----------+-----------|
    // ...      | (Entry array grows downwards)                 |
    // ---------+-----------------------------------------------|
    //          |          <<< FREE SPACE >>>                   |
    // ---------+-----------------------------------------------|
    // 4095     | (End of Page)                                 |
    // ---------------------------------------------------------|

    pub const ENTRY_SIZE: usize = std::mem::size_of::<DirectoryEntry>(); // 16 bytes

    /// Creates a new Directory page view from a raw buffer.
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
        let data_start = Self::DATA_START as u16;
        let num_entries = self.num_entries() as u16;
        let data_end = data_start + (num_entries * Self::ENTRY_SIZE as u16);
        self.header().free_space(data_end)
    }

    /// Gets the PageId of the next directory page, if any.
    pub fn next_directory_page_id(&self) -> Option<base::PageId> {
        let id = self.header().next_page_id();
        if id == 0 { None } else { Some(id) }
    }

    /// Gets the number of entries from the header.
    pub fn num_entries(&self) -> u16 {
        self.header().num_entries()
    }

    /// Gets a pointer to the entry at the given index.
    fn entry_ptr(&self, idx: usize) -> *const DirectoryEntry {
        let base = Self::DATA_START + idx * Self::ENTRY_SIZE;
        unsafe { self.raw.as_ptr().add(base) as *const DirectoryEntry }
    }

    /// Gets a mutable pointer to the entry at the given index.
    fn entry_ptr_mut(&mut self, idx: usize) -> *mut DirectoryEntry {
        let base = Self::DATA_START + idx * Self::ENTRY_SIZE;
        unsafe { self.raw.as_mut_ptr().add(base) as *mut DirectoryEntry }
    }

    pub fn entry_page_id(&self, idx: usize) -> Option<base::PageId> {
        if idx >= self.num_entries() as usize {
            return None;
        }
        unsafe { Some(PageId::from_le((*self.entry_ptr(idx)).page_id)) }
    }

    pub fn entry_file_offset(&self, idx: usize) -> Option<NonZeroU64> {
        if idx >= self.num_entries() as usize {
            return None;
        }
        unsafe { NonZeroU64::new(u64::from_le((*self.entry_ptr(idx)).file_offset)) }
    }

    pub fn entry_free_space(&self, idx: usize) -> Option<u32> {
        if idx >= self.num_entries() as usize {
            return None;
        }
        unsafe { Some(u32::from_le((*self.entry_ptr(idx)).free_space)) }
    }

    // === Indirect Getters ===

    /// Gets a copy of the entry at the given index.
    pub fn entry_at(&self, idx: usize) -> Option<DirectoryEntry> {
        if idx >= self.num_entries() as usize {
            return None;
        }
        unsafe {
            let entry = *self.entry_ptr(idx);
            Some(DirectoryEntry {
                page_id: PageId::from_le(entry.page_id),
                file_offset: u64::from_le(entry.file_offset),
                free_space: u32::from_le(entry.free_space),
            })
        }
    }

    // === Direct Setters ===

    /// Sets the PageId in the header.
    pub fn set_page_id(&mut self, id: base::PageId) {
        self.header_mut().set_page_id(id);
    }

    /// Sets the PageId of the next directory page in the header.
    pub fn set_next_directory_page_id(&mut self, id: Option<base::PageId>) {
        self.header_mut().set_next_page_id(id.unwrap_or(0));
    }

    /// Sets the free space value for a given entry.
    pub fn set_entry_free_space(&mut self, idx: usize, free: u32) {
        if idx >= self.num_entries() as usize {
            panic!("set_entry_free_space: index out of bounds");
        }
        unsafe {
            (*self.entry_ptr_mut(idx)).free_space = free.to_le();
        }
    }

    // === Indirect setters ===

    /// Adds a new entry to the end of the entry list.
    pub fn add_entry(&mut self, entry: DirectoryEntry) -> Result<(), errors::AddEntryError> {
        if self.free_space() < Self::ENTRY_SIZE as u32 {
            return Err(errors::AddEntryError::InsufficientSpace);
        }

        let num_entries = self.num_entries();

        unsafe {
            *self.entry_ptr_mut(num_entries as usize) = DirectoryEntry {
                page_id: entry.page_id.to_le(),
                file_offset: entry.file_offset.to_le(),
                free_space: entry.free_space.to_le(),
            };
        }

        self.header_mut().set_num_entries(num_entries + 1);
        Ok(())
    }

    /// Swaps the entry at `idx` with the last entry.
    fn swap_entries(&mut self, idx: usize, last_idx: usize) {
        assert!(idx < last_idx, "swap_entries: invalid indexes");
        unsafe {
            let last_entry = *self.entry_ptr(last_idx);
            *self.entry_ptr_mut(idx) = last_entry;
        }
    }

    /// Removes an entry by swapping it with the last entry and decrementing the count.
    pub fn remove_entry_at(&mut self, idx: usize) -> Result<(), errors::RemoveEntryError> {
        let num_entries = self.num_entries() as usize;
        if idx >= num_entries {
            return Err(errors::RemoveEntryError::IndexOutOfBounds);
        }

        let last_idx = num_entries - 1;
        if idx != last_idx {
            // Swap with the last entry
            self.swap_entries(idx, last_idx);
        }

        // Just decrementing the count effectively removes the last slot.
        // We don't bother zeroing out the old data.
        self.header_mut().set_num_entries(last_idx as u16);
        Ok(())
    }
}

pub mod errors {
    #[derive(Debug)]
    pub enum AddEntryError {
        InsufficientSpace,
    }

    #[derive(Debug)]
    pub enum RemoveEntryError {
        IndexOutOfBounds,
    }
}
