use crate::constants;
use crate::storage::page::page_base::{self, DiskPage};
use std::num::NonZeroU64;

// Stores the mapping from page_id -> file_offset
// Directory pages form a linked list
pub struct DirectoryPage {
    raw: [u8; constants::storage::DISK_PAGE_SIZE],
}

#[derive(Clone, Copy)]
pub struct DirectoryPageEntry {
    pub page_id: page_base::PageId,
    pub file_offset: NonZeroU64,
    pub free_space: u32,
}

impl DiskPage for DirectoryPage {
    const PAGE_KIND: u8 = page_base::PageKind::Directory as u8;

    fn raw(self: &Self) -> &[u8; constants::storage::DISK_PAGE_SIZE] {
        return &self.raw;
    }

    fn raw_mut(&mut self) -> &mut [u8; constants::storage::DISK_PAGE_SIZE] {
        return &mut self.raw;
    }
}

impl DirectoryPage {
    // === Memory layout ===
    //   0..  1 -> Page Kind  (u8)         -|
    //   4..  8 -> Free space (u32)         | Header (64 bytes)
    //   8.. 16 -> Page Id    (u64)         |
    //  16.. 64 -> Reserved for future use -|
    //  64.. 72 -> Next directory page's page id (Option<NonZeroU64>) -|
    //  72.. 74 -> # of entries (u16)                                  | (16 bytes)
    //  74.. 80 -> Empty                                              -|
    //  80.. 88 -> Page Id (NonZeroU64)     -|
    //  88.. 96 -> File offset (NonZeroU64)  | Directory entries (32 bytes)
    //  96..100 -> Free space (u32)          |
    //  100..111 -> Empty                   -|
    //  ...entries

    const ENTRY_SIZE: usize = 32;

    // Notes:
    // - Assumes 4K alignment for self.raw, might cause unexpected behaviour otherwise
    // - If a directory page has a next page, it's entries are guaranteed to contain the offset for that page
    // - Could add [#inline] to getters and setters. Look into it later.

    const fn new() -> Self {
        let mut page = Self {
            raw: [0u8; constants::storage::DISK_PAGE_SIZE],
        };
        page.set_page_kind(page_base::PageKind::Directory);
        page.set_free_space(
            constants::storage::DISK_PAGE_SIZE as u32
            - 64 // header
            - 16, // other fields
        );

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

    pub const fn next_directory_page_id(&self) -> Option<page_base::PageId> {
        unsafe {
            let ptr = self.raw.as_ptr().add(64) as *const u64;
            let val = u64::from_le(*ptr);
            page_base::PageId::new(val)
        }
    }

    pub const fn num_entries(&self) -> u16 {
        unsafe {
            let ptr = self.raw.as_ptr().add(72) as *const u16;
            u16::from_le(*ptr)
        }
    }

    pub const fn entry_page_id(&self, idx: usize) -> Option<page_base::PageId> {
        if idx >= self.num_entries() as usize {
            return None;
        }
        let base = 80 + idx * Self::ENTRY_SIZE;
        unsafe {
            let ptr = self.raw.as_ptr().add(base) as *const u64;
            let val = u64::from_le(*ptr);
            page_base::PageId::new(val)
        }
    }

    pub const fn entry_file_offset(&self, idx: usize) -> Option<NonZeroU64> {
        if idx >= self.num_entries() as usize {
            return None;
        }
        let base = 80 + idx * Self::ENTRY_SIZE + 8;
        unsafe {
            let ptr = self.raw.as_ptr().add(base) as *const u64;
            let val = u64::from_le(*ptr);
            NonZeroU64::new(val)
        }
    }

    pub const fn entry_free_space(&self, idx: usize) -> Option<u32> {
        if idx >= self.num_entries() as usize {
            return None;
        }
        let base = 80 + idx * Self::ENTRY_SIZE + 16;
        unsafe {
            let ptr = self.raw.as_ptr().add(base) as *const u32;
            Some(u32::from_le(*ptr))
        }
    }

    // === Indirect Getters ===

    pub const fn entry_at(&self, idx: usize) -> Option<DirectoryPageEntry> {
        if idx >= self.num_entries() as usize {
            return None;
        }
        Some(DirectoryPageEntry {
            page_id: self.entry_page_id(idx).unwrap(),
            file_offset: self.entry_file_offset(idx).unwrap(),
            free_space: self.entry_free_space(idx).unwrap(),
        })
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

    pub const fn set_next_directory_page_id(&mut self, id: NonZeroU64) {
        unsafe {
            let ptr = self.raw.as_mut_ptr().add(64) as *mut u64;
            *ptr = id.get().to_le();
        }
    }

    const fn set_num_entries(&mut self, n: u16) {
        unsafe {
            let ptr = self.raw.as_mut_ptr().add(72) as *mut u16;
            *ptr = n.to_le();
        }
    }

    const fn set_entry_page_id(&mut self, idx: usize, id: Option<page_base::PageId>) {
        let base = 80 + idx * Self::ENTRY_SIZE;
        unsafe {
            let ptr = self.raw.as_mut_ptr().add(base) as *mut u64;
            *ptr = match id {
                Some(nz_value) => nz_value.get().to_le(),
                None => 0u64.to_le(),
            }
        }
    }

    const fn set_entry_file_offset(&mut self, idx: usize, offset: Option<NonZeroU64>) {
        let base = 80 + idx * Self::ENTRY_SIZE + 8;
        unsafe {
            let ptr = self.raw.as_mut_ptr().add(base) as *mut u64;
            *ptr = match offset {
                Some(nz_value) => nz_value.get().to_le(),
                None => 0u64.to_le(),
            }
        }
    }

    const fn set_entry_free_space(&mut self, idx: usize, free: u32) {
        let base = 80 + idx * Self::ENTRY_SIZE + 16;
        unsafe {
            let ptr = self.raw.as_mut_ptr().add(base) as *mut u32;
            *ptr = free.to_le();
        }
    }

    // === Indirect setters ===

    pub const fn add_entry(
        &mut self,
        entry: DirectoryPageEntry,
    ) -> Result<(), errors::AddEntryError> {
        let free_space = self.free_space();
        if free_space < 32 {
            return Err(errors::AddEntryError::InsufficientSpace);
        }

        let num_entries = self.num_entries();
        self.set_num_entries(num_entries + 1);
        self.set_free_space(free_space - Self::ENTRY_SIZE as u32);

        self.set_entry_page_id(num_entries as usize, Some(entry.page_id));
        self.set_entry_file_offset(num_entries as usize, Some(entry.file_offset));
        self.set_entry_free_space(num_entries as usize, entry.free_space);

        Ok(())
    }

    // Just zeroes out the memory region of an entry
    const fn erase_entry(&mut self, idx: usize) {
        let base = 80 + idx * Self::ENTRY_SIZE + 8;
        assert!(
            base + Self::ENTRY_SIZE <= self.raw.len(),
            "Out of bounds erase"
        );

        unsafe {
            std::ptr::write_bytes(self.raw.as_mut_ptr().add(base), 0, Self::ENTRY_SIZE);
        }
    }

    const fn swap_entries(&mut self, idx_a: usize, idx_b: usize) {
        if idx_a == idx_b {
            return;
        }

        let base_a = 80 + idx_a * Self::ENTRY_SIZE;
        let base_b = 80 + idx_b * Self::ENTRY_SIZE;

        assert!(
            base_a + Self::ENTRY_SIZE <= self.raw.len(),
            "Out of bounds swap A"
        );
        assert!(
            base_b + Self::ENTRY_SIZE <= self.raw.len(),
            "Out of bounds swap B"
        );

        unsafe {
            let ptr_a = self.raw.as_mut_ptr().add(base_a);
            let ptr_b = self.raw.as_mut_ptr().add(base_b);

            let mut tmp = [0u8; Self::ENTRY_SIZE];
            std::ptr::copy_nonoverlapping(ptr_a, tmp.as_mut_ptr(), Self::ENTRY_SIZE);
            std::ptr::copy_nonoverlapping(ptr_b, ptr_a, Self::ENTRY_SIZE);
            std::ptr::copy_nonoverlapping(tmp.as_ptr(), ptr_b, Self::ENTRY_SIZE);
        }
    }

    pub const fn remove_entry_at(&mut self, idx: usize) -> Result<(), errors::RemoveEntryError> {
        let num_entries = self.num_entries() as usize;
        if idx >= num_entries {
            return Err(errors::RemoveEntryError::IndexOutOfBounds);
        }

        let free_space = self.free_space();
        self.set_free_space(free_space + Self::ENTRY_SIZE as u32);
        self.set_num_entries(num_entries as u16 - 1);

        self.erase_entry(idx);
        if idx != num_entries - 1 {
            self.swap_entries(idx, num_entries - 1);
        }

        Ok(())
    }
}

pub mod errors {
    pub enum AddEntryError {
        InsufficientSpace,
    }

    pub enum RemoveEntryError {
        IndexOutOfBounds,
    }
}
