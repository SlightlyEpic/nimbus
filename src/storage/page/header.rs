use crate::constants::storage::PAGE_SIZE;
use crate::storage::page::base::{PageId, PageKind};

// Bytes:   | +0        | +1        | +2        | +3        |
// ---------+-----------+-----------+-----------+-----------|
// 0..3     |              page_id (u32)                    |
// ---------+-----------+-----------+-----------+-----------|
// 4..7     |              parent_page_id (u32)             |
// ---------+-----------+-----------+-----------+-----------|
// 8..11    |              next_page_id (u32)               |
// ---------+-----------+-----------+-----------+-----------|
// 12..15   |              prev_page_id (u32)               |
// ---------+-----------+-----------+-----------+-----------|
// 16..19   | num_entries (u16) | free_space_ptr (u16)      |
// ---------+-----------+-----------+-----------+-----------|
// 20..23   | level (u16)       | page_kind(u8) | flags(u8) |
// ---------+-----------+-----------+-----------+-----------|
// 24..27   |              key_size (u32)                   |
// ---------+-----------+-----------+-----------+-----------|
// 28..31   |              reserved (u32)                   |
// ---------+-----------------------------------------------|
// 32       | (Header Ends)                                 |
//          | (Data Area Begins for B+ Tree pages)          |
/// A 32-byte header common to all page types.
/// It is always at the beginning of the 4KB page buffer.
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct PageHeader {
    page_id: PageId,         // 4 bytes
    parent_page_id: PageId,  // 4 bytes
    next_page_id: PageId,    // 4 bytes
    prev_page_id: PageId,    // 4 bytes
    num_entries: u16,        // 2 bytes
    free_space_pointer: u16, // 2 bytes
    level: u16,              // 2 bytes
    page_kind: u8,           // 1 byte
    flags: u8,               // 1 byte
    key_size: u32,           // 4 bytes
    reserved: u32,           // 4 bytes (Padding)
}

// Flags
const FLAG_IS_ROOT: u8 = 0b0000_0001;

impl PageHeader {
    pub const SIZE: usize = 32;

    /// Gets an immutable reference to the PageHeader from a raw page buffer.
    pub fn from_buf(buf: &[u8; PAGE_SIZE]) -> &Self {
        unsafe { &*(buf.as_ptr() as *const PageHeader) }
    }

    /// Gets a mutable reference to the PageHeader from a raw page buffer.
    pub fn from_buf_mut(buf: &mut [u8; PAGE_SIZE]) -> &mut Self {
        unsafe { &mut *(buf.as_mut_ptr() as *mut PageHeader) }
    }

    /// Initializes a new page buffer with default header values.
    pub fn init(&mut self, page_id: PageId, kind: PageKind) {
        *self = Self {
            page_id: page_id.to_le(),
            parent_page_id: 0u32.to_le(),
            next_page_id: 0u32.to_le(),
            prev_page_id: 0u32.to_le(),
            num_entries: 0u16.to_le(),
            free_space_pointer: (PAGE_SIZE as u16).to_le(), // Free space starts at the end
            level: 0u16.to_le(),
            page_kind: kind as u8,
            flags: 0,
            key_size: 0u32.to_le(),
            reserved: 0u32.to_le(),
        };
    }

    // --- Getters (with little-endian conversion) ---
    pub fn page_id(&self) -> PageId {
        PageId::from_le(self.page_id)
    }
    pub fn parent_page_id(&self) -> PageId {
        PageId::from_le(self.parent_page_id)
    }
    pub fn next_page_id(&self) -> PageId {
        PageId::from_le(self.next_page_id)
    }
    pub fn prev_page_id(&self) -> PageId {
        PageId::from_le(self.prev_page_id)
    }
    pub fn num_entries(&self) -> u16 {
        u16::from_le(self.num_entries)
    }
    pub fn free_space_pointer(&self) -> u16 {
        u16::from_le(self.free_space_pointer)
    }
    pub fn level(&self) -> u16 {
        u16::from_le(self.level)
    }
    pub fn page_kind(&self) -> PageKind {
        self.page_kind.into()
    }
    pub fn key_size(&self) -> u32 {
        u32::from_le(self.key_size)
    }

    pub fn is_root(&self) -> bool {
        (self.flags & FLAG_IS_ROOT) != 0
    }

    // --- Setters (with little-endian conversion) ---
    pub fn set_page_id(&mut self, id: PageId) {
        self.page_id = id.to_le();
    }
    pub fn set_parent_page_id(&mut self, id: PageId) {
        self.parent_page_id = id.to_le();
    }
    pub fn set_next_page_id(&mut self, id: PageId) {
        self.next_page_id = id.to_le();
    }
    pub fn set_prev_page_id(&mut self, id: PageId) {
        self.prev_page_id = id.to_le();
    }
    pub fn set_num_entries(&mut self, num: u16) {
        self.num_entries = num.to_le();
    }
    pub fn set_free_space_pointer(&mut self, ptr: u16) {
        self.free_space_pointer = ptr.to_le();
    }
    pub fn set_level(&mut self, level: u16) {
        self.level = level.to_le();
    }
    pub fn set_key_size(&mut self, size: u32) {
        self.key_size = size.to_le();
    }

    pub fn set_root(&mut self, is_root: bool) {
        if is_root {
            self.flags |= FLAG_IS_ROOT;
        } else {
            self.flags &= !FLAG_IS_ROOT;
        }
    }

    /// Calculates the amount of free space.
    /// Assumes 'data_start_offset' is the end of header/metadata.
    /// Assumes 'free_space_pointer' is the start of data.
    pub fn free_space(&self, data_start_offset: u16) -> u32 {
        let free_ptr = self.free_space_pointer() as u32;
        let data_start = data_start_offset as u32;

        if free_ptr < data_start {
            0 // Should not happen
        } else {
            free_ptr - data_start
        }
    }
}

// Implement From<u8> for PageKind to be used by the header
impl From<u8> for PageKind {
    fn from(value: u8) -> Self {
        match value {
            1 => PageKind::Directory,
            2 => PageKind::SlottedData,
            3 => PageKind::BPlusInner,
            4 => PageKind::BPlusLeaf,
            _ => PageKind::Invalid,
        }
    }
}
