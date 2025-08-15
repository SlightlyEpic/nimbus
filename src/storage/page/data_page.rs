use crate::constants;
use crate::storage::page::page;
use crate::storage::util::{self, SerdeDyn};

// Slotted page implementation
pub struct DataPage {
    raw: [u8; constants::storage::DISK_PAGE_SIZE],
}

impl page::DiskPage for DataPage {
    const PAGE_KIND: u8 = page::PageKind::Data as u8;

    fn raw(self: &Self) -> &[u8; constants::storage::DISK_PAGE_SIZE] {
        return &self.raw;
    }

    fn raw_mut(&mut self) -> &mut [u8; constants::storage::DISK_PAGE_SIZE] {
        return &mut self.raw;
    }
}
