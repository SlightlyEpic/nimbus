use crate::{constants, storage::page::directory_page::DirectoryPage};

pub enum PageKind {
    Directory = 1,
}

pub trait DiskPage {
    const PAGE_KIND: u8;

    fn serialize_for_disk(self: &Self) -> [u8; constants::storage::DISK_PAGE_SIZE];
    fn deserialize_from_disk(raw_page_data: &[u8; constants::storage::DISK_PAGE_SIZE]) -> Self;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PageId(u64);

impl PageId {
    pub fn new(id: u64) -> Self {
        PageId(id)
    }

    pub fn get(self) -> u64 {
        self.0
    }
}

pub enum Page {
    Directory(DirectoryPage),
}
