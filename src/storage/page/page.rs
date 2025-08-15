use crate::storage::{
    page::{data_page::DataPage, directory_page::DirectoryPage},
    util::SerdeDyn,
};

pub enum PageKind {
    Directory = 1,
    Data = 2,
}

pub trait DiskPage {
    const PAGE_KIND: u8;
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
    Data(DataPage),
}
