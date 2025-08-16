use std::num::NonZeroU64;

use crate::{
    constants,
    storage::page::{directory_page::DirectoryPage, slotted_data_page::SlottedDataPage},
};

pub enum PageKind {
    Directory = 1,
    SlottedData = 2,
}

pub trait DiskPage {
    const PAGE_KIND: u8;

    fn raw(&self) -> &[u8; constants::storage::DISK_PAGE_SIZE];
    fn raw_mut(&mut self) -> &mut [u8; constants::storage::DISK_PAGE_SIZE];
}

pub type PageId = NonZeroU64;

pub enum Page {
    Directory(DirectoryPage),
    SlottedData(SlottedDataPage),
}
