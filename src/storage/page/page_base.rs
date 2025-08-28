use crate::{
    constants,
    storage::page::{directory_page::DirectoryPage, slotted_data_page::SlottedDataPage},
};
use std::num::NonZeroU64;

pub type PageBuf = [u8; constants::storage::DISK_PAGE_SIZE];

pub enum PageKind {
    Directory = 1,
    SlottedData = 2,
}

pub trait DiskPage {
    const PAGE_KIND: u8;

    fn raw(&self) -> &PageBuf;
    fn raw_mut(&mut self) -> &mut PageBuf;
}

pub type PageId = NonZeroU64;

pub enum Page<'a> {
    Directory(DirectoryPage<'a>),
    SlottedData(SlottedDataPage<'a>),
}
