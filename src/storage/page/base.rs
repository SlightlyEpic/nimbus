use crate::{
    constants,
    storage::page::{directory::Directory, slotted_data::SlottedData},
};
use std::num::NonZeroU64;

pub type PageBuf = [u8; constants::storage::PAGE_SIZE];

// #anchor-pagekind-values
pub enum PageKind {
    Invalid = 0,
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
    Invalid(),
    Directory(Directory<'a>),
    SlottedData(SlottedData<'a>),
}

pub fn page_kind_from_buf(buf: &PageBuf) -> PageKind {
    // MAGIC: ensure that these values match the assigned PageKind values (#anchor-pagekind-values)
    match buf[0] {
        1 => PageKind::Directory,
        2 => PageKind::SlottedData,
        _ => PageKind::Invalid,
    }
}

/// initializes the raw buf_page
/// zeros out all the values except the pagekind
pub fn init_page_buf(buf: &mut PageBuf, kind: PageKind) {
    buf[0] = kind as u8;
    buf[1..64].fill(0);
}
