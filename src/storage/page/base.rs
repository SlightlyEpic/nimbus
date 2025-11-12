use crate::{
    constants,
    storage::page::{
        bplus_inner::BPlusInner, bplus_leaf::BPlusLeaf, directory::Directory,
        slotted_data::SlottedData,
    },
};
use std::num::NonZeroU64;

pub type PageBuf = [u8; constants::storage::PAGE_SIZE];

// #anchor-pagekind-values
#[derive(Debug, Copy, Clone)]
pub enum PageKind {
    Invalid = 0,
    Directory = 1,
    SlottedData = 2,
    BPlusInner = 3,
    BPlusLeaf = 4,
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
    BPlusInner(BPlusInner<'a>),
    BPlusLeaf(BPlusLeaf<'a>),
}

impl<'a> Page<'a> {
    pub fn raw(&self) -> &PageBuf {
        match self {
            Page::Directory(page) => page.raw(),
            Page::SlottedData(page) => page.raw(),
            Page::BPlusInner(page) => page.raw(),
            Page::BPlusLeaf(page) => page.raw(),
            Page::Invalid() => panic!("Cannot get raw() from Page::Invalid"),
        }
    }
}

pub fn page_kind_from_buf(buf: &PageBuf) -> PageKind {
    // MAGIC: ensure that these values match the assigned PageKind values (#anchor-pagekind-values)
    match buf[0] {
        1 => PageKind::Directory,
        2 => PageKind::SlottedData,
        3 => PageKind::BPlusInner,
        4 => PageKind::BPlusLeaf,
        _ => PageKind::Invalid,
    }
}

/// initializes the raw buf_page
/// zeros out all the values except the pagekind
pub fn init_page_buf(buf: &mut PageBuf, kind: PageKind) {
    buf.fill(0);
    buf[0] = kind as u8;
}
