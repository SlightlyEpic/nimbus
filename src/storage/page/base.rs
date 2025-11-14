use crate::{
    constants,
    storage::page::{
        bplus_inner::BPlusInner, bplus_leaf::BPlusLeaf, directory::Directory, header::PageHeader,
        slotted_data::SlottedData,
    },
};
use std::num::NonZeroU64;

pub type PageBuf = [u8; constants::storage::PAGE_SIZE];

// #anchor-pagekind-values
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
#[repr(u8)]
pub enum PageKind {
    Invalid = 0,
    Directory = 1,
    SlottedData = 2,
    BPlusInner = 3,
    BPlusLeaf = 4,
}

pub trait DiskPage {
    const PAGE_KIND: u8;
    const DATA_START: usize;

    fn raw(&self) -> &PageBuf;
    fn raw_mut(&mut self) -> &mut PageBuf;

    /// Gets an immutable reference to the page's header.
    fn header(&self) -> &PageHeader {
        PageHeader::from_buf(self.raw())
    }

    /// Gets a mutable reference to the page's header.
    fn header_mut(&mut self) -> &mut PageHeader {
        PageHeader::from_buf_mut(self.raw_mut())
    }
}

pub type PageId = u32;

/// A generic enum to view any page buffer as its correct page type.
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

    pub fn header(&self) -> &PageHeader {
        match self {
            Page::Directory(page) => page.header(),
            Page::SlottedData(page) => page.header(),
            Page::BPlusInner(page) => page.header(),
            Page::BPlusLeaf(page) => page.header(),
            Page::Invalid() => panic!("Cannot get header() from Page::Invalid"),
        }
    }

    pub fn header_mut(&mut self) -> &mut PageHeader {
        match self {
            Page::Directory(page) => page.header_mut(),
            Page::SlottedData(page) => page.header_mut(),
            Page::BPlusInner(page) => page.header_mut(),
            Page::BPlusLeaf(page) => page.header_mut(),
            Page::Invalid() => panic!("Cannot get header_mut() from Page::Invalid"),
        }
    }

    pub fn raw_mut(&mut self) -> &mut PageBuf {
        match self {
            Page::Directory(page) => page.raw_mut(),
            Page::SlottedData(page) => page.raw_mut(),
            Page::BPlusInner(page) => page.raw_mut(),
            Page::BPlusLeaf(page) => page.raw_mut(),
            Page::Invalid() => panic!("Cannot get raw_mut() from Page::Invalid"),
        }
    }
}

/// Reads the page kind from a raw buffer using the PageHeader.
pub fn page_kind_from_buf(buf: &PageBuf) -> PageKind {
    PageHeader::from_buf(buf).page_kind()
}

/// Initializes a raw page buffer by setting its PageHeader.
/// Note: The page_id is set to 0 (invalid) here.
/// The BufferPool is responsible for setting the correct PageId.
pub fn init_page_buf(buf: &mut PageBuf, kind: PageKind) {
    PageHeader::from_buf_mut(buf).init(0, kind);
}
