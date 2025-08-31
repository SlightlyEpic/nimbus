use crate::constants;
use crate::storage::buffer::Evictor;
use crate::storage::disk;
use crate::storage::page::{self, page_base};
use std::pin::Pin;

const FRAME_COUNT: usize = 128;

pub struct Frame {
    pinned: bool,
    dirty: bool,
    page_id: page_base::PageId,       // redundant field for faster reads
    buf_ptr: *mut page_base::PageBuf, // raw pointer into frames_backing_buf
}

impl Frame {
    #[inline]
    pub fn page_id(&self) -> page_base::PageId {
        self.page_id
    }

    #[inline]
    pub fn page_view(&self) -> page_base::Page<'_> {
        unsafe {
            let buf = &mut (*self.buf_ptr);
            let kind = page_base::page_kind_from_buf(buf);

            match kind {
                page_base::PageKind::Directory => {
                    page_base::Page::Directory(page::DirectoryPage::new(buf))
                }
                page_base::PageKind::SlottedData => {
                    page_base::Page::SlottedData(page::SlottedDataPage::new(buf))
                }
                page_base::PageKind::Invalid => page_base::Page::Invalid(),
            }
        }
    }
}

pub struct BufferPool<E: Evictor> {
    frames_backing_buf: Box<[u8; FRAME_COUNT * constants::storage::DISK_PAGE_SIZE]>,
    frames: [Option<Frame>; FRAME_COUNT],
    file_manager: disk::FileManager,
    free_frames: u32,
    evictor: E,
    _pin: std::marker::PhantomPinned,
}

impl<E: Evictor> BufferPool<E> {
    pub fn new(file_manager: disk::FileManager, evictor: E) -> Self {
        Self {
            frames_backing_buf: Box::new([0u8; FRAME_COUNT * constants::storage::DISK_PAGE_SIZE]),
            frames: std::array::from_fn(|_| None),
            file_manager,
            free_frames: FRAME_COUNT as u32,
            evictor,
            _pin: std::marker::PhantomPinned::default(),
        }
    }

    pub fn fetch_page(self: Pin<&mut Self>, page_id: page_base::PageId) -> Result<&Frame, ()> {
        // no need to mark dirty for readonly
        todo!();
    }

    pub fn fetch_page_mut(
        self: Pin<&mut Self>,
        page_id: page_base::PageId,
    ) -> Result<&mut Frame, ()> {
        // mark page dirty and then give it
        todo!();
    }

    pub fn exchange_for_mut(self: Pin<&mut Self>, frame: &Frame) -> &mut Frame {
        self.fetch_page_mut(frame.page_id)
            .expect("Frame to be present")
    }
}
