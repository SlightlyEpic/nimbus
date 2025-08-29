use crate::constants;
use crate::storage::buffer::Evictor;
use crate::storage::disk;
use crate::storage::page::page_base;

const FRAME_COUNT: usize = 128;

pub struct Frame<'a> {
    pinned: bool,
    dirty: bool,
    page_id: page_base::PageId, // redundant field for faster reads
    page_view: page_base::Page<'a>,
}

impl<'a> Frame<'a> {
    #[inline]
    pub fn page_id(&self) -> page_base::PageId {
        self.page_id
    }
}

pub struct BufferPool<E: Evictor> {
    frames_backing_buf: Box<[u8; FRAME_COUNT * constants::storage::DISK_PAGE_SIZE]>,
    frames_meta: [Option<Frame<'static>>; FRAME_COUNT],
    file_manager: disk::FileManager,
    free_frames: u32,
    evictor: E,
    _pin: std::marker::PhantomPinned,
}

impl<E: Evictor> BufferPool<E> {
    pub fn new(file_manager: disk::FileManager, evictor: E) -> Self {
        Self {
            frames_backing_buf: Box::new([0u8; FRAME_COUNT * constants::storage::DISK_PAGE_SIZE]),
            frames_meta: std::array::from_fn(|_| None),
            file_manager,
            free_frames: FRAME_COUNT as u32,
            evictor,
            _pin: std::marker::PhantomPinned::default(),
        }
    }
}
