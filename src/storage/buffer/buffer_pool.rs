use crate::constants;
use crate::storage::buffer::Evictor;
use crate::storage::disk;
use crate::storage::page::{self, page_base};
use std::collections::HashMap;
use std::pin::Pin;

const FRAME_COUNT: usize = 128;

#[derive(Copy, Clone)]
pub struct Frame {
    fid: u32, // frame_id: will just be the frame index
    ready: bool,
    pinned: bool,
    dirty: bool,
    page_id: page_base::PageId, // redundant field for faster reads, can be garbage as long as is_ready is false
    buf_ptr: *mut page_base::PageBuf, // raw pointer into frames_backing_buf
}

impl Frame {
    #[inline]
    pub fn page_id(&self) -> page_base::PageId {
        self.page_id
    }

    pub fn page_view(&mut self) -> page_base::Page<'_> {
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

    #[inline]
    pub fn fid(&self) -> u32 {
        self.fid
    }

    #[inline]
    pub fn pinned(&self) -> bool {
        self.pinned
    }

    #[inline]
    pub fn dirty(&self) -> bool {
        self.dirty
    }
}

struct FrameMeta {
    file_offset: u64,
}

pub struct BufferPool<E: Evictor> {
    frames_backing_buf: Box<[u8; FRAME_COUNT * constants::storage::PAGE_SIZE]>,
    frames: [Option<Frame>; FRAME_COUNT],
    frames_meta: HashMap<page_base::PageId, FrameMeta>, // because each frame is uniquely identified by its page_id
    free_frames: u32,

    file_manager: disk::FileManager,
    evictor: E,

    _pin: std::marker::PhantomPinned,
}

impl<E: Evictor> BufferPool<E> {
    pub fn new(file_manager: disk::FileManager, evictor: E) -> Self {
        Self {
            frames_backing_buf: Box::new([0u8; FRAME_COUNT * constants::storage::PAGE_SIZE]),
            frames: std::array::from_fn(|_| None),
            file_manager,
            free_frames: FRAME_COUNT as u32,
            evictor,
            frames_meta: HashMap::default(),
            _pin: std::marker::PhantomPinned::default(),
        }
    }

    pub fn mark_frame_dirty(self: Pin<&mut Self>, frame: &Frame) {
        unsafe {
            if let Some(f) = &mut self.get_unchecked_mut().frames[frame.fid as usize] {
                f.dirty = true;
            }
        }
    }

    pub fn fetch_page(self: Pin<&mut Self>, page_id: page_base::PageId) -> Result<&mut Frame, ()> {
        // mark page dirty and then give it
        todo!();
    }

    pub fn fetch_page_at_offset(
        self: Pin<&mut Self>,
        offset: u64,
    ) -> Result<&mut Frame, errors::FetchPageError> {
        let self_mut_ref = unsafe { self.get_unchecked_mut() };

        let frame_idx = self_mut_ref
            .find_free_frame_with_evict()
            .ok_or(errors::FetchPageError::BufferFull)?;

        let frame = self_mut_ref
            .alloc_frame_at(frame_idx)
            .expect("frame to be allocated");

        let buf_ptr = frame.buf_ptr;

        unsafe {
            self_mut_ref
                .file_manager
                .read_block_into(offset, &mut (*buf_ptr))
                .map_err(|_| errors::FetchPageError::IOError)?;
        }

        let frame = self_mut_ref.frames[frame_idx].as_mut().unwrap();

        frame.ready = true;
        self_mut_ref.evictor.notify_frame_alloc(frame);
        self_mut_ref.evictor.set_frame_evictable(frame, true);

        Ok(frame)
    }

    /// SAFETY: must follow pinning rules
    fn alloc_frame_at(
        self: &mut Self,
        frame_idx: usize,
    ) -> Result<&mut Frame, errors::AllocFrameError> {
        if self.frames[frame_idx].is_some() {
            return Err(errors::AllocFrameError::FrameOccupied);
        }

        unsafe {
            let buf_ptr = self.get_frame_buf_at(frame_idx);

            let frame = Frame {
                fid: frame_idx as u32,
                ready: false,
                dirty: false,
                pinned: false,
                page_id: page_base::PageId::new(1).unwrap(),
                buf_ptr: buf_ptr,
            };

            Ok(self.frames[frame_idx].insert(frame))
        }
    }

    /// SAFETY: must following pinning rules
    fn dealloc_frame_at(self: &mut Self, frame_idx: usize) {
        self.frames[frame_idx] = None;
        self.evictor.notify_frame_destroy(frame_idx as u32);
        self.free_frames += 1;
    }

    /// SAFETY: must following pinning rules
    fn find_free_frame_with_evict(self: &mut Self) -> Option<usize> {
        let free_frames = self.free_frames;
        if free_frames == 0 {
            let victim_frame_idx = self.evictor.pick_victim()?;
            self.dealloc_frame_at(victim_frame_idx as usize);
        }

        self.frames.iter().position(|frame| frame.is_none())
    }

    /// SAFETY: must following pinning rules
    fn find_free_frame(self: &Self) -> Option<usize> {
        if self.free_frames == 0 {
            return None;
        }
        self.frames.iter().position(|frame| frame.is_none())
    }

    /// NOTE: idx must be within 0..FRAME_COUNT
    /// SAFETY: must follow pinning rules
    unsafe fn get_frame_buf_at(self: &mut Self, idx: usize) -> *mut page_base::PageBuf {
        let offset = idx * constants::storage::PAGE_SIZE;
        unsafe {
            self.frames_backing_buf
                .as_mut_ptr()
                .add(offset)
                .cast::<page_base::PageBuf>()
        }
    }
}

pub mod errors {
    #[derive(Debug)]
    pub enum AllocFrameError {
        FrameOccupied,
    }

    #[derive(Debug)]
    pub enum FetchPageError {
        BufferFull,
        IOError,
    }
}

// TODO:
// Need to make something external to the buffer pool
// which knows how to traverse the page directory (currently)
// So that there can be other page lookup implementation later
