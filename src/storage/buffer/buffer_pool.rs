use crate::constants;
use crate::storage::buffer::Evictor;
use crate::storage::disk;
use crate::storage::page;
use crate::storage::page_locator::{PageLocator, locator};
use std::collections::HashMap;
use std::pin::Pin;

const FRAME_COUNT: usize = 128;

#[derive(Copy, Clone)]
pub struct Frame {
    fid: u32, // frame_id: will just be the frame index
    ready: bool,
    pinned: bool,
    dirty: bool,
    file_offset: u64,                  // can be garbage as long as is_ready is false
    page_id: page::base::PageId, // redundant field for faster reads, can be garbage as long as is_ready is false
    buf_ptr: *mut page::base::PageBuf, // raw pointer into frames_backing_buf
}

impl Frame {
    #[inline]
    pub fn page_id(&self) -> page::base::PageId {
        self.page_id
    }

    pub fn page_view(&mut self) -> page::base::Page<'_> {
        unsafe {
            let buf = &mut (*self.buf_ptr);
            let kind = page::base::page_kind_from_buf(buf);

            match kind {
                page::base::PageKind::Directory => {
                    page::base::Page::Directory(page::Directory::new(buf))
                }
                page::base::PageKind::SlottedData => {
                    page::base::Page::SlottedData(page::SlottedData::new(buf))
                }
                page::base::PageKind::Invalid => page::base::Page::Invalid(),
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

#[derive(Copy, Clone)]
struct FrameMeta {
    file_offset: u64,
    page_id: page::base::PageId,
    frame_id: u32,
}

pub struct BufferPoolCore {
    // All original fields except page_locator
    frames_backing_buf: Box<[u8; FRAME_COUNT * constants::storage::PAGE_SIZE]>,
    frames: [Option<Frame>; FRAME_COUNT],
    free_frames: u32,

    frames_meta_pid: HashMap<page::base::PageId, FrameMeta>,
    frames_meta_offset: HashMap<u64, FrameMeta>,

    file_manager: disk::FileManager,
    evictor: Box<dyn Evictor>,

    _pin: std::marker::PhantomPinned,
}

impl BufferPoolCore {
    pub fn mark_frame_dirty(self: Pin<&mut Self>, frame: &Frame) {
        unsafe {
            if let Some(f) = &mut self.get_unchecked_mut().frames[frame.fid as usize] {
                f.dirty = true;
            }
        }
    }

    pub fn pin_frame(self: Pin<&mut Self>, frame_id: u32) -> Result<(), errors::PinFrameError> {
        unsafe {
            let self_mut = self.get_unchecked_mut();
            if let Some(frame) = &mut self_mut.frames[frame_id as usize] {
                if frame.pinned() {
                    return Err(errors::PinFrameError::AlreadyPinned);
                }
                frame.pinned = true;
                self_mut.evictor.set_frame_evictable(frame, false);
                Ok(())
            } else {
                Err(errors::PinFrameError::FrameNotFound)
            }
        }
    }

    pub fn unpin_frame(self: Pin<&mut Self>, frame_id: u32) -> Result<(), errors::UnpinFrameError> {
        unsafe {
            let self_mut = self.get_unchecked_mut();
            if let Some(frame) = &mut self_mut.frames[frame_id as usize] {
                if !frame.pinned {
                    return Err(errors::UnpinFrameError::NotPinned);
                }

                frame.pinned = false;
                self_mut.evictor.set_frame_evictable(frame, true);

                Ok(())
            } else {
                Err(errors::UnpinFrameError::FrameNotFound)
            }
        }
    }

    pub fn flush_frame(
        mut self: Pin<&mut Self>,
        frame_id: u32,
    ) -> Result<(), errors::FlushFrameError> {
        unsafe {
            let self_mut = self.as_mut().get_unchecked_mut();
            let frame = self_mut.frames[frame_id as usize]
                .as_ref()
                .ok_or(errors::FlushFrameError::FrameNotFound)?;

            if !frame.dirty() {
                return Ok(());
            }

            let buf_ptr = frame.buf_ptr;
            let offset = frame.file_offset;

            self_mut
                .file_manager
                .write_block_from(offset, &(*buf_ptr))
                .map_err(|_| errors::FlushFrameError::IOError)?;

            if let Some(frame) = &mut self_mut.frames[frame_id as usize] {
                frame.dirty = false;
            }

            Ok(())
        }
    }

    pub fn flush_all(mut self: Pin<&mut Self>) -> Result<(), errors::FlushAllError> {
        for i in 0..FRAME_COUNT {
            if let Some(frame) = self.as_ref().get_ref().frames[i] {
                if frame.dirty {
                    self.as_mut()
                        .flush_frame(i as u32)
                        .map_err(|_| errors::FlushAllError::IOError)?;
                }
            }
        }
        Ok(())
    }

    pub fn fetch_page_at_offset(
        mut self: Pin<&mut Self>,
        offset: u64,
    ) -> Result<&mut Frame, errors::FetchPageError> {
        // is page is this offset already loaded?
        let is_loaded = self.as_mut().frames_meta_offset.contains_key(&offset);

        // if yes then no need to load, return that frame
        if is_loaded {
            let frame_id = self
                .as_mut()
                .frames_meta_offset
                .get(&offset)
                .unwrap()
                .frame_id;
            let frame = unsafe {
                self.get_unchecked_mut().frames[frame_id as usize]
                    .as_mut()
                    .unwrap()
            };

            return Ok(frame);
        }

        // if not then create a frame and load it
        let frame_idx = self
            .as_mut()
            .find_free_frame_with_evict()
            .ok_or(errors::FetchPageError::BufferFull)?;

        let frame = self
            .as_mut()
            .alloc_frame_at(frame_idx)
            .expect("frame to be allocated");

        let buf_ptr = frame.buf_ptr;

        unsafe {
            self.as_mut()
                .get_unchecked_mut()
                .file_manager
                .read_block_into(offset, &mut (*buf_ptr))
                .map_err(|_| errors::FetchPageError::IOError)?;
        }

        let frame = unsafe {
            self.as_mut().get_unchecked_mut().frames[frame_idx]
                .as_mut()
                .unwrap()
        };

        // fill in page specific details
        frame.ready = true;
        frame.file_offset = offset;
        frame.page_id = match frame.page_view() {
            page::base::Page::Directory(page) => page.page_id(),
            page::base::Page::SlottedData(page) => page.page_id(),
            page::base::Page::Invalid() => panic!("attempt to load invalid page"),
        };

        let frame_meta = FrameMeta {
            frame_id: frame_idx as u32,
            file_offset: frame.file_offset,
            page_id: frame.page_id,
        };
        unsafe {
            // bookkeeping
            self.as_mut()
                .get_unchecked_mut()
                .frames_meta_pid
                .insert(frame_meta.page_id, frame_meta);
            self.as_mut()
                .get_unchecked_mut()
                .frames_meta_offset
                .insert(frame_meta.file_offset, frame_meta);

            let self_mut_ref = self.as_mut().get_unchecked_mut();
            let frame = self_mut_ref.frames[frame_idx].as_mut().unwrap();
            // evictor bookkeeping
            self_mut_ref.evictor.notify_frame_alloc(frame);
            self_mut_ref.evictor.set_frame_evictable(frame, true);
        }

        let frame = unsafe { self.get_unchecked_mut().frames[frame_idx].as_mut().unwrap() };
        Ok(frame)
    }

    pub fn alloc_new_page(
        mut self: Pin<&mut Self>,
        page_kind: page::base::PageKind,
    ) -> Result<&mut Frame, errors::AllocNewPageError> {
        let frame_idx = self
            .as_mut()
            .find_free_frame_with_evict()
            .ok_or(errors::AllocNewPageError::BufferFull)?;

        let frame = self
            .as_mut()
            .alloc_frame_at(frame_idx)
            .map_err(|_| errors::AllocNewPageError::AllocError)?;

        let buf_ptr = frame.buf_ptr;

        let offset = unsafe {
            self.as_mut()
                .get_unchecked_mut()
                .file_manager
                .allocate_new_page_offset()
                .map_err(|_| errors::AllocNewPageError::IOError)?
        };

        unsafe {
            std::ptr::write_bytes(buf_ptr, 0, 1);
        }

        let frame = unsafe {
            self.as_mut().get_unchecked_mut().frames[frame_idx]
                .as_mut()
                .unwrap()
        };

        frame.ready = true;
        frame.file_offset = offset;
        frame.dirty = true;

        unsafe {
            let buf = &mut (*buf_ptr);
            page::base::init_page_buf(buf, page_kind);
        }

        frame.page_id = match frame.page_view() {
            page::base::Page::Directory(page) => page.page_id(),
            page::base::Page::SlottedData(page) => page.page_id(),
            page::base::Page::Invalid() => {
                return Err(errors::AllocNewPageError::InvalidPage);
            }
        };

        let frame_meta = FrameMeta {
            frame_id: frame_idx as u32,
            file_offset: frame.file_offset,
            page_id: frame.page_id,
        };

        unsafe {
            self.as_mut()
                .get_unchecked_mut()
                .frames_meta_pid
                .insert(frame_meta.page_id, frame_meta);
            self.as_mut()
                .get_unchecked_mut()
                .frames_meta_offset
                .insert(frame_meta.file_offset, frame_meta);

            let self_mut_ref = self.as_mut().get_unchecked_mut();
            let frame = self_mut_ref.frames[frame_idx].as_mut().unwrap();
            self_mut_ref.evictor.notify_frame_alloc(frame);
            self_mut_ref.evictor.set_frame_evictable(frame, true);
        }

        let frame = unsafe { self.get_unchecked_mut().frames[frame_idx].as_mut().unwrap() };
        Ok(frame)
    }

    pub fn alloc_frame_at(
        mut self: Pin<&mut Self>,
        frame_idx: usize,
    ) -> Result<&mut Frame, errors::AllocFrameError> {
        if self.frames[frame_idx].is_some() {
            return Err(errors::AllocFrameError::FrameOccupied);
        }

        unsafe {
            let buf_ptr = self.as_mut().get_frame_buf_at(frame_idx);

            let frame = Frame {
                fid: frame_idx as u32,
                ready: false,
                dirty: false,
                pinned: false,
                file_offset: 0,
                page_id: page::base::PageId::new(1).unwrap(),
                buf_ptr: buf_ptr,
            };

            Ok(self.get_unchecked_mut().frames[frame_idx].insert(frame))
        }
    }

    pub fn dealloc_frame_at(self: Pin<&mut Self>, frame_idx: usize) {
        let self_mut = unsafe { self.get_unchecked_mut() };
        let frame = self_mut.frames[frame_idx].unwrap();

        self_mut.frames[frame_idx] = None;

        // bookkeeping
        self_mut.frames_meta_pid.remove(&frame.page_id);
        self_mut.frames_meta_offset.remove(&frame.file_offset);
        self_mut.free_frames += 1;

        // evictor bookkeeping
        self_mut.evictor.notify_frame_destroy(frame_idx as u32);
    }

    pub fn find_free_frame_with_evict(mut self: Pin<&mut Self>) -> Option<usize> {
        let self_mut = unsafe { self.as_mut().get_unchecked_mut() };
        let free_frames = self_mut.free_frames;
        if free_frames == 0 {
            let victim_frame_idx = self_mut.evictor.pick_victim()?;
            self.as_mut().dealloc_frame_at(victim_frame_idx as usize);
        }

        self.frames.iter().position(|frame| frame.is_none())
    }

    pub fn find_free_frame(self: Pin<&Self>) -> Option<usize> {
        if self.free_frames == 0 {
            return None;
        }
        self.frames.iter().position(|frame| frame.is_none())
    }

    /// NOTE: idx must be within 0..FRAME_COUNT
    pub unsafe fn get_frame_buf_at(self: Pin<&mut Self>, idx: usize) -> *mut page::base::PageBuf {
        let offset = idx * constants::storage::PAGE_SIZE;
        unsafe {
            self.get_unchecked_mut()
                .frames_backing_buf
                .as_mut_ptr()
                .add(offset)
                .cast::<page::base::PageBuf>()
        }
    }
}

pub struct BufferPool {
    core: BufferPoolCore,
    page_locator: Box<dyn PageLocator>,
}

impl BufferPool {
    pub fn new(
        file_manager: disk::FileManager,
        evictor: Box<dyn Evictor>,
        page_locator: Box<dyn PageLocator>,
    ) -> Self {
        Self {
            core: BufferPoolCore {
                frames_backing_buf: Box::new([0u8; FRAME_COUNT * constants::storage::PAGE_SIZE]),
                frames: std::array::from_fn(|_| None),
                free_frames: FRAME_COUNT as u32,
                frames_meta_pid: HashMap::new(),
                frames_meta_offset: HashMap::new(),
                file_manager,
                evictor,
                _pin: std::marker::PhantomPinned::default(),
            },
            page_locator,
        }
    }

    fn core(self: Pin<&mut Self>) -> Pin<&mut BufferPoolCore> {
        unsafe { self.map_unchecked_mut(|s| &mut s.core) }
    }

    pub fn fetch_page(
        mut self: Pin<&mut Self>,
        page_id: page::base::PageId,
    ) -> Result<&mut Frame, errors::FetchPageError> {
        let (core, locator) = unsafe {
            let this = self.as_mut().get_unchecked_mut();
            (Pin::new_unchecked(&mut this.core), &mut this.page_locator)
        };

        let offset_result = locator.find_file_offset(page_id, core);

        let offset = offset_result.map_err(|err| match err {
            locator::errors::FindOffsetError::NotFound => errors::FetchPageError::NotFound,
            _ => errors::FetchPageError::IOError,
        })?;

        self.fetch_page_at_offset(offset)
    }

    pub fn fetch_page_at_offset(
        self: Pin<&mut Self>,
        offset: u64,
    ) -> Result<&mut Frame, errors::FetchPageError> {
        self.core().fetch_page_at_offset(offset)
    }

    pub fn alloc_new_page(
        self: Pin<&mut Self>,
        page_kind: page::base::PageKind,
    ) -> Result<&mut Frame, errors::AllocNewPageError> {
        self.core().alloc_new_page(page_kind)
    }

    pub fn pin_frame(self: Pin<&mut Self>, frame_id: u32) -> Result<(), errors::PinFrameError> {
        self.core().pin_frame(frame_id)
    }

    pub fn unpin_frame(self: Pin<&mut Self>, frame_id: u32) -> Result<(), errors::UnpinFrameError> {
        self.core().unpin_frame(frame_id)
    }

    pub fn flush_frame(self: Pin<&mut Self>, frame_id: u32) -> Result<(), errors::FlushFrameError> {
        self.core().flush_frame(frame_id)
    }

    pub fn flush_all(self: Pin<&mut Self>) -> Result<(), errors::FlushAllError> {
        self.core().flush_all()
    }

    pub fn mark_frame_dirty(self: Pin<&mut Self>, frame: &Frame) {
        self.core().mark_frame_dirty(frame)
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
        NotFound,
        AllocError,
        InvalidPage,
    }

    #[derive(Debug)]
    pub enum AllocNewPageError {
        BufferFull,
        IOError,
        AllocError,
        InvalidPage,
    }

    #[derive(Debug)]
    pub enum PinFrameError {
        FrameNotFound,
        AlreadyPinned,
    }

    pub enum UnpinFrameError {
        FrameNotFound,
        NotPinned,
    }

    #[derive(Debug)]
    pub enum FlushFrameError {
        FrameNotFound,
        IOError,
    }

    #[derive(Debug)]
    pub enum FlushAllError {
        IOError,
    }

    #[derive(Debug)]
    pub enum DeallocFrameError {
        FrameNotFound,
        FramePinned,
        FlushError,
    }
}
