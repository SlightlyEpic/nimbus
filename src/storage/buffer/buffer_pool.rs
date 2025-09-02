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
    file_offset: u64,                 // can be garbage as long as is_ready is false
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

#[derive(Copy, Clone)]
struct FrameMeta {
    file_offset: u64,
    page_id: page_base::PageId,
    frame_id: u32,
}

pub struct BufferPool {
    frames_backing_buf: Box<[u8; FRAME_COUNT * constants::storage::PAGE_SIZE]>,
    frames: [Option<Frame>; FRAME_COUNT],
    free_frames: u32,

    // need to keep metadata indexed by both page id and file offset because
    // those are the two ways a BufferPool user can access a page
    frames_meta_pid: HashMap<page_base::PageId, FrameMeta>, // because each frame is uniquely identified by its page_id
    frames_meta_offset: HashMap<u64, FrameMeta>,

    file_manager: disk::FileManager,
    evictor: Box<dyn Evictor>,

    _pin: std::marker::PhantomPinned,
}

impl BufferPool {
    pub fn new(file_manager: disk::FileManager, evictor: Box<dyn Evictor>) -> Self {
        Self {
            frames_backing_buf: Box::new([0u8; FRAME_COUNT * constants::storage::PAGE_SIZE]),
            frames: std::array::from_fn(|_| None),
            free_frames: FRAME_COUNT as u32,
            frames_meta_pid: HashMap::new(),
            frames_meta_offset: HashMap::new(),
            file_manager,
            evictor,
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

        // is page is this offset already loaded?
        let is_loaded = self_mut_ref.frames_meta_offset.contains_key(&offset);

        // if yes then no need to load, return that frame
        if is_loaded {
            let meta = self_mut_ref.frames_meta_offset.get(&offset).unwrap();
            return Ok(self_mut_ref.frames[meta.frame_id as usize]
                .as_mut()
                .unwrap());
        }

        // if not then create a frame and load it
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

        // fill in page specific details
        frame.ready = true;
        frame.file_offset = offset;
        frame.page_id = match frame.page_view() {
            page_base::Page::Directory(page) => page.page_id(),
            page_base::Page::SlottedData(page) => page.page_id(),
            page_base::Page::Invalid() => panic!("attempt to load invalid page"),
        };

        // bookkeeping
        let frame_meta = FrameMeta {
            frame_id: frame_idx as u32,
            file_offset: frame.file_offset,
            page_id: frame.page_id,
        };
        self_mut_ref
            .frames_meta_pid
            .insert(frame_meta.page_id, frame_meta);
        self_mut_ref
            .frames_meta_offset
            .insert(frame_meta.file_offset, frame_meta);

        // evictor bookkeeping
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
                file_offset: 0,
                page_id: page_base::PageId::new(1).unwrap(),
                buf_ptr: buf_ptr,
            };

            Ok(self.frames[frame_idx].insert(frame))
        }
    }

    /// SAFETY: must following pinning rules
    fn dealloc_frame_at(self: &mut Self, frame_idx: usize) {
        let frame = self.frames[frame_idx].unwrap();

        self.frames[frame_idx] = None;

        // bookkeeping
        self.frames_meta_pid.remove(&frame.page_id);
        self.frames_meta_offset.remove(&frame.file_offset);
        self.free_frames += 1;

        // evictor bookkeeping
        self.evictor.notify_frame_destroy(frame_idx as u32);
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
