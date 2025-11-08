use crate::constants;
use crate::storage::buffer::Evictor;
use crate::storage::disk;
use crate::storage::page;
use crate::storage::page_locator::{PageLocator, locator};
use std::collections::HashMap;
use std::pin::Pin;

pub const FRAME_COUNT: usize = 128;

#[derive(Copy, Clone)]
pub struct Frame {
    fid: u32, // frame_id: will just be the frame index
    ready: bool,
    pin_count: u32,
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
                page::base::PageKind::BPlusInner => {
                    page::base::Page::BPlusInner(page::BPlusInner::new(buf))
                }
                page::base::PageKind::BPlusLeaf => {
                    page::base::Page::BPlusLeaf(page::BPlusLeaf::new(buf))
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
        self.pin_count > 0
    }

    #[inline]
    pub fn dirty(&self) -> bool {
        self.dirty
    }

    #[inline]
    pub fn ready(&self) -> bool {
        self.ready
    }
    #[inline]
    pub fn file_offset(&self) -> u64 {
        self.file_offset
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
    pub fn mark_frame_dirty(self: Pin<&mut Self>, frame_id: u32) {
        unsafe {
            if let Some(f) = &mut self.get_unchecked_mut().frames[frame_id as usize] {
                f.dirty = true;
            }
        }
    }

    pub fn pin_frame(self: Pin<&mut Self>, frame_id: u32) -> Result<(), errors::PinFrameError> {
        if frame_id >= FRAME_COUNT as u32 {
            return Err(errors::PinFrameError::FrameNotFound);
        }

        unsafe {
            let self_mut = self.get_unchecked_mut();
            if let Some(frame) = &mut self_mut.frames[frame_id as usize] {
                if frame.pin_count == 0 {
                    self_mut.evictor.set_frame_evictable(frame, false);
                }
                frame.pin_count += 1;
                Ok(())
            } else {
                Err(errors::PinFrameError::FrameNotFound)
            }
        }
    }

    pub fn unpin_frame(self: Pin<&mut Self>, frame_id: u32) -> Result<(), errors::UnpinFrameError> {
        if frame_id >= FRAME_COUNT as u32 {
            return Err(errors::UnpinFrameError::FrameNotFound);
        }

        unsafe {
            let self_mut = self.get_unchecked_mut();
            if let Some(frame) = &mut self_mut.frames[frame_id as usize] {
                if !frame.pinned() {
                    return Err(errors::UnpinFrameError::NotPinned);
                }

                frame.pin_count -= 1;
                if frame.pin_count == 0 {
                    self_mut.evictor.set_frame_evictable(frame, true);
                }

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
        if frame_id >= FRAME_COUNT as u32 {
            return Err(errors::FlushFrameError::FrameNotFound);
        }
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
        // cache hit
        if is_loaded {
            let frame_id = self
                .as_mut()
                .frames_meta_offset
                .get(&offset)
                .unwrap()
                .frame_id;

            let _ = self.as_mut().pin_frame(frame_id);
            let frame = unsafe {
                self.get_unchecked_mut().frames[frame_id as usize]
                    .as_mut()
                    .unwrap()
            };

            return Ok(frame);
        }

        // if not then create a frame and load it
        // cache miss load it
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
            page::base::Page::BPlusInner(page) => page.page_id(),
            page::base::Page::BPlusLeaf(page) => page.page_id(),
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
            frame.pin_count = 1;
            self_mut_ref.evictor.set_frame_evictable(frame, false);
        }

        let frame = unsafe { self.get_unchecked_mut().frames[frame_idx].as_mut().unwrap() };
        Ok(frame)
    }

    pub fn alloc_new_page(
        mut self: Pin<&mut Self>,
        page_kind: page::base::PageKind,
        page_id: page::base::PageId,
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
        frame.page_id = page_id;

        unsafe {
            let buf = &mut (*buf_ptr);
            page::base::init_page_buf(buf, page_kind);
        }

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
                pin_count: 0,
                file_offset: 0,
                page_id: page::base::PageId::new(1).unwrap(),
                buf_ptr: buf_ptr,
            };

            let self_mut = self.get_unchecked_mut();
            self_mut.free_frames -= 1;
            Ok(self_mut.frames[frame_idx].insert(frame))
        }
    }

    pub fn dealloc_frame_at(mut self: Pin<&mut Self>, frame_idx: usize) {
        let (is_pinned, is_dirty, page_id, file_offset) = {
            let self_mut = unsafe { self.as_mut().get_unchecked_mut() };
            let frame = self_mut.frames[frame_idx].as_ref().unwrap();
            (
                frame.pinned(),
                frame.dirty(),
                frame.page_id(),
                frame.file_offset,
            )
        };

        if is_pinned {
            panic!("Frame Pinned cannot dealloc");
        }

        if is_dirty {
            if self.as_mut().flush_frame(frame_idx as u32).is_err() {
                panic!("Failed to flush dirty frame {} on dealloc", frame_idx);
            }
        }
        let self_mut = unsafe { self.as_mut().get_unchecked_mut() };
        self_mut.frames[frame_idx] = None;

        // bookkeeping
        self_mut.frames_meta_pid.remove(&page_id);
        self_mut.frames_meta_offset.remove(&file_offset);
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

    pub fn core(self: Pin<&mut Self>) -> Pin<&mut BufferPoolCore> {
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
        page_id: page::base::PageId,
    ) -> Result<&mut Frame, errors::AllocNewPageError> {
        self.core().alloc_new_page(page_kind, page_id)
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

    pub fn mark_frame_dirty(self: Pin<&mut Self>, frame_id: u32) {
        self.core().mark_frame_dirty(frame_id)
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

    #[derive(Debug)]
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

#[cfg(test)]
mod buffer_tests {
    use super::*;
    use crate::constants;
    use crate::storage::buffer::fifo_evictor::FifoEvictor;
    use crate::storage::disk::FileManager;
    use crate::storage::page::base::{PageId, PageKind};
    use crate::storage::page_locator::locator::{self, PageLocator};
    use std::fs;
    use std::path::PathBuf;

    use std::sync::atomic::{AtomicU64, Ordering};

    struct MockPageLocator;
    impl PageLocator for MockPageLocator {
        fn find_file_offset(
            &mut self,
            _page_id: PageId,
            _core: Pin<&mut BufferPoolCore>,
        ) -> Result<u64, locator::errors::FindOffsetError> {
            Err(locator::errors::FindOffsetError::NotFound)
        }
    }

    fn setup_buffer_pool_test(test_name: &str) -> (PathBuf, Pin<Box<BufferPool>>, AtomicU64) {
        let mut temp_dir = std::env::temp_dir();
        temp_dir.push(format!("nimbus_test_{}.db", test_name));
        let temp_file_path = temp_dir.clone();
        let temp_file_str = temp_file_path.to_str().expect("Invalid temp file path");

        let _ = fs::remove_file(&temp_file_path);

        let file_manager =
            FileManager::new(temp_file_str.to_string()).expect("Failed to create FileManager");

        let evictor = Box::new(FifoEvictor::new());

        let page_locator = Box::new(MockPageLocator);

        let buffer_pool = Box::pin(BufferPool::new(file_manager, evictor, page_locator));

        let page_id_cnt = AtomicU64::new(0);
        (temp_file_path, buffer_pool, page_id_cnt)
    }

    fn generate_test_page_id(counter: &AtomicU64) -> PageId {
        // ID start from 1.
        let next_id = counter.fetch_add(1, Ordering::SeqCst) + 1;
        PageId::new(next_id).expect("Page ID counter overflowed in test")
    }

    fn cleanup_temp_file(temp_file_path: &PathBuf) {
        let _ = fs::remove_file(temp_file_path);
    }

    #[test]
    fn test_fetch_page_at_offset_hit() {
        let (temp_path, mut buffer_pool, page_id_counter) =
            setup_buffer_pool_test("fetch_page_hit");

        let page_id = generate_test_page_id(&page_id_counter);
        // Allocate a page
        let page_kind = PageKind::SlottedData;
        let frame1 = buffer_pool
            .as_mut()
            .alloc_new_page(page_kind, page_id)
            .expect("Alloc page 1 failed");
        let offset1 = frame1.file_offset;
        let page_id1 = frame1.page_id();
        let frame_id1 = frame1.fid();

        let mut page_view = frame1.page_view();
        match &mut page_view {
            crate::storage::page::base::Page::SlottedData(page) => page.set_page_id(page_id1),
            _ => panic!("Expected SlottedData Page"),
        }
        assert_eq!(offset1, 0);

        // fetch the same page again by offset
        let frame2_result = buffer_pool.as_mut().fetch_page_at_offset(offset1);
        assert!(
            frame2_result.is_ok(),
            "Fetching page by offset (hit) failed"
        );
        let frame2 = frame2_result.unwrap();

        // verify it's the same frame and it's pinned
        assert_eq!(
            frame2.fid(),
            frame_id1,
            "Cache hit should return the same frame id"
        );
        assert_eq!(frame2.page_id(), page_id1, "Cache hit page id mismatch");
        assert!(
            frame2.pinned(),
            "Frame fetched via cache hit should be pinned"
        );

        //Unpin the frame (once is enough if no pin count)
        let unpin_res = buffer_pool.as_mut().unpin_frame(frame_id1);
        assert!(unpin_res.is_ok(), "Unpin failed");

        cleanup_temp_file(&temp_path);
    }

    #[test]
    fn test_fetch_page_at_offset_miss() {
        let (temp_path, mut buffer_pool, _) = setup_buffer_pool_test("fetch_page_miss");

        let page_id_on_disk = PageId::new(100).unwrap();
        let offset_on_disk: u64 = 0;
        let mut page_buf_disk = [0u8; constants::storage::PAGE_SIZE];

        {
            let mut fm_direct = FileManager::new(temp_path.to_str().unwrap().to_string()).unwrap();

            // Initialize buffer for SlottedData page
            page::base::init_page_buf(&mut page_buf_disk, PageKind::SlottedData);
            // Need to set PageId manually in the buffer
            page_buf_disk[8..16].copy_from_slice(&page_id_on_disk.get().to_le_bytes());

            // Write this buffer directly to the file
            fm_direct
                .write_block_from(offset_on_disk, &page_buf_disk)
                .expect("Failed to write initial page directly to disk");
        } // fm_direct goes out of scope, file is closed

        // fetch the page using the buffer pool, should be a cache miss
        let frame_result = buffer_pool.as_mut().fetch_page_at_offset(offset_on_disk);
        assert!(
            frame_result.is_ok(),
            "Fetching page from disk (miss) failed: {:?}",
            frame_result.err()
        );
        let frame = frame_result.unwrap();

        // Verify frame properties
        assert_eq!(frame.file_offset, offset_on_disk, "Frame offset mismatch");
        assert_eq!(frame.page_id(), page_id_on_disk, "Frame PageId mismatch");
        assert!(frame.ready(), "Fetched frame should be ready");
        assert!(frame.pinned(), "Fetched frame should be pinned");
        assert!(!frame.dirty(), "Frame loaded from disk should not be dirty");
        assert_eq!(frame.fid(), 0, "First fetched frame should have fid 0");

        let frame_id = frame.fid();

        let unpin_result = buffer_pool.as_mut().unpin_frame(frame_id);
        assert!(unpin_result.is_ok(), "Unpinning fetched frame failed");

        cleanup_temp_file(&temp_path);
    }

    #[test]
    fn test_buffer_pool_full_eviction() {
        let (temp_path, mut buffer_pool, page_id_cnt) = setup_buffer_pool_test("eviction");

        let mut allocated_offsets = Vec::new();

        // Fill the buffer pool completely
        for i in 0..FRAME_COUNT {
            let page_id = generate_test_page_id(&page_id_cnt);
            let frame = buffer_pool
                .as_mut()
                .alloc_new_page(PageKind::SlottedData, page_id)
                .expect(&format!("Failed to alloc page {}", i));

            let mut page_view = frame.page_view();
            match &mut page_view {
                page::base::Page::SlottedData(page) => page.set_page_id(page_id),
                _ => panic!("Expected SlottedData Page"),
            }

            allocated_offsets.push(frame.file_offset);
        }

        // Flush all frames to ensure they can be evicted cleanly
        buffer_pool.as_mut().flush_all().expect("Flush all failed");

        let page_id2 = generate_test_page_id(&page_id_cnt);
        let extra_frame_result = buffer_pool
            .as_mut()
            .alloc_new_page(PageKind::SlottedData, page_id2);
        assert!(
            extra_frame_result.is_ok(),
            "Allocating page beyond capacity failed: {:?}",
            extra_frame_result.err()
        );
        let extra_frame = extra_frame_result.unwrap();

        // Set the page_id in the buffer
        let mut page_view = extra_frame.page_view();
        match &mut page_view {
            page::base::Page::SlottedData(page) => page.set_page_id(page_id2),
            _ => panic!("Expected SlottedData Page"),
        }

        let extra_offset = extra_frame.file_offset;

        // Check that the first allocated page was evicted
        // by fetching it again - it should be re-read from disk
        let first_offset = allocated_offsets[0];
        let frame_after_evict_result = buffer_pool.as_mut().fetch_page_at_offset(first_offset);

        assert!(
            frame_after_evict_result.is_ok(),
            "Fetching evicted page failed"
        );
        let frame_after_evict = frame_after_evict_result.unwrap();

        // The newly allocated frame's offset should be different from the first one
        assert_ne!(
            extra_offset, first_offset,
            "New page offset should differ from the evicted page offset"
        );

        // Cleanup - unpin the frame we fetched (fetch_page_at_offset pins it)
        let evicted_frame_id = frame_after_evict.fid();
        buffer_pool
            .as_mut()
            .unpin_frame(evicted_frame_id)
            .expect("Unpin failed");

        cleanup_temp_file(&temp_path);
    }

    #[test]
    fn test_pinned_frame_prevents_eviction() {
        let (temp_path, mut buffer_pool, page_id_cnt) = setup_buffer_pool_test("pinned_eviction");

        let mut allocated_offsets = Vec::new();

        let page_id0 = generate_test_page_id(&page_id_cnt);
        let frame0 = buffer_pool
            .as_mut()
            .alloc_new_page(PageKind::SlottedData, page_id0)
            .unwrap();

        let mut page_view = frame0.page_view();
        match &mut page_view {
            page::base::Page::SlottedData(page) => page.set_page_id(page_id0),
            _ => panic!("Expected SlottedData Page"),
        }

        let fid0 = frame0.fid();
        let offset0 = frame0.file_offset;
        let page_id0 = frame0.page_id();

        buffer_pool
            .as_mut()
            .pin_frame(fid0)
            .expect("Failed to pin frame 0");
        allocated_offsets.push(offset0);

        // Fill the rest of the buffer pool (these frames remain unpinned)
        for i in 1..FRAME_COUNT {
            let page_id1 = generate_test_page_id(&page_id_cnt);
            let frame = buffer_pool
                .as_mut()
                .alloc_new_page(PageKind::SlottedData, page_id1)
                .expect(&format!("Failed to alloc page {}", i));

            // Set the page_id in the actual page buffer (needed for flushing)
            let mut page_view = frame.page_view();
            match &mut page_view {
                page::base::Page::SlottedData(page) => page.set_page_id(page_id1),
                _ => panic!("Expected SlottedData Page"),
            }

            allocated_offsets.push(frame.file_offset);
        }

        buffer_pool.as_mut().flush_all().expect("Flush all failed");

        let page_id = generate_test_page_id(&page_id_cnt);
        let extra_frame_result = buffer_pool
            .as_mut()
            .alloc_new_page(PageKind::SlottedData, page_id);
        assert!(
            extra_frame_result.is_ok(),
            "Allocating page beyond capacity failed: {:?}",
            extra_frame_result.err()
        );

        let frame0_refetch_result = buffer_pool.as_mut().fetch_page_at_offset(offset0);
        assert!(
            frame0_refetch_result.is_ok(),
            "Fetching pinned frame failed"
        );
        let frame0_refetch = frame0_refetch_result.unwrap();
        assert_eq!(
            frame0_refetch.fid(),
            fid0,
            "Pinned frame fid changed after eviction attempts"
        );
        assert_eq!(
            frame0_refetch.page_id(),
            page_id0,
            "Pinned frame page_id changed"
        );
        assert!(
            frame0_refetch.pinned(),
            "Refetched frame 0 should still be pinned"
        );

        buffer_pool
            .as_mut()
            .unpin_frame(fid0)
            .expect("Failed to unpin frame 0 (first unpin)");
        buffer_pool
            .as_mut()
            .unpin_frame(fid0)
            .expect("Failed to unpin frame 0 (second unpin)");

        cleanup_temp_file(&temp_path);
    }

    #[test]
    fn test_mark_frame_dirty() {
        let (temp_path, mut buffer_pool, page_id_cnt) = setup_buffer_pool_test("mark_dirty");

        let fid1;
        let offset1;
        {
            let page_id1 = generate_test_page_id(&page_id_cnt);
            // Scope for frame1 borrow
            let frame1 = buffer_pool
                .as_mut()
                .alloc_new_page(PageKind::SlottedData, page_id1)
                .unwrap();
            fid1 = frame1.fid();
            offset1 = frame1.file_offset;
            assert!(frame1.dirty(), "Frame 1 should start dirty");
        }
        buffer_pool
            .as_mut()
            .flush_frame(fid1)
            .expect("Flush failed");
        let fid_after_flush;
        {
            // Scope for frame1_check borrow
            let frame1_check = buffer_pool.as_mut().fetch_page_at_offset(offset1).unwrap();
            fid_after_flush = frame1_check.fid();
            assert!(!frame1_check.dirty(), "Frame 1 should be clean after flush");
        }

        // Mark the clean frame as dirty
        let fid_to_mark;
        {
            // Scope for frame_to_mark borrow
            let frame_to_mark_ref = buffer_pool.as_mut().fetch_page_at_offset(offset1).unwrap();
            fid_to_mark = frame_to_mark_ref.fid();
            buffer_pool.as_mut().mark_frame_dirty(fid_to_mark);
        }

        let fid_dirty_check;
        {
            // Scope for frame1_dirty_check borrow
            let frame1_dirty_check_ref =
                buffer_pool.as_mut().fetch_page_at_offset(offset1).unwrap();
            fid_dirty_check = frame1_dirty_check_ref.fid();
            assert!(
                frame1_dirty_check_ref.dirty(),
                "Frame 1 should be dirty after mark_frame_dirty"
            );
        }

        buffer_pool
            .as_mut()
            .unpin_frame(fid_after_flush)
            .expect("Unpin step 2 failed");
        buffer_pool
            .as_mut()
            .unpin_frame(fid_to_mark)
            .expect("Unpin step 3 failed");
        buffer_pool
            .as_mut()
            .unpin_frame(fid_dirty_check)
            .expect("Unpin step 4 failed");

        cleanup_temp_file(&temp_path);
    }

    #[test]
    fn test_flush_clean_frame() {
        let (temp_path, mut buffer_pool, page_id_cnt) = setup_buffer_pool_test("flush_clean");

        let page_id = generate_test_page_id(&page_id_cnt);
        let fid;
        let offset;
        {
            // Scope for frame borrow
            let frame = buffer_pool
                .as_mut()
                .alloc_new_page(PageKind::SlottedData, page_id)
                .unwrap();
            fid = frame.fid();
            offset = frame.file_offset;
            assert!(frame.dirty());
        }

        // Flush it to make it clean
        buffer_pool
            .as_mut()
            .flush_frame(fid)
            .expect("First flush failed");
        let fid_check;
        {
            // Scope for frame_check borrow
            let frame_check_ref = buffer_pool.as_mut().fetch_page_at_offset(offset).unwrap();
            fid_check = frame_check_ref.fid();
            assert!(!frame_check_ref.dirty(), "Frame should be clean");
        }

        let flush_again_result = buffer_pool.as_mut().flush_frame(fid);
        assert!(flush_again_result.is_ok(), "Flushing a clean frame failed");

        buffer_pool
            .as_mut()
            .unpin_frame(fid_check)
            .expect("Unpin failed");

        cleanup_temp_file(&temp_path);
    }

    #[test]
    fn test_flush_non_existent_frame() {
        let (temp_path, mut buffer_pool, _) = setup_buffer_pool_test("flush_non_existent");
        let non_existent_fid = 999;
        let flush_result = buffer_pool.as_mut().flush_frame(non_existent_fid);
        assert!(
            flush_result.is_err(),
            "Flushing non-existent frame should fail"
        );
        assert!(
            matches!(flush_result, Err(errors::FlushFrameError::FrameNotFound)),
            "Incorrect error type"
        );

        cleanup_temp_file(&temp_path);
    }

    #[test]
    #[should_panic]
    fn test_dealloc_pinned_frame_panics() {
        let (temp_path, mut buffer_pool, page_id_cnt) =
            setup_buffer_pool_test("dealloc_pinned_panics");

        let page_id = generate_test_page_id(&page_id_cnt);
        let frame = buffer_pool
            .as_mut()
            .alloc_new_page(PageKind::SlottedData, page_id)
            .unwrap();
        let fid = frame.fid();
        buffer_pool.as_mut().pin_frame(fid).expect("Pinning failed");

        // Directly call dealloc_frame_at (via BufferPoolCore) - This should panic
        let core = buffer_pool.as_mut().core();
        core.dealloc_frame_at(fid as usize); // This line should trigger the panic

        cleanup_temp_file(&temp_path);
    }
}
