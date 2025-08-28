use crate::constants;
use crate::storage::disk;
use crate::storage::page::page_base;
use std::collections::HashMap;

const FRAME_COUNT: usize = 128;

pub struct Frame<'a> {
    pinned: bool,
    dirty: bool,
    page_view: page_base::Page<'a>,
}

pub struct BufferPool {
    frames_backing_buf: Box<[u8; FRAME_COUNT * constants::storage::DISK_PAGE_SIZE]>,
    frames_meta: [Option<Frame<'static>>; FRAME_COUNT],
    file_manager: disk::FileManager,
    free_frames: u32,
    _pin: std::marker::PhantomPinned,
}

impl BufferPool {
    pub fn new(file_manager: disk::FileManager) -> Self {
        Self {
            frames_backing_buf: Box::new([0u8; FRAME_COUNT * constants::storage::DISK_PAGE_SIZE]),
            frames_meta: std::array::from_fn(|_| None),
            file_manager,
            free_frames: FRAME_COUNT as u32,
            _pin: std::marker::PhantomPinned::default(),
        }
    }
}
