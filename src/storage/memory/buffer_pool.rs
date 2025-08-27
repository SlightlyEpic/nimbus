use std::collections::HashMap;

use crate::storage::page::page_base::{Page, PageId};

pub struct BufferPoolFrame {
    pinned: bool,
    dirty: bool,
    page: Page,
}

pub struct BufferPool<'a> {
    frames: HashMap<PageId, &'a BufferPoolFrame>,
}

impl<'a> BufferPool<'a> {
    pub fn new() -> Self {
        Self {
            frames: HashMap::new(),
        }
    }
}
