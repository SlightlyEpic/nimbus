use std::{
    alloc::{Layout, alloc, dealloc},
    collections::HashMap,
    hash::Hash,
    ptr::{self, NonNull},
};

use crate::storage::page::page::{Page, PageId};

pub struct BufferPoolFrame<'a> {
    pinned: bool,
    dirty: bool,
    page: &'a Page,
}

pub struct BufferPool<'a> {
    frames: HashMap<PageId, &'a BufferPoolFrame<'a>>,
}

impl<'a> BufferPool<'a> {
    pub fn new() -> Self {
        Self {
            frames: HashMap::new(),
        }
    }
}
