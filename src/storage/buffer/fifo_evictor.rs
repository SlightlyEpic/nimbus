use crate::storage::{
    buffer::{Evictor, Frame},
    page::page_base,
};
use std::collections::{HashSet, VecDeque};

pub struct FifoEvictor {
    victim_queue: VecDeque<page_base::PageId>,
    pinned: VecDeque<bool>,
    pages_tracked: HashSet<page_base::PageId>,
}

impl FifoEvictor {
    pub fn new() -> Self {
        Self {
            victim_queue: VecDeque::new(),
            pinned: VecDeque::new(),
            pages_tracked: HashSet::new(),
        }
    }
}

impl Evictor for FifoEvictor {
    fn pick_victim(&mut self) -> Option<usize> {
        for i in 0..self.victim_queue.len() {
            if !self.pinned[i] {
                return Some(i);
            }
        }
        None
    }

    fn notify_frame_read(&mut self, frame: &Frame) {
        let frame_page_id = frame.page_id();
        if self.pages_tracked.contains(&frame_page_id) {
            return;
        }
        self.victim_queue.push_back(frame_page_id);
        self.pinned.push_back(false);
        self.pages_tracked.insert(frame_page_id);
    }

    fn notify_pin(&mut self, frame: &Frame) {
        let frame_page_id = frame.page_id();
        for i in 0..self.victim_queue.len() {
            if self.victim_queue[i] == frame_page_id {
                self.pinned[i] = true;
                break;
            }
        }
    }

    fn notify_unpin(&mut self, frame: &Frame) {
        let frame_page_id = frame.page_id();
        for i in 0..self.victim_queue.len() {
            if self.victim_queue[i] == frame_page_id {
                self.pinned[i] = false;
                break;
            }
        }
    }

    fn notify_eviction(&mut self, page_id: page_base::PageId) {
        for i in 0..self.victim_queue.len() {
            if self.victim_queue[i] == page_id {
                self.victim_queue.remove(i);
                self.pinned.remove(i);
                self.pages_tracked.remove(&page_id);
            }
        }
    }
}
