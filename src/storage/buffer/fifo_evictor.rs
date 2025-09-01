use crate::storage::buffer::{Evictor, Frame};
use std::collections::{HashMap, HashSet, VecDeque};

pub struct FifoEvictor<'a> {
    victim_queue: VecDeque<&'a Frame>,
    fid_idx_map: HashMap<u32, usize>,
    frames_tracked: HashSet<u32>,
    marked_for_eviction: HashSet<u32>,
}

impl<'a> FifoEvictor<'a> {
    pub fn new() -> Self {
        Self {
            victim_queue: VecDeque::new(),
            fid_idx_map: HashMap::new(),
            frames_tracked: HashSet::new(),
            marked_for_eviction: HashSet::new(),
        }
    }
}

impl<'a> Evictor<'a> for FifoEvictor<'a> {
    fn pick_victim(&mut self) -> Option<&'a Frame> {
        for i in 0..self.victim_queue.len() {
            let frame = self.victim_queue[i];
            // no need to check is_ready because there wont be any non-ready frames
            // in the victim queue
            if !frame.pinned() && !self.marked_for_eviction.contains(&frame.fid()) {
                self.marked_for_eviction.insert(frame.fid());
                self.victim_queue.remove(i);
                return Some(frame);
            }
        }
        None
    }

    // Frames that are not loaded into wont be queued for eviction
    fn notify_frame_alloc(&mut self, frame: &Frame) {}

    fn notify_frame_load(&mut self, frame: &'a Frame) {
        let frame_page_id = frame.fid();
        if self.frames_tracked.contains(&frame_page_id) {
            return;
        }
        self.victim_queue.push_back(frame);
        self.frames_tracked.insert(frame.fid());
    }

    fn notify_frame_flush(&mut self, frame: &Frame) {}

    fn notify_frame_pin(&mut self, frame: &Frame) {}

    fn notify_frame_unpin(&mut self, frame: &Frame) {}

    fn notify_frame_destroy(&mut self, frame_id: u32) {
        self.marked_for_eviction.remove(&frame_id);
    }
}
