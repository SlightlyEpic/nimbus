use crate::storage::buffer::{Evictor, Frame};
use std::collections::{HashMap, HashSet, VecDeque};

struct EvictorFrameMeta {
    pub fid: u32,
    // the evictor will not track pinned, dirty, ready etc.
    // it is the responsibility of the consumer to inform the
    // evictor when a frame's evictability changes
    pub evictable: bool,
    pub evicted: bool,
}

pub struct FifoEvictor {
    victim_queue: VecDeque<u32>, // frame_id
    fid_idx_map: HashMap<u32, usize>,
    frames_meta: HashMap<u32, EvictorFrameMeta>, // frame_id -> meta
}

impl FifoEvictor {
    pub fn new() -> Self {
        Self {
            victim_queue: VecDeque::new(),
            fid_idx_map: HashMap::new(),
            frames_meta: HashMap::new(),
        }
    }
}

impl Evictor for FifoEvictor {
    fn pick_victim(&mut self) -> Option<u32> {
        for i in 0..self.victim_queue.len() {
            let frame_id = self.victim_queue[i];
            let frame_meta = self.frames_meta.get(&frame_id).unwrap();
            if frame_meta.evictable {
                return Some(frame_id);
            }
        }
        None
    }

    fn notify_frame_alloc(&mut self, frame: &Frame) {
        let frame_id = frame.fid();
        if self.fid_idx_map.contains_key(&frame_id) {
            return;
        }
        self.victim_queue.push_back(frame_id);
        self.fid_idx_map
            .insert(frame_id, self.victim_queue.len() - 1);
    }

    fn set_frame_evictable(&mut self, frame: &Frame, evictable: bool) {
        let frame_id = frame.fid();
        if let Some(frame_meta) = self.frames_meta.get_mut(&frame_id) {
            frame_meta.evictable = evictable;
        }
    }

    fn notify_frame_destroy(&mut self, frame_id: u32) {
        let queue_idx = self.fid_idx_map.get(&frame_id);
        if queue_idx.is_none() {
            return;
        }
        let queue_idx = *queue_idx.unwrap();
        self.frames_meta.remove(&frame_id);
        self.fid_idx_map.remove(&frame_id);
        self.victim_queue.remove(queue_idx);
    }
}
