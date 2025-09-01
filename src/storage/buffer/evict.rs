use crate::storage::buffer::buffer_pool::Frame;

pub trait Evictor {
    fn pick_victim(&mut self) -> Option<u32>;

    fn notify_frame_alloc(&mut self, frame: &Frame);
    fn set_frame_evictable(&mut self, frame: &Frame, evictable: bool);
    fn notify_frame_destroy(&mut self, frame_id: u32);
}
