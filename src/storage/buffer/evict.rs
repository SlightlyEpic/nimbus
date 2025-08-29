use crate::storage::{buffer::buffer_pool::Frame, page::page_base};

pub trait Evictor {
    fn pick_victim(&mut self) -> Option<usize>;

    fn notify_frame_read(&mut self, frame: &Frame);
    fn notify_pin(&mut self, frame: &Frame);
    fn notify_unpin(&mut self, frame: &Frame);
    fn notify_eviction(&mut self, page_id: page_base::PageId);
}
