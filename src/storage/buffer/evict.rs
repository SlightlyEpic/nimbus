use crate::storage::buffer::buffer_pool::Frame;

pub trait Evictor<'a> {
    fn pick_victim(&mut self) -> Option<&'a Frame>;

    fn notify_frame_alloc(&mut self, frame: &'a Frame);
    fn notify_frame_load(&mut self, frame: &'a Frame);
    fn notify_frame_flush(&mut self, frame: &'a Frame);
    fn notify_frame_pin(&mut self, frame: &'a Frame);
    fn notify_frame_unpin(&mut self, frame: &'a Frame);
    fn notify_frame_destroy(&mut self, frame_id: u32);
}
