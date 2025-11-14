use crate::storage::buffer::BufferPool;
use crate::storage::heap::tuple::Tuple;
use std::pin::Pin;

pub trait Executor {
    /// init prepares the executor for execution.
    fn init(&mut self);
    /// next returns the next tuple from the executor.
    /// It takes the BufferPool as an argument, which it passes to its children.
    fn next(&mut self, bpm: Pin<&mut BufferPool>) -> Option<Tuple>;
}
