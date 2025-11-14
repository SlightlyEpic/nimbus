use super::executor::Executor;
use crate::storage::buffer::BufferPool;
use crate::storage::heap::tuple::Tuple;
use std::pin::Pin;

/// Filters tuples based on a predicate closure.
/// Example: WHERE age > 20
pub struct FilterExecutor<'a, P>
where
    P: Fn(&Tuple) -> bool,
{
    child: Box<dyn Executor + 'a>,
    predicate: P,
}

impl<'a, P> FilterExecutor<'a, P>
where
    P: Fn(&Tuple) -> bool,
{
    pub fn new(child: Box<dyn Executor + 'a>, predicate: P) -> Self {
        Self { child, predicate }
    }
}

impl<'a, P> Executor for FilterExecutor<'a, P>
where
    P: Fn(&Tuple) -> bool,
{
    fn init(&mut self) {
        self.child.init();
    }

    fn next(&mut self, mut bpm: Pin<&mut BufferPool>) -> Option<Tuple> {
        // Pull from child until we find a match or run out
        while let Some(tuple) = self.child.next(bpm.as_mut()) {
            // Pass bpm
            if (self.predicate)(&tuple) {
                return Some(tuple);
            }
        }
        None
    }
}
