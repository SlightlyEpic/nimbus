use crate::storage::heap::tuple::Tuple;

pub trait Executor {
    fn init(&mut self);
    fn next(&mut self) -> Option<Tuple>;
}
