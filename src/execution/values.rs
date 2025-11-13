use super::executor::Executor;
use crate::storage::heap::tuple::Tuple;

/// Generates tuples from a static list (e.g., "VALUES (1), (2), (3)")
pub struct ValuesExecutor {
    tuples: Vec<Tuple>,
    cursor: usize,
}

impl ValuesExecutor {
    pub fn new(tuples: Vec<Tuple>) -> Self {
        Self { tuples, cursor: 0 }
    }
}

impl Executor for ValuesExecutor {
    fn init(&mut self) {
        self.cursor = 0;
    }

    fn next(&mut self) -> Option<Tuple> {
        if self.cursor < self.tuples.len() {
            let tuple = self.tuples[self.cursor].clone();
            self.cursor += 1;
            Some(tuple)
        } else {
            None
        }
    }
}
