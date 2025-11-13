use super::executor::Executor;
use crate::storage::heap::tuple::Tuple;

// Indices of the columns to keep (e.g., [0, 2] keeps the 1st and 3rd columns)
pub struct ProjectionExecutor<'a> {
    child: Box<dyn Executor + 'a>,
    column_indices: Vec<usize>,
}

impl<'a> ProjectionExecutor<'a> {
    pub fn new(child: Box<dyn Executor + 'a>, column_indices: Vec<usize>) -> Self {
        Self {
            child,
            column_indices,
        }
    }
}

impl<'a> Executor for ProjectionExecutor<'a> {
    fn init(&mut self) {
        self.child.init();
    }

    fn next(&mut self) -> Option<Tuple> {
        if let Some(tuple) = self.child.next() {
            let mut new_values = Vec::new();

            for &idx in &self.column_indices {
                if idx < tuple.values.len() {
                    new_values.push(tuple.values[idx].clone());
                } else {
                    panic!("Projection index {} out of bounds", idx);
                }
            }

            Some(Tuple::new(new_values))
        } else {
            None
        }
    }
}
