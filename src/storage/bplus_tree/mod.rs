pub mod tree;
pub use tree::BPlusTree;
#[derive(Debug)]
pub struct SplitResult {
    pub split_key: Vec<u8>,
}
