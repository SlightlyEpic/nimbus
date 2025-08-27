use crate::storage::disk::file_manager::FileManager;
use std::path::PathBuf;

pub struct DiskManager {
    data_dir: String,
    files: Vec<FileManager>,
}

impl DiskManager {
    pub fn new(data_dir: String) -> Self {
        Self {
            data_dir,
            files: Vec::new(),
        }
    }

    pub fn add_file(&mut self, name: String) {
        let mut path = PathBuf::from(&self.data_dir);
        path.push(name);

        match FileManager::new(path.to_string_lossy().into()) {
            Ok(file_manager) => self.files.push(file_manager),
            Err(e) => eprintln!("Failed to add file: {}", e),
        }
    }
}
