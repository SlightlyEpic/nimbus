use std::fs::File;
use std::io;
use std::path::Path;

pub struct FileManager {
    file_path: String,
    file: File,
}

impl FileManager {
    pub fn new(file_path: String) -> io::Result<Self> {
        let path = Path::new(&file_path);

        if !path.exists() {
            File::create(path)?;
        }

        let file = File::options().read(true).write(true).open(&file_path)?;

        Ok(Self { file_path, file })
    }
}
