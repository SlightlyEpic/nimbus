use crate::constants;
use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;

pub struct FileManager {
    file_path: String,
    pub file: File,
}

impl FileManager {
    pub fn new(file_path: String) -> io::Result<Self> {
        let path = Path::new(&file_path);

        if !path.exists() {
            File::create(path)?;
        }

        if cfg!(windows) {
            panic!("Non UNIX systems are not supported");
        }

        let file = File::options()
            .read(true)
            .write(true)
            .custom_flags(libc::O_DIRECT)
            .open(&file_path)?;

        Ok(Self { file_path, file })
    }

    /// buf: Should be a PageBuf slice
    pub fn read_block_into(&mut self, offset: u64, buf: &mut [u8]) -> io::Result<()> {
        let byte_offset = offset * constants::storage::PAGE_SIZE as u64;
        self.file.seek(SeekFrom::Start(byte_offset))?;
        // SAFETY: buf must be 4K aligned and length multiple of 512 (kernel requirement).
        self.file.read_exact(buf)?;

        Ok(())
    }

    /// buf: Should be a PageBuf slice
    pub fn write_block_from(&mut self, offset: u64, buf: &[u8]) -> io::Result<()> {
        let byte_offset = offset * constants::storage::PAGE_SIZE as u64;
        self.file.seek(SeekFrom::Start(byte_offset))?;

        self.file.write_all(buf)?;

        Ok(())
    }
}
