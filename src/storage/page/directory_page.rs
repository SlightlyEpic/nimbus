use crate::constants;
use crate::storage::page::{DiskPage, page};
use crate::storage::util;
use std::num::NonZeroU64;

// Stores the mapping from page_id -> file_offset
// Directory pages form a linked list
pub struct DirectoryPage {
    page_id: page::PageId,
    next_offset: Option<NonZeroU64>,
    entries: Vec<DirectoryEntry>,
}

pub struct DirectoryEntry {
    page_id: page::PageId,
    file_offset: NonZeroU64,
    free_space: usize,
}

impl DiskPage for DirectoryPage {
    const PAGE_KIND: u8 = page::PageKind::Directory as u8;
}

impl util::SerdeFixed<{ constants::storage::DISK_PAGE_SIZE }> for DirectoryPage {
    fn serialize(self: &Self) -> [u8; constants::storage::DISK_PAGE_SIZE] {
        // Layout format:
        // <header>: Containes page kind, so from_disk_page can verify whether it is reading a directory page or not. Also contains a page_id
        // <next offset>: offset of the next directory page
        // <entries length>: number of entries
        // <entries>: serialized entries
        // <padding>: padding to reach the required size (if needed)
        let mut buf = [0u8; constants::storage::DISK_PAGE_SIZE];

        // page kind
        buf[0] = Self::PAGE_KIND;

        // page_id
        buf[1..9].copy_from_slice(&self.page_id.get().to_le_bytes());

        // next_offset
        let next = self.next_offset.map(|n| n.get()).unwrap_or(0);
        buf[9..17].copy_from_slice(&next.to_le_bytes());

        // entries length
        let len = self.entries.len() as u32;
        buf[17..21].copy_from_slice(&len.to_le_bytes());

        // Serialize each entry
        let mut pos = 21;
        for entry in &self.entries {
            // page_id
            buf[pos..pos + 8].copy_from_slice(&entry.page_id.get().to_le_bytes());
            pos += 8;

            // file_offset
            buf[pos..pos + 8].copy_from_slice(&entry.file_offset.get().to_le_bytes());
            pos += 8;

            // free_space
            let free_u32 = entry.free_space as u32;
            buf[pos..pos + 4].copy_from_slice(&free_u32.to_le_bytes());
            pos += 4;
        }

        buf
    }

    fn deserialize(raw: &[u8; constants::storage::DISK_PAGE_SIZE]) -> Self {
        // Reconstruct from the data array using the layout specified above

        // Page kind check
        let kind = raw[0];
        assert_eq!(kind, Self::PAGE_KIND, "Wrong page kind");

        // page_id
        let page_id_val = u64::from_le_bytes(raw[1..9].try_into().unwrap());
        let page_id = page::PageId::new(page_id_val);

        // next_offset
        let next_raw = u64::from_le_bytes(raw[9..17].try_into().unwrap());
        let next_offset = NonZeroU64::new(next_raw);

        // entries length
        let entries_len = u32::from_le_bytes(raw[17..21].try_into().unwrap()) as usize;

        // Deserialize entries
        let mut entries = Vec::with_capacity(entries_len);
        let mut pos = 21;
        for _ in 0..entries_len {
            let pid_val = u64::from_le_bytes(raw[pos..pos + 8].try_into().unwrap());
            pos += 8;

            let offset_val = u64::from_le_bytes(raw[pos..pos + 8].try_into().unwrap());
            pos += 8;

            let free_val = u32::from_le_bytes(raw[pos..pos + 4].try_into().unwrap()) as usize;
            pos += 4;

            entries.push(DirectoryEntry {
                page_id: page::PageId::new(pid_val),
                file_offset: NonZeroU64::new(offset_val).unwrap(),
                free_space: free_val,
            });
        }

        Self {
            page_id,
            next_offset,
            entries,
        }
    }
}
