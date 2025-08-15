use crate::constants;
use crate::storage::page::page;
use crate::storage::util::{self, SerdeDyn};

pub struct DataPage {
    page_id: page::PageId,
    // data: Vec<T>,
}

impl page::DiskPage for DataPage {
    const PAGE_KIND: u8 = page::PageKind::Data as u8;
}

impl util::SerdeFixed<{ constants::storage::DISK_PAGE_SIZE }> for DataPage {
    fn serialize(self: &Self) -> [u8; constants::storage::DISK_PAGE_SIZE] {
        let mut buf = [0u8; constants::storage::DISK_PAGE_SIZE];

        todo!();
    }

    fn deserialize(data: &[u8; constants::storage::DISK_PAGE_SIZE]) -> Self {
        todo!();
    }
}
