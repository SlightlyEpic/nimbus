use crate::storage::page::base::PageId;

/// A RowId uniquely identifies a row on a SlottedData page.
/// It's packed into a u64 to be stored as a value in the B+ Tree.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct RowId(u64);

impl RowId {
    /// Creates a new RowId from a PageId (u32) and slot number (u32).
    pub fn new(page_id: PageId, slot_num: u32) -> Self {
        // Pack PageId (u32) into the high 32 bits
        // and SlotNum (u32) into the low 32 bits.
        let page_id_u64 = page_id as u64;
        let slot_num_u64 = slot_num as u64;
        Self((page_id_u64 << 32) | slot_num_u64)
    }

    /// Returns the packed u64 value to be stored in the index.
    pub fn to_u64(self) -> u64 {
        self.0
    }

    /// Creates a RowId from a packed u64 value from the index.
    pub fn from_u64(value: u64) -> Self {
        Self(value)
    }

    /// Unpacks the PageId (high 32 bits).
    pub fn page_id(self) -> PageId {
        (self.0 >> 32) as PageId
    }

    /// Unpacks the SlotNum (low 32 bits).
    pub fn slot_num(self) -> u32 {
        (self.0 & 0xFFFFFFFF) as u32
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_row_id_packing() {
        let page_id: PageId = 123;
        let slot_num: u32 = 456;

        let rid = RowId::new(page_id, slot_num);
        let packed = rid.to_u64();

        // 123 << 32 | 456
        // 0x0000007B 000001C8
        let expected_packed: u64 = 528280977848;
        assert_eq!(packed, expected_packed);

        let unpacked_rid = RowId::from_u64(packed);
        assert_eq!(unpacked_rid.page_id(), page_id);
        assert_eq!(unpacked_rid.slot_num(), slot_num);
    }
}
