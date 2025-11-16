use crate::storage::bplus_tree::SplitResult;
use crate::storage::page::header::PageHeader;
use crate::{
    constants,
    storage::page::base::{self, DiskPage, PageId},
};

pub struct BPlusLeaf<'a> {
    raw: &'a mut base::PageBuf,
}

impl<'a> base::DiskPage for BPlusLeaf<'a> {
    const PAGE_KIND: u8 = base::PageKind::BPlusLeaf as u8;
    const DATA_START: usize = PageHeader::SIZE; // Data starts after the 32-byte header

    fn raw(self: &Self) -> &[u8; constants::storage::PAGE_SIZE] {
        &self.raw
    }
    fn raw_mut(&mut self) -> &mut [u8; constants::storage::PAGE_SIZE] {
        &mut self.raw
    }
}

impl<'a> BPlusLeaf<'a> {
    // Bytes:   | +0        | +1        | +2        | +3        |
    // ---------+-----------+-----------+-----------+-----------|
    // 0..31    |              PageHeader (32 bytes)              |
    //          | (page_kind = BPlusLeaf, level = 0)            |
    // ---------+-----------+-----------+-----------+-----------|
    // 32..     | Key 0 (N bytes) | RowId 0 (8 bytes)           |
    // ---------+-----------+-----------+-----------+-----------|
    // ...      | Key 1 (N bytes) | RowId 1 (8 bytes)           |
    // ---------+-----------+-----------+-----------+-----------|
    // ...      | (Entry array grows downwards)                 |
    // ---------+-----------------------------------------------|
    //          |          <<< FREE SPACE >>>                   |
    // ---------+-----------------------------------------------|
    // 4095     | (End of Page)                                 |
    // ---------------------------------------------------------|
    /// The value in a leaf node is a RowId, which we've packed into a u64.
    const VALUE_SIZE: usize = std::mem::size_of::<u64>(); // 8 bytes for RowId

    /// Returns the size of one key + value(RowId) entry.
    fn entry_size(&self) -> usize {
        self.get_key_size() as usize + Self::VALUE_SIZE
    }

    /// Returns a slice to the key at the given logical index.
    pub fn get_key_at(&self, index: usize) -> &[u8] {
        let key_size = self.get_key_size() as usize;
        let entry_size = self.entry_size();
        let offset = Self::DATA_START + index * entry_size;
        &self.raw[offset..offset + key_size]
    }

    /// Returns the value (packed RowId) at the given logical index.
    fn get_value_at(&self, index: usize) -> u64 {
        let key_size = self.get_key_size() as usize;
        let entry_size = self.entry_size();
        let offset = Self::DATA_START + index * entry_size + key_size;
        let bytes = self.raw[offset..offset + Self::VALUE_SIZE]
            .try_into()
            .expect("Invalid value slice");
        u64::from_le_bytes(bytes)
    }

    /// Writes a key-value entry to the given logical index.
    fn set_entry(&mut self, index: usize, key: &[u8], value: u64) {
        let key_size = self.get_key_size() as usize;
        let entry_size = self.entry_size();
        let offset = Self::DATA_START + index * entry_size;

        self.raw[offset..offset + key_size].copy_from_slice(key);
        self.raw[offset + key_size..offset + entry_size].copy_from_slice(&value.to_le_bytes());
    }

    /// Sets the value for an *existing* key at the given logical index.
    fn set_value_at(&mut self, index: usize, value: u64) {
        let key_size = self.get_key_size() as usize;
        let entry_size = self.entry_size();
        let offset = Self::DATA_START + index * entry_size + key_size;
        self.raw[offset..offset + Self::VALUE_SIZE].copy_from_slice(&value.to_le_bytes());
    }

    /// Creates a new BPlusLeaf page view from a raw buffer.
    pub fn new(raw: &'a mut base::PageBuf) -> Self {
        if raw.len() != constants::storage::PAGE_SIZE {
            panic!(
                "Invalid page buffer size: expected {}",
                constants::storage::PAGE_SIZE
            );
        }
        Self { raw }
    }

    /// Initializes a new BPlusLeaf page.
    pub fn init(&mut self, page_id: PageId, key_size: u32) {
        self.header_mut().init(page_id, base::PageKind::BPlusLeaf);
        self.header_mut().set_level(0);
        self.header_mut().set_key_size(key_size);
        // Data area is implicitly zeroed by init()
    }

    /// Calculates the amount of free space.
    pub fn free_space(&self) -> u32 {
        let data_used = self.num_entries() as u32 * self.entry_size() as u32;
        let data_start = Self::DATA_START as u32;
        (constants::storage::PAGE_SIZE as u32 - data_start) - data_used
    }

    // --- Header Getters ---
    pub fn page_level(&self) -> u16 {
        self.header().level()
    }
    pub fn page_id(&self) -> base::PageId {
        self.header().page_id()
    }
    pub fn prev_sibling(&self) -> Option<base::PageId> {
        let id = self.header().prev_page_id();
        if id == 0 { None } else { Some(id) }
    }
    pub fn next_sibling(&self) -> Option<base::PageId> {
        let id = self.header().next_page_id();
        if id == 0 { None } else { Some(id) }
    }
    pub fn num_entries(&self) -> u16 {
        self.header().num_entries()
    }
    pub fn get_key_size(&self) -> u32 {
        self.header().key_size()
    }

    // --- Header Setters ---
    pub fn set_page_id(&mut self, id: base::PageId) {
        self.header_mut().set_page_id(id);
    }
    pub fn set_level(&mut self, level: u16) {
        self.header_mut().set_level(level);
    }
    pub fn set_prev_sibling(&mut self, id: Option<base::PageId>) {
        self.header_mut().set_prev_page_id(id.unwrap_or(0));
    }
    pub fn set_next_sibling(&mut self, id: Option<base::PageId>) {
        self.header_mut().set_next_page_id(id.unwrap_or(0));
    }
    pub fn set_key_size(&mut self, key_size: u32) {
        self.header_mut().set_key_size(key_size);
    }

    // --- B+ Tree Logic ---

    pub fn min_keys(&self) -> u16 {
        self.calculate_max_keys() / 2
    }

    pub fn calculate_max_keys(&self) -> u16 {
        let space = constants::storage::PAGE_SIZE - Self::DATA_START;
        (space / self.entry_size()) as u16
    }

    pub fn is_underflow(&self) -> bool {
        self.num_entries() < self.min_keys()
    }

    pub fn can_give_key(&self) -> bool {
        self.num_entries() > self.min_keys()
    }

    /// Gets the value (packed RowId) for a given key.
    pub fn get_value(&self, key: &[u8]) -> Option<u64> {
        self.find_key_position(key)
            .map(|pos| self.get_value_at(pos))
    }

    /// Checks if the page has space for one more entry.
    pub fn has_space_for_key(&self) -> bool {
        self.free_space() >= (self.entry_size() as u32)
    }

    /// Inserts a key-value pair, maintaining sorted order.
    pub fn insert_sorted(&mut self, key: &[u8], value: u64) {
        let curr_size = self.num_entries() as usize;

        if let Some(pos) = self.find_key_position(key) {
            // Key already exists. Update its value.
            self.set_value_at(pos, value);
            return;
        }

        // Key not found, find insertion position
        let insert_pos = self.find_insert_position(key);
        let entry_size = self.entry_size();

        // Shift entries to the right
        if insert_pos < curr_size {
            let src = Self::DATA_START + insert_pos * entry_size;
            let dst = Self::DATA_START + (insert_pos + 1) * entry_size;
            let count = (curr_size - insert_pos) * entry_size;
            self.raw.copy_within(src..src + count, dst);
        }

        // Insert new entry
        self.set_entry(insert_pos, key, value);

        // Update metadata
        self.header_mut().set_num_entries((curr_size + 1) as u16);
    }

    /// Find the correct position to insert a key (maintaining sorted order)
    fn find_insert_position(&self, key: &[u8]) -> usize {
        let curr_size = self.num_entries() as usize;

        // Use binary search for better performance
        let mut left = 0;
        let mut right = curr_size;

        while left < right {
            let mid = left + (right - left) / 2;
            let mid_key = self.get_key_at(mid);
            if mid_key < key {
                left = mid + 1;
            } else {
                right = mid;
            }
        }
        left
    }

    /// Removes a key. Returns true if key was found and removed.
    pub fn remove_key(&mut self, key: &[u8]) -> bool {
        let curr_size = self.num_entries() as usize;

        // Find the key
        if let Some(pos) = self.find_key_position(key) {
            let entry_size = self.entry_size();

            // Shift entries to the left
            if pos < curr_size - 1 {
                let src = Self::DATA_START + (pos + 1) * entry_size;
                let dst = Self::DATA_START + pos * entry_size;
                let count = (curr_size - 1 - pos) * entry_size;
                self.raw.copy_within(src..src + count, dst);
            }

            // Update metadata
            self.header_mut().set_num_entries((curr_size - 1) as u16);
            true
        } else {
            false
        }
    }

    /// Find the position of a key, return None if not found
    fn find_key_position(&self, key: &[u8]) -> Option<usize> {
        let curr_size = self.num_entries() as usize;

        // Use binary search
        let mut left = 0;
        let mut right = curr_size;

        while left < right {
            let mid = left + (right - left) / 2;
            let mid_key = self.get_key_at(mid);
            match mid_key.cmp(key) {
                std::cmp::Ordering::Less => left = mid + 1,
                std::cmp::Ordering::Equal => return Some(mid),
                std::cmp::Ordering::Greater => right = mid,
            }
        }
        None
    }

    /// Splits the page, inserting the new key/value, and returns the split data.
    pub fn split_and_get_new_entries(
        &mut self,
        key: &[u8],
        value: u64,
    ) -> Result<(SplitResult, Vec<(Vec<u8>, u64)>), &'static str> {
        let key_size = self.get_key_size() as usize;
        if key.len() != key_size {
            return Err("split_and_get_new_entries: key length mismatch");
        }

        let curr_size = self.num_entries() as usize;
        let total_entries = curr_size + 1;

        // Build vector of all entries INCLUDING THE NEW ONE
        let mut all_entries: Vec<(Vec<u8>, u64)> = Vec::with_capacity(total_entries);
        for i in 0..curr_size {
            all_entries.push((self.get_key_at(i).to_vec(), self.get_value_at(i)));
        }

        // Find insert pos and add new entry
        let insert_pos = all_entries
            .binary_search_by(|(k, _)| k.as_slice().cmp(key))
            .unwrap_or_else(|e| e);
        all_entries.insert(insert_pos, (key.to_vec(), value));

        // Find split point
        let split_point = (total_entries + 1) / 2;

        // Overwrite this (left) page with entries [0..split_point)
        for i in 0..split_point {
            let (key, value) = &all_entries[i];
            self.set_entry(i, key, *value);
        }

        // Update metadata for this page
        self.header_mut().set_num_entries(split_point as u16);

        // Create vector for the new (right) page - entries [split_point..total_entries)
        let new_page_entries: Vec<(Vec<u8>, u64)> = all_entries[split_point..].to_vec();

        let split_key = all_entries[split_point].0.clone();

        Ok((SplitResult { split_key }, new_page_entries))
    }

    /// Get the first key in this leaf
    pub fn get_first_key(&self) -> Option<Vec<u8>> {
        if self.num_entries() == 0 {
            return None;
        }
        Some(self.get_key_at(0).to_vec())
    }

    /// Get the last key in this leaf
    pub fn get_last_key(&self) -> Option<Vec<u8>> {
        let curr_size = self.num_entries() as usize;
        if curr_size == 0 {
            return None;
        }
        Some(self.get_key_at(curr_size - 1).to_vec())
    }

    /// Move the last key-value pair to another leaf (for borrowing)
    pub fn move_last_to(&mut self, target: &mut BPlusLeaf) -> Option<Vec<u8>> {
        let curr_size = self.num_entries() as usize;
        if curr_size == 0 {
            return None;
        }
        let last_idx = curr_size - 1;
        let last_key = self.get_key_at(last_idx).to_vec();
        let last_value = self.get_value_at(last_idx);

        target.insert_at_beginning(&last_key, last_value);
        self.header_mut().set_num_entries(last_idx as u16); // This "removes" the last key

        // Return the key that was moved
        Some(last_key)
    }

    /// Move the first key-value pair to another leaf (for borrowing)
    pub fn move_first_to(&mut self, target: &mut BPlusLeaf) -> Option<Vec<u8>> {
        let curr_size = self.num_entries() as usize;
        if curr_size == 0 {
            return None;
        }

        let first_key = self.get_key_at(0).to_vec();
        let first_value = self.get_value_at(0);

        target.insert_sorted(&first_key, first_value); // Insert at its correct sorted pos
        self.remove_key(&first_key); // This handles shifting

        // Return the new first key of this leaf (if any)
        if self.num_entries() > 0 {
            Some(self.get_key_at(0).to_vec())
        } else {
            None
        }
    }

    /// Move the last key-value pair to the beginning of the target leaf
    /// Returns the key that was moved.
    pub fn move_last_to_beginning_of(&mut self, target: &mut BPlusLeaf) -> Vec<u8> {
        let curr_size = self.num_entries() as usize;
        let last_idx = curr_size - 1;
        let last_key = self.get_key_at(last_idx).to_vec();
        let last_value = self.get_value_at(last_idx);

        target.insert_at_beginning(&last_key, last_value);
        self.header_mut().set_num_entries(last_idx as u16);
        last_key
    }

    /// Move the first key-value pair to the end of the target leaf
    /// Returns the *new* first key of this leaf (for parent update)
    pub fn move_first_to_end_of(&mut self, target: &mut BPlusLeaf) -> Vec<u8> {
        let first_key = self.get_key_at(0).to_vec();
        let first_value = self.get_value_at(0);

        target.insert_sorted(&first_key, first_value);
        self.remove_key(&first_key);

        self.get_key_at(0).to_vec()
    }

    /// Insert a key-value pair at the beginning of the leaf
    fn insert_at_beginning(&mut self, key: &[u8], value: u64) {
        let curr_size = self.num_entries() as usize;
        let entry_size = self.entry_size();

        // Shift all entries right by one
        let src = Self::DATA_START;
        let dst = Self::DATA_START + entry_size;
        let count = curr_size * entry_size;
        self.raw.copy_within(src..src + count, dst);

        // Insert at position 0
        self.set_entry(0, key, value);

        self.header_mut().set_num_entries((curr_size + 1) as u16);
    }

    /// Merge all entries from other_leaf into this one
    pub fn merge_from(&mut self, other_leaf: &mut BPlusLeaf) {
        let other_size = other_leaf.num_entries() as usize;
        for i in 0..other_size {
            let key = other_leaf.get_key_at(i);
            let value = other_leaf.get_value_at(i);
            self.insert_sorted(key, value);
        }

        other_leaf.header_mut().set_num_entries(0);
        self.set_next_sibling(other_leaf.next_sibling());
    }
}
