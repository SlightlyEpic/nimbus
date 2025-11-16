use crate::storage::page::header::PageHeader;
use crate::{
    constants,
    storage::page::base::{self, DiskPage, PageId},
};

pub struct BPlusInner<'a> {
    raw: &'a mut base::PageBuf,
}

pub struct BPlusInnerSplitData {
    pub key_to_push_up: Vec<u8>,
    pub new_page_keys: Vec<Vec<u8>>,
    pub new_page_children: Vec<base::PageId>,
}

impl<'a> base::DiskPage for BPlusInner<'a> {
    const PAGE_KIND: u8 = base::PageKind::BPlusInner as u8;
    const DATA_START: usize = PageHeader::SIZE;

    fn raw(self: &Self) -> &[u8; constants::storage::PAGE_SIZE] {
        &self.raw
    }
    fn raw_mut(&mut self) -> &mut [u8; constants::storage::PAGE_SIZE] {
        &mut self.raw
    }
}

impl<'a> BPlusInner<'a> {
    // Bytes:   | +0        | +1        | +2        | +3        |
    // ---------+-----------+-----------+-----------+-----------|
    // 0..31    |              PageHeader (32 bytes)              |
    //          | (page_kind = BPlusInner, level > 0)           |
    // ---------+-----------+-----------+-----------+-----------|
    // 32..35   |              first_child_page_id (u32)          |
    // ---------+-----------+-----------+-----------+-----------|
    // 36..     | Key 0 (N bytes) | Child 1 PageId (u32)        |
    // ---------+-----------+-----------+-----------+-----------|
    // ...      | Key 1 (N bytes) | Child 2 PageId (u32)        |
    // ---------+-----------+-----------+-----------+-----------|
    // ...      | (Entry array grows downwards)                 |
    // ---------+-----------------------------------------------|
    //          |          <<< FREE SPACE >>>                   |
    // ---------+-----------------------------------------------|
    // 4095     | (End of Page)                                   |
    // ---------------------------------------------------------|

    /// The "value" in an inner node is a PageId, which is u32 (4 bytes).
    const VALUE_SIZE: usize = std::mem::size_of::<base::PageId>();

    const FIRST_CHILD_ID_START: usize = Self::DATA_START; // 32
    const FIRST_CHILD_ID_END: usize = Self::FIRST_CHILD_ID_START + Self::VALUE_SIZE; // 36
    const DATA_START_ENTRIES: usize = Self::FIRST_CHILD_ID_END; // 36

    /// Returns the size of one key + child_id entry.
    fn entry_size(&self) -> usize {
        self.get_key_size() as usize + Self::VALUE_SIZE
    }

    /// Returns a slice to the key at the given *entry* index (which is key index).
    pub fn get_key_at(&self, index: usize) -> &[u8] {
        let key_size = self.get_key_size() as usize;
        let entry_size = self.entry_size();
        let offset = Self::DATA_START_ENTRIES + index * entry_size;
        &self.raw[offset..offset + key_size]
    }

    /// Returns the child page ID at the given *entry* index (which is child index + 1).
    pub fn get_child_id_at_entry(&self, index: usize) -> base::PageId {
        let key_size = self.get_key_size() as usize;
        let entry_size = self.entry_size();
        let offset = Self::DATA_START_ENTRIES + index * entry_size + key_size;
        let bytes = self.raw[offset..offset + Self::VALUE_SIZE]
            .try_into()
            .expect("Invalid value slice");
        base::PageId::from_le_bytes(bytes)
    }

    /// Writes a key-child_id entry to the given logical index.
    pub fn set_entry(&mut self, index: usize, key: &[u8], child_id: base::PageId) {
        let key_size = self.get_key_size() as usize;
        let entry_size = self.entry_size();
        let offset = Self::DATA_START_ENTRIES + index * entry_size;

        self.raw[offset..offset + key_size].copy_from_slice(key);
        self.raw[offset + key_size..offset + entry_size].copy_from_slice(&child_id.to_le_bytes());
    }

    /// Creates a new BPlusInner page view from a raw buffer.
    pub fn new(raw: &'a mut base::PageBuf) -> Self {
        if raw.len() != constants::storage::PAGE_SIZE {
            panic!(
                "Invalid page buffer size: expected {}",
                constants::storage::PAGE_SIZE
            );
        }
        Self { raw }
    }

    /// Initializes a new BPlusInner page.
    pub fn init(&mut self, page_id: base::PageId, level: u16, key_size: u32) {
        self.header_mut().init(page_id, base::PageKind::BPlusInner);
        self.header_mut().set_level(level);
        self.header_mut().set_key_size(key_size);
        // Zero out the first child pointer area
        self.raw[Self::FIRST_CHILD_ID_START..Self::FIRST_CHILD_ID_END].fill(0);
    }

    /// Calculates the amount of free space.
    pub fn free_space(&self) -> u32 {
        let data_used = self.num_entries() as u32 * self.entry_size() as u32;
        let data_start = Self::DATA_START_ENTRIES as u32;
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
    pub fn parent_page_id(&self) -> PageId {
        self.header().parent_page_id()
    }
    pub fn is_root(&self) -> bool {
        self.header().is_root()
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
    pub fn set_parent_page_id(&mut self, id: PageId) {
        self.header_mut().set_parent_page_id(id);
    }
    pub fn set_root(&mut self, is_root: bool) {
        self.header_mut().set_root(is_root);
    }

    // --- B+ Tree Logic ---

    /// Gets the first key.
    pub fn get_first_key(&self) -> Option<Vec<u8>> {
        if self.num_entries() == 0 {
            return None;
        }
        Some(self.get_key_at(0).to_vec())
    }

    pub fn calculate_max_keys(&self) -> u16 {
        let space = constants::storage::PAGE_SIZE - Self::DATA_START_ENTRIES;
        (space / self.entry_size()) as u16
    }

    pub fn min_keys(&self) -> u16 {
        self.calculate_max_keys() / 2
    }

    pub fn is_underflow(&self) -> bool {
        self.num_entries() < self.min_keys()
    }

    pub fn can_give_key(&self) -> bool {
        self.num_entries() > self.min_keys()
    }

    pub fn has_space_for_key(&self) -> bool {
        self.free_space() >= (self.entry_size() as u32)
    }

    /// Returns the key associated with the child index (separator key).
    /// For child 0, there is no key, so this returns None.
    /// For child i > 0, it corresponds to key at i-1.
    pub fn key_at_child_index(&self, child_idx: usize) -> Option<Vec<u8>> {
        if child_idx == 0 {
            None
        } else {
            Some(self.get_key_at(child_idx - 1).to_vec())
        }
    }

    /// Finds the index of a specific child PageId.
    pub fn lookup_child_index(&self, child_id: base::PageId) -> Option<usize> {
        let num_keys = self.num_entries() as usize;
        // Check first child
        if self.get_child_at(0) == Some(child_id) {
            return Some(0);
        }
        // Check rest
        for i in 0..num_keys {
            let id = self.get_child_id_at_entry(i);
            if id == child_id {
                return Some(i + 1);
            }
        }
        None
    }

    /// Get child at logical index (0 to num_entries inclusive)
    pub fn get_child_at(&self, index: usize) -> Option<base::PageId> {
        let num_keys = self.num_entries() as usize;
        let num_children = num_keys + 1;

        if index >= num_children {
            return None;
        }

        if index == 0 {
            // Get First Child ID
            let bytes = self.raw[Self::FIRST_CHILD_ID_START..Self::FIRST_CHILD_ID_END]
                .try_into()
                .unwrap();
            let val = u32::from_le_bytes(bytes);
            if val == 0 { None } else { Some(val) }
        } else {
            // Get child ID from entry (index - 1)
            Some(self.get_child_id_at_entry(index - 1))
        }
    }

    /// Set child at logical index (0 to num_entries inclusive)
    pub fn set_child_at(&mut self, index: usize, child_id: base::PageId) {
        let num_keys = self.num_entries() as usize;
        let num_children = num_keys + 1;
        if index >= num_children {
            panic!("set_child_at: index out of bounds");
        }

        if index == 0 {
            // Set First Child ID
            self.raw[Self::FIRST_CHILD_ID_START..Self::FIRST_CHILD_ID_END]
                .copy_from_slice(&child_id.to_le_bytes());
        } else {
            // Set child ID in entry (index - 1)
            let key_size = self.get_key_size() as usize;
            let entry_size = self.entry_size();
            let offset = Self::DATA_START_ENTRIES + (index - 1) * entry_size + key_size;
            self.raw[offset..offset + Self::VALUE_SIZE].copy_from_slice(&child_id.to_le_bytes());
        }
    }

    /// Finds the logical index of the child pointer to follow for a given key.
    pub fn find_child_for_key(&self, key: &[u8]) -> usize {
        let num_keys = self.num_entries() as usize;

        // Binary search for the first key *greater* than the one we're looking for
        let mut left = 0;
        let mut right = num_keys;
        let mut result = 0; // Will be 0 if all keys are greater

        while left < right {
            let mid = left + (right - left) / 2;
            let mid_key = self.get_key_at(mid);

            match mid_key.cmp(key) {
                std::cmp::Ordering::Less | std::cmp::Ordering::Equal => {
                    // This key is <= our key, so we must go right.
                    // The child at `mid + 1` is the correct one.
                    result = mid + 1;
                    left = mid + 1;
                }
                std::cmp::Ordering::Greater => {
                    // This key is > our key. This *might* be the one,
                    // but we check the left side.
                    right = mid;
                }
            }
        }
        result // `result` is the correct *logical child index*
    }

    /// Inserts a new (key, child_id) pair at the specified *entry* index.
    pub fn insert_at(&mut self, entry_index: usize, key: &[u8], child_id: PageId) {
        let num_keys = self.num_entries() as usize;
        let entry_size = self.entry_size();

        // Shift entries to the right
        if entry_index < num_keys {
            let src = Self::DATA_START_ENTRIES + entry_index * entry_size;
            let dst = Self::DATA_START_ENTRIES + (entry_index + 1) * entry_size;
            let count = (num_keys - entry_index) * entry_size;
            self.raw.copy_within(src..src + count, dst);
        }

        // Insert new entry
        self.set_entry(entry_index, key, child_id);
        self.header_mut().set_num_entries((num_keys + 1) as u16);
    }

    /// Splits the inner node, inserting the new key/value, and returns the split data.
    pub fn split_and_get_new_entries(
        &mut self,
        key: &[u8],
        child_id: base::PageId,
    ) -> BPlusInnerSplitData {
        let curr_sz = self.num_entries() as usize;
        let total_keys = curr_sz + 1;
        let split_point = (total_keys + 1) / 2;

        let mut all_keys = Vec::with_capacity(total_keys);
        let mut all_children = Vec::with_capacity(total_keys + 1);

        // Find insertion point
        let insert_pos = self.find_child_for_key(key);

        // Copy children and keys before insertion point
        all_children.push(self.get_child_at(0).unwrap());
        for i in 0..insert_pos {
            if i < curr_sz {
                all_keys.push(self.get_key_at(i).to_vec());
                all_children.push(self.get_child_at(i + 1).unwrap());
            }
        }

        // Add the new entry
        all_keys.insert(insert_pos, key.to_vec());
        all_children.insert(insert_pos + 1, child_id);

        // Copy remaining children and keys
        for i in insert_pos..curr_sz {
            all_keys.push(self.get_key_at(i).to_vec());
            all_children.push(self.get_child_at(i + 1).unwrap());
        }

        // The key to push up is the one at the split point
        let key_to_push_up = all_keys.remove(split_point);

        // Overwrite this (left) page
        self.set_child_at(0, all_children[0]);
        for i in 0..split_point {
            self.set_entry(i, &all_keys[i], all_children[i + 1]);
        }
        self.header_mut().set_num_entries(split_point as u16);

        // Get entries for the new (right) page
        let new_page_keys = all_keys[split_point..].to_vec();
        let new_page_children = all_children[split_point + 1..].to_vec();

        // The first child of the new page is special
        let new_page_first_child = all_children[split_point];

        let mut children_for_data = vec![new_page_first_child];
        children_for_data.extend(new_page_children);

        BPlusInnerSplitData {
            key_to_push_up,
            new_page_keys,
            new_page_children: children_for_data,
        }
    }

    /// Removes the key (and its *right* child) at the given *entry* index.
    pub fn remove_at(&mut self, entry_index: usize) -> PageId {
        let num_keys = self.num_entries() as usize;
        let entry_size = self.entry_size();

        let removed_child_id = self.get_child_id_at_entry(entry_index);

        // Shift entries left
        if entry_index < num_keys - 1 {
            let src = Self::DATA_START_ENTRIES + (entry_index + 1) * entry_size;
            let dst = Self::DATA_START_ENTRIES + entry_index * entry_size;
            let count = (num_keys - 1 - entry_index) * entry_size;
            self.raw.copy_within(src..src + count, dst);
        }

        self.header_mut().set_num_entries((num_keys - 1) as u16);
        removed_child_id
    }

    pub fn move_last_to_beginning_of(
        &mut self,
        target: &mut BPlusInner,
        separator_key: &[u8],
    ) -> Vec<u8> {
        let num_keys = self.num_entries() as usize;

        let last_key = self.get_key_at(num_keys - 1).to_vec();
        let last_child = self.get_child_at(num_keys).unwrap();

        self.header_mut().set_num_entries((num_keys - 1) as u16);

        let target_first_child = target.get_child_at(0).unwrap();

        target.set_child_at(0, last_child);
        target.insert_at(0, separator_key, target_first_child);

        // Return the new separator key
        last_key
    }

    pub fn move_first_to_end_of(
        &mut self,
        target: &mut BPlusInner,
        separator_key: &[u8],
    ) -> Vec<u8> {

        let first_key = self.get_key_at(0).to_vec();
        let first_child = self.get_child_at(0).unwrap();
        let second_child = self.get_child_at(1).unwrap();

        let target_num_keys = target.num_entries() as usize;
        target.insert_at(target_num_keys, separator_key, first_child);

        self.set_child_at(0, second_child);
        self.remove_at(0);

        // Return the new separator key
        first_key
    }

    /// Merges all entries from `other_node` into this one, using the `separator_key`.
    pub fn merge_from(&mut self, other_node: &mut BPlusInner, separator_key: &[u8]) {
        let num_keys = self.num_entries() as usize;
        let other_num_keys = other_node.num_entries() as usize;

        // Add separator key and other's first child
        let other_first_child = other_node.get_child_at(0).unwrap();
        self.insert_at(num_keys, separator_key, other_first_child);

        // Add all remaining keys and children from other
        for i in 0..other_num_keys {
            let key = other_node.get_key_at(i);
            let child = other_node.get_child_at(i + 1).unwrap();
            self.insert_at(num_keys + 1 + i, key, child);
        }

        other_node.header_mut().set_num_entries(0);
    }
}
