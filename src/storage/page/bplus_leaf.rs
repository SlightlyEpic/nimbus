use crate::storage::bplus_tree::SplitResult;
use crate::{constants, storage::page::base};

pub struct BPlusLeaf<'a> {
    raw: &'a mut base::PageBuf,
}

impl<'a> base::DiskPage for BPlusLeaf<'a> {
    const PAGE_KIND: u8 = base::PageKind::BPlusLeaf as u8;

    fn raw(self: &Self) -> &[u8; constants::storage::PAGE_SIZE] {
        &self.raw
    }

    fn raw_mut(&mut self) -> &mut [u8; constants::storage::PAGE_SIZE] {
        &mut self.raw
    }
}

impl<'a> BPlusLeaf<'a> {
    // == Memory layout ==
    //   0..  1     -> Page Kind                        (u8)   -|
    //   1..  3     -> Level                            (u16)   |
    //   3..  4     -> Reserved                         (u8)    |
    //   4..  8     -> Free Space (bytes)               (u32)   |
    //   8.. 16     -> Page ID                         (u64)    | Header (64 bytes)
    //  16.. 24     -> Prev Sibling Page ID            (u64)    |
    //  24.. 32     -> Next Sibling Page ID            (u64)    |
    //  32.. 36     -> Current Vector Size (num pairs) (u32)    |
    //  36.. 40     -> Key Size                        (u32)    |
    //  40.. 64     -> Reserved                                -|
    //  64.. N      -> Entry 0 (Key | Value)
    //  N.. M      -> Entry 1 (Key | Value)
    //  ...
    //  Y.. Z      -> Entry N-1 (Key | Value)
    //  Z.. PAGE_SIZE -> Free Space
    //
    // Each Entry is (key_size + 8) bytes.

    const DATA_START: usize = 64;

    /// Returns the size of one key + value entry.
    fn entry_size(&self) -> usize {
        self.get_key_size() as usize + std::mem::size_of::<u64>()
    }

    /// Returns a slice to the key at the given logical index.
    fn get_key_at(&self, index: usize) -> &[u8] {
        let key_size = self.get_key_size() as usize;
        let entry_size = self.entry_size();
        let offset = Self::DATA_START + index * entry_size;
        &self.raw[offset..offset + key_size]
    }

    /// Returns the value (u64) at the given logical index.
    fn get_value_at(&self, index: usize) -> u64 {
        let key_size = self.get_key_size() as usize;
        let entry_size = self.entry_size();
        let offset = Self::DATA_START + index * entry_size + key_size;
        let bytes = self.raw[offset..offset + 8]
            .try_into()
            .expect("Invalid value slice");
        u64::from_le_bytes(bytes)
    }

    /// Writes a key-value entry to the given logical index.
    /// Assumes space has already been allocated (e.g., by shifting).
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
        self.raw[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
    }

    pub fn new(raw: &'a mut base::PageBuf) -> Self {
        if raw.len() != constants::storage::PAGE_SIZE {
            panic!(
                "Invalid page buffer size: expected {}",
                constants::storage::PAGE_SIZE
            );
        }
        Self { raw }
    }

    pub fn init(&mut self, page_id: base::PageId) {
        self.set_page_kind(base::PageKind::BPlusLeaf);
        self.set_level(0);
        self.set_free_space((constants::storage::PAGE_SIZE - Self::DATA_START) as u32);
        self.set_page_id(page_id);
        self.set_prev_sibling(None);
        self.set_next_sibling(None);
        self.set_curr_vec_sz(0);
        self.set_key_size(0);
        self.raw[3] = 0; // Reserved byte
        self.raw[40..Self::DATA_START].fill(0); // Reserved section
    }

    pub const fn page_kind(&self) -> u8 {
        self.raw[0]
    }
    pub fn page_level(&self) -> u16 {
        u16::from_le_bytes(self.raw[1..3].try_into().expect("Invalid page offset"))
    }
    pub fn free_space(&self) -> u32 {
        u32::from_le_bytes(
            self.raw[4..8]
                .try_into()
                .expect("Invalid free_space offset"),
        )
    }
    pub fn page_id(&self) -> base::PageId {
        let val = u64::from_le_bytes(self.raw[8..16].try_into().expect("Invalid page_id offset"));
        base::PageId::new(val).expect("Invalid page ID")
    }
    pub fn prev_sibling(&self) -> Option<base::PageId> {
        let val = u64::from_le_bytes(
            self.raw[16..24]
                .try_into()
                .expect("Invalid prev_sibling offset"),
        );
        base::PageId::new(val)
    }
    pub fn next_sibling(&self) -> Option<base::PageId> {
        let val = u64::from_le_bytes(
            self.raw[24..32]
                .try_into()
                .expect("Invalid next_sibling offset"),
        );
        base::PageId::new(val)
    }
    pub fn curr_vec_sz(&self) -> u32 {
        u32::from_le_bytes(
            self.raw[32..36]
                .try_into()
                .expect("Invalid curr_vec_sz offset"),
        )
    }
    pub fn get_key_size(&self) -> u32 {
        u32::from_le_bytes(
            self.raw[36..40]
                .try_into()
                .expect("Invalid key_size offset"),
        )
    }
    pub fn set_page_kind(&mut self, kind: base::PageKind) {
        self.raw[0] = kind as u8;
    }
    pub fn set_free_space(&mut self, free: u32) {
        self.raw[4..8].copy_from_slice(&free.to_le_bytes());
    }
    pub fn set_page_id(&mut self, id: base::PageId) {
        self.raw[8..16].copy_from_slice(&id.get().to_le_bytes());
    }
    pub fn set_level(&mut self, level: u16) {
        self.raw[1..3].copy_from_slice(&level.to_le_bytes());
    }
    pub fn set_prev_sibling(&mut self, id: Option<base::PageId>) {
        let bytes = match id {
            Some(page_id) => page_id.get().to_le_bytes(),
            None => 0u64.to_le_bytes(),
        };
        self.raw[16..24].copy_from_slice(&bytes);
    }
    pub fn set_next_sibling(&mut self, id: Option<base::PageId>) {
        let bytes = match id {
            Some(page_id) => page_id.get().to_le_bytes(),
            None => 0u64.to_le_bytes(),
        };
        self.raw[24..32].copy_from_slice(&bytes);
    }
    pub fn set_curr_vec_sz(&mut self, size: u32) {
        self.raw[32..36].copy_from_slice(&size.to_le_bytes());
    }
    pub fn set_key_size(&mut self, key_size: u32) {
        self.raw[36..40].copy_from_slice(&key_size.to_le_bytes());
    }

    pub fn get_value(&self, key: &[u8]) -> Option<u64> {
        let key_size = self.get_key_size() as usize;
        if key.len() != key_size {
            return None;
        }

        self.find_key_position(key)
            .map(|pos| self.get_value_at(pos))
    }

    /// Check if there's space for one more key-value pair
    pub fn has_space_for_key(&self) -> bool {
        self.free_space() >= (self.entry_size() as u32)
    }

    pub fn insert_sorted(&mut self, key: &[u8], value: u64) {
        let key_size = self.get_key_size() as usize;
        let curr_size = self.curr_vec_sz() as usize;

        if let Some(pos) = self.find_key_position(key) {
            // Key already exists. Update its value.
            self.set_value_at(pos, value);
            return;
        }

        // Key does not exist. Insert new entry.
        let insert_pos = self.find_insert_position(key);
        let entry_size = self.entry_size();

        // Shift existing entries right to make room
        if insert_pos < curr_size {
            let offset = Self::DATA_START + insert_pos * entry_size;
            let move_len = (curr_size - insert_pos) * entry_size;
            self.raw
                .copy_within(offset..offset + move_len, offset + entry_size);
        }

        // Insert the new key-value pair
        self.set_entry(insert_pos, key, value);

        // Update metadata
        self.set_curr_vec_sz((curr_size + 1) as u32);
        let free_space = self.free_space();
        self.set_free_space(free_space - (entry_size as u32));
    }

    /// Find the correct position to insert a key (maintaining sorted order)
    fn find_insert_position(&self, key: &[u8]) -> usize {
        let curr_size = self.curr_vec_sz() as usize;

        // Use binary search for better performance
        let mut left = 0;
        let mut right = curr_size;

        while left < right {
            let mid = left + (right - left) / 2;
            let stored_key = self.get_key_at(mid);

            if stored_key < key {
                left = mid + 1;
            } else {
                right = mid;
            }
        }

        left
    }

    pub fn remove_key(&mut self, key: &[u8]) -> bool {
        let key_size = self.get_key_size() as usize;
        let curr_size = self.curr_vec_sz() as usize;

        // Find the key
        if let Some(pos) = self.find_key_position(key) {
            let entry_size = self.entry_size();

            // Shift entries left to fill the gap
            if pos < curr_size - 1 {
                let offset = Self::DATA_START + pos * entry_size;
                let move_len = (curr_size - pos - 1) * entry_size;
                self.raw
                    .copy_within(offset + entry_size..offset + entry_size + move_len, offset);
            }

            // Update metadata
            self.set_curr_vec_sz((curr_size - 1) as u32);
            let free_space = self.free_space();
            self.set_free_space(free_space + (entry_size as u32));
            true
        } else {
            false
        }
    }

    /// Find the position of a key, return None if not found
    fn find_key_position(&self, key: &[u8]) -> Option<usize> {
        let curr_size = self.curr_vec_sz() as usize;

        // Use binary search
        let mut left = 0;
        let mut right = curr_size;

        while left < right {
            let mid = left + (right - left) / 2;
            let stored_key = self.get_key_at(mid);

            match stored_key.cmp(key) {
                core::cmp::Ordering::Equal => return Some(mid),
                core::cmp::Ordering::Less => left = mid + 1,
                core::cmp::Ordering::Greater => right = mid,
            }
        }

        None
    }

    /// Check if the node is below the minimum occupancy threshold
    pub fn is_underflow(&self) -> bool {
        let max_keys = self.calculate_max_keys();
        let min_keys = (max_keys + 1) / 2; // Ceiling of max_keys / 2
        self.curr_vec_sz() < min_keys
    }

    /// Check if the node can give a key to a sibling (has more than minimum)
    pub fn can_give_key(&self) -> bool {
        let max_keys = self.calculate_max_keys();
        let min_keys = (max_keys + 1) / 2;
        self.curr_vec_sz() > min_keys
    }

    /// Calculate maximum number of keys this leaf can hold
    pub fn calculate_max_keys(&self) -> u32 {
        if self.get_key_size() == 0 {
            // Avoid division by zero if key size not set
            return 0;
        }
        let available_space = constants::storage::PAGE_SIZE - Self::DATA_START;
        (available_space / self.entry_size()) as u32
    }

    pub fn split_and_get_new_entries(
        &mut self,
        key: &[u8],
        value: u64,
    ) -> Result<(SplitResult, Vec<(Vec<u8>, u64)>), &'static str> {
        let key_size = self.get_key_size() as usize;
        if key.len() != key_size {
            return Err("split_and_get_new_entries: key length mismatch");
        }

        let curr_size = self.curr_vec_sz() as usize;

        // Build vector of all entries INCLUDING THE NEW ONE
        let mut all_entries: Vec<(Vec<u8>, u64)> = Vec::with_capacity(curr_size + 1);
        for i in 0..curr_size {
            all_entries.push((self.get_key_at(i).to_vec(), self.get_value_at(i)));
        }

        // Find where new key should go
        let insert_pos = all_entries
            .iter()
            .position(|(k, _)| k.as_slice() >= key)
            .unwrap_or(all_entries.len());

        // Check if key already exists
        if insert_pos < all_entries.len() && all_entries[insert_pos].0.as_slice() == key {
            // Update existing key's value
            all_entries[insert_pos].1 = value;
        } else {
            // Insert new key
            all_entries.insert(insert_pos, (key.to_vec(), value));
        }

        let total_entries = all_entries.len();
        if total_entries == 0 {
            return Err("split_and_get_new_entries: no entries to split");
        }

        let split_point = total_entries / 2;

        // --- Rebuild this (left) page ---
        // Clear the data area
        self.raw[Self::DATA_START..].fill(0);

        // Write the first half of entries
        for i in 0..split_point {
            let (ref k, v) = all_entries[i];
            self.set_entry(i, k, v);
        }

        // Update metadata for this page
        self.set_curr_vec_sz(split_point as u32);
        let used_space = split_point * self.entry_size();
        self.set_free_space(
            (constants::storage::PAGE_SIZE as u32 - Self::DATA_START as u32)
                .saturating_sub(used_space as u32),
        );

        // Create vector for the new (right) page - entries [split_point..total_entries)
        let new_page_entries: Vec<(Vec<u8>, u64)> = all_entries[split_point..].to_vec();

        let split_key = all_entries[split_point].0.clone();

        Ok((SplitResult { split_key }, new_page_entries))
    }

    /// Get the first key in this leaf
    pub fn get_first_key(&self) -> Option<Vec<u8>> {
        if self.curr_vec_sz() == 0 {
            return None;
        }
        Some(self.get_key_at(0).to_vec())
    }

    /// Get the last key in this leaf
    pub fn get_last_key(&self) -> Option<Vec<u8>> {
        let curr_size = self.curr_vec_sz() as usize;
        if curr_size == 0 {
            return None;
        }
        Some(self.get_key_at(curr_size - 1).to_vec())
    }

    /// Move the last key-value pair to another leaf
    pub fn move_last_to(&mut self, target: &mut BPlusLeaf) -> Option<Vec<u8>> {
        let curr_size = self.curr_vec_sz() as usize;
        if curr_size == 0 {
            return None;
        }

        let last_key = self.get_key_at(curr_size - 1).to_vec();
        let last_value = self.get_value_at(curr_size - 1);

        // Remove from this leaf *first*
        self.remove_key(&last_key);

        // Insert into target at the beginning
        target.insert_at_beginning(&last_key, last_value);

        // The key to update in the parent is the *new* first key of the target
        Some(target.get_key_at(0).to_vec())
    }

    /// Move the first key-value pair to another leaf
    pub fn move_first_to(&mut self, target: &mut BPlusLeaf) -> Option<Vec<u8>> {
        let curr_size = self.curr_vec_sz() as usize;
        if curr_size == 0 {
            return None;
        }

        let first_key = self.get_key_at(0).to_vec();
        let first_value = self.get_value_at(0);

        // Insert into target at the end (insert_sorted handles this)
        target.insert_sorted(&first_key, first_value);

        // Remove from this leaf
        self.remove_key(&first_key);

        // Return the new first key of this leaf (if any)
        if self.curr_vec_sz() > 0 {
            Some(self.get_key_at(0).to_vec())
        } else {
            None
        }
    }

    /// Insert a key-value pair at the beginning of the leaf
    fn insert_at_beginning(&mut self, key: &[u8], value: u64) {
        let key_size = self.get_key_size() as usize;
        let curr_size = self.curr_vec_sz() as usize;
        let entry_size = self.entry_size();

        // Shift all entries right by one
        if curr_size > 0 {
            let offset = Self::DATA_START;
            let move_len = curr_size * entry_size;
            self.raw
                .copy_within(offset..offset + move_len, offset + entry_size);
        }

        // Insert at position 0
        self.set_entry(0, key, value);

        // Update metadata
        self.set_curr_vec_sz((curr_size + 1) as u32);
        let free_space = self.free_space();
        self.set_free_space(free_space - (entry_size as u32));
    }

    /// Merge all entries from another leaf into this one
    pub fn merge_from(&mut self, other: &mut BPlusLeaf) {
        let other_size = other.curr_vec_sz() as usize;
        if other_size == 0 {
            return;
        }

        for i in 0..other_size {
            let key = other.get_key_at(i);
            let value = other.get_value_at(i);
            self.insert_sorted(key, value);
        }

        other.set_curr_vec_sz(0);
        other.set_free_space((constants::storage::PAGE_SIZE - Self::DATA_START) as u32);
    }
}
