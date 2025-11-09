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
    // === Memory layout ===
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
    //  64.. N      -> Keys growing forward: key0, key1, ... (each key_size bytes)
    //  M.. PAGE_SIZE -> Values growing backward: value0 @ PAGE_SIZE-8, value1 @ PAGE_SIZE-16, etc. (each u64)
    //  Free space between N and M.
    //  Physical order of values in memory: value(n-1), ..., value0 (reversed logical order)

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
        self.set_free_space((constants::storage::PAGE_SIZE - 64) as u32);
        self.set_page_id(page_id);
        self.set_prev_sibling(None);
        self.set_next_sibling(None);
        self.set_curr_vec_sz(0);
        self.set_key_size(0);
        self.raw[3] = 0; // Reserved byte
        self.raw[40..64].fill(0); // Reserved section
    }

    // Metadata getters
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

    pub fn get_keys(&self) -> &[u8] {
        let size = self.curr_vec_sz() as usize;
        let key_size = self.get_key_size() as usize;
        let start = 64;
        let end = start + size * key_size;
        if end > constants::storage::PAGE_SIZE {
            panic!("Invalid keys vector size");
        }
        &self.raw[start..end]
    }

    pub fn get_value_ptrs(&self) -> Vec<u64> {
        let size = self.curr_vec_sz() as usize;
        if size == 0 {
            return vec![];
        }

        // Values are stored in reverse order physically:
        // raw[PAGE_SIZE - 8 .. PAGE_SIZE] => value(n-1)
        // raw[PAGE_SIZE - 16 .. PAGE_SIZE - 8] => value(n-2)
        // We want logical order: value(0), value(1), ..., value(n-1)
        let start = constants::storage::PAGE_SIZE - size * core::mem::size_of::<u64>();
        let end = constants::storage::PAGE_SIZE;
        if start >= end {
            panic!("Invalid value pointers vector size");
        }

        let mut out = Vec::with_capacity(size);
        self.raw[start..end]
            .chunks_exact(core::mem::size_of::<u64>())
            .rev()
            .for_each(|chunk| {
                let arr: [u8; 8] = chunk.try_into().expect("Invalid chunk size");
                out.push(u64::from_le_bytes(arr));
            });

        out
    }

    pub fn get_value(&self, key: &[u8]) -> Option<u64> {
        let key_size = self.get_key_size() as usize;
        if key.len() != key_size {
            return None;
        }

        let num_pairs = self.curr_vec_sz() as usize;
        if num_pairs == 0 {
            return None;
        }

        let keys = self.get_keys();

        let mut left = 0usize;
        let mut right = num_pairs;

        while left < right {
            let mid = left + (right - left) / 2;
            let key_start = mid * key_size;
            let stored_key = &keys[key_start..key_start + key_size];

            match stored_key.cmp(key) {
                core::cmp::Ordering::Equal => {
                    let physical_value_index = num_pairs - 1 - mid;
                    let value_offset = constants::storage::PAGE_SIZE
                        - (physical_value_index + 1) * core::mem::size_of::<u64>();

                    let end = value_offset + core::mem::size_of::<u64>();
                    if end > constants::storage::PAGE_SIZE {
                        return None;
                    }

                    let bytes: [u8; 8] = self.raw[value_offset..end]
                        .try_into()
                        .expect("Invalid value bytes length");

                    return Some(u64::from_le_bytes(bytes));
                }
                core::cmp::Ordering::Less => left = mid + 1,
                core::cmp::Ordering::Greater => right = mid,
            }
        }

        None
    }

    /// Check if there's space for one more key-value pair
    pub fn has_space_for_key(&self) -> bool {
        let required_space = self.get_key_size() + 8; // key + value
        self.free_space() >= required_space
    }

    pub fn insert_sorted(&mut self, key: &[u8], value: u64) {
        let key_size = self.get_key_size() as usize;
        let curr_size = self.curr_vec_sz() as usize;

        if let Some(pos) = self.find_key_position(key) {
            // Key already exists. Update its value.
            // Physical storage of values is reversed
            let physical_value_index = curr_size - 1 - pos;
            let value_offset = constants::storage::PAGE_SIZE
                - (physical_value_index + 1) * core::mem::size_of::<u64>();
            let end = value_offset + core::mem::size_of::<u64>();

            self.raw[value_offset..end].copy_from_slice(&value.to_le_bytes());
            // No size or free space change, so we can return early.
            return;
        }

        // Find insertion position (since key doesn't exist)
        let insert_pos = self.find_insert_position(key);

        // Shift existing keys right to make room
        if insert_pos < curr_size {
            let keys_start = 64;
            let src_start = keys_start + insert_pos * key_size;
            let dst_start = src_start + key_size;
            let move_len = (curr_size - insert_pos) * key_size;
            self.raw
                .copy_within(src_start..src_start + move_len, dst_start);
        }

        // Shift existing values right to make room
        if insert_pos < curr_size {
            for i in (insert_pos..curr_size).rev() {
                let src_physical_index = curr_size - 1 - i;
                let dst_physical_index = curr_size - i; // One position further from end

                let src_offset = constants::storage::PAGE_SIZE
                    - (src_physical_index + 1) * core::mem::size_of::<u64>();
                let dst_offset = constants::storage::PAGE_SIZE
                    - (dst_physical_index + 1) * core::mem::size_of::<u64>();

                let val = u64::from_le_bytes(
                    self.raw[src_offset..src_offset + core::mem::size_of::<u64>()]
                        .try_into()
                        .expect("Invalid offset"),
                );
                self.raw[dst_offset..dst_offset + core::mem::size_of::<u64>()]
                    .copy_from_slice(&val.to_le_bytes());
            }
        }

        // Insert the new key-value pair
        let keys_start = 64;
        let key_pos = keys_start + insert_pos * key_size;

        // Calculate physical position for the new value
        let new_physical_index = (curr_size + 1) - 1 - insert_pos;
        let value_pos =
            constants::storage::PAGE_SIZE - (new_physical_index + 1) * core::mem::size_of::<u64>();

        self.raw[key_pos..key_pos + key_size].copy_from_slice(key);
        self.raw[value_pos..value_pos + core::mem::size_of::<u64>()]
            .copy_from_slice(&value.to_le_bytes());

        // Update metadata
        self.set_curr_vec_sz((curr_size + 1) as u32);

        let free_space = self.free_space();
        self.set_free_space(free_space - (key_size as u32 + 8));
    }

    /// Find the correct position to insert a key (maintaining sorted order)
    fn find_insert_position(&self, key: &[u8]) -> usize {
        let key_size = self.get_key_size() as usize;
        let curr_size = self.curr_vec_sz() as usize;
        let keys = self.get_keys();

        // Use binary search for better performance
        let mut left = 0;
        let mut right = curr_size;

        while left < right {
            let mid = left + (right - left) / 2;
            let key_start = mid * key_size;
            let stored_key = &keys[key_start..key_start + key_size];

            if stored_key <= key {
                left = mid + 1;
            } else {
                right = mid;
            }
        }

        left
    }

    /// Remove a key and return true if found
    pub fn remove_key(&mut self, key: &[u8]) -> bool {
        let key_size = self.get_key_size() as usize;
        let curr_size = self.curr_vec_sz() as usize;

        // Find the key
        if let Some(pos) = self.find_key_position(key) {
            // Shift keys left to fill the gap
            if pos < curr_size - 1 {
                let keys_start = 64;
                let src_start = keys_start + (pos + 1) * key_size;
                let dst_start = keys_start + pos * key_size;
                let move_len = (curr_size - pos - 1) * key_size;
                self.raw
                    .copy_within(src_start..src_start + move_len, dst_start);
            }

            // Shift values left to fill the gap
            // We need to remove the value at logical position pos
            if pos < curr_size - 1 {
                for i in pos..(curr_size - 1) {
                    let src_logical_index = i + 1;
                    let dst_logical_index = i;

                    let src_physical_index = curr_size - 1 - src_logical_index;
                    let dst_physical_index = (curr_size - 1) - 1 - dst_logical_index;

                    let src_offset = constants::storage::PAGE_SIZE
                        - (src_physical_index + 1) * core::mem::size_of::<u64>();
                    let dst_offset = constants::storage::PAGE_SIZE
                        - (dst_physical_index + 1) * core::mem::size_of::<u64>();

                    let val = u64::from_le_bytes(
                        self.raw[src_offset..src_offset + core::mem::size_of::<u64>()]
                            .try_into()
                            .expect("Invalid offset"),
                    );
                    self.raw[dst_offset..dst_offset + core::mem::size_of::<u64>()]
                        .copy_from_slice(&val.to_le_bytes());
                }
            }

            // Update metadata
            self.set_curr_vec_sz((curr_size - 1) as u32);

            let free_space = self.free_space();
            self.set_free_space(free_space + (key_size as u32 + 8));
            true
        } else {
            false
        }
    }

    /// Find the position of a key, return None if not found
    fn find_key_position(&self, key: &[u8]) -> Option<usize> {
        let key_size = self.get_key_size() as usize;
        let curr_size = self.curr_vec_sz() as usize;
        let keys = self.get_keys();

        // Use binary search
        let mut left = 0;
        let mut right = curr_size;

        while left < right {
            let mid = left + (right - left) / 2;
            let key_start = mid * key_size;
            let stored_key = &keys[key_start..key_start + key_size];

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
        let available_space = constants::storage::PAGE_SIZE - 64; // Subtract header size
        let space_per_entry = self.get_key_size() + 8; // key + value
        (available_space / space_per_entry as usize) as u32
    }

    /// Split this leaf with a new key, distributing entries between old and new leaf
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

        // Build vector of all entries (logical order)
        let mut all_entries: Vec<(Vec<u8>, u64)> = Vec::with_capacity(curr_size + 1);
        {
            let keys_buf = self.get_keys();
            let values_vec = self.get_value_ptrs();
            for i in 0..curr_size {
                let ks = i * key_size;
                all_entries.push((keys_buf[ks..ks + key_size].to_vec(), values_vec[i]));
            }
        }

        // Try to find existing key -> if found, update value (no duplicate)
        if let Some(idx) = all_entries.iter().position(|(k, _)| k.as_slice() == key) {
            all_entries[idx].1 = value;
        } else {
            // find insertion position (first key > new key)
            let insert_pos = all_entries
                .iter()
                .position(|(k, _)| k.as_slice() > key)
                .unwrap_or(all_entries.len());
            all_entries.insert(insert_pos, (key.to_vec(), value));
        }

        // Determine split point (even split; right side may have equal or +1 entries)
        let total_entries = all_entries.len();
        if total_entries == 0 {
            return Err("split_and_get_new_entries: no entries to split");
        }
        let split_point = total_entries / 2; // entries [0..split_point) -> left, [split_point..end) -> right

        // Rebuild left (this) leaf with first half
        self.set_curr_vec_sz(0);
        self.set_free_space((constants::storage::PAGE_SIZE - 64) as u32);
        for i in 0..split_point {
            let (ref k, v) = all_entries[i];
            self.insert_sorted(k, v);
        }

        // Create vector for the new (right) leaf
        let mut new_page_entries: Vec<(Vec<u8>, u64)> =
            Vec::with_capacity(total_entries - split_point);
        for i in split_point..total_entries {
            new_page_entries.push(all_entries[i].clone());
        }

        // split_key is the first key of the new (right) leaf
        let split_key = all_entries[split_point].0.clone();

        Ok((SplitResult { split_key }, new_page_entries))
    }

    /// Get the first key in this leaf
    pub fn get_first_key(&self) -> Option<Vec<u8>> {
        if self.curr_vec_sz() == 0 {
            return None;
        }

        let key_size = self.get_key_size() as usize;
        let keys = self.get_keys();
        Some(keys[0..key_size].to_vec())
    }

    /// Get the last key in this leaf
    pub fn get_last_key(&self) -> Option<Vec<u8>> {
        let curr_size = self.curr_vec_sz() as usize;
        if curr_size == 0 {
            return None;
        }

        let key_size = self.get_key_size() as usize;
        let keys = self.get_keys();
        let last_key_start = (curr_size - 1) * key_size;
        Some(keys[last_key_start..last_key_start + key_size].to_vec())
    }

    /// Move the last key-value pair to another leaf
    pub fn move_last_to(&mut self, target: &mut BPlusLeaf) -> Option<Vec<u8>> {
        let curr_size = self.curr_vec_sz() as usize;
        if curr_size == 0 {
            return None;
        }

        let key_size = self.get_key_size() as usize;
        let keys = self.get_keys();
        let values = self.get_value_ptrs();

        let last_key_start = (curr_size - 1) * key_size;
        let last_key = &keys[last_key_start..last_key_start + key_size];
        let last_value = values[curr_size - 1];

        // Insert into target at the beginning
        target.insert_at_beginning(last_key, last_value);
        let moved_key = Some(last_key.to_vec());

        // Remove from this leaf
        self.set_curr_vec_sz((curr_size - 1) as u32);

        let free_space = self.free_space();
        self.set_free_space(free_space + (key_size as u32 + 8));
        moved_key
    }

    /// Move the first key-value pair to another leaf
    pub fn move_first_to(&mut self, target: &mut BPlusLeaf) -> Option<Vec<u8>> {
        let curr_size = self.curr_vec_sz() as usize;
        if curr_size == 0 {
            return None;
        }

        let key_size = self.get_key_size() as usize;
        let keys = self.get_keys();
        let values = self.get_value_ptrs();

        let first_key = &keys[0..key_size];
        let first_value = values[0];

        // Insert into target at the end
        target.insert_sorted(first_key, first_value);

        // Get the new first key (if any) before removing
        let new_first_key = if curr_size > 1 {
            Some(keys[key_size..key_size * 2].to_vec())
        } else {
            None
        };

        // Remove from this leaf by shifting everything left
        if curr_size > 1 {
            // Shift keys left
            let keys_start = 64;
            let src_start = keys_start + key_size;
            let dst_start = keys_start;
            let move_len = (curr_size - 1) * key_size;
            self.raw
                .copy_within(src_start..src_start + move_len, dst_start);

            // Shift values left
            for i in 0..(curr_size - 1) {
                let src_logical_index = i + 1;
                let dst_logical_index = i;

                let src_physical_index = curr_size - 1 - src_logical_index;
                let dst_physical_index = (curr_size - 1) - 1 - dst_logical_index;

                let src_offset = constants::storage::PAGE_SIZE
                    - (src_physical_index + 1) * core::mem::size_of::<u64>();
                let dst_offset = constants::storage::PAGE_SIZE
                    - (dst_physical_index + 1) * core::mem::size_of::<u64>();

                let val = u64::from_le_bytes(
                    self.raw[src_offset..src_offset + core::mem::size_of::<u64>()]
                        .try_into()
                        .expect("Invalid offset"),
                );
                self.raw[dst_offset..dst_offset + core::mem::size_of::<u64>()]
                    .copy_from_slice(&val.to_le_bytes());
            }
        }

        self.set_curr_vec_sz((curr_size - 1) as u32);
        let free_space = self.free_space();
        self.set_free_space(free_space + (key_size as u32 + 8));
        new_first_key
    }

    /// Insert a key-value pair at the beginning of the leaf
    fn insert_at_beginning(&mut self, key: &[u8], value: u64) {
        let key_size = self.get_key_size() as usize;
        let curr_size = self.curr_vec_sz() as usize;

        // Shift keys right
        if curr_size > 0 {
            let keys_start = 64;
            let src_start = keys_start;
            let dst_start = keys_start + key_size;
            let move_len = curr_size * key_size;
            self.raw
                .copy_within(src_start..src_start + move_len, dst_start);
        }

        // Shift values right
        if curr_size > 0 {
            for i in (0..curr_size).rev() {
                let src_physical_index = curr_size - 1 - i;
                let dst_physical_index = (curr_size + 1) - 1 - (i + 1);

                let src_offset = constants::storage::PAGE_SIZE
                    - (src_physical_index + 1) * core::mem::size_of::<u64>();
                let dst_offset = constants::storage::PAGE_SIZE
                    - (dst_physical_index + 1) * core::mem::size_of::<u64>();

                let val = u64::from_le_bytes(
                    self.raw[src_offset..src_offset + core::mem::size_of::<u64>()]
                        .try_into()
                        .expect("Invalid offset"),
                );
                self.raw[dst_offset..dst_offset + core::mem::size_of::<u64>()]
                    .copy_from_slice(&val.to_le_bytes());
            }
        }

        // Insert at position 0
        let keys_start = 64;
        let new_physical_index = (curr_size + 1) - 1 - 0;
        let value_pos =
            constants::storage::PAGE_SIZE - (new_physical_index + 1) * core::mem::size_of::<u64>();

        self.raw[keys_start..keys_start + key_size].copy_from_slice(key);
        self.raw[value_pos..value_pos + core::mem::size_of::<u64>()]
            .copy_from_slice(&value.to_le_bytes());

        // Update metadata
        self.set_curr_vec_sz((curr_size + 1) as u32);
        let free_space = self.free_space();
        self.set_free_space(free_space - (key_size as u32 + 8));
    }

    /// Merge all entries from another leaf into this one
    pub fn merge_from(&mut self, other: &mut BPlusLeaf) {
        let other_size = other.curr_vec_sz() as usize;
        if other_size == 0 {
            return;
        }

        let key_size = self.get_key_size() as usize;
        let other_keys = other.get_keys();
        let other_values = other.get_value_ptrs();

        // Add all entries from other leaf
        for i in 0..other_size {
            let key_start = i * key_size;
            let key = &other_keys[key_start..key_start + key_size];
            let value = other_values[i];
            self.insert_sorted(key, value);
        }

        // Clear other leaf
        other.set_curr_vec_sz(0);
        other.set_free_space(constants::storage::PAGE_SIZE as u32 - 64);
    }
}
