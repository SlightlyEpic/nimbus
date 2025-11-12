use crate::{constants, storage::page::base};

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
    fn raw(self: &Self) -> &[u8; constants::storage::PAGE_SIZE] {
        &self.raw
    }
    fn raw_mut(&mut self) -> &mut [u8; constants::storage::PAGE_SIZE] {
        &mut self.raw
    }
}

impl<'a> BPlusInner<'a> {
    // == Memory layout ==
    //   0..  1     -> Page Kind                        (u8)   -|
    //   1..  3     -> Level                            (u16)   |
    //   3..  4     -> Reserved                         (u8)    |
    //   4..  8     -> Free Space (bytes)               (u32)   |
    //   8.. 16     -> Page ID                         (u64)    | Header (64 bytes)
    //  16.. 24     -> Prev Sibling Page ID            (u64)    |
    //  24.. 32     -> Next Sibling Page ID            (u64)    |
    //  32.. 36     -> Current Vector Size (num keys)  (u32)    |
    //  36.. 40     -> Key Size                        (u32)    |
    //  40.. 64     -> Reserved                                -|
    //  64.. 72     -> First Child Page ID             (u64)
    //  72.. N      -> Entry 0 (Key | Child Page ID)
    //  N.. M      -> Entry 1 (Key | Child Page ID)
    //  ...
    //  Y.. Z      -> Entry N-1 (Key | Child Page ID)
    //  Z.. PAGE_SIZE -> Free Space
    //
    // Note: An inner page with N keys has N+1 children.
    // Child 0 is First Child Page ID.
    // Child i (for i > 0) is the Page ID in Entry (i-1).

    const HEADER_END: usize = 64;
    const FIRST_CHILD_ID_START: usize = Self::HEADER_END;
    const FIRST_CHILD_ID_END: usize = Self::FIRST_CHILD_ID_START + std::mem::size_of::<u64>();
    const DATA_START: usize = Self::FIRST_CHILD_ID_END;

    /// Returns the size of one key + child_id entry.
    fn entry_size(&self) -> usize {
        self.get_key_size() as usize + std::mem::size_of::<u64>()
    }

    /// Returns a slice to the key at the given *entry* index (which is key index).
    pub fn get_key_at(&self, index: usize) -> &[u8] {
        let key_size = self.get_key_size() as usize;
        let entry_size = self.entry_size();
        let offset = Self::DATA_START + index * entry_size;
        &self.raw[offset..offset + key_size]
    }

    /// Returns the child page ID at the given *entry* index (which is child index + 1).
    fn get_child_id_at_entry(&self, index: usize) -> base::PageId {
        let key_size = self.get_key_size() as usize;
        let entry_size = self.entry_size();
        let offset = Self::DATA_START + index * entry_size + key_size;
        let bytes = self.raw[offset..offset + 8]
            .try_into()
            .expect("Invalid value slice");
        base::PageId::new(u64::from_le_bytes(bytes)).expect("Invalid Page ID in child entry")
    }

    /// Writes a key-child_id entry to the given logical index.
    pub fn set_entry(&mut self, index: usize, key: &[u8], child_id: base::PageId) {
        let key_size = self.get_key_size() as usize;
        let entry_size = self.entry_size();
        let offset = Self::DATA_START + index * entry_size;

        self.raw[offset..offset + key_size].copy_from_slice(key);
        self.raw[offset + key_size..offset + entry_size]
            .copy_from_slice(&child_id.get().to_le_bytes());
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

    pub fn init(&mut self, page_id: base::PageId, level: u16) {
        self.set_page_kind(base::PageKind::BPlusInner);
        self.set_level(level);
        self.set_free_space((constants::storage::PAGE_SIZE - Self::DATA_START) as u32);
        self.set_page_id(page_id);
        self.set_prev_sibling(None);
        self.set_next_sibling(None);
        self.set_curr_vec_sz(0);
        self.set_key_size(0);
        self.raw[3] = 0;
        self.raw[40..Self::HEADER_END].fill(0);
        self.raw[Self::FIRST_CHILD_ID_START..Self::FIRST_CHILD_ID_END].fill(0);
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

    pub fn get_first_key(&self) -> Option<Vec<u8>> {
        if self.curr_vec_sz() == 0 {
            return None;
        }
        Some(self.get_key_at(0).to_vec())
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

    pub fn calculate_max_keys(&self) -> u32 {
        if self.get_key_size() == 0 {
            // Avoid division by zero if key size not set
            return 0;
        }
        let available_space = constants::storage::PAGE_SIZE - Self::DATA_START;
        (available_space / self.entry_size()) as u32
    }

    pub fn has_space_for_key(&self) -> bool {
        self.free_space() >= (self.entry_size() as u32)
    }

    // Get child at logical index (0 to curr_vec_sz inclusive)
    pub fn get_child_at(&self, index: usize) -> Option<base::PageId> {
        let num_keys = self.curr_vec_sz() as usize;
        let num_children = num_keys + 1;

        if index >= num_children {
            return None;
        }

        if index == 0 {
            // Get First Child ID
            let bytes = self.raw[Self::FIRST_CHILD_ID_START..Self::FIRST_CHILD_ID_END]
                .try_into()
                .unwrap();
            base::PageId::new(u64::from_le_bytes(bytes))
        } else {
            // Get child ID from entry (index - 1)
            Some(self.get_child_id_at_entry(index - 1))
        }
    }

    pub fn set_child_at(&mut self, index: usize, child_id: base::PageId) {
        let num_keys = self.curr_vec_sz() as usize;
        let num_children = num_keys + 1;
        if index >= num_children {
            panic!("set_child_at: index out of bounds");
        }

        if index == 0 {
            // Set First Child ID
            self.raw[Self::FIRST_CHILD_ID_START..Self::FIRST_CHILD_ID_END]
                .copy_from_slice(&child_id.get().to_le_bytes());
        } else {
            // Set child ID in entry (index - 1)
            let key_size = self.get_key_size() as usize;
            let entry_size = self.entry_size();
            let offset = Self::DATA_START + (index - 1) * entry_size + key_size;
            self.raw[offset..offset + 8].copy_from_slice(&child_id.get().to_le_bytes());
        }
    }

    pub fn split_and_get_new_entries(
        &mut self,
        key: &[u8],
        child_id: base::PageId,
    ) -> BPlusInnerSplitData {
        let curr_sz = self.curr_vec_sz() as usize;

        let mut all_keys = Vec::new();
        let mut all_children = vec![self.get_child_at(0).unwrap()];

        for i in 0..curr_sz {
            all_keys.push(self.get_key_at(i).to_vec());
            all_children.push(self.get_child_id_at_entry(i));
        }

        let insert_pos = self.find_insert_position(key);

        // Insert new key and child
        all_keys.insert(insert_pos, key.to_vec());
        all_children.insert(insert_pos + 1, child_id);

        // Find split point
        let total_entries = all_keys.len();
        let split_point = total_entries / 2; // This key will be pushed up

        let split_key = all_keys[split_point].clone();

        // Clear old node and rebuild with first half
        self.raw[Self::DATA_START..].fill(0);
        self.set_child_at(0, all_children[0]);

        for i in 0..split_point {
            self.set_entry(i, &all_keys[i], all_children[i + 1]);
        }

        self.set_curr_vec_sz(split_point as u32);
        let used_space = split_point * self.entry_size();
        self.set_free_space(
            (constants::storage::PAGE_SIZE as u32 - Self::DATA_START as u32)
                .saturating_sub(used_space as u32),
        );

        let mut new_page_keys = Vec::new();
        let mut new_page_children = Vec::new();

        // The first child of the new node is the one *after* the split_key
        new_page_children.push(all_children[split_point + 1]);
        for i in (split_point + 1)..total_entries {
            new_page_keys.push(all_keys[i].clone());
            new_page_children.push(all_children[i + 1]);
        }

        BPlusInnerSplitData {
            key_to_push_up: split_key,
            new_page_keys,
            new_page_children,
        }
    }

    // Insert key and child ptr at given position, shifting keys and children arrays
    pub fn insert_sorted(&mut self, key: &[u8], child_ptr: base::PageId) {
        let key_size = self.get_key_size() as usize;
        let curr_size = self.curr_vec_sz() as usize;

        let insert_pos = self.find_insert_position(key);
        let entry_size = self.entry_size();

        // Shift existing entries right to make room
        if insert_pos < curr_size {
            let offset = Self::DATA_START + insert_pos * entry_size;
            let move_len = (curr_size - insert_pos) * entry_size;
            self.raw
                .copy_within(offset..offset + move_len, offset + entry_size);
        }

        // Insert new key and child
        self.set_entry(insert_pos, key, child_ptr);

        // Update metadata
        self.set_curr_vec_sz((curr_size + 1) as u32);
        let free_space = self.free_space();
        self.set_free_space(free_space - (entry_size as u32));
    }

    // Find insert position using binary search for better performance
    fn find_insert_position(&self, key: &[u8]) -> usize {
        let curr_size = self.curr_vec_sz() as usize;

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

    pub fn find_child_page(&self, key: &[u8]) -> Option<base::PageId> {
        let num_keys = self.curr_vec_sz() as usize;

        // Use binary search to find the correct key index
        let mut left = 0;
        let mut right = num_keys;
        let mut child_index = 0; // Default to first child

        while left < right {
            let mid = left + (right - left) / 2;
            let stored_key = self.get_key_at(mid);

            if key < stored_key {
                right = mid;
            } else {
                left = mid + 1;
            }
        }

        // left is now the index of the first key greater than or equal to key.
        // This means we should follow the child pointer at index left.
        child_index = left;

        self.get_child_at(child_index)
    }

    /// Removes a key at `key_index` AND its corresponding child pointer (at `key_index + 1`).
    /// This is used when merging two nodes.
    pub fn remove_entry_at(&mut self, key_index: usize) -> Vec<u8> {
        let curr_size = self.curr_vec_sz() as usize;
        let entry_size = self.entry_size();

        // Get the key we are removing
        let key_to_remove = self.get_key_at(key_index).to_vec();

        // Shift all entries *after* this one left
        if key_index < curr_size - 1 {
            let offset = Self::DATA_START + key_index * entry_size;
            let move_len = (curr_size - key_index - 1) * entry_size;
            self.raw
                .copy_within(offset + entry_size..offset + entry_size + move_len, offset);
        }

        // Update metadata
        self.set_curr_vec_sz((curr_size - 1) as u32);
        self.set_free_space(self.free_space() + entry_size as u32);

        key_to_remove
    }

    // Find key's position, return None if not found
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

    pub fn find_child_page_index(&self, key: &[u8]) -> usize {
        let num_keys = self.curr_vec_sz() as usize;

        // Use binary search to find the correct key index
        let mut left = 0;
        let mut right = num_keys;

        while left < right {
            let mid = left + (right - left) / 2;
            let stored_key = self.get_key_at(mid);

            if key < stored_key {
                right = mid;
            } else {
                left = mid + 1;
            }
        }
        // left is now the index of the first key >= key.
        // This means we should follow the child pointer at index left.
        left
    }

    pub fn min_keys(&self) -> u32 {
        // (max_keys + 1) / 2 corresponds to ceil(max_keys / 2)
        (self.calculate_max_keys() + 1) / 2
    }

    /// Check if the node is below the minimum occupancy threshold
    pub fn is_underflow(&self) -> bool {
        self.curr_vec_sz() < self.min_keys()
    }

    /// Check if the node can give a key to a sibling (has more than minimum)
    pub fn can_give_key(&self) -> bool {
        self.curr_vec_sz() > self.min_keys()
    }

    /// Moves the last key/child pair from this node to the beginning of target node.
    /// This also requires moving the separator_key from the parent down into the target.
    /// Returns the key that was at the end of this node.
    pub fn move_last_to_beginning_of(
        &mut self,
        target: &mut BPlusInner,
        separator_key: &[u8],
    ) -> Vec<u8> {
        let curr_size = self.curr_vec_sz() as usize;

        // Get the last key and child from this node
        let last_key = self.get_key_at(curr_size - 1).to_vec();
        let last_child = self.get_child_id_at_entry(curr_size - 1);

        // Remove the last entry from this node
        self.set_curr_vec_sz((curr_size - 1) as u32);
        self.set_free_space(self.free_space() + self.entry_size() as u32);

        // Now, update the target node
        let target_old_first_child = target.get_child_at(0).unwrap();

        // The target's new first key will be the separator_key from the parent
        // The child associated with this new key will be the target_old_first_child
        target.insert_at_beginning(separator_key, target_old_first_child);

        // Finally, set the target's first child to be the last_child we moved
        target.set_child_at(0, last_child);

        // Return the key we removed, which will become the new separator in the parent
        last_key
    }

    /// Moves the first key/child pair from this node to the end of target node.
    /// This also requires moving the separator_key from the parent down into the target.
    /// Returns the new first key of this node (which will be the new separator).
    pub fn move_first_to_end_of(
        &mut self,
        target: &mut BPlusInner,
        separator_key: &[u8],
    ) -> Vec<u8> {
        let curr_size = self.curr_vec_sz() as usize;

        // Get the first key and its *associated child* (which is at index 1)
        let first_key = self.get_key_at(0).to_vec();
        let first_key_child = self.get_child_id_at_entry(0);

        // Get the *very first child* (index 0), which will become the new first child
        let new_first_child = self.get_child_at(0).unwrap();

        // 1. Add to target: The parent's separator key and this node's first key's child
        target.insert_sorted(separator_key, first_key_child);

        // 2. Remove from this node
        // Shift all entries left by one
        let entry_size = self.entry_size();
        let offset = Self::DATA_START;
        let move_len = (curr_size - 1) * entry_size;
        self.raw
            .copy_within(offset + entry_size..offset + entry_size + move_len, offset);

        // Set the new first child
        self.set_child_at(0, new_first_child);

        // Update metadata
        self.set_curr_vec_sz((curr_size - 1) as u32);
        self.set_free_space(self.free_space() + entry_size as u32);

        // Return this node's new first key
        self.get_key_at(0).to_vec()
    }

    /// Insert a key-child_id pair at the beginning of the node's entries.
    /// This is used when borrowing from a left sibling.
    fn insert_at_beginning(&mut self, key: &[u8], child_id: base::PageId) {
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
        self.set_entry(0, key, child_id);

        // Update metadata
        self.set_curr_vec_sz((curr_size + 1) as u32);
        let free_space = self.free_space();
        self.set_free_space(free_space - (entry_size as u32));
    }

    /// Merge all entries from other_node into this one.
    /// This also requires pulling down the separator_key from the parent.
    pub fn merge_from(&mut self, other_node: &mut BPlusInner, separator_key: &[u8]) {
        let other_size = other_node.curr_vec_sz() as usize;

        // Pull down the separator key from the parent.
        // Its child will be the first child of the other_node.
        let other_first_child = other_node.get_child_at(0).unwrap();
        self.insert_sorted(separator_key, other_first_child);

        // Copy all key/child entries from the other_node
        for i in 0..other_size {
            let key = other_node.get_key_at(i);
            let child = other_node.get_child_id_at_entry(i);
            self.insert_sorted(key, child);
        }

        // Clear the other node
        other_node.set_curr_vec_sz(0);
        other_node.set_free_space((constants::storage::PAGE_SIZE - Self::DATA_START) as u32);
        self.set_next_sibling(other_node.next_sibling());
    }

    pub fn populate_entries(&mut self, keys: &[Vec<u8>], children: &[base::PageId]) {
        assert_eq!(
            keys.len(),
            children.len(),
            "Keys and children arrays must have the same length"
        );

        let num_entries = keys.len();

        self.raw[Self::DATA_START..].fill(0);

        for i in 0..num_entries {
            self.set_entry(i, &keys[i], children[i]);
        }

        self.set_curr_vec_sz(num_entries as u32);
        let used_space = num_entries * self.entry_size();
        self.set_free_space(
            (constants::storage::PAGE_SIZE as u32 - Self::DATA_START as u32)
                .saturating_sub(used_space as u32),
        );
    }
}
