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
    //  Note: number of child pointers is always number of keys + 1

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
        self.set_free_space((constants::storage::PAGE_SIZE - 64) as u32);
        self.set_page_id(page_id);
        self.set_prev_sibling(None);
        self.set_next_sibling(None);
        self.set_curr_vec_sz(0);
        self.set_key_size(0);
        self.raw[3] = 0; // Reserved byte
        self.raw[40..64].fill(0); // Reserved section
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

    pub fn get_key_size(&self) -> u32 {
        u32::from_le_bytes(
            self.raw[36..40]
                .try_into()
                .expect("Invalid key_size offset"),
        )
    }

    pub fn calculate_max_keys(&self) -> u32 {
        let available_space = constants::storage::PAGE_SIZE - 64;
        let space_per_entry = self.get_key_size() + 8; // key + child_id

        (available_space / space_per_entry as usize) as u32
    }

    pub fn has_space_for_key(&self) -> bool {
        let required_space = self.get_key_size() + 8; // key + child_id
        self.free_space() >= required_space
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

    pub fn get_key_at(&self, index: usize) -> Option<&[u8]> {
        let num_keys = self.curr_vec_sz() as usize;
        if index >= num_keys {
            return None;
        }

        let key_size = self.get_key_size();
        let keys = self.get_keys();
        let key_start = index * key_size as usize;

        Some(&keys[key_start..key_start + key_size as usize])
    }

    // Get all child page IDs, length = curr_vec_sz + 1
    pub fn get_child_ptrs(&self) -> Vec<base::PageId> {
        let num_keys = self.curr_vec_sz() as usize;
        let num_children = num_keys + 1;
        if num_children == 0 {
            return vec![];
        }

        // Child pointers stored backward at end of page
        // Physical order: child(n), child(n-1), ..., child(0)
        // We need to return them in logical order: child(0), child(1), ..., child(n)
        let start = constants::storage::PAGE_SIZE - num_children * core::mem::size_of::<u64>();
        let end = constants::storage::PAGE_SIZE;
        if start > end {
            panic!("Invalid child pointers vector size");
        }

        self.raw[start..end]
            .chunks_exact(core::mem::size_of::<u64>())
            .rev() // Reverse to get logical order
            .map(|chunk| {
                base::PageId::new(u64::from_le_bytes(
                    chunk.try_into().expect("Invalid chunk size"),
                ))
                .expect("Invalid page ID in child pointers")
            })
            .collect()
    }

    // Get child at index (0 to curr_vec_sz inclusive)
    pub fn get_child_at(&self, index: usize) -> Option<base::PageId> {
        let num_keys = self.curr_vec_sz() as usize;
        let num_children = num_keys + 1;

        if index >= num_children {
            return None;
        }

        // Calculate offset for child at logical index
        // Physical index = (num_children - 1 - index)
        let physical_index = num_children - 1 - index;
        let offset =
            constants::storage::PAGE_SIZE - (physical_index + 1) * core::mem::size_of::<u64>();

        let bytes = self.raw[offset..offset + core::mem::size_of::<u64>()]
            .try_into()
            .expect("Invalid chunk size");

        base::PageId::new(u64::from_le_bytes(bytes))
    }

    pub fn set_child_at(&mut self, index: usize, child_id: base::PageId) {
        let num_keys = self.curr_vec_sz() as usize;
        let num_children = num_keys + 1;
        if index >= num_children {
            panic!("set_child_at: index out of bounds");
        }

        let physical_index = num_children - 1 - index;
        let offset =
            constants::storage::PAGE_SIZE - (physical_index + 1) * core::mem::size_of::<u64>();

        self.raw[offset..offset + core::mem::size_of::<u64>()]
            .copy_from_slice(&child_id.get().to_le_bytes());
    }

    pub fn split_and_get_new_entries(
        &mut self,
        key: &[u8],
        child_id: base::PageId,
    ) -> BPlusInnerSplitData {
        let curr_sz = self.curr_vec_sz() as usize;

        // Create temporary vector of all entries (including new one)
        let mut all_keys = Vec::new();
        let mut all_children = self.get_child_ptrs(); // Logical order: child(0)...child(n)

        // Find insertion position
        let insert_pos = self.find_insert_position(key);

        // Add existing keys
        for i in 0..curr_sz {
            all_keys.push(self.get_key_at(i).unwrap().to_vec());
        }

        // Insert new key and child
        all_keys.insert(insert_pos, key.to_vec());
        all_children.insert(insert_pos + 1, child_id);

        // Find split point
        let total_entries = all_keys.len();
        let split_point = total_entries / 2; // This key will be pushed up

        let split_key = all_keys[split_point].clone();

        // Clear old node and rebuild with first half
        self.set_curr_vec_sz(0);
        self.set_free_space(constants::storage::PAGE_SIZE as u32 - 64);

        for i in 0..split_point {
            self.insert_sorted(&all_keys[i], all_children[i + 1]);
        }
        self.set_child_at(0, all_children[0]);

        // Create vectors for the new (right) node
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

        // Shift keys right to make room
        if insert_pos < curr_size {
            let keys_start = 64;
            let src_start = keys_start + insert_pos * key_size;
            let dst_start = src_start + key_size;
            let move_len = (curr_size - insert_pos) * key_size;
            self.raw
                .copy_within(src_start..src_start + move_len, dst_start);
        }

        // Insert new key
        let key_pos = 64 + insert_pos * key_size;
        self.raw[key_pos..key_pos + key_size].copy_from_slice(key);

        // Shift child pointers to make room for new pointer at position insert_pos + 1
        // We need to insert a new child pointer at logical position insert_pos + 1
        let curr_children = curr_size + 1;
        let new_children = curr_children + 1;

        // NEW_CHILD goes at logical_pos = insert_pos + 1
        let new_child_physical_index = new_children - 1 - (insert_pos + 1);

        for i in 0..=insert_pos {
            let src_physical_index = (curr_children - 1) - i;
            let dst_physical_index = (new_children - 1) - i; // This is src_physical_index + 1

            if src_physical_index != dst_physical_index {
                let src_offset = constants::storage::PAGE_SIZE
                    - (src_physical_index + 1) * core::mem::size_of::<u64>();
                let dst_offset = constants::storage::PAGE_SIZE
                    - (dst_physical_index + 1) * core::mem::size_of::<u64>();

                let value = u64::from_le_bytes(
                    self.raw[src_offset..src_offset + core::mem::size_of::<u64>()]
                        .try_into()
                        .expect("Invalid offset"),
                );
                self.raw[dst_offset..dst_offset + core::mem::size_of::<u64>()]
                    .copy_from_slice(&value.to_le_bytes());
            }
        }
        // Insert new child pointer at logical position insert_pos + 1
        let child_ptr_offset = constants::storage::PAGE_SIZE
            - (new_child_physical_index + 1) * core::mem::size_of::<u64>();
        self.raw[child_ptr_offset..child_ptr_offset + core::mem::size_of::<u64>()]
            .copy_from_slice(&child_ptr.get().to_le_bytes());

        // Update metadata
        self.set_curr_vec_sz((curr_size + 1) as u32);
        let free_space = self.free_space();
        self.set_free_space(free_space - (key_size as u32 + 8));
    }

    // Find insert position using binary search for better performance
    fn find_insert_position(&self, key: &[u8]) -> usize {
        let key_size = self.get_key_size() as usize;
        let curr_size = self.curr_vec_sz() as usize;
        let keys = self.get_keys();

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

    pub fn find_child_page(&self, key: &[u8]) -> Option<base::PageId> {
        let key_size = self.get_key_size() as usize;
        let num_keys = self.curr_vec_sz() as usize;

        if num_keys == 0 {
            // No keys, just one child pointer
            return self.get_child_at(0);
        }

        let keys = self.get_keys();

        // Use binary search to find the correct child
        let mut left = 0;
        let mut right = num_keys;

        while left < right {
            let mid = left + (right - left) / 2;
            let key_start = mid * key_size;
            let stored_key = &keys[key_start..key_start + key_size];

            if key < stored_key {
                right = mid;
            } else {
                left = mid + 1;
            }
        }

        self.get_child_at(left)
    }

    // Remove a key and its associated child pointer (if found)
    pub fn remove_key(&mut self, key: &[u8]) -> bool {
        let key_size = self.get_key_size() as usize;
        let curr_size = self.curr_vec_sz() as usize;

        if let Some(pos) = self.find_key_position(key) {
            // Shift keys left
            if pos < curr_size - 1 {
                let keys_start = 64;
                let src_start = keys_start + (pos + 1) * key_size;
                let dst_start = keys_start + pos * key_size;
                let move_len = (curr_size - pos - 1) * key_size;
                self.raw
                    .copy_within(src_start..src_start + move_len, dst_start);
            }

            // Remove child pointer at position pos + 1 and shift remaining pointers
            let curr_children = curr_size + 1;
            let new_children = curr_children - 1;

            // Shift child pointers left (remove pointer at logical position pos + 1)
            // Children at logical_pos [0, pos] must be "pushed" down
            // to a lower physical index (higher memory address).
            // We iterate forwards to "push" the data.
            for i in 0..=pos {
                let src_logical_index = i;
                let dst_logical_index = i;

                let src_physical_index = (curr_children - 1) - src_logical_index;
                let dst_physical_index = (new_children - 1) - dst_logical_index; // This is src_physical_index - 1

                let src_offset = constants::storage::PAGE_SIZE
                    - (src_physical_index + 1) * core::mem::size_of::<u64>();
                let dst_offset = constants::storage::PAGE_SIZE
                    - (dst_physical_index + 1) * core::mem::size_of::<u64>();

                let value = u64::from_le_bytes(
                    self.raw[src_offset..src_offset + core::mem::size_of::<u64>()]
                        .try_into()
                        .expect("Invalid offset"),
                );
                self.raw[dst_offset..dst_offset + core::mem::size_of::<u64>()]
                    .copy_from_slice(&value.to_le_bytes());
            }
            self.set_curr_vec_sz((curr_size - 1) as u32);
            self.set_free_space(self.free_space() + (key_size as u32 + 8));
            true
        } else {
            false
        }
    }

    // Find key's position, return None if not found
    fn find_key_position(&self, key: &[u8]) -> Option<usize> {
        let key_size = self.get_key_size() as usize;
        let curr_size = self.curr_vec_sz() as usize;
        let keys = self.get_keys();

        for i in 0..curr_size {
            let key_start = i * key_size;
            if &keys[key_start..key_start + key_size] == key {
                return Some(i);
            }
        }
        None
    }
}
