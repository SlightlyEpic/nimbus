use crate::{constants, storage::page::base};

pub struct BPlusInner<'a> {
    raw: &'a mut base::PageBuf,
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
    //   1..  3     -> Level                            (u16)    |
    //   3..  4     -> Reserved                         (u8)    |
    //   4..  8     -> Free Space (bytes)               (u32)   |
    //   8.. 16     -> Page ID                         (u64)   | Header (64 bytes)
    //  16.. 24     -> Prev Sibling Page ID            (u64)   |
    //  24.. 32     -> Next Sibling Page ID            (u64)   |
    //  32.. 36     -> Current Vector Size (num pairs) (u32)   |
    //  36.. 40     -> Key Size                        (u32)   |
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
        let mut result = Self { raw };
        result.set_page_kind(base::PageKind::BPlusInner);
        result
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
        let start = constants::storage::PAGE_SIZE - size * core::mem::size_of::<u64>();
        let end = constants::storage::PAGE_SIZE;
        if start >= end {
            panic!("Invalid value pointers vector size");
        }
        self.raw[start..end]
            .chunks_exact(core::mem::size_of::<u64>())
            .rev()
            .map(|chunk| u64::from_le_bytes(chunk.try_into().expect("Invalid chunk size")))
            .collect()
    }

    pub fn push_pair(&mut self, key: &[u8], value: u64) {
        let key_size = self.get_key_size() as usize;
        if key.len() != key_size {
            panic!(
                "Key size mismatch: expected {}, got {}",
                key_size,
                key.len()
            );
        }
        let required_space = key_size as u32 + 8;
        if self.free_space() < required_space {
            panic!("Not enough space for additional key-value pair");
        }

        let curr_size = self.curr_vec_sz() as usize;

        // Calculate current positions
        let keys_start = 64;
        let key_pos = keys_start + curr_size * key_size;
        let values_low = constants::storage::PAGE_SIZE - curr_size * core::mem::size_of::<u64>();
        let val_pos = values_low - core::mem::size_of::<u64>();

        if key_pos + key_size > val_pos {
            panic!("No space for additional key-value pair");
        }

        self.raw[key_pos..key_pos + key_size].copy_from_slice(key);
        self.raw[val_pos..val_pos + core::mem::size_of::<u64>()]
            .copy_from_slice(&value.to_le_bytes());

        // Update metadata
        self.set_curr_vec_sz((curr_size + 1) as u32);
        self.set_free_space(self.free_space() - required_space);
    }

    pub fn get_value(&self, key: &[u8]) -> Option<u64> {
        let key_size = self.get_key_size() as usize;
        if key.len() != key_size {
            return None; // Key size mismatch
        }

        let num_pairs = self.curr_vec_sz() as usize;
        let keys = self.get_keys();

        // Binary search
        let mut left = 0;
        let mut right = num_pairs;

        while left < right {
            let mid = left + (right - left) / 2;
            let key_start = mid * key_size;
            let stored_key = &keys[key_start..key_start + key_size];

            match stored_key.cmp(key) {
                core::cmp::Ordering::Equal => {
                    // Found the key, calculate corresponding value offset
                    let value_idx = mid;
                    let value_offset = constants::storage::PAGE_SIZE
                        - (value_idx + 1) * core::mem::size_of::<u64>();
                    let bytes = self.raw[value_offset..value_offset + core::mem::size_of::<u64>()]
                        .try_into()
                        .expect("Invalid value offset");
                    return Some(u64::from_le_bytes(bytes));
                }
                core::cmp::Ordering::Less => left = mid + 1,
                core::cmp::Ordering::Greater => right = mid,
            }
        }

        None // Key not found
    }
}
