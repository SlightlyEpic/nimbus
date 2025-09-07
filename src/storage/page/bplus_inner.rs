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
    //   1..  2     -> Node Type                        (u8)    |
    //   2..  3     -> Level                            (u8)    |
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

    pub const fn new<'b: 'a>(raw: &'b mut base::PageBuf) -> Self {
        Self { raw }
    }

    pub const fn page_kind(&self) -> u8 {
        self.raw[0]
    }

    pub const fn node_type(&self) -> u8 {
        self.raw[1]
    }

    pub const fn page_level(&self) -> u8 {
        self.raw[2]
    }

    pub const fn free_space(&self) -> u32 {
        unsafe {
            let ptr = self.raw.as_ptr().add(4) as *const u32;
            u32::from_le(*ptr)
        }
    }

    pub const fn page_id(&self) -> base::PageId {
        unsafe {
            let ptr = self.raw.as_ptr().add(8) as *const u64;
            let val = u64::from_le(*ptr);
            base::PageId::new(val).unwrap()
        }
    }

    pub const fn prev_sibling(&self) -> Option<base::PageId> {
        unsafe {
            let ptr = self.raw.as_ptr().add(16) as *const u64;
            let val = u64::from_le(*ptr);
            base::PageId::new(val)
        }
    }

    pub const fn next_sibling(&self) -> Option<base::PageId> {
        unsafe {
            let ptr = self.raw.as_ptr().add(24) as *const u64;
            let val = u64::from_le(*ptr);
            base::PageId::new(val)
        }
    }

    pub const fn curr_vec_sz(&self) -> u32 {
        unsafe {
            let ptr = self.raw.as_ptr().add(32) as *const u32;
            u32::from_le(*ptr)
        }
    }

    pub const fn get_key_size(&self) -> u32 {
        unsafe {
            let ptr = self.raw.as_ptr().add(36) as *const u32;
            u32::from_le(*ptr)
        }
    }

    const fn set_page_kind(&mut self, kind: base::PageKind) {
        self.raw[0] = kind as u8;
    }

    const fn set_free_space(&mut self, free: u32) {
        unsafe {
            let ptr = self.raw.as_mut_ptr().add(4) as *mut u32;
            *ptr = free.to_le();
        }
    }

    pub const fn set_page_id(&mut self, id: base::PageId) {
        unsafe {
            let ptr = self.raw.as_mut_ptr().add(8) as *mut u64;
            *ptr = id.get().to_le();
        }
    }

    pub const fn set_level(&mut self, level: u8) {
        unsafe {
            let ptr = self.raw.as_mut_ptr().add(2) as *mut u8;
            *ptr = level;
        }
    }

    pub const fn set_node_type(&mut self, node_type: u8) {
        unsafe {
            let ptr = self.raw.as_mut_ptr().add(1) as *mut u8;
            *ptr = node_type;
        }
    }

    pub const fn set_prev_sibling(&mut self, id: Option<base::PageId>) {
        unsafe {
            let ptr = self.raw.as_mut_ptr().add(16) as *mut u64;
            *ptr = match id {
                Some(page_id) => page_id.get().to_le(),
                None => 0,
            };
        }
    }

    pub const fn set_next_sibling(&mut self, id: Option<base::PageId>) {
        unsafe {
            let ptr = self.raw.as_mut_ptr().add(24) as *mut u64;
            *ptr = match id {
                Some(page_id) => page_id.get().to_le(),
                None => 0,
            };
        }
    }

    pub const fn set_curr_vec_sz(&mut self, size: u32) {
        unsafe {
            let ptr = self.raw.as_mut_ptr().add(32) as *mut u32;
            *ptr = size.to_le();
        }
    }

    const fn set_key_size(&mut self, key_size: u32) {
        unsafe {
            let ptr = self.raw.as_mut_ptr().add(36) as *mut u32;
            *ptr = key_size.to_le();
        }
    }

    /// Gets the slice of keys as raw bytes (concatenated).
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

    /// Gets the vector of values (u64) in logical order.
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
        unsafe {
            let ptr = self.raw.as_ptr().add(start) as *const u64;
            let slice = core::slice::from_raw_parts(ptr, size);
            slice.iter().rev().map(|&x| u64::from_le(x)).collect()
        }
    }

    /// Pushes a key-value pair to the page.
    pub fn push_pair(&mut self, key: &[u8], value: u64) {
        let key_size = self.get_key_size() as u32;
        if key.len() != key_size as usize {
            panic!(
                "Key size mismatch: expected {}, got {}",
                key_size,
                key.len()
            );
        }
        let required_space = key_size + 8;
        if self.free_space() < required_space {
            panic!("Not enough space for additional key-value pair");
        }

        let curr_size = self.curr_vec_sz() as usize;

        // Calculate current positions
        let keys_start = 64;
        let key_pos = keys_start + curr_size * (key_size as usize);
        let values_low = constants::storage::PAGE_SIZE - curr_size * core::mem::size_of::<u64>();
        let val_pos = values_low - core::mem::size_of::<u64>();

        // Check for overlap (redundant since free_space checks it)
        if key_pos + (key_size as usize) > val_pos {
            panic!("No space for additional key-value pair");
        }

        unsafe {
            // Write key (grow forward)
            let ptr_key = self.raw.as_mut_ptr().add(key_pos);
            core::ptr::copy_nonoverlapping(key.as_ptr(), ptr_key, key_size as usize);

            // Write value (grow backward)
            let ptr_val = self.raw.as_mut_ptr().add(val_pos) as *mut u64;
            *ptr_val = value.to_le();
        }

        // Update metadata
        self.set_curr_vec_sz((curr_size + 1) as u32);
        self.set_free_space(self.free_space() - required_space);
    }
}
