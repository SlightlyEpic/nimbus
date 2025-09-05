use crate::{constants, storage::page::base};

pub struct BPlusInner<'a> {
    raw: &'a mut base::PageBuf,
}

impl<'a> base::DiskPage for BPlusInner<'a> {
    const PAGE_KIND: u8 = base::PageKind::BPlusInner as u8;

    fn raw(self: &Self) -> &[u8; constants::storage::PAGE_SIZE] {
        return &self.raw;
    }

    fn raw_mut(&mut self) -> &mut [u8; constants::storage::PAGE_SIZE] {
        return &mut self.raw;
    }
}

const KEY_END_ADDR: usize = ((constants::storage::PAGE_SIZE - 64) / 2) + 64;

impl<'a> BPlusInner<'a> {
    // === Memory layout ===
    //   0..  1     -> Page Kind                        (u8)   -|
    //   1..  2     -> BPlus Node type (inner/leaf)     (u8)    |
    //   2..  3     -> level                                    |
    //   3..  4     -> num free slots                           |
    //   4..  8     -> empty pageId vec offset          (u32)   |
    //   8.. 16     -> Page Id                          (u64)   | Header (64 bytes)
    //  16.. 32     -> prev sibling page id             (u64)   |
    //  32.. 40     -> next sibling page id             (u64)   |
    //  40.. 44     -> curr vec sz                      (u32)   |
    //  44.. 64     -> empty                                   -|
    //  64.. N      -> vec[u64: pageId]
    //  N.. PAGE_SIZE -> vec[u64: children ptr() / record id(leaf)]
    //  N = (PAGE_SIZE - 64) / 2

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

    pub const fn free_slots(&self) -> u32 {
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
            let ptr = self.raw.as_ptr().add(32) as *const u64;
            let val = u64::from_le(*ptr);
            base::PageId::new(val)
        }
    }

    pub const fn curr_vec_sz(&self) -> u32 {
        unsafe {
            let ptr = self.raw.as_ptr().add(40) as *const u32;
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
            let ptr = self.raw.as_mut_ptr().add(32) as *mut u64;
            *ptr = match id {
                Some(page_id) => page_id.get().to_le(),
                None => 0,
            };
        }
    }

    pub const fn set_curr_vec_sz(&mut self, size: u32) {
        unsafe {
            let ptr = self.raw.as_mut_ptr().add(40) as *mut u32;
            *ptr = size.to_le();
        }
    }

    pub fn get_page_ids(&self) -> &[u64] {
        let size = self.curr_vec_sz() as usize;
        let start = 64;
        let end = start + size * core::mem::size_of::<u64>();
        if end > KEY_END_ADDR {
            panic!("Invalid page IDs vector size");
        }
        unsafe {
            let ptr = self.raw.as_ptr().add(start) as *const u64;
            core::slice::from_raw_parts(ptr, size)
        }
    }

    pub fn get_value_ptrs(&self) -> &[u64] {
        let size = self.curr_vec_sz() as usize;
        let start = KEY_END_ADDR;
        let end = start + size * core::mem::size_of::<u64>();
        if end > constants::storage::PAGE_SIZE {
            panic!("Invalid Value pointers vector size");
        }
        unsafe {
            let ptr = self.raw.as_ptr().add(start) as *const u64;
            core::slice::from_raw_parts(ptr, size)
        }
    }

    pub fn push_pair(&mut self, page_id: u64, value: u64) {
        if self.free_slots() == 0 {
            // need to return a conformation value if error than create a new page
            panic!("No free slots available for additional key-value pair");
        }

        let curr_size = self.curr_vec_sz() as usize;

        // ---- Key write ----
        let keys_start = 64;
        let new_keys_end = keys_start + (curr_size + 1) * core::mem::size_of::<u64>();
        if new_keys_end > KEY_END_ADDR {
            // need to return a conformation value if error than create a new page
            panic!("No space for additional page ID");
        }

        // ---- Value write ----
        let vals_start = KEY_END_ADDR;
        let new_vals_end = vals_start + (curr_size + 1) * core::mem::size_of::<u64>();
        if new_vals_end > constants::storage::PAGE_SIZE {
            // need to return a conformation value if error than create a new page
            panic!("No space for additional value (child ptr / record id)");
        }

        unsafe {
            // Write key
            let ptr_key = self
                .raw
                .as_mut_ptr()
                .add(keys_start + curr_size * core::mem::size_of::<u64>())
                as *mut u64;
            *ptr_key = page_id.to_le();

            // Write value
            let ptr_val = self
                .raw
                .as_mut_ptr()
                .add(vals_start + curr_size * core::mem::size_of::<u64>())
                as *mut u64;
            *ptr_val = value.to_le();
        }

        // Update metadata
        self.set_curr_vec_sz((curr_size + 1) as u32);
        self.set_free_space(self.free_slots() - 1);
    }
}
