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

impl<'a> BPlusInner<'a> {
    // === Memory layout ===
    //   0..  1     -> Page Kind                (u8)   -|
    //   1..  2     -> BPlus Type(inner/leaf)   (u8)    |
    //   2..  3     -> level
    //   3..  8     -> free space                       |
    //   8.. 16     -> Page Id                  (u64)   | Header (64 bytes)
    //  16.. 32     -> prev sibling page id
    //  32.. 40     -> next sibling page id
    //  40.. 44     -> curr vec sz              (u32)   |
    //
    //  44.. 64     -> free space                      -|
    //  64.. 2080   -> vec[u64: pageId]
    //  2080.. 4096 -> vec[u64: children ptr / record id]

    pub const fn new<'b: 'a>(raw: &'b mut base::PageBuf) -> Self {
        let mut page = Self { raw };
        page.set_page_kind(base::PageKind::BPlusInner);
        page.set_free_space(constants::storage::PAGE_SIZE as u32 - 64);

        page
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
}
