use crate::rt_type::primitives::{self, TableAttribute};

pub struct LayoutReadWriter<'a> {
    layout: &'a primitives::TableLayout,
}

impl<'a> LayoutReadWriter<'a> {
    fn new(layout: &'a primitives::TableLayout) -> LayoutReadWriter<'a> {
        Self { layout }
    }

    fn find_offset(&self, attr: &TableAttribute) -> Option<u16> {
        for lay_attr in &self.layout.attr_layouts {
            if lay_attr.attr_name == attr.name {
                return Some(lay_attr.offset);
            }
        }
        None
    }

    pub fn read_attr<const SIZE: usize>(
        &self,
        attr: &primitives::TableAttribute,
        page: &[u8; SIZE],
        base_offset: u16,
    ) -> Option<primitives::AttributeValue> {
        if attr.kind != primitives::AttributeKind::U8 {
            return None;
        }

        let offset = base_offset + self.find_offset(attr)?;
        let offset_us = offset as usize;
        let slice = &page[offset_us..offset_us + attr.kind.size_of()];
        let bytes = slice.try_into().expect("offset to be within array bounds");
        let value = attr.kind.from_le_bytes(bytes);

        Some(value)
    }
}
