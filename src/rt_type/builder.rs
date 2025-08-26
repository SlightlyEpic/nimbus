use crate::rt_type::primitives;

pub struct TableTypeBuilder {
    built: bool,
    table_type: primitives::TableType,
}

impl primitives::AttributeKind {
    fn size(self) -> usize {
        match self {
            primitives::AttributeKind::U8 => 1,
            primitives::AttributeKind::U16 => 2,
            primitives::AttributeKind::U32 => 4,
            primitives::AttributeKind::U64 => 8,
            primitives::AttributeKind::U128 => 16,
            primitives::AttributeKind::I8 => 1,
            primitives::AttributeKind::I16 => 2,
            primitives::AttributeKind::I32 => 4,
            primitives::AttributeKind::I64 => 8,
            primitives::AttributeKind::I128 => 16,
            primitives::AttributeKind::F32 => 4,
            primitives::AttributeKind::F64 => 8,
            primitives::AttributeKind::Bool => 1,
            primitives::AttributeKind::Char(max_size) => max_size,
        }
    }
}

impl TableTypeBuilder {
    fn build(&mut self) -> primitives::TableType {
        if self.built {
            return primitives::TableType::clone(&self.table_type);
        }

        self.built = true;

        // Figure out the layout
        let mut curr_offset = 0;
        let mut last_size = 0;
        for attr in &self.table_type.attributes {
            let attr_align = attr.kind.alignment();
            if curr_offset % attr_align != 0 {
                curr_offset += attr_align - (curr_offset % attr_align);
            }
            self.table_type
                .layout
                .attr_layouts
                .push(primitives::LayoutAttrData {
                    attr_name: String::clone(&attr.name),
                    offset: curr_offset as u16,
                });
            last_size = attr.kind.size();
        }

        let mut total_size = curr_offset + last_size;
        if total_size % 8 != 0 {
            total_size += 8 - (total_size % 8);
        }

        self.table_type.layout.size = total_size;

        primitives::TableType::clone(&self.table_type)
    }

    fn add(&mut self, attr: primitives::TableAttribute) -> &Self {
        self.table_type.attributes.push(attr);
        self
    }
}
