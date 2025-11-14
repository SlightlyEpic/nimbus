use crate::rt_type::primitives::{AttributeKind, AttributeValue, TableType};
use crate::storage::heap::row::RowId;
use std::convert::TryInto;

#[derive(Debug, PartialEq, Clone)]
pub struct Tuple {
    pub values: Vec<AttributeValue>,
    pub rid: Option<RowId>,
}

impl Tuple {
    pub fn new(values: Vec<AttributeValue>) -> Self {
        Self { values, rid: None }
    }

    pub fn new_with_rid(values: Vec<AttributeValue>, rid: RowId) -> Self {
        Self {
            values,
            rid: Some(rid),
        }
    }
    /// Serializes the tuple into a packed byte vector (Variable Length).
    pub fn to_bytes(&self, schema: &TableType) -> Result<Vec<u8>, String> {
        let mut buffer = Vec::new();

        if self.values.len() != schema.attributes.len() {
            return Err("Tuple values count does not match schema".to_string());
        }

        for (i, attr) in schema.attributes.iter().enumerate() {
            let val = &self.values[i];
            match (val, &attr.kind) {
                // --- Unsigned Integers ---
                (AttributeValue::U8(v), AttributeKind::U8) => buffer.push(*v),
                (AttributeValue::U16(v), AttributeKind::U16) => {
                    buffer.extend_from_slice(&v.to_be_bytes())
                }
                (AttributeValue::U32(v), AttributeKind::U32) => {
                    buffer.extend_from_slice(&v.to_be_bytes())
                }
                (AttributeValue::U64(v), AttributeKind::U64) => {
                    buffer.extend_from_slice(&v.to_be_bytes())
                }

                // --- Signed Integers ---
                (AttributeValue::I8(v), AttributeKind::I8) => {
                    buffer.extend_from_slice(&v.to_be_bytes())
                }
                (AttributeValue::I16(v), AttributeKind::I16) => {
                    buffer.extend_from_slice(&v.to_be_bytes())
                }
                (AttributeValue::I32(v), AttributeKind::I32) => {
                    buffer.extend_from_slice(&v.to_be_bytes())
                }
                (AttributeValue::I64(v), AttributeKind::I64) => {
                    buffer.extend_from_slice(&v.to_be_bytes())
                }

                // --- Floats & Bools ---
                (AttributeValue::F64(v), AttributeKind::F64) => {
                    buffer.extend_from_slice(&v.to_be_bytes())
                }
                (AttributeValue::Bool(v), AttributeKind::Bool) => {
                    buffer.push(if *v { 1 } else { 0 })
                }

                // --- Strings ---
                (AttributeValue::Char(s), AttributeKind::Char(len)) => {
                    let bytes = s.as_bytes();
                    if bytes.len() > *len {
                        return Err(format!("Char too long: {} > {}", bytes.len(), len));
                    }
                    buffer.push(bytes.len() as u8);
                    buffer.extend_from_slice(bytes);
                    for _ in 0..(*len - bytes.len()) {
                        buffer.push(0);
                    }
                }
                (AttributeValue::Varchar(s), AttributeKind::Varchar) => {
                    let bytes = s.as_bytes();
                    let len = bytes.len();
                    if len > u16::MAX as usize {
                        return Err("Varchar too long for u16 length prefix".to_string());
                    }
                    buffer.extend_from_slice(&(len as u16).to_be_bytes());
                    buffer.extend_from_slice(bytes);
                }

                _ => return Err(format!("Type mismatch for col {}", attr.name)),
            }
        }

        Ok(buffer)
    }

    pub fn from_bytes(data: &[u8], schema: &TableType) -> Result<Self, String> {
        let mut values = Vec::new();
        let mut cursor = 0;

        for attr in &schema.attributes {
            if cursor >= data.len() {
                // Only Varchar/Char might legitimately be empty if checks weren't strict,
                // but generally this means truncated data.
                return Err("Unexpected end of tuple data".to_string());
            }

            let val = match attr.kind {
                // --- Unsigned Integers ---
                AttributeKind::U8 => {
                    let v = data[cursor];
                    cursor += 1;
                    AttributeValue::U8(v)
                }
                AttributeKind::U16 => {
                    let bytes = data[cursor..cursor + 2]
                        .try_into()
                        .map_err(|_| "Read err")?;
                    cursor += 2;
                    AttributeValue::U16(u16::from_be_bytes(bytes))
                }
                AttributeKind::U32 => {
                    let bytes = data[cursor..cursor + 4]
                        .try_into()
                        .map_err(|_| "Read err")?;
                    cursor += 4;
                    AttributeValue::U32(u32::from_be_bytes(bytes))
                }
                AttributeKind::U64 => {
                    let bytes = data[cursor..cursor + 8]
                        .try_into()
                        .map_err(|_| "Read err")?;
                    cursor += 8;
                    AttributeValue::U64(u64::from_be_bytes(bytes))
                }

                // --- Signed Integers ---
                AttributeKind::I8 => {
                    let v = i8::from_be_bytes([data[cursor]]);
                    cursor += 1;
                    AttributeValue::I8(v)
                }
                AttributeKind::I16 => {
                    let bytes = data[cursor..cursor + 2]
                        .try_into()
                        .map_err(|_| "Read err")?;
                    cursor += 2;
                    AttributeValue::I16(i16::from_be_bytes(bytes))
                }
                AttributeKind::I32 => {
                    let bytes = data[cursor..cursor + 4]
                        .try_into()
                        .map_err(|_| "Read err")?;
                    cursor += 4;
                    AttributeValue::I32(i32::from_be_bytes(bytes))
                }
                AttributeKind::I64 => {
                    let bytes = data[cursor..cursor + 8]
                        .try_into()
                        .map_err(|_| "Read err")?;
                    cursor += 8;
                    AttributeValue::I64(i64::from_be_bytes(bytes))
                }

                // --- Floats & Bools ---
                AttributeKind::F64 => {
                    let bytes = data[cursor..cursor + 8]
                        .try_into()
                        .map_err(|_| "Read err")?;
                    cursor += 8;
                    AttributeValue::F64(f64::from_be_bytes(bytes))
                }
                AttributeKind::Bool => {
                    let v = data[cursor] != 0;
                    cursor += 1;
                    AttributeValue::Bool(v)
                }

                // --- Strings ---
                AttributeKind::Char(len) => {
                    let fixed_block_size = len + 1; // 1 byte for length + max data size

                    // 1. Fixed Block Bounds Check
                    if cursor + fixed_block_size > data.len() {
                        return Err("Buffer overrun reading Char".to_string());
                    }

                    // 2. Read string length
                    let str_len = data[cursor] as usize;
                    if str_len > len {
                        return Err("Corrupted Char length".to_string());
                    }

                    // 3. Read string content
                    let s = String::from_utf8(data[cursor + 1..cursor + 1 + str_len].to_vec())
                        .map_err(|_| "Invalid UTF8")?;

                    // 4. Advance cursor by the full fixed size, skipping padding (Fix 2)
                    cursor += fixed_block_size;

                    AttributeValue::Char(s)
                }

                AttributeKind::Varchar => {
                    // 1. Read Length Prefix (2 bytes)
                    let len_end = cursor + 2;
                    if len_end > data.len() {
                        return Err("Buffer overrun reading Varchar length prefix".to_string());
                    }
                    let len_bytes = data[cursor..len_end].try_into().map_err(|_| "Read err")?;
                    let str_len = u16::from_be_bytes(len_bytes) as usize;

                    // 2. Total Data Bounds Check (Fix 1)
                    let data_end = len_end + str_len;
                    if data_end > data.len() {
                        return Err(format!(
                            "Buffer overrun reading Varchar data (expected {}, got {})",
                            data_end,
                            data.len()
                        ));
                    }

                    // 3. Read and Deserialize String
                    let s = String::from_utf8(data[len_end..data_end].to_vec())
                        .map_err(|_| "Invalid UTF8")?;

                    // 4. Advance cursor
                    cursor = data_end;

                    AttributeValue::Varchar(s)
                }
                _ => return Err("Unsupported type deserialization".to_string()),
            };
            values.push(val);
        }

        Ok(Self { values, rid: None })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rt_type::primitives::{
        AttributeKind, AttributeValue, LayoutAttrData, TableAttribute, TableLayout, TableType,
    };
    use crate::storage::buffer::BufferPool;
    use crate::storage::buffer::fifo_evictor::FifoEvictor;
    use crate::storage::disk::FileManager;
    use crate::storage::heap::heap_file::HeapFile;
    use crate::storage::page::base::PageKind;
    use crate::storage::page_locator::locator::DirectoryPageLocator;
    use std::fs;
    use std::sync::atomic::AtomicU32;

    // Helper to manually construct a schema without relying on the Builder
    fn create_complex_schema() -> TableType {
        let attrs = vec![
            TableAttribute {
                name: "tiny_int".to_string(),
                kind: AttributeKind::I8,
                nullable: false,
                is_internal: false,
            },
            TableAttribute {
                name: "big_int".to_string(),
                kind: AttributeKind::U64,
                nullable: false,
                is_internal: false,
            },
            TableAttribute {
                name: "float_val".to_string(),
                kind: AttributeKind::F64,
                nullable: false,
                is_internal: false,
            },
            TableAttribute {
                name: "is_active".to_string(),
                kind: AttributeKind::Bool,
                nullable: false,
                is_internal: false,
            },
            TableAttribute {
                name: "short_text".to_string(),
                kind: AttributeKind::Char(10),
                nullable: false,
                is_internal: false,
            },
        ];

        // Manual layout calculation (simplified packing)
        // I8 (0) -> 1 byte
        // Padding 7 bytes
        // U64 (8) -> 8 bytes
        // F64 (16) -> 8 bytes
        // Bool (24) -> 1 byte
        // Char(10) (25) -> 11 bytes (1 len + 10 data)
        // End offset = 25 + 11 = 36.
        // Round up to 8-byte alignment = 40.
        TableType {
            attributes: attrs,
            layout: TableLayout {
                size: 40, // FIX: Increased from 32 to 40
                attr_layouts: vec![
                    LayoutAttrData {
                        attr_name: "tiny_int".to_string(),
                        offset: 0,
                    },
                    LayoutAttrData {
                        attr_name: "big_int".to_string(),
                        offset: 8,
                    }, // Aligned to 8
                    LayoutAttrData {
                        attr_name: "float_val".to_string(),
                        offset: 16,
                    },
                    LayoutAttrData {
                        attr_name: "is_active".to_string(),
                        offset: 24,
                    },
                    LayoutAttrData {
                        attr_name: "short_text".to_string(),
                        offset: 25,
                    },
                ],
            },
        }
    }

    #[test]
    fn test_tuple_all_types() {
        let schema = create_complex_schema();

        let original = Tuple::new(vec![
            AttributeValue::I8(-120),
            AttributeValue::U64(1234567890123456),
            AttributeValue::F64(3.14159),
            AttributeValue::Bool(true),
            AttributeValue::Char("Testing".to_string()),
        ]);

        let bytes = original.to_bytes(&schema).expect("Serialization failed");
        let deserialized = Tuple::from_bytes(&bytes, &schema).expect("Deserialization failed");

        // Verify specific values to ensure types retained accuracy
        match &deserialized.values[0] {
            AttributeValue::I8(v) => assert_eq!(*v, -120),
            _ => panic!("Wrong type I8"),
        }
        match &deserialized.values[1] {
            AttributeValue::U64(v) => assert_eq!(*v, 1234567890123456),
            _ => panic!("Wrong type U64"),
        }
        match &deserialized.values[2] {
            AttributeValue::F64(v) => assert!((v - 3.14159).abs() < f64::EPSILON),
            _ => panic!("Wrong type F64"),
        }
        match &deserialized.values[3] {
            AttributeValue::Bool(v) => assert_eq!(*v, true),
            _ => panic!("Wrong type Bool"),
        }
        match &deserialized.values[4] {
            AttributeValue::Char(v) => assert_eq!(v, "Testing"),
            _ => panic!("Wrong type Char"),
        }
    }

    #[test]
    fn test_string_boundaries() {
        // Schema with Char(5). Size = 5 + 1 = 6.
        let schema = TableType {
            attributes: vec![TableAttribute {
                name: "text".to_string(),
                kind: AttributeKind::Char(5),
                nullable: false,
                is_internal: false,
            }],
            layout: TableLayout {
                size: 6, // 1 byte len + 5 bytes data
                attr_layouts: vec![LayoutAttrData {
                    attr_name: "text".to_string(),
                    offset: 0,
                }],
            },
        };

        // Case 1: Empty String
        let t1 = Tuple::new(vec![AttributeValue::Char("".to_string())]);
        let b1 = t1.to_bytes(&schema).unwrap();
        let d1 = Tuple::from_bytes(&b1, &schema).unwrap();
        if let AttributeValue::Char(s) = &d1.values[0] {
            assert_eq!(s, "");
        } else {
            panic!();
        }

        // Case 2: Max Length String (5 chars)
        let t2 = Tuple::new(vec![AttributeValue::Char("12345".to_string())]);
        let b2 = t2.to_bytes(&schema).unwrap();
        let d2 = Tuple::from_bytes(&b2, &schema).unwrap();
        if let AttributeValue::Char(s) = &d2.values[0] {
            assert_eq!(s, "12345");
        } else {
            panic!();
        }

        // Case 3: String too long (Should fail)
        let t3 = Tuple::new(vec![AttributeValue::Char("123456".to_string())]);
        assert!(t3.to_bytes(&schema).is_err());
    }

    #[test]
    fn test_schema_validation() {
        let schema = create_complex_schema(); // Expects 5 cols

        // Case 1: Too few columns
        let t1 = Tuple::new(vec![AttributeValue::I8(1)]);
        assert!(t1.to_bytes(&schema).is_err());

        // Case 2: Too many columns
        let t2 = Tuple::new(vec![
            AttributeValue::I8(1),
            AttributeValue::U64(1),
            AttributeValue::F64(1.0),
            AttributeValue::Bool(true),
            AttributeValue::Char("A".to_string()),
            AttributeValue::Bool(false), // Extra
        ]);
        assert!(t2.to_bytes(&schema).is_err());
    }

    #[test]
    fn test_type_mismatch() {
        let schema = TableType {
            attributes: vec![TableAttribute {
                name: "id".to_string(),
                kind: AttributeKind::U32,
                nullable: false,
                is_internal: false,
            }],
            // Layout is ignored in Packed Tuple logic, so empty is fine
            layout: TableLayout {
                size: 0,
                attr_layouts: vec![],
            },
        };

        // Pass a String where U32 is expected
        let t1 = Tuple::new(vec![AttributeValue::Char("Bad".to_string())]);
        let res = t1.to_bytes(&schema);

        assert!(res.is_err());

        // look for "Type mismatch"
        let err = res.unwrap_err();
        assert!(
            err.contains("Type mismatch"),
            "Expected 'Type mismatch' error, got: '{}'",
            err
        );
    }

    #[test]
    fn test_heap_file_integration_persistence() {
        let test_file = "test_tuple_persist.db";
        let _ = fs::remove_file(test_file);
        let file_manager = FileManager::new(test_file.to_string()).unwrap();
        let evictor = Box::new(FifoEvictor::new());
        let locator = Box::new(DirectoryPageLocator::new());
        let mut bp = Box::pin(BufferPool::new(file_manager, evictor, locator));

        // Bootstrap directory
        let frame = bp.as_mut().alloc_new_page(PageKind::Directory, 1).unwrap();
        let fid = frame.fid();
        bp.as_mut().unpin_frame(fid).unwrap();
        let counter = AtomicU32::new(1);

        let schema = create_complex_schema();
        let t1 = Tuple::new(vec![
            AttributeValue::I8(10),
            AttributeValue::U64(9999),
            AttributeValue::F64(1.23),
            AttributeValue::Bool(false),
            AttributeValue::Char("Persist".to_string()),
        ]);

        // Insert
        let mut heap = HeapFile::new(0, 0);
        let bytes = t1.to_bytes(&schema).unwrap();
        let rid = heap
            .insert(bp.as_mut(), &counter, &bytes)
            .expect("Insert failed");

        // Simulate restart (flush and re-read)
        bp.as_mut().flush_all().unwrap();

        let read_bytes = HeapFile::get(bp.as_mut(), rid).expect("Get failed");
        let t2 = Tuple::from_bytes(&read_bytes, &schema).expect("Deserialize failed");

        assert_eq!(t1, t2);

        fs::remove_file(test_file).unwrap();
    }

    fn create_varchar_schema() -> TableType {
        TableType {
            attributes: vec![
                TableAttribute {
                    name: "id".into(),
                    kind: AttributeKind::U64,
                    nullable: false,
                    is_internal: false,
                },
                TableAttribute {
                    name: "bio".into(),
                    kind: AttributeKind::Varchar,
                    nullable: false,
                    is_internal: false,
                },
            ],
            layout: TableLayout {
                size: 0,
                attr_layouts: vec![],
            }, // Layout is now dynamic/ignored
        }
    }

    #[test]
    fn test_varchar_space_saving() {
        let schema = create_varchar_schema();

        // Tuple 1: Short String (Bio: "Hi")
        // Expected Size: 8 (U64) + 2 (Len) + 2 (Data) = 12 bytes
        let t1 = Tuple::new(vec![
            AttributeValue::U64(1),
            AttributeValue::Varchar("Hi".to_string()),
        ]);
        let b1 = t1.to_bytes(&schema).unwrap();
        assert_eq!(b1.len(), 12, "Short string should take exactly 12 bytes");

        // Tuple 2: Long String (Bio: "Hello World")
        // Expected Size: 8 (U64) + 2 (Len) + 11 (Data) = 21 bytes
        let t2 = Tuple::new(vec![
            AttributeValue::U64(2),
            AttributeValue::Varchar("Hello World".to_string()),
        ]);
        let b2 = t2.to_bytes(&schema).unwrap();
        assert_eq!(b2.len(), 21, "Longer string should take more bytes");

        // Verify Deserialization works for both
        let d1 = Tuple::from_bytes(&b1, &schema).unwrap();
        let d2 = Tuple::from_bytes(&b2, &schema).unwrap();

        match &d1.values[1] {
            AttributeValue::Varchar(s) => assert_eq!(s, "Hi"),
            _ => panic!("Wrong type"),
        }
        match &d2.values[1] {
            AttributeValue::Varchar(s) => assert_eq!(s, "Hello World"),
            _ => panic!("Wrong type"),
        }
    }
}
