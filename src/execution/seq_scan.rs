use super::executor::Executor;
use crate::catalog::manager::Catalog;
use crate::rt_type::primitives::TableType;
use crate::storage::buffer::BufferPool;
use crate::storage::heap::row::RowId;
use crate::storage::heap::tuple::Tuple;
use crate::storage::page::base::DiskPage;
use crate::storage::page::base::{Page, PageId};
use std::pin::Pin;

pub struct SeqScanExecutor<'a> {
    catalog: &'a Catalog,
    table_oid: u32,
    schema: TableType,
    // Iterator state is now managed inside the executor
    current_page_id: PageId,
    current_slot_index: u16,
    done: bool,
}

impl<'a> SeqScanExecutor<'a> {
    pub fn new(catalog: &'a Catalog, table_oid: u32) -> Result<Self, String> {
        let schema = catalog
            .get_table_schema(table_oid)
            .ok_or_else(|| format!("Table OID {} not found", table_oid))?;

        Ok(Self {
            catalog,
            table_oid,
            schema,
            current_page_id: 0, // Will be set in init
            current_slot_index: 0,
            done: false,
        })
    }
}

impl<'a> Executor for SeqScanExecutor<'a> {
    fn init(&mut self) {
        // Find the first page of the table
        self.current_page_id = self
            .catalog
            .get_table_root_page(self.table_oid)
            .unwrap_or(0);
        self.current_slot_index = 0;
        self.done = self.current_page_id == 0;
    }

    fn next(&mut self, mut bpm: Pin<&mut BufferPool>) -> Option<Tuple> {
        if self.done {
            return None;
        }

        loop {
            // Loop structure from your old HeapIterator
            if self.current_page_id == 0 {
                self.done = true;
                return None;
            }

            let frame_result = bpm.as_mut().fetch_page(self.current_page_id);
            if frame_result.is_err() {
                self.done = true;
                return None;
            }

            let frame = frame_result.unwrap();
            let frame_id = frame.fid();

            let mut next_page_id_to_scan = 0;
            let mut found_data: Option<(RowId, Vec<u8>)> = None;

            {
                let mut page_view = frame.page_view();

                if let Page::SlottedData(slotted) = &mut page_view {
                    let num_slots = slotted.num_slots();

                    while self.current_slot_index < num_slots {
                        let idx = self.current_slot_index as usize;
                        self.current_slot_index += 1; // Advance for next call

                        if let Some(data) = slotted.slot_data(idx) {
                            let rid = RowId::new(self.current_page_id, idx as u32);
                            found_data = Some((rid, data.to_vec()));
                            break; // Found a tuple
                        }
                    }

                    if found_data.is_none() {
                        // Reached end of this page, move to next
                        next_page_id_to_scan = slotted.header().next_page_id();
                    }
                } else {
                    // Invalid page type
                    bpm.as_mut().unpin_frame(frame_id).ok();
                    self.done = true;
                    return None;
                }
            } // page_view borrow ends

            bpm.as_mut().unpin_frame(frame_id).ok();

            if let Some((rid, tuple_bytes)) = found_data {
                // Found data, deserialize and return it
                if let Ok(mut tuple) = Tuple::from_bytes(&tuple_bytes, &self.schema) {
                    tuple.rid = Some(rid); // Attach RID
                    return Some(tuple);
                }
                // if deserialize fails, continue loop
            } else if next_page_id_to_scan != 0 {
                // No data found on this page, move to next
                self.current_page_id = next_page_id_to_scan;
                self.current_slot_index = 0;
            } else {
                // No data and no next page
                self.done = true;
                return None;
            }
        }
    }
}
