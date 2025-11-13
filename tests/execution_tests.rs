use nimbus::catalog::manager::Catalog;
use nimbus::catalog::schema::SYSTEM_TABLES_ID;
use nimbus::execution::executor::Executor;
use nimbus::execution::filter::FilterExecutor;
use nimbus::execution::insert::InsertExecutor;
use nimbus::execution::projection::ProjectionExecutor;
use nimbus::execution::seq_scan::SeqScanExecutor;
use nimbus::execution::update::UpdateExecutor;
use nimbus::execution::values::ValuesExecutor;
use nimbus::rt_type::primitives::{
    AttributeKind, AttributeValue, TableAttribute, TableLayout, TableType,
};

use nimbus::execution::index_scan::IndexScanExecutor;
use nimbus::storage::buffer::BufferPool;
use nimbus::storage::buffer::fifo_evictor::FifoEvictor;
use nimbus::storage::disk::FileManager;
use nimbus::storage::heap::tuple::Tuple;
use nimbus::storage::page_locator::locator::DirectoryPageLocator;
use std::fs;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

fn setup_catalog(db_name: &str) -> (Arc<Mutex<BufferPool>>, Catalog) {
    let _ = fs::create_dir_all("test_db");

    let file_path = format!("test_db/{}", db_name);
    let _ = fs::remove_file(&file_path);

    let fm = FileManager::new(file_path).unwrap();
    let bp = Arc::new(Mutex::new(BufferPool::new(
        fm,
        Box::new(FifoEvictor::new()),
        Box::new(DirectoryPageLocator::new()),
    )));
    let catalog = Catalog::new(bp.clone());
    (bp, catalog)
}

#[test]
fn test_seq_scan_system_tables() {
    let (bp, catalog) = setup_catalog("test_scan_sys.db");

    let mut bp_guard = bp.lock().unwrap();
    let mut pinned_bp = unsafe { Pin::new_unchecked(&mut *bp_guard) };

    let mut scan = SeqScanExecutor::new(&catalog, SYSTEM_TABLES_ID).expect("Failed to create scan");
    scan.init(); // No bpm

    let mut count = 0;
    while let Some(tuple) = scan.next(pinned_bp.as_mut()) {
        // Pass bpm
        println!("Found system row: {:?}", tuple);
        count += 1;
    }
    // Must find at least 'system_tables' and 'system_columns'
    assert!(
        count >= 2,
        "System tables were not populated during bootstrap!"
    );
}

#[test]
fn test_insert_and_filter() {
    let (bp, mut catalog) = setup_catalog("test_filter.db");

    // 1. Create Table
    let schema = TableType {
        attributes: vec![TableAttribute {
            name: "age".into(),
            kind: AttributeKind::U32,
            nullable: false,
            is_internal: false,
        }],
        layout: TableLayout {
            size: 0,
            attr_layouts: vec![],
        },
    };
    let table_oid = catalog.create_table("users", schema).unwrap();

    // 2. Insert Data
    let tuples = vec![
        Tuple::new(vec![AttributeValue::U32(10)]),
        Tuple::new(vec![AttributeValue::U32(25)]),
        Tuple::new(vec![AttributeValue::U32(30)]),
    ];
    let values_exec = Box::new(ValuesExecutor::new(tuples));

    let mut bp_guard = bp.lock().unwrap(); // Lock for insert
    let mut pinned_bp = unsafe { Pin::new_unchecked(&mut *bp_guard) };

    let mut insert_exec = InsertExecutor::new(values_exec, &catalog, table_oid).unwrap(); // No bpm

    insert_exec.init(); // No bpm
    insert_exec.next(pinned_bp.as_mut()); // Execute insert, pass bpm

    // 3. Scan & Filter
    let scan_exec = Box::new(SeqScanExecutor::new(&catalog, table_oid).unwrap()); // No bpm

    // Filter: WHERE age > 20
    let mut filter_exec = FilterExecutor::new(scan_exec, |t: &Tuple| match t.values[0] {
        AttributeValue::U32(age) => age > 20,
        _ => false,
    });

    filter_exec.init(); // No bpm

    let mut output_rows = 0;
    while let Some(t) = filter_exec.next(pinned_bp.as_mut()) {
        // Pass bpm
        if let AttributeValue::U32(age) = t.values[0] {
            assert!(age > 20);
        }
        output_rows += 1;
    }

    assert_eq!(output_rows, 2, "Should filter out 10, keeping 25 and 30");
}

#[test]
fn test_filter_execution() {
    let (bp, mut catalog) = setup_catalog("test_filter_exec.db");

    // 1. Create Table
    let schema = TableType {
        attributes: vec![TableAttribute {
            name: "age".into(),
            kind: AttributeKind::U32,
            nullable: false,
            is_internal: false,
        }],
        layout: TableLayout {
            size: 0,
            attr_layouts: vec![],
        },
    };
    let table_oid = catalog.create_table("users", schema).unwrap();

    // 2. Insert Data: 10, 25, 30
    let tuples = vec![
        Tuple::new(vec![AttributeValue::U32(10)]),
        Tuple::new(vec![AttributeValue::U32(25)]),
        Tuple::new(vec![AttributeValue::U32(30)]),
    ];
    let values_exec = Box::new(ValuesExecutor::new(tuples));

    let mut bp_guard = bp.lock().unwrap();
    let mut pinned_bp = unsafe { Pin::new_unchecked(&mut *bp_guard) };

    let mut insert_exec = InsertExecutor::new(values_exec, &catalog, table_oid).unwrap(); // No bpm

    insert_exec.init(); // No bpm
    insert_exec.next(pinned_bp.as_mut()); // Execute insert, pass bpm

    // 3. Filter: WHERE age > 20
    let scan_exec = Box::new(SeqScanExecutor::new(&catalog, table_oid).unwrap()); // No bpm

    let mut filter_exec = FilterExecutor::new(scan_exec, |t: &Tuple| match t.values[0] {
        AttributeValue::U32(age) => age > 20,
        _ => false,
    });

    filter_exec.init(); // No bpm

    let mut output_rows = 0;
    while let Some(t) = filter_exec.next(pinned_bp.as_mut()) {
        // Pass bpm
        println!("Filtered Row: {:?}", t);
        if let AttributeValue::U32(age) = t.values[0] {
            assert!(age > 20);
        }
        output_rows += 1;
    }

    assert_eq!(output_rows, 2, "Should filter out 10, keeping 25 and 30");
}

#[test]
fn test_projection_execution() {
    let (bp, mut catalog) = setup_catalog("test_projection.db");

    let schema = TableType {
        attributes: vec![
            TableAttribute {
                name: "name".into(),
                kind: AttributeKind::Varchar,
                nullable: false,
                is_internal: false,
            },
            TableAttribute {
                name: "age".into(),
                kind: AttributeKind::U32,
                nullable: false,
                is_internal: false,
            },
        ],
        layout: TableLayout {
            size: 0,
            attr_layouts: vec![],
        },
    };
    let table_oid = catalog.create_table("users", schema).unwrap();

    // 2. Insert Data: ("Alice", 30), ("Bob", 20)
    let tuples = vec![
        Tuple::new(vec![
            AttributeValue::Varchar("Alice".into()),
            AttributeValue::U32(30),
        ]),
        Tuple::new(vec![
            AttributeValue::Varchar("Bob".into()),
            AttributeValue::U32(20),
        ]),
    ];
    let values_exec = Box::new(ValuesExecutor::new(tuples));

    let mut bp_guard = bp.lock().unwrap();
    let mut pinned_bp = unsafe { Pin::new_unchecked(&mut *bp_guard) };

    let mut insert_exec = InsertExecutor::new(values_exec, &catalog, table_oid).unwrap(); // No bpm
    insert_exec.init(); // No bpm
    insert_exec.next(pinned_bp.as_mut()); // Pass bpm

    // 3. Scan & Project: SELECT age FROM users
    // "age" is at index 1
    let scan_exec = Box::new(SeqScanExecutor::new(&catalog, table_oid).unwrap()); // No bpm

    let mut proj_exec = ProjectionExecutor::new(scan_exec, vec![1]); // Keep only column 1 (age)
    proj_exec.init(); // No bpm

    let t1 = proj_exec
        .next(pinned_bp.as_mut())
        .expect("Should have result"); // Pass bpm
    assert_eq!(t1.values.len(), 1);
    assert_eq!(t1.values[0], AttributeValue::U32(30));

    let t2 = proj_exec
        .next(pinned_bp.as_mut())
        .expect("Should have result"); // Pass bpm
    assert_eq!(t2.values.len(), 1);
    assert_eq!(t2.values[0], AttributeValue::U32(20));

    assert!(proj_exec.next(pinned_bp.as_mut()).is_none()); // Pass bpm
}

#[test]
fn test_index_maintenance() {
    let (bp, mut catalog) = setup_catalog("test_idx_maint.db");

    // 1. Create Table
    let schema = TableType {
        attributes: vec![
            TableAttribute {
                name: "id".into(),
                kind: AttributeKind::U32,
                nullable: false,
                is_internal: false,
            },
            TableAttribute {
                name: "val".into(),
                kind: AttributeKind::U32,
                nullable: false,
                is_internal: false,
            },
        ],
        layout: TableLayout {
            size: 0,
            attr_layouts: vec![],
        },
    };
    let table_oid = catalog.create_table("data", schema.clone()).unwrap();

    // 2. Create Index on 'id' (column 0)
    let idx_oid = catalog.create_index("idx_id", "data", "id").unwrap();

    // 3. Insert Data: (100, 1), (200, 2)
    let tuples = vec![
        Tuple::new(vec![AttributeValue::U32(100), AttributeValue::U32(1)]),
        Tuple::new(vec![AttributeValue::U32(200), AttributeValue::U32(2)]),
    ];
    let values_exec = Box::new(ValuesExecutor::new(tuples));

    let mut bp_guard = bp.lock().unwrap();
    let mut pinned_bp = unsafe { Pin::new_unchecked(&mut *bp_guard) };

    let mut insert_exec = InsertExecutor::new(values_exec, &catalog, table_oid).unwrap(); // No bpm

    insert_exec.init(); // No bpm
    insert_exec.next(pinned_bp.as_mut()); // Pass bpm

    // 4. Verify via Index Scan (Lookup 200)
    // Key: 200
    let key_bytes = 200u32.to_be_bytes().to_vec();
    let mut idx_scan = IndexScanExecutor::new(&catalog, idx_oid, key_bytes).unwrap(); // No bpm

    idx_scan.init(); // No bpm
    let tuple = idx_scan
        .next(pinned_bp.as_mut())
        .expect("Index lookup failed for key 200"); // Pass bpm

    assert_eq!(tuple.values[0], AttributeValue::U32(200));
    assert_eq!(tuple.values[1], AttributeValue::U32(2));
}

#[test]
fn test_update_execution() {
    let (bp, mut catalog) = setup_catalog("test_update.db");

    // 1. Create Table & Index
    let schema = TableType {
        attributes: vec![
            TableAttribute {
                name: "id".into(),
                kind: AttributeKind::U32,
                nullable: false,
                is_internal: false,
            },
            TableAttribute {
                name: "val".into(),
                kind: AttributeKind::U32,
                nullable: false,
                is_internal: false,
            },
        ],
        layout: TableLayout {
            size: 0,
            attr_layouts: vec![],
        },
    };
    let table_oid = catalog.create_table("data", schema.clone()).unwrap();
    let idx_oid = catalog.create_index("idx_id", "data", "id").unwrap();

    // 2. Insert (1, 100)
    let tuples = vec![Tuple::new(vec![
        AttributeValue::U32(1),
        AttributeValue::U32(100),
    ])];
    let values_exec = Box::new(ValuesExecutor::new(tuples));

    let mut bp_guard = bp.lock().unwrap();
    let mut pinned_bp = unsafe { Pin::new_unchecked(&mut *bp_guard) };

    let mut insert_exec = InsertExecutor::new(values_exec, &catalog, table_oid).unwrap(); // No bpm
    insert_exec.init(); // No bpm
    insert_exec.next(pinned_bp.as_mut()); // Pass bpm

    // 3. Update: SET val = 200 WHERE id = 1
    // Scan part
    // Index Scan for id=1
    let key_bytes = 1u32.to_be_bytes().to_vec();
    let scan_exec = Box::new(IndexScanExecutor::new(&catalog, idx_oid, key_bytes).unwrap()); // No bpm

    // Update Logic: Change val (col 1) to 200
    let mut update_exec = UpdateExecutor::new(scan_exec, &catalog, table_oid, |old_t| {
        let mut new_vals = old_t.values.clone();
        new_vals[1] = AttributeValue::U32(200); // Update val
        Tuple::new(new_vals)
    }) // No bpm
    .unwrap();

    update_exec.init(); // No bpm
    let res = update_exec
        .next(pinned_bp.as_mut())
        .expect("Update should return count"); // Pass bpm

    if let AttributeValue::U32(count) = res.values[0] {
        assert_eq!(count, 1);
    }

    // 4. Verify: Scan should see (1, 200)
    // Note: We need a new scan executor because the old one is consumed
    let scan_check = SeqScanExecutor::new(&catalog, table_oid).unwrap(); // No bpm
    let mut filter_check = FilterExecutor::new(Box::new(scan_check), |t| {
        if let AttributeValue::U32(id) = t.values[0] {
            id == 1
        } else {
            false
        }
    });

    filter_check.init(); // No bpm
    let updated_tuple = filter_check
        .next(pinned_bp.as_mut())
        .expect("Should find updated row"); // Pass bpm
    assert_eq!(updated_tuple.values[1], AttributeValue::U32(200));
}
