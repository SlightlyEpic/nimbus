use nimbus::catalog::manager::Catalog;
use nimbus::catalog::schema::SYSTEM_TABLES_ID;
use nimbus::execution::executor::Executor;
use nimbus::execution::filter::FilterExecutor;
use nimbus::execution::insert::InsertExecutor;
use nimbus::execution::projection::ProjectionExecutor;
use nimbus::execution::seq_scan::SeqScanExecutor;
use nimbus::execution::values::ValuesExecutor;
use nimbus::rt_type::primitives::{
    AttributeKind, AttributeValue, TableAttribute, TableLayout, TableType,
};
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

    let mut scan = SeqScanExecutor::new(pinned_bp.as_mut(), &catalog, SYSTEM_TABLES_ID)
        .expect("Failed to create scan");
    scan.init();

    let mut count = 0;
    while let Some(tuple) = scan.next() {
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
    let mut insert_exec = InsertExecutor::new(values_exec, &catalog, table_oid).unwrap();

    insert_exec.init();
    insert_exec.next(); // Execute insert

    // 3. Scan & Filter
    let mut bp_guard = bp.lock().unwrap();
    let mut pinned_bp = unsafe { Pin::new_unchecked(&mut *bp_guard) };

    let scan_exec =
        Box::new(SeqScanExecutor::new(pinned_bp.as_mut(), &catalog, table_oid).unwrap());

    // Filter: WHERE age > 20
    let mut filter_exec = FilterExecutor::new(scan_exec, |t: &Tuple| match t.values[0] {
        AttributeValue::U32(age) => age > 20,
        _ => false,
    });

    filter_exec.init();

    let mut output_rows = 0;
    while let Some(t) = filter_exec.next() {
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
    let mut insert_exec = InsertExecutor::new(values_exec, &catalog, table_oid).unwrap();

    insert_exec.init();
    insert_exec.next(); // Execute insert

    // 3. Filter: WHERE age > 20
    let mut bp_guard = bp.lock().unwrap();
    // Use unsafe pin because BufferPool is PhantomPinned
    let mut pinned_bp = unsafe { Pin::new_unchecked(&mut *bp_guard) };

    let scan_exec =
        Box::new(SeqScanExecutor::new(pinned_bp.as_mut(), &catalog, table_oid).unwrap());

    let mut filter_exec = FilterExecutor::new(scan_exec, |t: &Tuple| match t.values[0] {
        AttributeValue::U32(age) => age > 20,
        _ => false,
    });

    filter_exec.init();

    let mut output_rows = 0;
    while let Some(t) = filter_exec.next() {
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
    let mut insert_exec = InsertExecutor::new(values_exec, &catalog, table_oid).unwrap();
    insert_exec.init();
    insert_exec.next();

    // 3. Scan & Project: SELECT age FROM users
    // "age" is at index 1
    let mut bp_guard = bp.lock().unwrap();
    let mut pinned_bp = unsafe { Pin::new_unchecked(&mut *bp_guard) };

    let scan_exec =
        Box::new(SeqScanExecutor::new(pinned_bp.as_mut(), &catalog, table_oid).unwrap());

    let mut proj_exec = ProjectionExecutor::new(scan_exec, vec![1]); // Keep only column 1 (age)
    proj_exec.init();

    let t1 = proj_exec.next().expect("Should have result");
    assert_eq!(t1.values.len(), 1);
    assert_eq!(t1.values[0], AttributeValue::U32(30));

    let t2 = proj_exec.next().expect("Should have result");
    assert_eq!(t2.values.len(), 1);
    assert_eq!(t2.values[0], AttributeValue::U32(20));

    assert!(proj_exec.next().is_none());
}
