use nimbus::catalog::manager::Catalog;
use nimbus::execution::executor::Executor;
use nimbus::parser;
use nimbus::planner::Planner;
use nimbus::rt_type::primitives::{AttributeKind, TableAttribute, TableLayout, TableType};
use nimbus::storage::buffer::BufferPool;
use nimbus::storage::buffer::fifo_evictor::FifoEvictor;
use nimbus::storage::disk::FileManager;
use nimbus::storage::page_locator::locator::DirectoryPageLocator;
use rustyline::DefaultEditor;
use rustyline::error::ReadlineError;
use std::fs;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use nimbus::cli;

fn main() {
    // Clear screen at startup
    print!("\x1B[2J\x1B[1;1H");

    // Print beautiful centered header with colors
    print_centered_header();

    let db_name = "nimbus.db";
    let _ = fs::create_dir_all("test_db");
    let file_path = format!("test_db/{}", db_name);

    let fm = FileManager::new(file_path.clone()).unwrap();
    let bp = Arc::new(Mutex::new(BufferPool::new(
        fm,
        Box::new(FifoEvictor::new()),
        Box::new(DirectoryPageLocator::new()),
    )));

    let mut catalog = Catalog::new(bp.clone());
    let mut rl = DefaultEditor::new().unwrap();

    loop {
        // --- 1. Read and Parse Input ---
        let ast = match rl.readline("nimbus> ") {
            Ok(line) => {
                let trimmed = line.trim();

                // Handle special commands
                if trimmed == ".exit" {
                    println!("\n\x1B[1;32mGoodbye!\x1B[0m\n");
                    break;
                }

                if trimmed == ".help" {
                    print_help();
                    continue;
                }

                if trimmed.starts_with(".describe ") || trimmed.starts_with(".desc ") {
                    let table_name = if trimmed.starts_with(".describe ") {
                        trimmed.strip_prefix(".describe ").unwrap()
                    } else {
                        trimmed.strip_prefix(".desc ").unwrap()
                    };
                    describe_table(&catalog, table_name);
                    continue;
                }

                if trimmed.is_empty() {
                    continue;
                }

                rl.add_history_entry(line.as_str()).ok();

                match parser::parse(&line) {
                    Ok(ast) => ast,
                    Err(e) => {
                        println!("\x1B[1;31m✗ Parse Error:\x1B[0m {}", e);
                        continue;
                    }
                }
            }
            Err(ReadlineError::Interrupted) => {
                println!("\n\x1B[1;32mCTRL-C detected. Goodbye!\x1B[0m\n");
                break;
            }
            Err(ReadlineError::Eof) => {
                println!("\n\x1B[1;32mCTRL-D detected. Goodbye!\x1B[0m\n");
                break;
            }
            Err(err) => {
                println!("\x1B[1;31m✗ Error:\x1B[0m {:?}", err);
                break;
            }
        };

        // --- 2. Handle DDL and DML operations ---
        match ast {
            parser::AstStatement::CreateTable {
                table_name,
                columns,
            } => {
                let mut attributes = Vec::new();
                for (name, data_type) in columns {
                    let kind = match data_type {
                        parser::AstDataType::U32 => AttributeKind::U32,
                        parser::AstDataType::Varchar => AttributeKind::Varchar,
                    };
                    attributes.push(TableAttribute {
                        name,
                        kind,
                        nullable: false,
                        is_internal: false,
                    });
                }

                let schema = TableType {
                    attributes,
                    layout: TableLayout {
                        size: 0,
                        attr_layouts: vec![],
                    },
                };

                match catalog.create_table(&table_name, schema) {
                    Ok(_) => println!("\x1B[1;32m✓ Table '{}' created\x1B[0m", table_name),
                    Err(e) => println!("\x1B[1;31m✗ Error:\x1B[0m {}", e),
                }
            }
            parser::AstStatement::CreateIndex {
                index_name,
                table_name,
                column_name,
            } => match catalog.create_index(&index_name, &table_name, &column_name) {
                Ok(_) => println!(
                    "\x1B[1;32m✓ Index '{}' created on {}.{}\x1B[0m",
                    index_name, table_name, column_name
                ),
                Err(e) => println!("\x1B[1;31m✗ Error:\x1B[0m {}", e),
            },
            other => {
                // DML Execution - handle in separate scope to avoid borrow conflicts
                execute_dml_query(&catalog, &bp, other);
            }
        }
    }

    // Graceful shutdown
    println!("\x1B[1;34mFlushing data to disk...\x1B[0m");
    let mut bp_guard = bp.lock().unwrap();
    let mut pinned_bp = unsafe { Pin::new_unchecked(&mut *bp_guard) };
    pinned_bp.flush_all().expect("Failed to flush all pages.");
    println!("\x1B[1;32m✓ All data flushed to {}.\x1B[0m", file_path);
}

/// Execute DML queries (SELECT, INSERT, UPDATE, DELETE)
/// This is in a separate function to ensure catalog borrow is scoped properly
fn execute_dml_query<'a>(
    catalog: &'a Catalog,
    bp: &Arc<Mutex<BufferPool>>,
    ast: parser::AstStatement,
) {
    let planner = Planner::new(catalog);

    let plan = match planner.plan(ast.clone()) {
        Ok(plan) => plan,
        Err(e) => {
            println!("\x1B[1;31m✗ Plan Error:\x1B[0m {}", e);
            return;
        }
    };

    let mut bp_guard = bp.lock().unwrap();
    let mut pinned_bp = unsafe { Pin::new_unchecked(&mut *bp_guard) };

    cli::display_query_result(plan, &ast, catalog, pinned_bp.as_mut());
}

/// Print centered header
fn print_centered_header() {
    // Simple centering - assume 80 column terminal as default
    let padding = "                   "; // 19 spaces for ~80 char centering

    println!(); // Empty line at top
    println!(
        "{}\x1B[1;36m╔═══════════════════════════════════════╗\x1B[0m",
        padding
    );
    println!(
        "{}\x1B[1;36m║\x1B[0m                                       \x1B[1;36m║\x1B[0m",
        padding
    );
    println!(
        "{}\x1B[1;36m║\x1B[0m          \x1B[1;35mNimbusDB v0.1.0\x1B[0m              \x1B[1;36m║\x1B[0m",
        padding
    );
    println!(
        "{}\x1B[1;36m║\x1B[0m                                       \x1B[1;36m║\x1B[0m",
        padding
    );
    println!(
        "{}\x1B[1;36m║\x1B[0m          \x1B[1;33mAn OLTP Database\x1B[0m             \x1B[1;36m║\x1B[0m",
        padding
    );
    println!(
        "{}\x1B[1;36m║\x1B[0m                                       \x1B[1;36m║\x1B[0m",
        padding
    );
    println!(
        "{}\x1B[1;36m╚═══════════════════════════════════════╝\x1B[0m",
        padding
    );

    // Print help hint centered
    let hint_padding = "          "; // Centered hint
    println!(
        "{}\x1B[1;32mType '.help' for commands or '.exit' to quit\x1B[0m\n",
        hint_padding
    );
}

/// Print help information
fn print_help() {
    println!("\n\x1B[1;35m═══════════════════════════════════════════════════════════════\x1B[0m");
    println!(
        "\x1B[1;35m                        NimbusDB Commands                          \x1B[0m"
    );
    println!("\x1B[1;35m═══════════════════════════════════════════════════════════════\x1B[0m\n");

    println!("\x1B[1;36mSpecial Commands:\x1B[0m");
    println!("  \x1B[1;33m.help\x1B[0m                    Show this help message");
    println!("  \x1B[1;33m.exit\x1B[0m                    Exit NimbusDB");
    println!("  \x1B[1;33m.describe <table>\x1B[0m        Show table structure");
    println!("  \x1B[1;33m.desc <table>\x1B[0m            Short form of .describe\n");

    println!("\x1B[1;36mSQL Statements:\x1B[0m");
    println!("  \x1B[1;33mCREATE TABLE\x1B[0m             Create a new table");
    println!("    \x1B[2mExample: CREATE TABLE users (id INT, name VARCHAR);\x1B[0m");
    println!();
    println!("  \x1B[1;33mCREATE INDEX\x1B[0m             Create an index on a column");
    println!("    \x1B[2mExample: CREATE INDEX idx_id ON users(id);\x1B[0m");
    println!();
    println!("  \x1B[1;33mINSERT\x1B[0m                   Insert rows into a table");
    println!("    \x1B[2mExample: INSERT INTO users (id, name) VALUES (1, 'Alice');\x1B[0m");
    println!();
    println!("  \x1B[1;33mSELECT\x1B[0m                   Query data from a table");
    println!("    \x1B[2mExample: SELECT * FROM users;\x1B[0m");
    println!("    \x1B[2mExample: SELECT name FROM users WHERE id = 1;\x1B[0m");
    println!();
    println!("  \x1B[1;33mUPDATE\x1B[0m                   Update rows in a table");
    println!("    \x1B[2mExample: UPDATE users SET name = 'Bob' WHERE id = 1;\x1B[0m");
    println!();
    println!("  \x1B[1;33mDELETE\x1B[0m                   Delete rows from a table");
    println!("    \x1B[2mExample: DELETE FROM users WHERE id = 1;\x1B[0m\n");

    println!("\x1B[1;35m═══════════════════════════════════════════════════════════════\x1B[0m\n");
}

/// Describe table structure
fn describe_table(catalog: &Catalog, table_name: &str) {
    match catalog.get_table_oid(table_name) {
        Some(oid) => match catalog.get_table_schema(oid) {
            Some(schema) => {
                println!("\n\x1B[1;36mTable:\x1B[0m \x1B[1;33m{}\x1B[0m", table_name);
                println!("\x1B[1;36mOID:\x1B[0m {}", oid);
                println!("\x1B[1;36mColumns:\x1B[0m\n");

                use tabled::{builder::Builder, settings::Style};
                let mut table_builder = Builder::default();
                table_builder.push_record(vec!["Column Name", "Type", "Nullable"]);

                for attr in &schema.attributes {
                    let type_name = match attr.kind {
                        AttributeKind::U8 => "U8",
                        AttributeKind::U16 => "U16",
                        AttributeKind::U32 => "U32",
                        AttributeKind::U64 => "U64",
                        AttributeKind::I8 => "I8",
                        AttributeKind::I16 => "I16",
                        AttributeKind::I32 => "I32",
                        AttributeKind::I64 => "I64",
                        AttributeKind::F32 => "F32",
                        AttributeKind::F64 => "F64",
                        AttributeKind::U128 => "U128",
                        AttributeKind::I128 => "I128",
                        AttributeKind::Bool => "BOOL",
                        AttributeKind::Char(_) => "CHAR",
                        AttributeKind::Varchar => "VARCHAR",
                    };

                    let nullable = if attr.nullable { "YES" } else { "NO" };
                    table_builder.push_record(vec![&attr.name, type_name, nullable]);
                }

                let mut table = table_builder.build();
                table.with(Style::rounded());
                println!("{}\n", table);
            }
            None => println!(
                "\x1B[1;31m✗ Error:\x1B[0m Schema not found for table '{}'\n",
                table_name
            ),
        },
        None => println!(
            "\x1B[1;31m✗ Error:\x1B[0m Table '{}' not found\n",
            table_name
        ),
    }
}
