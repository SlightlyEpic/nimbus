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

fn main() {
    println!("NimbusDB (v0.1.0)");
    println!("Type '.exit' to quit.");

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
        let readline = rl.readline("nimbus> ");
        match readline {
            Ok(line) => {
                if line.trim() == ".exit" {
                    println!("Goodbye!");
                    break;
                }

                if line.trim().is_empty() {
                    continue;
                }

                rl.add_history_entry(line.as_str()).ok();

                let ast = match parser::parse(&line) {
                    Ok(ast) => ast,
                    Err(e) => {
                        println!("Parse Error: {}", e);
                        continue;
                    }
                };

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
                            Ok(_) => println!("OK"),
                            Err(e) => println!("Error: {}", e),
                        }
                    }
                    parser::AstStatement::CreateIndex {
                        index_name,
                        table_name,
                        column_name,
                    } => match catalog.create_index(&index_name, &table_name, &column_name) {
                        Ok(_) => println!("OK"),
                        Err(e) => println!("Error: {}", e),
                    },
                    other => {
                        let planner = Planner::new(&catalog);
                        let mut plan = match planner.plan(other) {
                            Ok(plan) => plan,
                            Err(e) => {
                                println!("Plan Error: {}", e);
                                continue;
                            }
                        };

                        let mut bp_guard = bp.lock().unwrap();
                        let mut pinned_bp = unsafe { Pin::new_unchecked(&mut *bp_guard) };

                        plan.init();
                        let mut row_count = 0;
                        while let Some(tuple) = plan.next(pinned_bp.as_mut()) {
                            println!("{:?}", tuple.values);
                            row_count += 1;
                        }

                        if row_count > 0 {
                            println!("Rows returned: {}", row_count);
                        } else {
                            println!("OK");
                        }
                    }
                }
            }
            Err(ReadlineError::Interrupted) => {
                println!("CTRL-C");
                break;
            }
            Err(ReadlineError::Eof) => {
                println!("CTRL-D");
                break;
            }
            Err(err) => {
                println!("Error: {:?}", err);
                break;
            }
        }
    }

    let mut bp_guard = bp.lock().unwrap();
    let mut pinned_bp = unsafe { Pin::new_unchecked(&mut *bp_guard) };
    pinned_bp.flush_all().expect("Failed to flush all pages.");
    println!("All data flushed to {}.", file_path);
}
