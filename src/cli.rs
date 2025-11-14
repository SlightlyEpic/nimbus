use crate::catalog::manager::Catalog;
use crate::execution::executor::Executor;
use crate::parser::AstStatement;
use crate::rt_type::primitives::AttributeValue;
use crate::storage::buffer::BufferPool;
use std::pin::Pin;

// Dependencies for table printing
use tabled::{builder::Builder, settings::Style};

// Helper function to convert AttributeValue to String for printing
fn attribute_value_to_string(val: &AttributeValue) -> String {
    match val {
        AttributeValue::U8(v) => v.to_string(),
        AttributeValue::U16(v) => v.to_string(),
        AttributeValue::U32(v) => v.to_string(),
        AttributeValue::U64(v) => v.to_string(),
        AttributeValue::I8(v) => v.to_string(),
        AttributeValue::I16(v) => v.to_string(),
        AttributeValue::I32(v) => v.to_string(),
        AttributeValue::I64(v) => v.to_string(),
        AttributeValue::F32(v) => format!("{:.2}", v),
        AttributeValue::F64(v) => format!("{:.2}", v),
        AttributeValue::Bool(v) => if *v { "true" } else { "false" }.to_string(),
        AttributeValue::Char(v) => v.clone(),
        AttributeValue::Varchar(v) => v.clone(),
        _ => "NULL".to_string(),
    }
}

// Helper function to extract count from DML result tuple
fn tuple_to_count(val: &AttributeValue) -> Option<u32> {
    match val {
        AttributeValue::U32(v) => Some(*v),
        _ => None,
    }
}

/// Executes a query plan and displays the results in a formatted table (for SELECT)
/// or prints the rows affected (for INSERT/DELETE/UPDATE).
pub fn display_query_result<'a>(
    mut plan: Box<dyn Executor + 'a>,
    ast: &AstStatement,
    catalog: &Catalog,
    mut pinned_bp: Pin<&mut BufferPool>,
) {
    plan.init();
    let mut row_count = 0;
    let mut table_builder = Builder::default();
    let mut is_select = false;
    let mut header_set = false;

    while let Some(tuple) = plan.next(pinned_bp.as_mut()) {
        if row_count == 0 {
            // Check if this is a SELECT or a DML operation (which returns count)
            if let AstStatement::Select {
                table_name,
                selection,
                ..
            } = ast.clone()
            {
                is_select = true;

                // Get header information from Catalog/Planner
                if let Some(oid) = catalog.get_table_oid(&table_name) {
                    if let Some(schema) = catalog.get_table_schema(oid) {
                        let header: Vec<String> = if selection.len() == 1 && selection[0] == "*" {
                            // Full scan: use all column names
                            schema
                                .attributes
                                .iter()
                                .map(|attr| attr.name.clone())
                                .collect()
                        } else {
                            selection.clone()
                        };

                        table_builder.push_record(header);
                        header_set = true;
                    }
                }
            } else {
                // DML operation (Insert, Delete, Update)
                if let Some(count_val) = tuple.values.get(0) {
                    if let Some(count) = tuple_to_count(count_val) {
                        print_dml_result(ast, count);
                        return; // Done processing DML result
                    }
                }
            }
        }

        // Process results for SELECT
        if is_select {
            if header_set {
                let row: Vec<String> = tuple.values.iter().map(attribute_value_to_string).collect();
                table_builder.push_record(row);
            }
        }
        row_count += 1;
    }

    if is_select {
        if row_count == 0 {
            println!("\n\x1B[1;33mNo rows returned\x1B[0m\n");
        } else {
            let mut table = table_builder.build();
            table.with(Style::rounded());
            println!("\n{}", table.to_string());
            println!(
                "\x1B[1;36m{} row{} returned\x1B[0m\n",
                row_count,
                if row_count == 1 { "" } else { "s" }
            );
        }
    } else if row_count == 0 {
        print_dml_result(ast, 0);
    }
}

/// Pretty print DML operation results
fn print_dml_result(ast: &AstStatement, count: u32) {
    let operation = match ast {
        AstStatement::Insert { .. } => "inserted",
        AstStatement::Delete { .. } => "deleted",
        AstStatement::Update { .. } => "updated",
        _ => "affected",
    };

    println!(
        "\nOK. {} row{} {}\n",
        count,
        if count == 1 { "" } else { "s" },
        operation
    );
}
