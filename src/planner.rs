use crate::catalog::manager::Catalog;
use crate::execution::delete::DeleteExecutor;
use crate::execution::executor::Executor;
use crate::execution::filter::FilterExecutor;
use crate::execution::index_scan::IndexScanExecutor;
use crate::execution::insert::InsertExecutor;
use crate::execution::projection::ProjectionExecutor;
use crate::execution::seq_scan::SeqScanExecutor;
use crate::execution::update::UpdateExecutor;
use crate::execution::values::ValuesExecutor;
use crate::parser::{AstDataType, AstStatement, AstValue};
use crate::rt_type::primitives::{
    AttributeKind, AttributeValue, TableAttribute, TableLayout, TableType,
};
use crate::storage::heap::tuple::{self, Tuple};
use std::collections::HashMap;

pub struct Planner<'a> {
    catalog: &'a Catalog,
}

impl<'a> Planner<'a> {
    pub fn new(catalog: &'a Catalog) -> Self {
        Self { catalog }
    }

    pub fn plan(&self, ast: AstStatement) -> Result<Box<dyn Executor + 'a>, String> {
        match ast {
            AstStatement::Insert {
                table_name,
                columns,
                values,
            } => self.plan_insert(table_name, columns, values),
            AstStatement::Select {
                table_name,
                selection,
                filter,
            } => self.plan_select(table_name, selection, filter),
            AstStatement::Delete { table_name, filter } => self.plan_delete(table_name, filter),
            AstStatement::Update {
                table_name,
                assignments,
                filter,
            } => self.plan_update(table_name, assignments, filter),
            AstStatement::CreateTable { .. } => {
                Err("CREATE TABLE not supported in query plan".to_string())
            }
            AstStatement::CreateIndex { .. } => {
                Err("CREATE INDEX not supported in query plan".to_string())
            }
            AstStatement::ShowTables => Err("SHOW TABLES not supported in query plan".to_string()),
            AstStatement::DropTable { .. } => {
                Err("DROP TABLE not supported in query plan".to_string())
            }
            AstStatement::Clear => Err("CLEAR not supported in query plan".to_string()),
            AstStatement::UseDatabase { .. } => {
                Err("USE DATABASE not supported in query plan".to_string())
            }
        }
    }

    fn plan_insert(
        &self,
        table_name: String,
        columns: Vec<String>,
        rows: Vec<Vec<AstValue>>,
    ) -> Result<Box<dyn Executor + 'a>, String> {
        let table_oid = self
            .catalog
            .get_table_oid(&table_name)
            .ok_or(format!("Table not found: {}", table_name))?;

        let schema = self
            .catalog
            .get_table_schema(table_oid)
            .ok_or(format!("Schema not found for OID: {}", table_oid))?;

        let schema_col_map: HashMap<_, _> = schema
            .attributes
            .iter()
            .enumerate()
            .map(|(i, attr)| (attr.name.as_str(), i))
            .collect();

        let col_indices: Vec<usize> = columns
            .iter()
            .map(|col_name| {
                schema_col_map
                    .get(col_name.as_str())
                    .copied()
                    .ok_or_else(|| format!("Column {} not found in table {}", col_name, table_name))
            })
            .collect::<Result<_, String>>()?;

        let tuples = rows
            .into_iter()
            .map(|row| {
                if row.len() != col_indices.len() {
                    return Err("Column count mismatch".to_string());
                }
                let mut values = schema
                    .attributes
                    .iter()
                    .map(|_| AttributeValue::U32(0))
                    .collect::<Vec<_>>();
                for (i, val) in row.into_iter().enumerate() {
                    let schema_idx = col_indices[i];
                    values[schema_idx] = convert_ast_value(val)?;
                }
                Ok::<Tuple, String>(Tuple::new(values))
            })
            .collect::<Result<Vec<_>, String>>()?;

        let values_exec = Box::new(ValuesExecutor::new(tuples));
        let insert_exec = Box::new(InsertExecutor::new(values_exec, self.catalog, table_oid)?);
        Ok(insert_exec)
    }

    fn plan_select(
        &self,
        table_name: String,
        selection: Vec<String>,
        filter: Option<(String, AstValue)>,
    ) -> Result<Box<dyn Executor + 'a>, String> {
        let table_oid = self
            .catalog
            .get_table_oid(&table_name)
            .ok_or(format!("Table not found: {}", table_name))?;

        let schema = self
            .catalog
            .get_table_schema(table_oid)
            .ok_or(format!("Schema not found for OID: {}", table_oid))?;

        let scan_exec: Box<dyn Executor + 'a> =
            self.build_scan_with_filter(table_oid, &table_name, &schema, filter)?;

        if selection.len() == 1 && selection[0] == "*" {
            return Ok(scan_exec);
        }

        let schema_col_map: HashMap<_, _> = schema
            .attributes
            .iter()
            .enumerate()
            .map(|(i, attr)| (attr.name.as_str(), i))
            .collect();

        let col_indices: Vec<usize> = selection
            .iter()
            .map(|col_name| {
                schema_col_map
                    .get(col_name.as_str())
                    .copied()
                    .ok_or_else(|| format!("Column {} not found in table {}", col_name, table_name))
            })
            .collect::<Result<_, String>>()?;

        let proj_exec = Box::new(ProjectionExecutor::new(scan_exec, col_indices));
        Ok(proj_exec)
    }

    fn plan_delete(
        &self,
        table_name: String,
        filter: Option<(String, AstValue)>,
    ) -> Result<Box<dyn Executor + 'a>, String> {
        let table_oid = self
            .catalog
            .get_table_oid(&table_name)
            .ok_or(format!("Table not found: {}", table_name))?;

        let schema = self
            .catalog
            .get_table_schema(table_oid)
            .ok_or(format!("Schema not found for OID: {}", table_oid))?;

        let child_exec: Box<dyn Executor + 'a> =
            self.build_scan_with_filter(table_oid, &table_name, &schema, filter)?;

        // FIX: Wrap in Ok() and use ? on the inner Result
        Ok(Box::new(DeleteExecutor::new(
            child_exec,
            self.catalog,
            table_oid,
        )))
    }

    fn plan_update(
        &self,
        table_name: String,
        assignments: Vec<(String, AstValue)>,
        filter: Option<(String, AstValue)>,
    ) -> Result<Box<dyn Executor + 'a>, String> {
        let table_oid = self
            .catalog
            .get_table_oid(&table_name)
            .ok_or(format!("Table not found: {}", table_name))?;

        let schema = self
            .catalog
            .get_table_schema(table_oid)
            .ok_or(format!("Schema not found for OID: {}", table_oid))?;

        let child_exec: Box<dyn Executor + 'a> =
            self.build_scan_with_filter(table_oid, &table_name, &schema, filter)?;

        // Map column names to schema index and runtime value
        let update_map: Vec<(usize, AttributeValue)> = assignments
            .into_iter()
            .map(|(col_name, ast_val)| {
                let (col_idx, _) = schema
                    .attributes
                    .iter()
                    .enumerate()
                    .find(|(_, a)| a.name == col_name)
                    .ok_or(format!("Column {} not found for update", col_name))?;
                let runtime_val = convert_ast_value(ast_val)?;
                Ok((col_idx, runtime_val))
            })
            .collect::<Result<_, String>>()?;

        // Create the closure for UpdateExecutor
        // Move the update_map into the closure
        let update_fn = move |old_tuple: &Tuple| {
            let mut new_tuple = old_tuple.clone();
            for (idx, new_val) in update_map.iter() {
                // Ensure the new value type matches the schema (basic check)
                if new_tuple.values[*idx].is_same_kind(new_val) {
                    new_tuple.values[*idx] = new_val.clone();
                } else {
                    // In a simple system, panicking on unexpected type mismatch is acceptable
                    // as the user's AST didn't contain type info.
                    panic!("Update value type mismatch in column index {}", idx);
                }
            }
            new_tuple
        };

        // FIX: Wrap in Ok() and use ? on the inner Result
        Ok(Box::new(UpdateExecutor::new(
            child_exec,
            self.catalog,
            table_oid,
            update_fn,
        )?))
    }

    // Helper function to consolidate filter/index logic used by SELECT, DELETE, UPDATE
    fn build_scan_with_filter(
        &self,
        table_oid: u32,
        table_name: &str,
        schema: &TableType,
        filter: Option<(String, AstValue)>,
    ) -> Result<Box<dyn Executor + 'a>, String> {
        if let Some((filter_col_name, filter_ast_val)) = filter {
            let index_oid = self
                .catalog
                .find_index_for_column(table_name, &filter_col_name);

            if let Some(oid) = index_oid {
                let key_bytes = convert_value_to_key(filter_ast_val)?;
                // FIX: Wrap in Ok() and use ? on the inner Result
                Ok(Box::new(IndexScanExecutor::new(
                    self.catalog,
                    oid,
                    key_bytes,
                )?))
            } else {
                let scan = Box::new(SeqScanExecutor::new(self.catalog, table_oid)?);
                let (col_idx, _) = schema
                    .attributes
                    .iter()
                    .enumerate()
                    .find(|(_, a)| a.name == filter_col_name)
                    .ok_or(format!(
                        "Column {} not found in WHERE clause",
                        filter_col_name
                    ))?;

                let filter_val_runtime = convert_ast_value(filter_ast_val)?;

                // FIX: Wrap in Ok()
                Ok(Box::new(FilterExecutor::new(scan, move |t: &Tuple| {
                    t.values[col_idx] == filter_val_runtime
                })))
            }
        } else {
            // FIX: Wrap in Ok() and use ? on the inner Result
            Ok(Box::new(SeqScanExecutor::new(self.catalog, table_oid)?))
        }
    }
}

fn convert_ast_value(val: AstValue) -> Result<AttributeValue, String> {
    match val {
        AstValue::U32(v) => Ok(AttributeValue::U32(v)),
        AstValue::Varchar(s) => Ok(AttributeValue::Varchar(s)),
    }
}

fn convert_value_to_key(val: AstValue) -> Result<Vec<u8>, String> {
    match val {
        AstValue::U32(v) => Ok(v.to_be_bytes().to_vec()),
        _ => Err("Index key must be an integer".to_string()),
    }
}

// Added helper method for Tuple to check type kind equality (used in update_fn closure)
impl AttributeValue {
    fn is_same_kind(&self, other: &AttributeValue) -> bool {
        // This is a simplified check for a basic database
        std::mem::discriminant(self) == std::mem::discriminant(other)
    }
}
