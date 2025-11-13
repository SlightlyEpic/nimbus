use crate::catalog::manager::Catalog;
use crate::execution::delete::DeleteExecutor;
use crate::execution::executor::Executor;
use crate::execution::filter::FilterExecutor;
use crate::execution::index_scan::IndexScanExecutor;
use crate::execution::insert::InsertExecutor;
use crate::execution::projection::ProjectionExecutor;
use crate::execution::seq_scan::SeqScanExecutor;
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
            AstStatement::CreateTable {
                table_name,
                columns,
            } => Err("CREATE TABLE not supported in query plan".to_string()),
            AstStatement::CreateIndex {
                index_name,
                table_name,
                column_name,
            } => Err("CREATE INDEX not supported in query plan".to_string()),
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

        let scan_exec: Box<dyn Executor + 'a> = if let Some((filter_col_name, filter_val)) = filter
        {
            let index_oid = self
                .catalog
                .find_index_for_column(&table_name, &filter_col_name);

            if let Some(oid) = index_oid {
                let key_bytes = convert_value_to_key(filter_val.clone())?;
                Box::new(IndexScanExecutor::new(self.catalog, oid, key_bytes)?)
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

                let filter_val_runtime = convert_ast_value(filter_val)?;

                Box::new(FilterExecutor::new(scan, move |t: &Tuple| {
                    t.values[col_idx] == filter_val_runtime
                }))
            }
        } else {
            Box::new(SeqScanExecutor::new(self.catalog, table_oid)?)
        };

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
