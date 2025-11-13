use sqlparser::ast::{
    BinaryOperator, Expr, Ident, ObjectName, Query, SetExpr, Statement, TableFactor, Value,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

#[derive(Debug)]
pub enum AstStatement {
    Insert {
        table_name: String,
        columns: Vec<String>,
        values: Vec<Vec<AstValue>>,
    },
    Select {
        table_name: String,
        selection: Vec<String>,
        filter: Option<(String, AstValue)>,
    },
    CreateTable {
        table_name: String,
        columns: Vec<(String, AstDataType)>,
    },
    CreateIndex {
        index_name: String,
        table_name: String,
        column_name: String,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum AstValue {
    U32(u32),
    Varchar(String),
}

#[derive(Debug, Clone)]
pub enum AstDataType {
    U32,
    Varchar,
}

/// Parses a SQL string into our simplified AST.
pub fn parse(sql: &str) -> Result<AstStatement, String> {
    let dialect = GenericDialect {};
    let mut ast = Parser::parse_sql(&dialect, sql).map_err(|e| e.to_string())?;

    if ast.len() != 1 {
        return Err("Expected exactly one SQL statement.".to_string());
    }

    match ast.remove(0) {
        Statement::Insert {
            table_name,
            columns,
            source,
            ..
        } => {
            let table = table_name.0.get(0).unwrap().value.clone();
            let cols = columns
                .into_iter()
                .map(|ident| ident.value)
                .collect::<Vec<_>>();

            let query = source.ok_or("INSERT must have a VALUES clause".to_string())?;
            if let SetExpr::Values(values) = *query.body {
                let rows = values
                    .rows
                    .into_iter()
                    .map(|row| {
                        row.into_iter()
                            .map(|expr| match expr {
                                Expr::Value(v) => convert_sql_value(v),
                                _ => Err("INSERT VALUES must be literals".to_string()),
                            })
                            .collect::<Result<Vec<_>, String>>()
                    })
                    .collect::<Result<Vec<_>, String>>()?;

                Ok(AstStatement::Insert {
                    table_name: table,
                    columns: cols,
                    values: rows,
                })
            } else {
                Err("Unsupported INSERT source (must be VALUES)".to_string())
            }
        }
        Statement::Query(query) => {
            if let SetExpr::Select(select) = *query.body {
                let table_name = if let Some(table) = select.from.get(0) {
                    match &table.relation {
                        TableFactor::Table { name, .. } => name.0.get(0).unwrap().value.clone(),
                        _ => return Err("Unsupported SELECT relation".to_string()),
                    }
                } else {
                    return Err("SELECT must have a FROM clause".to_string());
                };

                let selection = select
                    .projection
                    .into_iter()
                    .map(|item| item.to_string())
                    .collect();

                let filter = if let Some(expr) = select.selection {
                    if let Expr::BinaryOp { left, op, right } = expr {
                        if op == BinaryOperator::Eq {
                            let col = left.to_string();
                            let val = convert_sql_value(match *right {
                                Expr::Value(v) => v,
                                _ => return Err("Filter value must be a literal".to_string()),
                            })?;
                            Some((col, val))
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                } else {
                    None
                };

                Ok(AstStatement::Select {
                    table_name,
                    selection,
                    filter,
                })
            } else {
                Err("Unsupported query type (must be SELECT)".to_string())
            }
        }
        Statement::CreateTable { name, columns, .. } => {
            let table_name = name.0.get(0).unwrap().value.clone();
            let cols = columns
                .into_iter()
                .map(|col_def| {
                    let name = col_def.name.value.clone();
                    let typ = convert_sql_type(col_def.data_type)?;
                    Ok::<(String, AstDataType), String>((name, typ))
                })
                .collect::<Result<Vec<_>, _>>()?;

            Ok(AstStatement::CreateTable {
                table_name,
                columns: cols,
            })
        }
        Statement::CreateIndex {
            name,
            table_name,
            columns,
            ..
        } => {
            let index_name = name
                .ok_or("Index name is required".to_string())?
                .0
                .get(0)
                .unwrap()
                .value
                .clone();
            let table = table_name.0.get(0).unwrap().value.clone();
            let col_name = columns.get(0).unwrap().expr.to_string();
            Ok(AstStatement::CreateIndex {
                index_name,
                table_name: table,
                column_name: col_name,
            })
        }
        _ => Err("Unsupported SQL statement type.".to_string()),
    }
}

fn convert_sql_value(sql_val: Value) -> Result<AstValue, String> {
    match sql_val {
        Value::Number(s, _) => Ok(AstValue::U32(
            s.parse().map_err(|_| "Failed to parse number")?,
        )),
        Value::SingleQuotedString(s) => Ok(AstValue::Varchar(s)),
        _ => Err("Unsupported value type.".to_string()),
    }
}

fn convert_sql_type(sql_type: sqlparser::ast::DataType) -> Result<AstDataType, String> {
    match sql_type {
        sqlparser::ast::DataType::Int(_) | sqlparser::ast::DataType::Integer(_) => {
            Ok(AstDataType::U32)
        }
        sqlparser::ast::DataType::Varchar(_) | sqlparser::ast::DataType::Text => {
            Ok(AstDataType::Varchar)
        }
        _ => Err("Unsupported column data type.".to_string()),
    }
}
