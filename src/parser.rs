use sqlparser::ast::{
    BinaryOperator, Expr, SetExpr, Statement, TableFactor, Value,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

#[derive(Debug, Clone)]
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
    Update {
        table_name: String,
        assignments: Vec<(String, AstValue)>,
        filter: Option<(String, AstValue)>,
    },
    Delete {
        table_name: String,
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
    ShowTables,
    DropTable {
        table_name: String,
    },
    Clear,
    UseDatabase {
        path: String,
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

pub fn parse(sql: &str) -> Result<AstStatement, String> {
    let trimmed = sql.trim();

    if trimmed.eq_ignore_ascii_case(".clear") {
        return Ok(AstStatement::Clear);
    }

    if trimmed.eq_ignore_ascii_case(".tables") || trimmed.eq_ignore_ascii_case("show tables") {
        return Ok(AstStatement::ShowTables);
    }

    let dialect = GenericDialect {};
    let mut ast = Parser::parse_sql(&dialect, sql).map_err(|e| e.to_string())?;

    if ast.len() != 1 {
        return Err("Expected exactly one SQL statement.".to_string());
    }

    match ast.remove(0) {
        Statement::Drop {
            object_type, names, ..
        } => {
            use sqlparser::ast::ObjectType;
            match object_type {
                ObjectType::Table => {
                    let table_name = names
                        .first()
                        .and_then(|obj_name| obj_name.0.get(0))
                        .map(|ident| ident.value.clone())
                        .ok_or("DROP TABLE requires a table name")?;
                    Ok(AstStatement::DropTable { table_name })
                }
                _ => Err("Only DROP TABLE is supported".to_string()),
            }
        }
        Statement::Use { db_name } => {
            let path = db_name.value;
            Ok(AstStatement::UseDatabase { path })
        }

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
        // Keep all existing Statement matches (Query, Update, Delete, CreateTable, CreateIndex)
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

                let filter = parse_optional_filter(select.selection)?;

                Ok(AstStatement::Select {
                    table_name,
                    selection,
                    filter,
                })
            } else {
                Err("Unsupported query type (must be SELECT)".to_string())
            }
        }
        Statement::Update {
            table,
            assignments,
            selection,
            ..
        } => {
            let table_name = match table.relation {
                TableFactor::Table { name, .. } => name.0.get(0).unwrap().value.clone(),
                _ => return Err("Unsupported UPDATE relation".to_string()),
            };

            let assignments = assignments
                .into_iter()
                .map(|assignment| {
                    let col_name = assignment.id.last().unwrap().value.clone();
                    let value = match assignment.value {
                        Expr::Value(v) => convert_sql_value(v),
                        _ => Err("UPDATE SET value must be a literal".to_string()),
                    }?;
                    Ok((col_name, value))
                })
                .collect::<Result<Vec<_>, String>>()?;

            let filter = parse_optional_filter(selection)?;

            Ok(AstStatement::Update {
                table_name,
                assignments,
                filter,
            })
        }
        Statement::Delete {
            from, selection, ..
        } => {
            use sqlparser::ast::FromTable;

            let table_name = match from {
                FromTable::WithFromKeyword(tables) => {
                    if let Some(table_with_joins) = tables.get(0) {
                        match &table_with_joins.relation {
                            TableFactor::Table { name, .. } => name.0.get(0).unwrap().value.clone(),
                            _ => return Err("Unsupported DELETE relation".to_string()),
                        }
                    } else {
                        return Err("DELETE must have a FROM clause".to_string());
                    }
                }
                FromTable::WithoutKeyword(tables) => {
                    if let Some(table_with_joins) = tables.get(0) {
                        match &table_with_joins.relation {
                            TableFactor::Table { name, .. } => name.0.get(0).unwrap().value.clone(),
                            _ => return Err("Unsupported DELETE relation".to_string()),
                        }
                    } else {
                        return Err("DELETE must specify a table".to_string());
                    }
                }
            };

            let filter = parse_optional_filter(selection)?;

            Ok(AstStatement::Delete { table_name, filter })
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

fn parse_optional_filter(expr: Option<Expr>) -> Result<Option<(String, AstValue)>, String> {
    if let Some(expr) = expr {
        if let Expr::BinaryOp { left, op, right } = expr {
            if op == BinaryOperator::Eq {
                let col = left.to_string();
                let val = convert_sql_value(match *right {
                    Expr::Value(v) => v,
                    _ => return Err("Filter value must be a literal".to_string()),
                })?;
                return Ok(Some((col, val)));
            }
        }
        return Err("Unsupported WHERE clause (must be simple equality)".to_string());
    }
    Ok(None)
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
