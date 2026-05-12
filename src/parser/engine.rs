use crate::storage::btree::{BPlusTree, Node, NodeType};
use anyhow::{Result, anyhow};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use sqlparser::ast::{Statement, Query, SetExpr, TableFactor, Expr, Value};
use serde::{Serialize, Deserialize};
use std::collections::HashMap;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TableSchema {
    pub name: String,
    pub columns: Vec<String>,
}

pub struct Engine {
    pub btree: BPlusTree,
    pub schemas: HashMap<String, TableSchema>,
}

impl Engine {
    pub fn new(db_path: &str, wal_path: &str) -> Result<Self> {
        let btree = BPlusTree::open(db_path, wal_path)?;
        let mut engine = Engine {
            btree,
            schemas: HashMap::new(),
        };
        engine.load_schemas()?;
        Ok(engine)
    }

    fn load_schemas(&mut self) -> Result<()> {
        if let Some(data) = self.btree.get(b"__schemas__")? {
            self.schemas = bincode::deserialize(&data)?;
        }
        Ok(())
    }

    fn save_schemas(&mut self) -> Result<()> {
        let data = bincode::serialize(&self.schemas)?;
        self.btree.insert(b"__schemas__".to_vec(), data)?;
        Ok(())
    }

    pub fn execute(&mut self, sql: &str) -> Result<String> {
        let dialect = GenericDialect {};
        let ast = Parser::parse_sql(&dialect, sql)?;

        let mut results = Vec::new();
        for stmt in ast {
            results.push(self.execute_statement(stmt)?);
        }
        Ok(results.join("\n"))
    }

    pub fn execute_statement(&mut self, stmt: Statement) -> Result<String> {
        match stmt {
            Statement::CreateTable { name, columns, .. } => {
                let table_name = name.to_string();
                let col_names = columns.iter().map(|c| c.name.to_string()).collect();
                let schema = TableSchema {
                    name: table_name.clone(),
                    columns: col_names,
                };
                self.schemas.insert(table_name, schema);
                self.save_schemas()?;
                Ok("Table created".to_string())
            }
            Statement::Insert { table_name, columns, source, .. } => {
                let source = source.ok_or_else(|| anyhow!("Insert source missing"))?;
                self.handle_insert(table_name.to_string(), columns, source)
            }
            Statement::Query(query) => {
                self.handle_select(*query)
            }
            Statement::Update { table, assignments, selection, .. } => {
                self.handle_update(table.to_string(), assignments, selection)
            }
            Statement::Delete { from, selection, .. } => {
                let table_name = match &from[0].relation {
                    TableFactor::Table { name, .. } => name.to_string(),
                    _ => return Err(anyhow!("Unsupported delete from")),
                };
                self.handle_delete(table_name, selection)
            }
            _ => Err(anyhow!("Unsupported statement type")),
        }
    }

    fn handle_insert(&mut self, table_name: String, _cols: Vec<sqlparser::ast::Ident>, source: Box<Query>) -> Result<String> {
        let schema = self.schemas.get(&table_name).ok_or_else(|| anyhow!("Table not found"))?.clone();

        if let SetExpr::Values(values) = &*source.body {
            let mut count = 0;
            for row in &values.rows {
                let mut row_data = HashMap::new();
                for (i, expr) in row.iter().enumerate() {
                    if i < schema.columns.len() {
                        let val = match expr {
                            Expr::Value(Value::Number(n, _)) => n.clone(),
                            Expr::Value(Value::SingleQuotedString(s)) => s.clone(),
                            _ => "NULL".to_string(),
                        };
                        row_data.insert(schema.columns[i].clone(), val);
                    }
                }
                let id = self.get_next_id(&table_name)?;
                let key = format!("{}:{}", table_name, id);
                let val = bincode::serialize(&row_data)?;
                self.btree.insert(key.into_bytes(), val)?;
                count += 1;
            }
            Ok(format!("Inserted {} rows", count))
        } else {
            Err(anyhow!("Unsupported insert source"))
        }
    }

    fn handle_select(&mut self, query: Query) -> Result<String> {
        let limit = query.limit.and_then(|e| {
            if let Expr::Value(Value::Number(n, _)) = e {
                n.parse::<usize>().ok()
            } else {
                None
            }
        });

        if let SetExpr::Select(select) = &*query.body {
            let table_name = match &select.from[0].relation {
                TableFactor::Table { name, .. } => name.to_string(),
                _ => return Err(anyhow!("Unsupported from clause")),
            };

            let schema = self.schemas.get(&table_name).ok_or_else(|| anyhow!("Table not found"))?.clone();
            let rows = self.scan_table(&table_name)?;

            let mut filtered_rows = Vec::new();
            for (_key, row_data) in rows {
                if self.evaluate_where(&select.selection, &row_data)? {
                    filtered_rows.push(row_data);
                }
            }

            if let Some(l) = limit {
                filtered_rows.truncate(l);
            }

            let mut output = String::new();
            output.push_str(&schema.columns.join(" | "));
            output.push_str("\n");

            for row in filtered_rows {
                let row_str: Vec<String> = schema.columns.iter()
                    .map(|col| row.get(col).cloned().unwrap_or_default())
                    .collect();
                output.push_str(&row_str.join(" | "));
                output.push_str("\n");
            }

            Ok(output)
        } else {
            Err(anyhow!("Unsupported select body"))
        }
    }

    fn handle_update(&mut self, table_name: String, assignments: Vec<sqlparser::ast::Assignment>, selection: Option<Expr>) -> Result<String> {
        let rows = self.scan_table(&table_name)?;
        let mut count = 0;

        for (key, mut row_data) in rows {
            if self.evaluate_where(&selection, &row_data)? {
                for assignment in &assignments {
                    let col = assignment.id[0].value.clone();
                    let val = self.eval_expr(&assignment.value, &row_data)?;
                    row_data.insert(col, val);
                }
                let val = bincode::serialize(&row_data)?;
                self.btree.insert(key.into_bytes(), val)?;
                count += 1;
            }
        }
        Ok(format!("Updated {} rows", count))
    }

    fn handle_delete(&mut self, table_name: String, selection: Option<Expr>) -> Result<String> {
        let rows = self.scan_table(&table_name)?;
        let mut count = 0;

        for (key, row_data) in rows {
            if self.evaluate_where(&selection, &row_data)? {
                self.btree.insert(key.into_bytes(), Vec::new())?;
                count += 1;
            }
        }
        Ok(format!("Deleted {} rows", count))
    }

    fn scan_table(&mut self, table_name: &str) -> Result<Vec<(String, HashMap<String, String>)>> {
        let mut results = Vec::new();
        let prefix = format!("{}:", table_name);

        let num_pages = self.btree.pager.num_pages();
        for i in 0..num_pages {
            let node = Node::from_bytes(&self.btree.pager.read_page(i)?)?;
            if node.node_type == NodeType::Leaf {
                for (j, key) in node.keys.iter().enumerate() {
                    let key_str = String::from_utf8_lossy(key);
                    if key_str.starts_with(&prefix) {
                        if node.values[j].is_empty() { continue; }
                        let row: HashMap<String, String> = bincode::deserialize(&node.values[j])?;
                        results.push((key_str.to_string(), row));
                    }
                }
            }
        }
        Ok(results)
    }

    fn evaluate_where(&self, selection: &Option<Expr>, row: &HashMap<String, String>) -> Result<bool> {
        match selection {
            Some(Expr::BinaryOp { left, op, right }) => {
                let left_val = self.eval_expr(left, row)?;
                let right_val = self.eval_expr(right, row)?;
                match op {
                    sqlparser::ast::BinaryOperator::Eq => Ok(left_val == right_val),
                    sqlparser::ast::BinaryOperator::NotEq => Ok(left_val != right_val),
                    _ => Err(anyhow!("Unsupported operator")),
                }
            }
            None => Ok(true),
            _ => Err(anyhow!("Unsupported WHERE clause")),
        }
    }

    fn eval_expr(&self, expr: &Expr, row: &HashMap<String, String>) -> Result<String> {
        match expr {
            Expr::Identifier(ident) => Ok(row.get(&ident.value).cloned().unwrap_or_default()),
            Expr::Value(Value::Number(n, _)) => Ok(n.clone()),
            Expr::Value(Value::SingleQuotedString(s)) => Ok(s.clone()),
            _ => Err(anyhow!("Unsupported expression")),
        }
    }

    fn get_next_id(&mut self, table_name: &str) -> Result<u64> {
        let key = format!("__id_gen:{}", table_name);
        let id = match self.btree.get(key.as_bytes())? {
            Some(data) => if data.is_empty() { 1 } else { bincode::deserialize::<u64>(&data)? + 1 },
            None => 1,
        };
        self.btree.insert(key.into_bytes(), bincode::serialize(&id)?)?;
        Ok(id)
    }
}
