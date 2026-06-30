use crate::storage::btree::{BPlusTree, NodeType, Record};
use crate::storage::wal::WalEntry;
use crate::storage::{HardwareManager, HardwareSpecs};
use anyhow::{anyhow, Result};
use chrono::Utc;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use sqlparser::ast::{Expr, Query, SetExpr, Statement, TableFactor, BinaryOperator};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TableSchema {
    pub name: String,
    pub columns: Vec<String>,
    pub auto_increment_col: Option<String>,
    pub next_id: u64,
}

pub struct Transaction {
    pub id: u64,
    pub snapshot_version: u64,
    pub updates: HashMap<Vec<u8>, Vec<u8>>,
}

pub struct EngineState {
    pub db_path: String,
    pub wal_path: String,
    pub btree: BPlusTree,
    pub schemas: DashMap<String, TableSchema>,
    pub current_version: Arc<std::sync::atomic::AtomicU64>,
}

pub struct Engine {
    pub state: Arc<EngineState>,
    pub active_transactions: DashMap<u32, Transaction>,
    pub wasm_engine: wasmi::Engine,
    pub hardware_specs: HardwareSpecs,
}

impl Engine {
    pub async fn new(db_path: &str, wal_path: &str) -> Result<Self> {
        let hardware_specs = HardwareManager::scan();
        let memory_tier = Arc::new(crate::storage::MemoryTier::new(1024));
        let btree = BPlusTree::open(db_path, wal_path, Arc::clone(&memory_tier)).await?;

        let schemas = DashMap::new();
        let current_version = Arc::new(std::sync::atomic::AtomicU64::new(0));

        if let Some(data) = btree.get(b"__schemas__").await? {
            Self::apply_meta_recovery(b"__schemas__", &data, &schemas, &current_version);
        }
        if let Some(data) = btree.get(b"__version__").await? {
            Self::apply_meta_recovery(b"__version__", &data, &schemas, &current_version);
        }

        let wal_entries = btree.wal.read_all().await?;
        for entry in wal_entries {
            match entry {
                WalEntry::RecordUpdate { key, data } => {
                    Self::apply_meta_recovery(&key, &data, &schemas, &current_version);
                    memory_tier.insert(key, data);
                }
                WalEntry::RecordBatch { entries } => {
                    for (key, data) in entries {
                        Self::apply_meta_recovery(&key, &data, &schemas, &current_version);
                        memory_tier.insert(key, data);
                    }
                }
                _ => {}
            }
        }

        let engine_state = Arc::new(EngineState {
            db_path: db_path.to_string(),
            wal_path: wal_path.to_string(),
            btree, schemas, current_version,
        });

        let state_for_drain = Arc::clone(&engine_state);
        std::thread::spawn(move || {
            tokio_uring::start(async move {
                let state_for_flush = Arc::clone(&state_for_drain);
                tokio::spawn(async move {
                    loop {
                        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
                        let mut dirty = Vec::new();
                        for r in state_for_flush.btree.memory_tier.dirty_pages.iter() { dirty.push((*r.key(), r.value().clone())); }
                        if !dirty.is_empty() {
                            for (pid, data) in dirty {
                                let mut page = [0u8; crate::storage::pager::PAGE_SIZE];
                                page[..data.len().min(4096)].copy_from_slice(&data[..data.len().min(4096)]);
                                let _ = state_for_flush.btree.pager.write_page(pid, &page).await;
                                state_for_flush.btree.memory_tier.dirty_pages.remove(&pid);
                            }
                            let _ = state_for_flush.btree.pager.sync().await;
                        }
                    }
                });

                loop {
                    let mut entries = Vec::new();
                    for _ in 0..5000 { if let Some(e) = state_for_drain.btree.wal.pop_entry() { entries.push(e); } else { break; } }
                    if entries.is_empty() { tokio::time::sleep(tokio::time::Duration::from_millis(5)).await; continue; }
                    for entry in entries {
                        match entry {
                            WalEntry::RecordUpdate { key, data } => { let _ = state_for_drain.btree.insert(key, data).await; }
                            WalEntry::RecordBatch { entries } => { for (k, v) in entries { let _ = state_for_drain.btree.insert(k, v).await; } }
                            _ => {}
                        }
                    }
                }
            });
        });

        Ok(Engine { state: engine_state, active_transactions: DashMap::new(), wasm_engine: wasmi::Engine::default(), hardware_specs })
    }

    fn apply_meta_recovery(key: &[u8], data: &[u8], schemas: &DashMap<String, TableSchema>, current_version: &Arc<std::sync::atomic::AtomicU64>) {
        if key == b"__schemas__" {
            if let Ok(rec) = bincode::deserialize::<Record>(data) {
                if let Ok(sm) = bincode::deserialize::<HashMap<String, TableSchema>>(&rec.value) {
                    for (k, v) in sm { schemas.insert(k, v); }
                }
            }
        } else if key == b"__version__" {
            if let Ok(rec) = bincode::deserialize::<Record>(data) {
                if let Ok(v) = bincode::deserialize::<u64>(&rec.value) { current_version.fetch_max(v, std::sync::atomic::Ordering::SeqCst); }
            }
        }
    }

    pub async fn execute(&self, sql: &str, conn_id: u32) -> Result<String> {
        let sql_upper = sql.trim().to_uppercase();
        if sql_upper == "FLUSH" {
            self.state.btree.wal.flush_pipeline().await?;
            while self.state.btree.wal.queue_len() > 0 {
                 tokio::time::sleep(tokio::time::Duration::from_millis(5)).await;
            }
            // Wait for drain queue to be applied to MemoryTier
            while self.state.btree.wal.drain_queue_len() > 0 {
                 tokio::time::sleep(tokio::time::Duration::from_millis(5)).await;
            }
            // Wait for dirty pages to be flushed to Pager
            while !self.state.btree.memory_tier.dirty_pages.is_empty() {
                 tokio::time::sleep(tokio::time::Duration::from_millis(5)).await;
            }
            let _ = self.state.btree.pager.sync().await;
            return Ok("Flushed".to_string());
        }
        if sql_upper == "BEGIN" || sql_upper == "BEGIN TRANSACTION" {
            self.active_transactions.insert(conn_id, Transaction { id: Utc::now().timestamp_micros() as u64, snapshot_version: self.state.current_version.load(std::sync::atomic::Ordering::SeqCst), updates: HashMap::new() });
            return Ok("Transaction started".to_string());
        }
        if sql_upper == "COMMIT" {
            if let Some((_, tx)) = self.active_transactions.remove(&conn_id) {
                for key in tx.updates.keys() {
                    if let Some(data) = self.state.btree.memory_tier.get(key).or(self.state.btree.get(key).await?) {
                        let record: Record = bincode::deserialize(&data)?;
                        if record.version > tx.snapshot_version { return Err(anyhow!("Transaction conflict detected (OCC)")); }
                    }
                }
                let version = self.state.current_version.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
                let mut batch = Vec::new();
                for (k, v) in tx.updates {
                    let mut rec: Record = bincode::deserialize(&v)?; rec.version = version;
                    let v_final = bincode::serialize(&rec)?;
                    self.state.btree.memory_tier.insert(k.clone(), v_final.clone()); batch.push((k, v_final));
                }
                let v_record = Record { value: bincode::serialize(&version)?, version, is_deleted: false, timestamp: Utc::now().timestamp() };
                let v_bytes = bincode::serialize(&v_record)?;
                self.state.btree.memory_tier.insert(b"__version__".to_vec(), v_bytes.clone());
                batch.push((b"__version__".to_vec(), v_bytes));
                self.state.btree.wal.enqueue(WalEntry::RecordBatch { entries: batch })?;
                self.state.btree.wal.flush_pipeline().await?;
                return Ok("Transaction committed".to_string());
            }
            return Err(anyhow!("No active transaction"));
        }
        if sql_upper == "ROLLBACK" { self.active_transactions.remove(&conn_id); return Ok("Transaction rolled back".to_string()); }

        let ast = Parser::parse_sql(&GenericDialect {}, sql)?;
        let mut results = Vec::new();
        for stmt in ast { results.push(self.execute_statement(stmt, conn_id).await?); }
        Ok(results.join("\n"))
    }

    async fn execute_statement(&self, stmt: Statement, conn_id: u32) -> Result<String> {
        match stmt {
            Statement::CreateTable { name, columns, .. } => {
                let ai = columns.iter().find(|c| {
                    let type_s = c.data_type.to_string().to_uppercase();
                    type_s.contains("SERIAL") || c.options.iter().any(|o| { let s = o.to_string().to_uppercase(); s.contains("AUTO_INCREMENT") || s.contains("SERIAL") })
                }).map(|c| c.name.to_string());
                let schema = TableSchema { name: name.to_string(), columns: columns.iter().map(|c| c.name.to_string()).collect(), auto_increment_col: ai, next_id: 1 };
                self.state.schemas.insert(name.to_string(), schema);
                self.persist_meta().await?; Ok("Table created".to_string())
            }
            Statement::Insert { table_name, columns, source, .. } => self.handle_insert(table_name.to_string(), columns, source.ok_or_else(|| anyhow!("No source"))?, conn_id).await,
            Statement::Query(q) => self.handle_query(*q, conn_id).await,
            Statement::Update { table, assignments, selection, .. } => {
                let tname = match &table.relation { TableFactor::Table { name, .. } => name.to_string(), _ => return Err(anyhow!("Unsupported")) };
                self.handle_update(tname, assignments, selection, conn_id).await
            }
            Statement::Delete { from, selection, .. } => {
                let tname = match &from[0].relation { TableFactor::Table { name, .. } => name.to_string(), _ => return Err(anyhow!("Unsupported")) };
                self.handle_delete(tname, selection, conn_id).await
            }
            _ => Err(anyhow!("Unsupported statement")),
        }
    }

    async fn persist_meta(&self) -> Result<()> {
        let version = self.state.current_version.load(std::sync::atomic::Ordering::SeqCst);
        let mut smap = HashMap::new(); for r in self.state.schemas.iter() { smap.insert(r.key().clone(), r.value().clone()); }
        let s_rec = Record { value: bincode::serialize(&smap)?, version, is_deleted: false, timestamp: Utc::now().timestamp() };
        let s_bytes = bincode::serialize(&s_rec)?;
        let v_rec = Record { value: bincode::serialize(&version)?, version, is_deleted: false, timestamp: Utc::now().timestamp() };
        let v_bytes = bincode::serialize(&v_rec)?;
        self.state.btree.wal.enqueue(WalEntry::RecordBatch { entries: vec![(b"__schemas__".to_vec(), s_bytes), (b"__version__".to_vec(), v_bytes)] })?;
        self.state.btree.wal.flush_pipeline().await?;
        Ok(())
    }

    fn evaluate_where(expr: &Expr, row: &HashMap<String, String>) -> bool {
        match expr {
            Expr::BinaryOp { left, op, right } => {
                let g = |e: &Expr| -> String {
                    match e {
                        Expr::Identifier(i) => {
                            let s = i.to_string();
                            row.get(&s)
                                .or_else(|| row.keys().find(|k| k.ends_with(&format!(".{}", s))).and_then(|k| row.get(k)))
                                .cloned()
                                .unwrap_or_default()
                        },
                        Expr::CompoundIdentifier(p) => {
                            let s = p.iter().map(|x| x.to_string()).collect::<Vec<_>>().join(".");
                            row.get(&s).or(row.get(&p.last().unwrap().to_string())).cloned().unwrap_or_default()
                        },
                        Expr::Value(v) => v.to_string().replace("'", ""),
                        _ => String::new()
                    }
                };
                let l = g(left); let r = g(right);
                match op {
                    BinaryOperator::Eq => l == r, BinaryOperator::NotEq => l != r,
                    BinaryOperator::Gt | BinaryOperator::Lt | BinaryOperator::GtEq | BinaryOperator::LtEq => {
                        let ln = l.parse::<f64>().unwrap_or_else(|_| if l.is_empty() { 0.0 } else { f64::NAN });
                        let rn = r.parse::<f64>().unwrap_or_else(|_| if r.is_empty() { 0.0 } else { f64::NAN });
                        if ln.is_nan() || rn.is_nan() { match op { BinaryOperator::Gt => l > r, BinaryOperator::Lt => l < r, BinaryOperator::GtEq => l >= r, BinaryOperator::LtEq => l <= r, _ => false } }
                        else { match op { BinaryOperator::Gt => ln > rn, BinaryOperator::Lt => ln < rn, BinaryOperator::GtEq => ln >= rn, BinaryOperator::LtEq => ln <= rn, _ => false } }
                    }
                    BinaryOperator::And => Self::evaluate_where(left, row) && Self::evaluate_where(right, row),
                    BinaryOperator::Or => Self::evaluate_where(left, row) || Self::evaluate_where(right, row),
                    _ => false
                }
            }
            Expr::Nested(i) => Self::evaluate_where(i, row), _ => true
        }
    }

    async fn handle_insert(&self, tname: String, cols: Vec<sqlparser::ast::Ident>, source: Box<Query>, conn_id: u32) -> Result<String> {
        let mut schema = self.state.schemas.get(&tname).ok_or_else(|| anyhow!("Table not found: {}", tname))?.clone();
        if let SetExpr::Values(values) = &*source.body {
            let in_tx = self.active_transactions.contains_key(&conn_id);
            let version = if in_tx { self.active_transactions.get(&conn_id).unwrap().snapshot_version } else { self.state.current_version.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1 };
            let mut batch = Vec::new(); let mut itoa_buf = itoa::Buffer::new();
            for row in &values.rows {
                let mut rvec = vec!["NULL".to_string(); schema.columns.len()];
                if cols.is_empty() {
                    let mut vi = 0;
                    for (i, cn) in schema.columns.iter().enumerate() {
                        if Some(cn) == schema.auto_increment_col.as_ref() { rvec[i] = itoa_buf.format(schema.next_id).to_string(); }
                        else if vi < row.len() { rvec[i] = row[vi].to_string().replace("'", ""); vi += 1; }
                    }
                } else {
                    for (i, ident) in cols.iter().enumerate() {
                        if let Some(p) = schema.columns.iter().position(|c| c == &ident.to_string()) {
                            rvec[p] = row[i].to_string().replace("'", "");
                        }
                    }
                    if let Some(ai) = &schema.auto_increment_col {
                        let p = schema.columns.iter().position(|c| c == ai).unwrap();
                        if rvec[p] == "NULL" { rvec[p] = itoa_buf.format(schema.next_id).to_string(); }
                    }
                }
                let mut current_id = schema.next_id;
                if let Some(ai) = &schema.auto_increment_col {
                    let p = schema.columns.iter().position(|c| c == ai).unwrap();
                    if let Ok(iv) = rvec[p].parse::<u64>() { current_id = iv; }
                }
                if current_id >= schema.next_id { schema.next_id = current_id + 1; }
                let key = format!("{}:{}", tname, current_id).into_bytes();
                let rec = Record { value: bincode::serialize(&rvec)?, version, is_deleted: false, timestamp: Utc::now().timestamp() };
                let rbytes = bincode::serialize(&rec)?;
                if let Some(mut tx) = self.active_transactions.get_mut(&conn_id) { tx.updates.insert(key, rbytes); }
                else { self.state.btree.memory_tier.insert(key.clone(), rbytes.clone()); batch.push((key, rbytes)); }
            }
            if !batch.is_empty() { self.state.btree.wal.enqueue(WalEntry::RecordBatch { entries: batch })?; }
            self.state.schemas.insert(tname.clone(), schema);
            if !in_tx { self.persist_meta().await?; }
            Ok(format!("Inserted {} rows", values.rows.len()))
        } else { Err(anyhow!("Unsupported")) }
    }

    async fn handle_update(&self, tname: String, assigns: Vec<sqlparser::ast::Assignment>, selection: Option<Expr>, conn_id: u32) -> Result<String> {
        let version = self.active_transactions.get(&conn_id).map(|tx| tx.snapshot_version).unwrap_or(self.state.current_version.load(std::sync::atomic::Ordering::SeqCst));
        let rows = self.scan_table_with_filter(&self.state, &tname, version, selection.as_ref()).await?;
        let schema = self.state.schemas.get(&tname).unwrap().clone();
        for row in &rows {
            let key = row.get("__key__").unwrap().as_bytes().to_vec();
            let data = self.state.btree.memory_tier.get(&key).or(self.state.btree.get(&key).await?).ok_or_else(|| anyhow!("Not found"))?;
            let mut rec: Record = bincode::deserialize(&data)?;
            let mut rvec: Vec<String> = bincode::deserialize(&rec.value)?;
            for a in &assigns { if let Some(p) = schema.columns.iter().position(|c| c == &a.id[0].to_string()) { rvec[p] = a.value.to_string().replace("'", ""); } }
            rec.value = bincode::serialize(&rvec)?; rec.version = version + 1;
            let bytes = bincode::serialize(&rec)?;
            if let Some(mut tx) = self.active_transactions.get_mut(&conn_id) { tx.updates.insert(key, bytes); }
            else {
                let v = self.state.current_version.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
                let mut r2 = rec.clone(); r2.version = v; let b2 = bincode::serialize(&r2)?;
                self.state.btree.memory_tier.insert(key.clone(), b2.clone()); self.state.btree.wal.enqueue(WalEntry::RecordUpdate { key, data: b2 })?;
            }
        }
        if !self.active_transactions.contains_key(&conn_id) { self.state.btree.wal.flush_pipeline().await?; }
        Ok(format!("Updated {} rows", rows.len()))
    }

    async fn handle_delete(&self, tname: String, selection: Option<Expr>, conn_id: u32) -> Result<String> {
        let version = self.active_transactions.get(&conn_id).map(|tx| tx.snapshot_version).unwrap_or(self.state.current_version.load(std::sync::atomic::Ordering::SeqCst));
        let rows = self.scan_table_with_filter(&self.state, &tname, version, selection.as_ref()).await?;
        for row in &rows {
            let key = row.get("__key__").unwrap().as_bytes().to_vec();
            let data = self.state.btree.memory_tier.get(&key).or(self.state.btree.get(&key).await?).ok_or_else(|| anyhow!("Not found"))?;
            let mut rec: Record = bincode::deserialize(&data)?;
            rec.is_deleted = true; rec.version = version + 1;
            let bytes = bincode::serialize(&rec)?;
            if let Some(mut tx) = self.active_transactions.get_mut(&conn_id) { tx.updates.insert(key, bytes); }
            else {
                let v = self.state.current_version.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
                let mut r2 = rec.clone(); r2.version = v; let b2 = bincode::serialize(&r2)?;
                self.state.btree.memory_tier.insert(key.clone(), b2.clone()); self.state.btree.wal.enqueue(WalEntry::RecordUpdate { key, data: b2 })?;
            }
        }
        if !self.active_transactions.contains_key(&conn_id) { self.state.btree.wal.flush_pipeline().await?; }
        Ok(format!("Deleted {} rows", rows.len()))
    }

    async fn handle_query(&self, query: Query, conn_id: u32) -> Result<String> {
        let version = self.active_transactions.get(&conn_id).map(|tx| tx.snapshot_version).unwrap_or(self.state.current_version.load(std::sync::atomic::Ordering::SeqCst));
        if let SetExpr::Select(select) = &*query.body {
            let tname = match &select.from[0].relation { TableFactor::Table { name, .. } => name.to_string(), _ => return Err(anyhow!("Unsupported")) };
            let schema = self.state.schemas.get(&tname).ok_or_else(|| anyhow!("Not found"))?.clone();
            let mut rows = self.scan_table_with_filter(&self.state, &tname, version, select.selection.as_ref()).await?;
            let mut dcols: Vec<String> = schema.columns.iter().map(|c| format!("{}.{}", tname, c)).collect();
            if !select.from[0].joins.is_empty() {
                let join = &select.from[0].joins[0]; let rtname = match &join.relation { TableFactor::Table { name, .. } => name.to_string(), _ => String::new() };
                let rschema = self.state.schemas.get(&rtname).unwrap().clone(); let rrows = self.scan_table(&self.state, &rtname, version).await?;
                dcols.extend(rschema.columns.iter().map(|c| format!("{}.{}", rtname, c)));
                let mut joined = Vec::new();
                for l in &rows { for r in &rrows {
                    let mut combined = l.clone(); for (k, v) in r { combined.insert(k.clone(), v.clone()); }
                    if match &join.join_operator { sqlparser::ast::JoinOperator::Inner(sqlparser::ast::JoinConstraint::On(e)) => Self::evaluate_where(e, &combined), _ => true } { joined.push(combined); }
                } }
                rows = joined;
            }
            if select.projection.iter().any(|p| matches!(p, sqlparser::ast::SelectItem::UnnamedExpr(Expr::Function(_)))) {
                let mut results = Vec::new();
                let mut rcols = Vec::new();
                for proj in &select.projection { if let sqlparser::ast::SelectItem::UnnamedExpr(Expr::Function(f)) = proj {
                    let name = f.name.to_string().to_uppercase(); rcols.push(name.clone());
                    let col = if let Some(sqlparser::ast::FunctionArg::Unnamed(sqlparser::ast::FunctionArgExpr::Expr(Expr::Identifier(i)))) = f.args.get(0) { i.to_string() } else { "*".to_string() };
                    let vals: Vec<f64> = rows.iter().filter_map(|r| r.get(&col).or(r.keys().find(|k| k.ends_with(&format!(".{}", col))).and_then(|k| r.get(k))).and_then(|v| v.parse().ok())).collect();
                    results.push(match name.as_str() { "COUNT" => rows.len() as f64, "SUM" => vals.iter().sum(), "AVG" => if vals.is_empty() { 0.0 } else { vals.iter().sum::<f64>() / vals.len() as f64 }, "MIN" => vals.iter().copied().fold(f64::INFINITY, f64::min), "MAX" => vals.iter().copied().fold(f64::NEG_INFINITY, f64::max), _ => 0.0 }.to_string());
                } }
                let mut output = rcols.join(" | ") + "\n" + &"-".repeat(rcols.join(" | ").len()) + "\n";
                output += &(results.join(" | ") + "\n");
                return Ok(output);
            }
            let mut output = dcols.join(" | ") + "\n" + &"-".repeat(dcols.join(" | ").len()) + "\n";
            rows.sort_by_key(|r| r.get("__key__").cloned().unwrap_or_default());
            for row in rows {
                let mut vals = Vec::new();
                for c in &dcols {
                    let mut val = row.get(c).cloned();
                    if val.is_none() {
                        let p: Vec<&str> = c.split('.').collect();
                        if p.len() == 2 { val = row.get(p[1]).cloned(); }
                    }
                    vals.push(val.unwrap_or_else(|| "NULL".to_string()));
                }
                output += &(vals.join(" | ") + "\n");
            }
            Ok(output)
        } else { Err(anyhow!("Unsupported")) }
    }

    async fn scan_table(&self, state: &EngineState, tname: &str, version: u64) -> Result<Vec<HashMap<String, String>>> { self.scan_table_with_filter(state, tname, version, None).await }

    async fn scan_table_with_filter(&self, state: &EngineState, tname: &str, version: u64, filter: Option<&Expr>) -> Result<Vec<HashMap<String, String>>> {
        let prefix = format!("{}:", tname); let mut rows = Vec::new(); let mut processed = std::collections::HashSet::new();
        let schema = state.schemas.get(tname).ok_or_else(|| anyhow!("Schema missing: {}", tname))?.clone();
        let mut add_row = |key: &[u8], val: &[u8], rows: &mut Vec<HashMap<String, String>>| -> Result<()> {
            if processed.contains(key) { return Ok(()); }
            let rec: Record = match bincode::deserialize::<Record>(val) {
                Ok(r) => r,
                Err(_) => return Ok(()),
            };
            if rec.version <= version {
                processed.insert(key.to_vec());
                if !rec.is_deleted {
                    let rvec: Vec<String> = match bincode::deserialize::<Vec<String>>(&rec.value) { Ok(v) => v, Err(_) => { let map: HashMap<String, String> = bincode::deserialize(&rec.value)?; schema.columns.iter().map(|c| map.get(c).cloned().unwrap_or_default()).collect() } };
                    let mut row = HashMap::new(); row.insert("__key__".to_string(), String::from_utf8_lossy(key).to_string());
                    for (i, v) in rvec.into_iter().enumerate() { if i < schema.columns.len() { let k = &schema.columns[i]; row.insert(k.clone(), v.clone()); row.insert(format!("{}.{}", tname, k), v); } }
                    if filter.map(|f| Self::evaluate_where(f, &row)).unwrap_or(true) { rows.push(row); }
                }
            }
            Ok(())
        };
        for entry in state.btree.memory_tier.memtable.map.iter() { if entry.key().starts_with(prefix.as_bytes()) { add_row(entry.key(), entry.value(), &mut rows)?; } }
        for shard in &state.btree.memory_tier.turbo_cache { for r in shard.iter() { if r.key().starts_with(prefix.as_bytes()) { add_row(r.key(), r.value(), &mut rows)?; } } }

        let mut curr = state.btree.root_page_id;
        loop {
            let node = state.btree.read_node(curr).await?;
            if node.node_type == NodeType::Leaf || node.children.is_empty() { break; }
            curr = node.children[0];
        }

        let mut visited = std::collections::HashSet::new();
        loop {
            if !visited.insert(curr) { break; }
            let node = state.btree.read_node(curr).await?;
            for (i, k) in node.keys.iter().enumerate() { if k.starts_with(prefix.as_bytes()) { add_row(k, &node.values[i], &mut rows)?; } }
            if let Some(n) = node.next_leaf { if n == curr { break; } curr = n; } else { break; }
        }
        Ok(rows)
    }

    pub async fn undo(&self) -> Result<()> { let v = self.state.current_version.load(std::sync::atomic::Ordering::SeqCst); if v > 0 { self.state.current_version.store(v - 1, std::sync::atomic::Ordering::SeqCst); } Ok(()) }
    pub async fn redo(&self) -> Result<()> { self.state.current_version.fetch_add(1, std::sync::atomic::Ordering::SeqCst); Ok(()) }
}
