use crate::storage::btree::{BPlusTree, Node, NodeType, Record};
use crate::storage::pager::{PAGE_SIZE};
use crate::storage::wal::WalEntry;
use crate::storage::{HardwareManager, HardwareSpecs};
use crate::parser::scheduler::{DeterministicScheduler};
use anyhow::{anyhow, Result};
use chrono::Utc;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use sqlparser::ast::{Expr, Query, SetExpr, Statement, TableFactor, Value};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use wasmi::*;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TableSchema {
    pub name: String,
    pub columns: Vec<String>,
    pub auto_increment_col: Option<String>,
    pub next_id: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct View {
    pub name: String,
    pub definition: String,
}

pub struct Transaction {
    pub id: u64,
    pub snapshot_version: u64,
    pub updates: HashMap<Vec<u8>, Vec<u8>>,
    pub read_set: HashSet<Vec<u8>>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct IndexSchema {
    pub name: String,
    pub table_name: String,
    pub column: String,
}

pub struct EngineState {
    pub db_path: String,
    pub wal_path: String,
    pub btree: BPlusTree,
    pub schemas: DashMap<String, TableSchema>,
    pub views: DashMap<String, View>,
    pub indices: DashMap<String, IndexSchema>,
    pub current_version: Arc<std::sync::atomic::AtomicU64>,
    pub scheduler: DeterministicScheduler,
}

pub struct Engine {
    pub state: Arc<EngineState>,
    pub active_transactions: DashMap<u32, Transaction>,
    pub wasm_engine: wasmi::Engine,
    pub hardware_specs: HardwareSpecs,
    pub autopilot: bool,
}

impl Engine {
    pub async fn new(db_path: &str, wal_path: &str) -> Result<Self> {
        let hardware_specs = HardwareManager::scan();
        let memory_tier = Arc::new(crate::storage::TieredMemory::new(1024));
        let btree = BPlusTree::open(db_path, wal_path, memory_tier).await?;

        let schemas = DashMap::new();
        let current_version = Arc::new(std::sync::atomic::AtomicU64::new(0));

        if let Some(data) = btree.get(b"__schemas__").await? {
            let record: Record = bincode::deserialize(&data)?;
            let schemas_map: HashMap<String, TableSchema> = bincode::deserialize(&record.value)?;
            for (k, v) in schemas_map {
                schemas.insert(k, v);
            }
        }
        if let Some(data) = btree.get(b"__version__").await? {
            let record: Record = bincode::deserialize(&data)?;
            current_version.store(
                bincode::deserialize(&record.value)?,
                std::sync::atomic::Ordering::SeqCst,
            );
        }

        let engine_state = Arc::new(EngineState {
            db_path: db_path.to_string(),
            wal_path: wal_path.to_string(),
            btree,
            schemas,
            views: DashMap::new(),
            indices: DashMap::new(),
            current_version,
            scheduler: DeterministicScheduler::new(),
        });

        let state_for_drain = Arc::clone(&engine_state);
        tokio::spawn(async move {
            loop {
                let mut entries = Vec::new();
                for _ in 0..5000 {
                    if let Some(entry) = state_for_drain.btree.wal.pop_entry() {
                        entries.push(entry);
                    } else {
                        break;
                    }
                }

                if entries.is_empty() {
                    tokio::time::sleep(tokio::time::Duration::from_millis(5)).await;
                    continue;
                }

                for entry in entries {
                    match entry {
                        WalEntry::RecordUpdate { key, data } => {
                            let _ = state_for_drain.btree.insert(key, data).await;
                        }
                        WalEntry::PageUpdate { page_id, data } => {
                            let mut page = [0u8; PAGE_SIZE];
                            let len = data.len().min(PAGE_SIZE);
                            page[..len].copy_from_slice(&data[..len]);
                            let _ = state_for_drain.btree.pager.write_page(page_id, &page).await;
                        }
                        _ => {}
                    }
                }
                let _ = state_for_drain.btree.pager.sync().await;
            }
        });

        Ok(Engine {
            state: engine_state,
            active_transactions: DashMap::new(),
            wasm_engine: wasmi::Engine::default(),
            hardware_specs,
            autopilot: true,
        })
    }

    pub async fn universal_search(&self, query: &str) -> Result<String> {
        let mut results = Vec::new();
        results.push(format!("Universal Search Results for: '{}'", query));
        results.push("-".repeat(40));

        let mut current_page_id = self.state.btree.root_page_id;
        loop {
            let page_res = self.state.btree.pager.read_page(current_page_id).await;

            if let Ok(page) = page_res {
                let node = Node::from_bytes(&page)?;
                if node.node_type == NodeType::Leaf {
                    for (i, key) in node.keys.iter().enumerate() {
                        let key_str = String::from_utf8_lossy(key);
                        if key_str.starts_with("__") {
                            continue;
                        }
                        let record: Record = bincode::deserialize(&node.values[i])?;
                        if !record.is_deleted {
                            let row_str = String::from_utf8_lossy(&record.value);
                            if row_str.contains(query) {
                                results.push(format!("Match in Key {}: {}", key_str, row_str));
                            }
                        }
                    }
                    if let Some(next) = node.next_leaf {
                        current_page_id = next;
                    } else {
                        break;
                    }
                } else {
                    if node.children.is_empty() {
                        break;
                    }
                    current_page_id = node.children[0];
                }
            } else {
                break;
            }
        }
        Ok(results.join("\n"))
    }

    pub async fn execute(&self, sql: &str, conn_id: u32) -> Result<String> {
        let sql_upper = sql.trim().to_uppercase();

        // --- 0. Virtualized System Catalog (pg_catalog) Interception ---
        if sql_upper.contains("PG_CLASS") || sql_upper.contains("PG_NAMESPACE") {
            return self.handle_catalog_query(sql).await;
        }

        if sql_upper == "FLUSH" {
            self.state.btree.wal.flush_pipeline().await?;
            while self.state.btree.wal.pop_entry().is_some() {} // Wait for drain (simplified)
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            return Ok("Flushed".to_string());
        }
        if sql_upper.starts_with("SEARCH") {
            let query = sql.trim_start_matches("SEARCH").trim().replace("'", "");
            return self.universal_search(&query).await;
        }
        if sql_upper == "BEGIN" || sql_upper == "BEGIN TRANSACTION" {
            let version = self
                .state
                .current_version
                .load(std::sync::atomic::Ordering::SeqCst);
            self.active_transactions.insert(
                conn_id,
                Transaction {
                    id: Utc::now().timestamp_micros() as u64,
                    snapshot_version: version,
                    updates: HashMap::new(),
                    read_set: HashSet::new(),
                },
            );
            return Ok("Transaction started".to_string());
        }
        if sql_upper == "COMMIT" {
            if let Some((_, tx)) = self.active_transactions.remove(&conn_id) {
                // 1. Acquire Locks via Deterministic Sequencer
                let write_set: HashSet<Vec<u8>> = tx.updates.keys().cloned().collect();
                let lock_rx = self.state.scheduler.acquire(tx.id, tx.read_set.clone(), write_set.clone()).await?;

                // Wait for all prior conflicting transactions to release their locks
                lock_rx.await??;

                // 2. Perform OCC validation & Commit
                let commit_res = self.perform_commit(tx, write_set.clone()).await;

                // 3. Explicitly Release Locks
                let _ = self.state.scheduler.release(conn_id as u64, write_set).await;

                return commit_res;
            }
            return Err(anyhow!("No active transaction"));
        }

        if sql_upper == "ROLLBACK" {
            if let Some((_, tx)) = self.active_transactions.remove(&conn_id) {
                 let write_set = tx.updates.keys().cloned().collect();
                 let _ = self.state.scheduler.release(tx.id, write_set).await;
            }
            return Ok("Transaction rolled back".to_string());
        }

        let dialect = GenericDialect {};
        let ast = Parser::parse_sql(&dialect, sql)?;
        let mut results = Vec::new();
        for stmt in ast {
            let mut retries = 0;
            loop {
                match self.execute_statement(stmt.clone(), conn_id).await {
                    Ok(res) => {
                        results.push(res);
                        break;
                    }
                    Err(e) if e.to_string().contains("conflict") && retries < 5 => {
                        tokio::time::sleep(tokio::time::Duration::from_millis(10 * (1 << retries))).await;
                        retries += 1;
                    }
                    Err(e) => return Err(e),
                }
            }
        }
        Ok(results.join("\n"))
    }

    async fn perform_commit(&self, tx: Transaction, write_set: HashSet<Vec<u8>>) -> Result<String> {
        for key in &write_set {
            let data_opt = self.state.btree.memory_tier.get(key);
            let data_res = if data_opt.is_some() { Ok(data_opt) } else { self.state.btree.get(key).await };
            if let Ok(Some(data)) = data_res {
                let record: Record = bincode::deserialize(&data)?;
                if record.version > tx.snapshot_version {
                    return Err(anyhow!("Transaction conflict detected (OCC)"));
                }
            }
        }

        let version = self.state.current_version.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
        for (k, v) in tx.updates {
            let mut record: Record = bincode::deserialize(&v)?;
            record.version = version;
            let v_new = bincode::serialize(&record)?;
            self.state.btree.memory_tier.insert_kv(k.clone(), v_new.clone());
            self.state.btree.wal.enqueue(WalEntry::RecordUpdate { key: k, data: v_new })?;
        }

        self.state.btree.wal.flush_pipeline().await?;
        Ok("Transaction committed".to_string())
    }

    async fn handle_catalog_query(&self, _sql: &str) -> Result<String> {
        let mut output = "relname | relkind | relnamespace\n".to_string();
        output += "---------------------------------\n";
        for r in self.state.schemas.iter() {
            output += &format!("{} | r | 11\n", r.key());
        }
        Ok(output)
    }

    async fn execute_statement(&self, stmt: Statement, conn_id: u32) -> Result<String> {
        let sql = stmt.to_string();
        if sql.starts_with("CALL") {
            let parts: Vec<&str> = sql.split_whitespace().collect();
            if parts.len() > 1 {
                let module_path = parts[1].replace("'", "");
                let wasm_binary = std::fs::read(&module_path)?;
                let result = self.call_wasm(&wasm_binary)?;
                return Ok(format!("WASM Result: {}", result));
            }
        }
        match stmt {
            Statement::CreateIndex { name, table_name, columns, .. } => {
                let index_name = name.as_ref().map(|n| n.to_string()).unwrap_or_else(|| "unnamed_idx".to_string());
                let schema = IndexSchema {
                    name: index_name.clone(),
                    table_name: table_name.to_string(),
                    column: columns[0].expr.to_string(),
                };
                self.state.indices.insert(index_name, schema.clone());
                
                // Build index from existing data
                let version = self.state.current_version.load(std::sync::atomic::Ordering::Relaxed);
                let rows = self.scan_table(&self.state, &table_name.to_string(), version).await?;
                for row in rows {
                    if let (Some(val), Some(key)) = (row.get(&schema.column), row.get("__key__")) {
                        let idx_key = format!("idx:{}:{}:{}", schema.table_name, schema.column, val);
                        self.state.btree.memory_tier.insert_kv(idx_key.clone().into_bytes(), key.as_bytes().to_vec());
                        self.state.btree.wal.enqueue(WalEntry::RecordUpdate {
                            key: idx_key.into_bytes(),
                            data: key.as_bytes().to_vec()
                        })?;
                    }
                }

                Ok("Index created".to_string())
            }
            Statement::CreateTable { name, columns, .. } => {
                let auto_increment_col = columns.iter().find(|c| {
                    c.options.iter().any(|o| {
                        let opt_str = o.to_string().to_uppercase();
                        opt_str.contains("AUTO_INCREMENT") || opt_str.contains("SERIAL")
                    })
                }).map(|c| c.name.to_string());

                let schema = TableSchema {
                    name: name.to_string(),
                    columns: columns.iter().map(|c| c.name.to_string()).collect(),
                    auto_increment_col,
                    next_id: 1,
                };
                self.state.schemas.insert(name.to_string(), schema.clone());

                let mut schemas_map = HashMap::new();
                for r in self.state.schemas.iter() {
                    schemas_map.insert(r.key().clone(), r.value().clone());
                }

                let schema_bytes = bincode::serialize(&schemas_map)?;
                let record = Record {
                    value: schema_bytes,
                    version: self.state.current_version.load(std::sync::atomic::Ordering::SeqCst),
                    is_deleted: false,
                    timestamp: Utc::now().timestamp(),
                };
                self.state.btree.insert(b"__schemas__".to_vec(), bincode::serialize(&record)?).await?;

                Ok("Table created".to_string())
            }
            Statement::Explain { statement, .. } => {
                Ok(format!("Execution Plan (Titan-Prime):\n- io_uring I/O\n- O_DIRECT DMA\n- Lock-Free ArrayQueue\n- Ping-Pong Double Buffering\n- Statement: {:?}", statement))
            }
            Statement::Insert { table_name, source, .. } => {
                let source = source.ok_or_else(|| anyhow!("Source missing"))?;
                self.handle_insert(table_name.to_string(), source, conn_id).await
            }
            Statement::Query(query) => self.handle_query(*query, conn_id).await,
            Statement::Update { table, assignments, selection, .. } => {
                let table_name = match &table.relation {
                    TableFactor::Table { name, .. } => name.to_string(),
                    _ => return Err(anyhow!("Unsupported update target")),
                };
                self.handle_update(table_name, assignments, selection, conn_id).await
            }
            Statement::Delete { from, selection, .. } => {
                let table_name = match &from[0].relation {
                    TableFactor::Table { name, .. } => name.to_string(),
                    _ => return Err(anyhow!("Unsupported delete target")),
                };
                self.handle_delete(table_name, selection, conn_id).await
            }
            _ => Err(anyhow!("Unsupported statement")),
        }
    }

    fn evaluate_where(expr: &Expr, row: &HashMap<String, String>) -> bool {
        match expr {
            Expr::BinaryOp { left, op, right } => {
                let get_data = |e: &Expr| -> String {
                    match e {
                        Expr::Identifier(ident) => {
                            let id = ident.to_string();
                            if let Some(v) = row.get(&id) {
                                return v.clone();
                            }
                            row.keys()
                                .find(|k| k.ends_with(&format!(".{}", id)))
                                .and_then(|k| row.get(k).cloned())
                                .unwrap_or_default()
                        }
                        Expr::CompoundIdentifier(parts) => {
                            let full_id = parts
                                .iter()
                                .map(|p| p.to_string())
                                .collect::<Vec<_>>()
                                .join(".");
                            if let Some(v) = row.get(&full_id) {
                                return v.clone();
                            }
                            let col = parts[parts.len() - 1].to_string();
                            row.get(&col).cloned().unwrap_or_default()
                        }
                        Expr::Value(v) => v.to_string().replace("'", ""),
                        _ => String::new(),
                    }
                };

                let left_val = get_data(left);
                let right_val = get_data(right);

                match op {
                    sqlparser::ast::BinaryOperator::Eq => {
                        if left_val.is_empty() && right_val.is_empty() {
                            false
                        } else {
                            left_val == right_val
                        }
                    }
                    sqlparser::ast::BinaryOperator::NotEq => left_val != right_val,
                    sqlparser::ast::BinaryOperator::Gt => left_val > right_val,
                    sqlparser::ast::BinaryOperator::Lt => left_val < right_val,
                    _ => false,
                }
            }
            Expr::Nested(inner) => Self::evaluate_where(inner, row),
            _ => true,
        }
    }

    async fn handle_update(
        &self,
        table_name: String,
        assignments: Vec<sqlparser::ast::Assignment>,
        selection: Option<Expr>,
        conn_id: u32,
    ) -> Result<String> {
        let version = self
            .active_transactions
            .get(&conn_id)
            .map(|tx| tx.snapshot_version)
            .unwrap_or(
                self.state
                    .current_version
                    .load(std::sync::atomic::Ordering::SeqCst),
            );
        let mut count = 0;

        let mut keys_to_update = Vec::new();
        let rows = self.scan_table_with_filter(&self.state, &table_name, version, selection.as_ref()).await?;
        for row in rows {
            if selection.as_ref().map(|s| Self::evaluate_where(s, &row)).unwrap_or(true) {
                if let Some(key) = row.get("__key__") {
                    keys_to_update.push(key.as_bytes().to_vec());
                    if let Some(mut tx) = self.active_transactions.get_mut(&conn_id) {
                        tx.read_set.insert(key.as_bytes().to_vec());
                    }
                }
            }
        }

        for key in keys_to_update {
            let data_opt = self.state.btree.memory_tier.get(&key);
            let data = if let Some(v) = data_opt { v } else { self.state.btree.get(&key).await?.ok_or_else(|| anyhow!("Record not found during update"))? };
            let mut record: Record = bincode::deserialize(&data)?;
            let mut row_data: HashMap<String, String> = bincode::deserialize(&record.value)?;

            for assignment in &assignments {
                let col = assignment.id[0].to_string();
                let val = assignment.value.to_string().replace("'", "");
                row_data.insert(col, val);
            }

            record.value = bincode::serialize(&row_data)?;
            record.version = version + 1;
            let val = bincode::serialize(&record)?;

            if let Some(mut tx) = self.active_transactions.get_mut(&conn_id) {
                tx.updates.insert(key, val);
            } else {
                let v = self.state.current_version.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
                let mut record_mut: Record = bincode::deserialize(&val)?;
                record_mut.version = v;
                let val_final = bincode::serialize(&record_mut)?;

                self.state.btree.memory_tier.insert_kv(key.clone(), val_final.clone());
                self.state.btree.wal.enqueue(WalEntry::RecordUpdate { key, data: val_final })?;
            }
            count += 1;
        }
        if !self.active_transactions.contains_key(&conn_id) {
            self.state.btree.wal.flush_pipeline().await?;
        }
        Ok(format!("Updated {} rows", count))
    }

    async fn handle_delete(
        &self,
        table_name: String,
        selection: Option<Expr>,
        conn_id: u32,
    ) -> Result<String> {
        let version = self
            .active_transactions
            .get(&conn_id)
            .map(|tx| tx.snapshot_version)
            .unwrap_or(
                self.state
                    .current_version
                    .load(std::sync::atomic::Ordering::SeqCst),
            );
        let mut count = 0;

        let mut keys_to_delete = Vec::new();
        let rows = self.scan_table_with_filter(&self.state, &table_name, version, selection.as_ref()).await?;
        for row in rows {
            if selection.as_ref().map(|s| Self::evaluate_where(s, &row)).unwrap_or(true) {
                if let Some(key) = row.get("__key__") {
                    keys_to_delete.push(key.as_bytes().to_vec());
                    if let Some(mut tx) = self.active_transactions.get_mut(&conn_id) {
                        tx.read_set.insert(key.as_bytes().to_vec());
                    }
                }
            }
        }

        for key in keys_to_delete {
            let data_opt = self.state.btree.memory_tier.get(&key);
            let data = if let Some(v) = data_opt { v } else { self.state.btree.get(&key).await?.ok_or_else(|| anyhow!("Record not found during delete"))? };
            let mut record: Record = bincode::deserialize(&data)?;
            record.is_deleted = true;
            record.version = version + 1;
            let val = bincode::serialize(&record)?;

            if let Some(mut tx) = self.active_transactions.get_mut(&conn_id) {
                tx.updates.insert(key, val);
            } else {
                let v = self.state.current_version.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
                let mut record_mut: Record = bincode::deserialize(&val)?;
                record_mut.version = v;
                let val_final = bincode::serialize(&record_mut)?;

                self.state.btree.memory_tier.insert_kv(key.clone(), val_final.clone());
                self.state.btree.wal.enqueue(WalEntry::RecordUpdate { key, data: val_final })?;
            }
            count += 1;
        }
        if !self.active_transactions.contains_key(&conn_id) {
            self.state.btree.wal.flush_pipeline().await?;
        }
        Ok(format!("Deleted {} rows", count))
    }

    #[inline(always)]
    pub async fn handle_insert(
        &self,
        table_name: String,
        source: Box<Query>,
        conn_id: u32,
    ) -> Result<String> {
        let schema = self
            .state
            .schemas
            .get(&table_name)
            .ok_or_else(|| anyhow!("Table not found"))?
            .clone();

        if let SetExpr::Values(values) = &*source.body {
            let mut count = 0;
            let mut wal_entries = Vec::new();

            let mut next_id = schema.next_id;
            for row in &values.rows {
                let mut row_data = HashMap::new();
                
                if let Some(ref ai_col) = schema.auto_increment_col {
                    row_data.insert(ai_col.clone(), next_id.to_string());
                    next_id += 1;
                }

                for (i, expr) in row.iter().enumerate() {
                    let col_idx = if schema.auto_increment_col.is_some() { i + 1 } else { i };
                    if col_idx < schema.columns.len() {
                        let val = match expr {
                            Expr::Value(Value::Number(n, _)) => n.clone(),
                            Expr::Value(Value::SingleQuotedString(s)) => s.clone(),
                            _ => "NULL".to_string(),
                        };
                        row_data.insert(schema.columns[col_idx].clone(), val);
                    }
                }
                let id = rand::random::<u64>();
                let key_str = format!("{}:{:016x}", table_name, id);
                let key_vec = key_str.into_bytes();

                let version = self.state.current_version.load(std::sync::atomic::Ordering::Relaxed);
                
                let record = Record {
                    value: bincode::serialize(&row_data)?,
                    version,
                    is_deleted: false,
                    timestamp: Utc::now().timestamp(),
                };
                let val = bincode::serialize(&record)?;

                if let Some(mut tx) = self.active_transactions.get_mut(&conn_id) {
                    tx.updates.insert(key_vec, val);
                } else {
                    self.state.btree.memory_tier.insert_kv(key_vec.clone(), val.clone());
                    wal_entries.push(WalEntry::RecordUpdate { key: key_vec, data: val });
                    
                    for r in self.state.indices.iter() {
                        let idx = r.value();
                        if idx.table_name == table_name {
                            if let Some(col_val) = row_data.get(&idx.column) {
                                let idx_key = format!("idx:{}:{}:{}", table_name, idx.column, col_val);
                            let key_id = format!("{}:{:016x}", table_name, id).into_bytes();
                                wal_entries.push(WalEntry::RecordUpdate {
                                    key: idx_key.into_bytes(),
                                    data: key_id
                                });
                            }
                        }
                    }
                }
                count += 1;
            }

            for entry in wal_entries {
                self.state.btree.wal.enqueue(entry)?;
            }

            if next_id != schema.next_id {
                let mut updated_schema = schema.clone();
                updated_schema.next_id = next_id;
                self.state.schemas.insert(table_name.clone(), updated_schema);
                
                let mut schemas_map = HashMap::new();
                for r in self.state.schemas.iter() {
                    schemas_map.insert(r.key().clone(), r.value().clone());
                }
                let record = Record {
                    value: bincode::serialize(&schemas_map)?,
                    version: self.state.current_version.load(std::sync::atomic::Ordering::Relaxed),
                    is_deleted: false,
                    timestamp: Utc::now().timestamp(),
                };
                self.state.btree.wal.enqueue(WalEntry::RecordUpdate {
                    key: b"__schemas__".to_vec(),
                    data: bincode::serialize(&record)?
                })?;
            }

            self.state.btree.wal.flush_pipeline().await?;
            Ok(format!("Inserted {} rows", count))
        } else {
            Err(anyhow!("Unsupported source"))
        }
    }

    async fn handle_query(&self, query: Query, conn_id: u32) -> Result<String> {
        let version = self
            .active_transactions
            .get(&conn_id)
            .map(|tx| tx.snapshot_version)
            .unwrap_or(
                self.state
                    .current_version
                    .load(std::sync::atomic::Ordering::SeqCst),
            );

        if let SetExpr::Select(select) = &*query.body {
            let table_name = match &select.from[0].relation {
                TableFactor::Table { name, .. } => name.to_string(),
                _ => return Err(anyhow!("Unsupported table")),
            };

            let schema = self
                .state
                .schemas
                .get(&table_name)
                .ok_or_else(|| anyhow!("Table {} not found", table_name))?
                .clone();
            let mut left_rows = self.scan_table_with_filter(&self.state, &table_name, version, select.selection.as_ref()).await?;

            // Record read set for transactions
            if let Some(mut tx) = self.active_transactions.get_mut(&conn_id) {
                for row in &left_rows {
                    if let Some(key) = row.get("__key__") {
                        tx.read_set.insert(key.as_bytes().to_vec());
                    }
                }
            }

            let mut display_cols: Vec<String> = schema
                .columns
                .iter()
                .map(|c| format!("{}.{}", table_name, c))
                .collect();

            if select.from[0].joins.len() > 0 {
                let join = &select.from[0].joins[0];
                let right_table = match &join.relation {
                    TableFactor::Table { name, .. } => name.to_string(),
                    _ => return Err(anyhow!("Unsupported join table")),
                };
                let right_schema = self
                    .state
                    .schemas
                    .get(&right_table)
                    .ok_or_else(|| anyhow!("Table {} not found", right_table))?
                    .clone();
                let right_rows = self.scan_table(&self.state, &right_table, version).await?;

                // Record read set for right rows
                if let Some(mut tx) = self.active_transactions.get_mut(&conn_id) {
                    for row in &right_rows {
                        if let Some(key) = row.get("__key__") {
                            tx.read_set.insert(key.as_bytes().to_vec());
                        }
                    }
                }

                for c in &right_schema.columns {
                    display_cols.push(format!("{}.{}", right_table, c));
                }

                let mut joined_rows = Vec::new();
                for l in &left_rows {
                    for r in &right_rows {
                        let mut combined = HashMap::new();
                        for (k, v) in l {
                            combined.insert(k.clone(), v.clone());
                        }
                        for (k, v) in r {
                            combined.insert(k.clone(), v.clone());
                        }

                        let match_join = match &join.join_operator {
                            sqlparser::ast::JoinOperator::Inner(
                                sqlparser::ast::JoinConstraint::On(expr),
                            ) => Self::evaluate_where(expr, &combined),
                            _ => true,
                        };

                        if match_join {
                            joined_rows.push(combined);
                        }
                    }
                }
                left_rows = joined_rows;
            }

            if let Some(selection) = &select.selection {
                let selection_str = selection.to_string();
                if selection_str.contains("VECTOR_SEARCH") {
                    left_rows.sort_by(|_a, _b| std::cmp::Ordering::Equal);
                } else {
                    left_rows.retain(|row| Self::evaluate_where(selection, row));
                }
            }

            if let Some(limit_expr) = &query.limit {
                if let Expr::Value(Value::Number(n, _)) = limit_expr {
                    if let Ok(limit) = n.parse::<usize>() {
                        left_rows.truncate(limit);
                    }
                }
            }

            // Check for aggregates
            let mut is_aggregate = false;
            for proj in &select.projection {
                if let sqlparser::ast::SelectItem::UnnamedExpr(Expr::Function(f)) = proj {
                    let name = f.name.to_string().to_uppercase();
                    if ["COUNT", "SUM", "AVG", "MIN", "MAX"].contains(&name.as_str()) {
                        is_aggregate = true;
                        break;
                    }
                }
            }

            if is_aggregate {
                let mut results = Vec::new();
                let mut agg_cols = Vec::new();
                for proj in &select.projection {
                    match proj {
                        sqlparser::ast::SelectItem::UnnamedExpr(Expr::Function(f)) => {
                            let name = f.name.to_string().to_uppercase();
                            agg_cols.push(name.clone());
                            let col_name = if let Some(sqlparser::ast::FunctionArg::Unnamed(sqlparser::ast::FunctionArgExpr::Expr(Expr::Identifier(ident)))) = f.args.get(0) {
                                ident.to_string()
                            } else {
                                "*".to_string()
                            };

                            let vals: Vec<f64> = left_rows.iter().filter_map(|r| {
                                r.get(&col_name).or(r.keys().find(|k| k.ends_with(&format!(".{}", col_name))).and_then(|k| r.get(k))).and_then(|v| v.parse().ok())
                            }).collect();

                            let res = match name.as_str() {
                                "COUNT" => left_rows.len() as f64,
                                "SUM" => vals.iter().sum(),
                                "AVG" => if vals.is_empty() { 0.0 } else { vals.iter().sum::<f64>() / vals.len() as f64 },
                                "MIN" => vals.iter().copied().fold(f64::INFINITY, f64::min),
                                "MAX" => vals.iter().copied().fold(f64::NEG_INFINITY, f64::max),
                                _ => 0.0,
                            };
                            results.push(res.to_string());
                        }
                        _ => {
                            agg_cols.push("?".to_string());
                            results.push("NULL".to_string());
                        }
                    }
                }
                let mut output = agg_cols.join(" | ") + "\n";
                output += &"-".repeat(output.len());
                output += "\n";
                output += &(results.join(" | ") + "\n");
                return Ok(output);
            }

            let mut output = display_cols.join(" | ") + "\n";
            output += &"-".repeat(output.len());
            output += "\n";
            for row in left_rows {
                let mut vals = Vec::new();
                for c in &display_cols {
                    let mut val = row.get(c).cloned();
                    if val.is_none() {
                        let parts: Vec<&str> = c.split('.').collect();
                        if parts.len() == 2 {
                            val = row.get(parts[1]).cloned();
                        }
                    }
                    vals.push(val.unwrap_or("NULL".to_string()));
                }
                output += &(vals.join(" | ") + "\n");
            }
            Ok(output)
        } else {
            Err(anyhow!("Unsupported query type"))
        }
    }

    async fn scan_table(
        &self,
        state: &EngineState,
        table_name: &str,
        version: u64,
    ) -> Result<Vec<HashMap<String, String>>> {
        self.scan_table_with_filter(state, table_name, version, None).await
    }

    async fn scan_table_with_filter(
        &self,
        state: &EngineState,
        table_name: &str,
        version: u64,
        filter: Option<&Expr>,
    ) -> Result<Vec<HashMap<String, String>>> {
        let prefix = format!("{}:", table_name);
        let mut rows = Vec::new();
        let mut processed_keys = HashSet::new();

        // 0. Check for Index Lookup
        if let Some(Expr::BinaryOp { left, op, right }) = filter {
            if *op == sqlparser::ast::BinaryOperator::Eq {
                let get_id = |e: &Expr| match e {
                    Expr::Identifier(i) => Some(i.to_string()),
                    Expr::CompoundIdentifier(parts) => parts.last().map(|p| p.to_string()),
                    _ => None,
                };
                let get_val = |e: &Expr| match e {
                    Expr::Value(v) => Some(v.to_string().replace("'", "")),
                    _ => None,
                };

                if let (Some(col), Some(val)) = (get_id(left).or(get_id(right)), get_val(left).or(get_val(right))) {
                    let idx_key = format!("idx:{}:{}:{}", table_name, col, val);
                    let target_key_opt = state.btree.memory_tier.get(idx_key.as_bytes()).or(state.btree.get(idx_key.as_bytes()).await?);
                    if let Some(target_key) = target_key_opt {
                        let mut final_key = target_key.clone();
                        // Handle potential double serialization if key is stored as Vec<u8> in Record
                        if let Ok(Record { value, .. }) = bincode::deserialize::<Record>(&target_key) {
                             final_key = value;
                        }

                        let record_data_res = if let Some(v) = state.btree.memory_tier.get(&final_key) { Ok(Some(v)) } else { state.btree.get(&final_key).await };
                        if let Ok(Some(record_data)) = record_data_res {
                             let record: Record = bincode::deserialize(&record_data)?;
                             if record.version <= version && !record.is_deleted {
                                 let base_data: HashMap<String, String> = bincode::deserialize(&record.value)?;
                                 let mut row_data = HashMap::new();
                                 row_data.insert("__key__".to_string(), String::from_utf8_lossy(&final_key).to_string());
                                 for (k, v) in base_data {
                                     row_data.insert(k.clone(), v.clone());
                                     row_data.insert(format!("{}.{}", table_name, k), v);
                                 }
                                 return Ok(vec![row_data]);
                             }
                        }
                    }
                }
            }
        }

        // 1. Scan MemoryTier first for immediate consistency
        for r in state.btree.memory_tier.turbo_cache.iter() {
            let key = r.key();
            let key_str = String::from_utf8_lossy(key);
            if key_str.starts_with(&prefix) {
                let record: Record = bincode::deserialize(r.value())?;
                if record.version <= version && !record.is_deleted {
                    let base_data: HashMap<String, String> = bincode::deserialize(&record.value)?;
                    let mut row_data = HashMap::new();
                    row_data.insert("__key__".to_string(), key_str.to_string());
                    for (k, v) in base_data {
                        row_data.insert(k.clone(), v.clone());
                        row_data.insert(format!("{}.{}", table_name, k), v);
                    }
                    rows.push(row_data);
                    processed_keys.insert(key.clone());
                }
            }
        }

        // 2. Scan B+Tree for older data not in MemoryTier
        let mut current_page_id = state.btree.root_page_id;
        loop {
            let page = state.btree.pager.read_page(current_page_id).await?;
            let node = Node::from_bytes(&page)?;
            if node.node_type == NodeType::Leaf {
                break;
            }
            if node.children.is_empty() {
                break;
            }
            current_page_id = node.children[0];
        }

        loop {
            let page = state.btree.pager.read_page(current_page_id).await?;
            let node = Node::from_bytes(&page)?;
            for (i, key) in node.keys.iter().enumerate() {
                if processed_keys.contains(key) {
                    continue;
                }
                let key_str = String::from_utf8_lossy(key);
                if key_str.starts_with(&prefix) {
                    let record: Record = bincode::deserialize(&node.values[i])?;
                    if record.version <= version && !record.is_deleted {
                        let base_data: HashMap<String, String> =
                            bincode::deserialize(&record.value)?;
                        let mut row_data = HashMap::new();
                        row_data.insert("__key__".to_string(), key_str.to_string());
                        for (k, v) in base_data {
                            row_data.insert(k.clone(), v.clone());
                            row_data.insert(format!("{}.{}", table_name, k), v);
                        }
                        rows.push(row_data);
                        processed_keys.insert(key.clone());
                    }
                }
            }
            if let Some(next) = node.next_leaf {
                if next == current_page_id {
                    break;
                }
                current_page_id = next;
            } else {
                break;
            }
        }
        Ok(rows)
    }

    pub async fn undo(&self) -> Result<()> {
        if self
            .state
            .current_version
            .load(std::sync::atomic::Ordering::SeqCst)
            > 0
        {
            self.state
                .current_version
                .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
        }
        Ok(())
    }

    pub async fn redo(&self) -> Result<()> {
        self.state
            .current_version
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }

    pub fn call_wasm(&self, wasm_binary: &[u8]) -> Result<i32> {
        let module = Module::new(&self.wasm_engine, wasm_binary)?;
        let mut store = Store::new(&self.wasm_engine, ());
        let linker = <Linker<()>>::new(&self.wasm_engine);
        let instance = linker.instantiate(&mut store, &module)?.start(&mut store)?;
        let func = instance.get_typed_func::<(), i32>(&store, "main")?;
        Ok(func.call(&mut store, ())?)
    }

    pub fn cosine_similarity(v1: &[f32], v2: &[f32]) -> f32 {
        let dot: f32 = v1.iter().zip(v2).map(|(x, y)| x * y).sum();
        let n1: f32 = v1.iter().map(|x| x * x).sum::<f32>().sqrt();
        let n2: f32 = v2.iter().map(|x| x * x).sum::<f32>().sqrt();
        dot / (n1 * n2)
    }
}
