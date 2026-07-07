//! postgres sink — upsert/insert via the host `sql-batch` import (§7).
//! Pure SQL generation lives in [`sql`]; WASM glue is component-only.

mod sql;

#[cfg(target_arch = "wasm32")]
mod wasm_glue {
    use crate::sql::{
        ddl, group_by_signature, resolve_columns, rollback_delete_stmt, union_columns,
        upsert_stmt, PgConfig,
    };
    use hyperpipe_sdk::serde_json::{self, Value};
    use hyperpipe_sdk::{export_sink, Batch, ControlRecord, InitInfo, Sink};

    struct PgSink {
        cfg: PgConfig,
        created: bool,
    }

    impl Sink for PgSink {
        fn init(config: Value, _ctx: &InitInfo) -> Result<Self, String> {
            Ok(PgSink {
                cfg: PgConfig::from_json(&config)?,
                created: false,
            })
        }

        fn write(&mut self, batch: Batch) -> Result<(), String> {
            // Resolve each record into (column, value) pairs.
            let rows: Vec<Vec<(String, Value)>> = batch
                .records
                .iter()
                .map(|r| resolve_columns(r, &self.cfg.column_map))
                .filter(|c| !c.is_empty())
                .collect();
            if rows.is_empty() {
                return Ok(());
            }
            let row_count = rows.len();

            let mut statements: Vec<(String, Vec<u8>)> = Vec::with_capacity(row_count + 1);

            // Auto-DDL on first write (opt-in), logged loudly. The sample is
            // the UNION of columns across the batch, not just the first row.
            if self.cfg.create_table && !self.created {
                let sample = union_columns(&rows);
                let ddl_stmt = ddl(&self.cfg.table, &sample, &self.cfg.unique_key);
                hp_host::log_warn(&format!("postgres auto-DDL: {ddl_stmt}"));
                statements.push((ddl_stmt, Vec::new()));
            }

            // One statement per column signature: binding a row against a
            // statement from a differently-shaped row would misalign columns.
            for group in group_by_signature(rows) {
                let stmt =
                    upsert_stmt(&self.cfg.table, &group[0], &self.cfg.unique_key, self.cfg.upsert);
                for row in &group {
                    let params: Vec<Value> = row.iter().map(|(_, v)| v.clone()).collect();
                    let params_json = serde_json::to_vec(&params).map_err(|e| e.to_string())?;
                    statements.push((stmt.clone(), params_json));
                }
            }

            hp_host::sql_batch(&self.cfg.connection, &statements)?;
            hp_host::metric_add("postgres.rows", row_count as u64);
            self.created = true;
            Ok(())
        }

        /// Reorg rollback: void every row past the fork. Opt-in via
        /// `config.rollback` (needs a block-number column in the table).
        fn on_control(&mut self, ctrl: ControlRecord) -> Result<(), String> {
            let ControlRecord::Rollback {
                chain_id,
                invalidate_after_block,
            } = ctrl
            else {
                return Ok(());
            };
            let Some(block_col) = self.cfg.rollback_block_column.clone() else {
                hp_host::log_warn(&format!(
                    "postgres: rollback control (chain {chain_id}, blocks > {invalidate_after_block}) \
                     received but config.rollback is not set — stale rows are NOT deleted"
                ));
                return Ok(());
            };
            let stmt = rollback_delete_stmt(
                &self.cfg.table,
                &block_col,
                self.cfg.rollback_chain_column.as_deref(),
            );
            let params = match self.cfg.rollback_chain_column {
                Some(_) => serde_json::json!([invalidate_after_block, chain_id]),
                None => serde_json::json!([invalidate_after_block]),
            };
            let params_json = serde_json::to_vec(&params).map_err(|e| e.to_string())?;
            match hp_host::sql_batch(&self.cfg.connection, &[(stmt, params_json)]) {
                Ok(n) => {
                    hp_host::log_warn(&format!(
                        "postgres rollback: deleted {n} rows (chain {chain_id}, blocks > {invalidate_after_block})"
                    ));
                    hp_host::metric_add("postgres.rollback_deleted", n);
                    Ok(())
                }
                // Table not created yet (auto-DDL runs on first write): nothing
                // to invalidate, don't fail the control batch.
                Err(e) if self.cfg.create_table && !self.created => {
                    hp_host::log_warn(&format!(
                        "postgres rollback skipped (table not created yet): {e}"
                    ));
                    Ok(())
                }
                Err(e) => Err(e),
            }
        }
    }

    export_sink!(PgSink);
}
