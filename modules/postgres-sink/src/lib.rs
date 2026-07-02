//! postgres sink — upsert/insert via the host `sql-batch` import (§7).
//! Pure SQL generation lives in [`sql`]; WASM glue is component-only.

mod sql;

#[cfg(target_arch = "wasm32")]
mod wasm_glue {
    use crate::sql::{ddl, resolve_columns, upsert_stmt, PgConfig};
    use hyperpipe_sdk::serde_json::{self, Value};
    use hyperpipe_sdk::{export_sink, Batch, InitInfo, Sink};

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

            let stmt = upsert_stmt(&self.cfg.table, &rows[0], &self.cfg.unique_key, self.cfg.upsert);

            let mut statements: Vec<(String, Vec<u8>)> = Vec::with_capacity(rows.len() + 1);

            // Auto-DDL on first write (opt-in), logged loudly.
            if self.cfg.create_table && !self.created {
                let ddl_stmt = ddl(&self.cfg.table, &rows[0], &self.cfg.unique_key);
                hp_host::log_warn(&format!("postgres auto-DDL: {ddl_stmt}"));
                statements.push((ddl_stmt, Vec::new()));
            }

            for row in &rows {
                let params: Vec<Value> = row.iter().map(|(_, v)| v.clone()).collect();
                let params_json = serde_json::to_vec(&params).map_err(|e| e.to_string())?;
                statements.push((stmt.clone(), params_json));
            }

            hp_host::sql_batch(&self.cfg.connection, &statements)?;
            hp_host::metric_add("postgres.rows", rows.len() as u64);
            self.created = true;
            Ok(())
        }
    }

    export_sink!(PgSink);
}
