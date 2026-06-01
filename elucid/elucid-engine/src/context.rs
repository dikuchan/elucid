use std::path::{Path, PathBuf};

use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::{DataFrame, SessionConfig, SessionContext, *};

use crate::planner::QueryPlanner;

pub struct Context {
    context: SessionContext,
    data_dir_path: PathBuf,
}

impl Context {
    pub fn new<P: AsRef<Path>>(data_dir_path: P) -> Self {
        let config = SessionConfig::new().with_information_schema(true);
        let context = SessionContext::new_with_config(config);
        Self {
            context,
            data_dir_path: data_dir_path.as_ref().to_owned(),
        }
    }

    pub async fn execute(&self, source: &str) -> Result<DataFrame> {
        let pipeline = elucid_language::analyze(source)
            .map_err(|e| DataFusionError::Plan(format!("Query analysis error: {e}")))?;

        if !self.context.table_exist(pipeline.source().dataset())? {
            self.register_table(pipeline.source().dataset()).await?;
        }

        let planner = QueryPlanner::new(&self.context);
        let plan = planner.create_logical_plan(pipeline).await?;

        self.context.execute_logical_plan(plan).await
    }

    async fn register_table(&self, table_name: &str) -> Result<()> {
        let table_path = self.data_dir_path.join(table_name);
        if !table_path.exists() {
            return Err(DataFusionError::Execution(format!(
                "Table '{}' does not exist (directory not found: {:?})",
                table_name, table_path,
            )));
        }
        let table_path_str = table_path
            .to_str()
            .ok_or(DataFusionError::Execution("Invalid table path".to_owned()))?;

        let options = ParquetReadOptions::new().parquet_pruning(true);
        self.context
            .register_parquet(table_name, table_path_str, options)
            .await?;

        Ok(())
    }
}
