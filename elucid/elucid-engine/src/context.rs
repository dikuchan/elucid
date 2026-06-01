use std::path::{Path, PathBuf};

use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::{DataFrame, SessionConfig, SessionContext, *};
use elucid_language::parser;

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
        let query = parser::parse(source).map_err(|error| {
            match error.eprint(source) {
                Ok(()) => {}
                Err(error) => return DataFusionError::IoError(error),
            };
            DataFusionError::Plan(format!("Parse error: {:?}", error))
        })?;

        if !self.context.table_exist(query.source())? {
            self.register_table(query.source()).await?;
        }

        let planner = QueryPlanner::new(&self.context);
        let plan = planner.create_logical_plan(query).await?;

        self.context
            .execute_logical_plan(plan)
            .await
            .map_err(|error| error.into())
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
