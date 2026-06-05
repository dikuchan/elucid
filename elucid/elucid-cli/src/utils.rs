use std::env;
use std::path::PathBuf;

use anyhow::anyhow;

pub(crate) fn get_data_dir(data_dir_path: Option<PathBuf>) -> anyhow::Result<PathBuf> {
    if let Some(data_dir_path) = data_dir_path {
        Ok(data_dir_path)
    } else {
        let home_dir_path = env::home_dir().ok_or(anyhow!("Cannot access home directory"))?;
        Ok(home_dir_path.join(".elucid").join("data"))
    }
}
